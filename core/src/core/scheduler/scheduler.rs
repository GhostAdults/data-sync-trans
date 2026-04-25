use super::checkpoint::CheckpointStore;
use super::cmd::{Schedule, TaskDoneEvent, TaskDoneResult, TaskInfo};
use super::control::{
    load_job_config_from_path, SchedulerCommand, SchedulerControlHandle, SchedulerError,
    SchedulerResponse,
};
use super::cron::CronTracker;
use super::repl::ReplLoop;
use super::task_slot::TaskSlot;
use crate::core::runner::{run_sync, RunStatus};
use anyhow::Result;
use relus_common::job_config::{JobConfig, SyncMode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const MAX_CONCURRENCY: usize = 64;

pub struct TaskScheduler {
    slots: HashMap<String, TaskSlot>,
    configs: HashMap<String, (Arc<JobConfig>, bool)>,
    done_rx: mpsc::Receiver<TaskDoneEvent>,
    done_tx: mpsc::Sender<TaskDoneEvent>,
    cmd_rx: mpsc::Receiver<SchedulerCommand>,
    cmd_tx: mpsc::Sender<SchedulerCommand>,
    cron_tracker: CronTracker,
    #[allow(dead_code)]
    checkpoint: CheckpointStore,
    shutdown_token: CancellationToken,
    repl_cancel: CancellationToken,
    repl_alive: Arc<AtomicBool>,
}

impl TaskScheduler {
    pub fn new(checkpoint_path: PathBuf) -> Result<Self> {
        let (done_tx, done_rx) = mpsc::channel(256);
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let checkpoint = CheckpointStore::open(&checkpoint_path)?;
        Ok(Self {
            slots: HashMap::new(),
            configs: HashMap::new(),
            done_rx,
            done_tx,
            cmd_rx,
            cmd_tx,
            cron_tracker: CronTracker::new(),
            checkpoint,
            shutdown_token: CancellationToken::new(),
            repl_cancel: CancellationToken::new(),
            repl_alive: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn control_handle(&self) -> SchedulerControlHandle {
        SchedulerControlHandle::new(self.cmd_tx.clone())
    }

    /// 启动 REPL 交互循环（在独立线程中运行，不阻塞 tokio runtime）
    pub fn start_repl(&self) {
        let repl = Arc::new(ReplLoop::new(self.control_handle(), self.repl_cancel.clone()));
        let repl_alive = self.repl_alive.clone();
        repl_alive.store(true, Ordering::Relaxed);
        std::thread::spawn(move || {
            repl.run();
            repl_alive.store(false, Ordering::Relaxed);
        });
        info!("[TaskScheduler] REPL started");
    }

    /// 核心调度循环，阻塞直到 exit 或 ctrl_c
    pub async fn run(&mut self) {
        info!("[TaskScheduler] started");

        loop {
            let cron_deadline = self
                .cron_tracker
                .next_due()
                .unwrap_or_else(|| std::time::Instant::now() + super::cron::IDLE_SLEEP);
            let cron_sleep =
                tokio::time::sleep_until(tokio::time::Instant::from_std(cron_deadline));
            tokio::pin!(cron_sleep);

            tokio::select! {
                // Cron 定时触发
                _ = &mut cron_sleep => {
                    let now = std::time::Instant::now();
                    let due = self.cron_tracker.pop_due(now);
                    for job_id in due {
                        self.fire_cron_job(&job_id).await;
                    }
                }

                // 任务完成回调
                done = self.done_rx.recv() => {
                    if let Some(done) = done {
                        self.handle_task_done(done);
                    }
                }

                // Scheduler 控制命令
                cmd = self.cmd_rx.recv() => {
                    if let Some(cmd) = cmd {
                        if self.handle_scheduler_command(cmd).await {
                            break;
                        }
                    }
                }

                // 优雅停机
                _ = tokio::signal::ctrl_c() => {
                    info!("[TaskScheduler] ctrl_c received, shutting down");
                    self.repl_cancel.cancel();
                    self.graceful_shutdown().await;
                    break;
                }
            }
        }
        info!("[TaskScheduler] stopped");
    }

    async fn handle_scheduler_command(&mut self, cmd: SchedulerCommand) -> bool {
        match cmd {
            SchedulerCommand::QueryTasks { job_id, reply } => {
                let result = Ok(SchedulerResponse::Tasks {
                    tasks: self.filter_tasks(job_id),
                    repl_alive: self.repl_alive.load(Ordering::Relaxed),
                });
                let _ = reply.send(result);
                false
            }
            SchedulerCommand::SubmitTask {
                job_id: _,
                path,
                reply,
            } => {
                let result = match load_job_config_from_path(&path) {
                    Ok((job_id, config, schedule)) => self
                        .submit_task(job_id.clone(), config, schedule)
                        .map(|id| SchedulerResponse::TaskSubmitted { job_id: id })
                        .map_err(map_submit_error),
                    Err(err) => Err(err),
                };
                let _ = reply.send(result);
                false
            }
            SchedulerCommand::CancelTask { job_id, reply } => {
                let result = self
                    .cancel_task(&job_id)
                    .map(|()| SchedulerResponse::TaskCancelled { job_id })
                    .map_err(map_submit_error);
                let _ = reply.send(result);
                false
            }
            SchedulerCommand::Shutdown { reply } => {
                self.repl_cancel.cancel();
                info!("[TaskScheduler] exit command received, shutting down");
                self.graceful_shutdown().await;
                let _ = reply.send(Ok(SchedulerResponse::ShutdownRequested));
                true
            }
        }
    }

    fn filter_tasks(&self, job_id: Option<String>) -> Vec<TaskInfo> {
        let tasks = self.list_tasks();
        match job_id {
            Some(id) => tasks.into_iter().filter(|task| task.job_id == id).collect(),
            None => tasks,
        }
    }

    fn cancel_task(&mut self, job_id: &str) -> Result<()> {
        match self.slots.get(job_id) {
            Some(slot) => {
                slot.cancel_token.cancel();
                Ok(())
            }
            None => Err(anyhow::Error::new(SchedulerError::JobNotFound {
                job_id: job_id.to_string(),
            })),
        }
    }

    /// 提交任务（程序启动时调用）
    pub fn submit_task(
        &mut self,
        job_id: String,
        config: Arc<JobConfig>,
        schedule: Schedule,
    ) -> Result<String> {
        if self.slots.contains_key(&job_id) || self.cron_tracker.get_schedule(&job_id).is_some() {
            return Err(anyhow::Error::new(SchedulerError::JobAlreadyExists {
                job_id: job_id.clone(),
            }));
        }

        let running = self
            .slots
            .values()
            .filter(|s| matches!(s.phase, super::task_slot::TaskPhase::Running))
            .count();
        if running >= MAX_CONCURRENCY {
            return Err(anyhow::Error::new(SchedulerError::MaxConcurrencyReached {
                running,
                limit: MAX_CONCURRENCY,
            }));
        }

        let is_cdc = config.sync_mode == Some(SyncMode::Incremental);
        self.configs
            .insert(job_id.clone(), (config.clone(), is_cdc));

        match &schedule {
            Schedule::Immediate => {
                self.spawn_task(job_id.clone(), config, is_cdc);
            }
            Schedule::Once(_) | Schedule::Cron(_) => {
                if is_cdc {
                    self.spawn_task(job_id.clone(), config, true);
                } else {
                    self.cron_tracker.register(job_id.clone(), schedule);
                    info!("[TaskScheduler] job '{}' scheduled", job_id);
                }
            }
        }
        Ok(job_id)
    }

    async fn fire_cron_job(&mut self, job_id: &str) {
        if let Some(slot) = self.slots.get(job_id) {
            if matches!(slot.phase, super::task_slot::TaskPhase::Running) {
                warn!("[TaskScheduler] cron job '{}' still running, skip", job_id);
                if let Some(schedule) = self.cron_tracker.get_schedule(job_id).cloned() {
                    self.cron_tracker.register(job_id.to_string(), schedule);
                }
                return;
            }
        }

        let (config, is_cdc) = match self.configs.get(job_id) {
            Some((c, cdc)) => (c.clone(), *cdc),
            None => {
                warn!("[TaskScheduler] cron job '{}' has no saved config", job_id);
                return;
            }
        };

        info!("[TaskScheduler] cron job '{}' fired", job_id);
        self.spawn_task(job_id.to_string(), config, is_cdc);
    }

    fn spawn_task(&mut self, job_id: String, config: Arc<JobConfig>, is_cdc: bool) {
        let cancel_token = CancellationToken::new();
        let done_tx = self.done_tx.clone();
        let id = job_id.clone();
        let token_clone = cancel_token.clone();

        tokio::spawn(async move {
            let result = run_sync(config, token_clone).await;
            let done_result = match result {
                Ok(r) => match r.status {
                    RunStatus::Success | RunStatus::Partial => TaskDoneResult::Success {
                        records_read: r.stats.records_read,
                        records_written: r.stats.records_written,
                        elapsed_secs: r.stats.elapsed_secs,
                    },
                    RunStatus::Shutdown => TaskDoneResult::Cancelled,
                    RunStatus::Failed => {
                        TaskDoneResult::Failed(r.error.unwrap_or_else(|| "unknown".to_string()))
                    }
                },
                Err(e) => TaskDoneResult::Failed(e.to_string()),
            };
            let _ = done_tx
                .send(TaskDoneEvent {
                    job_id: id,
                    is_cdc,
                    result: done_result,
                })
                .await;
        });

        let slot = TaskSlot::new(job_id.clone(), is_cdc, None, cancel_token);
        self.slots.insert(job_id.clone(), slot);
        info!("[TaskScheduler] job '{}' spawned (cdc={})", job_id, is_cdc);
    }

    fn handle_task_done(&mut self, done: TaskDoneEvent) {
        let job_id = &done.job_id;
        let is_cdc = done.is_cdc;
        let result_summary = match &done.result {
            TaskDoneResult::Success {
                records_read,
                records_written,
                elapsed_secs,
            } => {
                format!(
                    "success (read={}, written={}, {:.2}s)",
                    records_read, records_written, elapsed_secs
                )
            }
            TaskDoneResult::Failed(msg) => format!("failed: {}", msg),
            TaskDoneResult::Cancelled => "cancelled".to_string(),
        };

        if let Some(slot) = self.slots.get_mut(job_id) {
            let was_running = matches!(slot.phase, super::task_slot::TaskPhase::Running);
            slot.update_from_done(done.result);

            info!(
                "[TaskScheduler] job '{}' done: {} ({})",
                job_id,
                slot.phase.as_str(),
                result_summary
            );

            if was_running {
                print_task_done(job_id, is_cdc, &slot.phase);
            }
        }
    }

    async fn graceful_shutdown(&mut self) {
        self.shutdown_token.cancel();

        info!(
            "[TaskScheduler] graceful shutdown, cancelling {} tasks",
            self.slots.len()
        );
        for slot in self.slots.values() {
            slot.cancel_token.cancel();
        }

        let running_count = self
            .slots
            .values()
            .filter(|s| matches!(s.phase, super::task_slot::TaskPhase::Running))
            .count();

        if running_count == 0 {
            info!("[TaskScheduler] no running tasks, exit immediately");
            return;
        }

        let deadline = tokio::time::sleep(tokio::time::Duration::from_secs(30));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                done = self.done_rx.recv() => {
                    if let Some(done) = done {
                        if let Some(slot) = self.slots.get_mut(&done.job_id) {
                            slot.update_from_done(done.result);
                        }
                    }
                    let still_running = self.slots.values()
                        .any(|s| matches!(s.phase, super::task_slot::TaskPhase::Running));
                    if !still_running {
                        break;
                    }
                }
                _ = &mut deadline => {
                    warn!("[TaskScheduler] shutdown timeout, forcing exit");
                    break;
                }
            }
        }
    }

    pub fn list_tasks(&self) -> Vec<TaskInfo> {
        let mut tasks: Vec<TaskInfo> = self.slots.values().map(|s| s.to_info()).collect();
        for (job_id, schedule) in self.cron_tracker.iter_entries() {
            if !self.slots.contains_key(job_id) {
                tasks.push(TaskInfo {
                    job_id: job_id.clone(),
                    phase: "scheduled".to_string(),
                    is_cdc: false,
                    started_at: None,
                    schedule: Some(match schedule {
                        Schedule::Immediate => "immediate".to_string(),
                        Schedule::Once(dt) => dt.to_rfc3339(),
                        Schedule::Cron(expr) => format!("cron({})", expr),
                    }),
                    stats: None,
                    error: None,
                });
            }
        }
        tasks
    }
}

fn map_submit_error(error: anyhow::Error) -> SchedulerError {
    if let Some(error) = error.downcast_ref::<SchedulerError>() {
        return error.clone();
    }

    let message = error.to_string();
    if let Some(job_id) = extract_job_id(&message, "already exists") {
        return SchedulerError::JobAlreadyExists { job_id };
    }
    if let Some(job_id) = extract_job_id(&message, "not found") {
        return SchedulerError::JobNotFound { job_id };
    }
    SchedulerError::Internal { message }
}

fn extract_job_id(message: &str, suffix: &str) -> Option<String> {
    let prefix = "job '";
    let rest = message.strip_prefix(prefix)?;
    let (job_id, tail) = rest.split_once('"')?;
    if tail.contains(suffix) {
        Some(job_id.to_string())
    } else {
        None
    }
}

/// 终端打印任务完成信息
fn print_task_done(job_id: &str, is_cdc: bool, phase: &super::task_slot::TaskPhase) {
    match phase {
        super::task_slot::TaskPhase::Completed(stats) => {
            let tp = if stats.elapsed_secs > 0.0 {
                stats.records_written as f64 / stats.elapsed_secs
            } else {
                0.0
            };
            if is_cdc {
                println!(
                    "[{}] CDC 任务已停止\n 已读出 {} records\n 已写入 {} records\n 耗时 {:.2}s",
                    job_id, stats.records_read, stats.records_written, stats.elapsed_secs
                );
            } else {
                println!(
                    "[{}] complete:\n 任务读出 {} records\n 任务写入 {} records\n 耗时 {:.2}s\n TP {:.0} rec/s",
                    job_id, stats.records_read, stats.records_written, stats.elapsed_secs, tp
                );
            }
        }
        super::task_slot::TaskPhase::Failed(msg) => {
            if is_cdc {
                println!("[{}] CDC 任务异常退出: {}", job_id, msg);
            } else {
                println!("[{}] failed: {}", job_id, msg);
            }
        }
        super::task_slot::TaskPhase::Cancelled => {
            if is_cdc {
                println!("[{}] CDC 任务已停止", job_id);
            } else {
                println!("[{}] cancelled", job_id);
            }
        }
        _ => {}
    }
}
