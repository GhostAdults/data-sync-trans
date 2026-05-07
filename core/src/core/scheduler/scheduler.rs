use super::checkpoint::CheckpointStore;
use super::cmd::{Schedule, TaskDoneEvent, TaskDoneResult, TaskInfo};
use super::control::{
    load_job_config_from_path, SchedulerCommand, SchedulerControlHandle, SchedulerError,
    SchedulerResponse,
};
use super::cron::CronTracker;
use super::repl::ReplLoop;
use super::task_slot::{TaskPhase, TaskSlot};
use crate::core::runner::{start_task, RunStatus};
use anyhow::Result;
use relus_common::job_config::{JobConfig, SyncMode};
use std::any::Any;
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
    configs: HashMap<String, (Arc<JobConfig>, bool, String)>,
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
        let repl = Arc::new(ReplLoop::new(
            self.control_handle(),
            self.repl_cancel.clone(),
        ));
        let repl_alive = self.repl_alive.clone();
        repl_alive.store(true, Ordering::Relaxed);
        std::thread::spawn(move || {
            repl.run();
            repl_alive.store(false, Ordering::Relaxed);
        });
        info!("[TaskScheduler] REPL started");
    }

    pub async fn run(&mut self) {
        self.run_once().await;
        info!("[TaskScheduler] stopped");
    }

    async fn run_once(&mut self) {
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
                    Ok((job_name, config, schedule)) => self
                        .submit_task(job_name.clone(), config, schedule)
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
        job_name: String,
        config: Arc<JobConfig>,
        schedule: Schedule,
    ) -> Result<String> {
        let running = self
            .slots
            .values()
            .filter(|s| matches!(s.phase, TaskPhase::Running))
            .count();
        if running >= MAX_CONCURRENCY {
            return Err(anyhow::Error::new(SchedulerError::MaxConcurrencyReached {
                running,
                limit: MAX_CONCURRENCY,
            }));
        }

        if let Some(existing) = self.active_job_for_name(&job_name) {
            return Err(anyhow::Error::new(SchedulerError::JobAlreadyExists {
                job_id: existing,
            }));
        }

        let is_cdc = config.sync_mode == Some(SyncMode::Incremental);
        let job_id = self.next_run_job_id(&job_name);
        self.configs.insert(
            job_id.clone(),
            (Arc::clone(&config), is_cdc, job_name.clone()),
        );

        match &schedule {
            Schedule::Immediate => {
                self.spawn_task(job_id.clone(), job_name, config, is_cdc);
            }
            Schedule::Once(_) | Schedule::Cron(_) => {
                if is_cdc {
                    self.spawn_task(job_id.clone(), job_name, config, true);
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
            if matches!(slot.phase, TaskPhase::Running) {
                warn!("[TaskScheduler] cron job '{}' still running, skip", job_id);
                if let Some(schedule) = self.cron_tracker.get_schedule(job_id).cloned() {
                    self.cron_tracker.register(job_id.to_string(), schedule);
                }
                return;
            }
        }

        let (config, is_cdc, job_name) = match self.configs.get(job_id) {
            Some((c, cdc, name)) => (Arc::clone(c), *cdc, name.clone()),
            None => {
                warn!("[TaskScheduler] cron job '{}' has no saved config", job_id);
                return;
            }
        };

        info!("[TaskScheduler] cron job '{}' fired", job_id);
        self.spawn_task(job_id.to_string(), job_name, config, is_cdc);
    }

    fn spawn_task(
        &mut self,
        job_id: String,
        job_name: String,
        config: Arc<JobConfig>,
        is_cdc: bool,
    ) {
        let cancel_token = CancellationToken::new();
        let done_tx = self.done_tx.clone();
        let id = job_id.clone();
        let token_clone = cancel_token.clone();

        tokio::spawn(async move {
            let run_handle = tokio::spawn(async move { start_task(config, token_clone).await });
            let done_result = match run_handle.await {
                Ok(result) => task_result_to_done(result),
                Err(error) => TaskDoneResult::Failed(join_error_message(error)),
            };
            let _ = done_tx
                .send(TaskDoneEvent {
                    job_id: id,
                    is_cdc,
                    result: done_result,
                })
                .await;
        });

        let slot = TaskSlot::new(job_id.clone(), job_name, is_cdc, None, cancel_token);
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
            let was_running = matches!(slot.phase, TaskPhase::Running);
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
            .filter(|s| matches!(s.phase, TaskPhase::Running))
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
                        .any(|s| matches!(s.phase, TaskPhase::Running));
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

    fn active_job_for_name(&self, job_name: &str) -> Option<String> {
        self.slots
            .values()
            .find(|slot| slot.job_name == job_name && matches!(slot.phase, TaskPhase::Running))
            .map(|slot| slot.job_id.clone())
    }

    fn next_run_job_id(&self, job_name: &str) -> String {
        let base = sanitize_job_id_base(job_name);
        let now = chrono::Utc::now().format("%Y%m%d%H%M%S%3f");
        let candidate = format!("{}-{}", base, now);
        if !self.slots.contains_key(&candidate)
            && self.cron_tracker.get_schedule(&candidate).is_none()
        {
            return candidate;
        }

        let mut seq = 1usize;
        loop {
            let candidate = format!("{}-{}-{}", base, now, seq);
            if !self.slots.contains_key(&candidate)
                && self.cron_tracker.get_schedule(&candidate).is_none()
            {
                return candidate;
            }
            seq += 1;
        }
    }
}

fn task_result_to_done(result: Result<crate::core::runner::RunResult>) -> TaskDoneResult {
    match result {
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
    }
}

fn join_error_message(error: tokio::task::JoinError) -> String {
    if error.is_panic() {
        return format!(
            "task panicked: {}",
            panic_payload_message(error.into_panic())
        );
    }

    if error.is_cancelled() {
        return "task was cancelled".to_string();
    }

    error.to_string()
}

fn panic_payload_message(payload: Box<dyn Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        message.to_string()
    } else {
        "unknown panic payload".to_string()
    }
}

fn sanitize_job_id_base(job_name: &str) -> String {
    let base = job_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    if base.is_empty() {
        "job".to_string()
    } else {
        base
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

#[cfg(test)]
mod tests {
    use super::*;
    use relus_common::data_source_config::DataSourceConfig;
    use std::collections::BTreeMap;

    fn scheduler() -> TaskScheduler {
        let dir = tempfile::tempdir().expect("temp dir");
        TaskScheduler::new(dir.path().join("checkpoints.redb")).expect("scheduler")
    }

    fn job_config(sync_mode: Option<SyncMode>) -> Arc<JobConfig> {
        let source = DataSourceConfig {
            name: "source".to_string(),
            source_type: "api".to_string(),
            is_table_mode: true,
            query_sql: None,
            writer_mode: None,
            config: serde_json::json!({}),
        };
        let target = DataSourceConfig {
            name: "target".to_string(),
            source_type: "api".to_string(),
            is_table_mode: true,
            query_sql: None,
            writer_mode: None,
            config: serde_json::json!({}),
        };

        Arc::new(JobConfig {
            source,
            target,
            column_mapping: BTreeMap::new(),
            column_types: None,
            sync_mode,
            batch_size: None,
            channel_buffer_size: None,
            job_id: None,
            schedule: None,
        })
    }

    #[test]
    fn submitted_job_id_is_runtime_instance_not_raw_job_name() {
        let mut scheduler = scheduler();

        let job_id = scheduler
            .submit_task(
                "daily-job".to_string(),
                job_config(Some(SyncMode::Fullsnapshot)),
                Schedule::Cron("*/5 * * * *".to_string()),
            )
            .expect("submit");

        assert_ne!(job_id, "daily-job");
        assert!(job_id.starts_with("daily-job-"));
    }

    #[test]
    fn completed_job_name_can_be_submitted_again() {
        let mut scheduler = scheduler();
        let completed_id = "daily-job-previous".to_string();
        let cancel_token = CancellationToken::new();
        let mut slot = TaskSlot::new(
            completed_id.clone(),
            "daily-job".to_string(),
            false,
            None,
            cancel_token,
        );
        slot.phase = TaskPhase::Completed(super::super::cmd::TaskStats {
            records_read: 1,
            records_written: 1,
            records_failed: 0,
            elapsed_secs: 1.0,
        });
        scheduler.slots.insert(completed_id.clone(), slot);

        let new_id = scheduler
            .submit_task(
                "daily-job".to_string(),
                job_config(Some(SyncMode::Fullsnapshot)),
                Schedule::Cron("*/5 * * * *".to_string()),
            )
            .expect("resubmit completed job name");

        assert_ne!(new_id, completed_id);
        assert!(new_id.starts_with("daily-job-"));
    }

    #[test]
    fn active_cdc_with_same_job_name_must_be_cancelled_first() {
        let mut scheduler = scheduler();
        let active_id = "cdc-job-active".to_string();
        let slot = TaskSlot::new(
            active_id.clone(),
            "cdc-job".to_string(),
            true,
            None,
            CancellationToken::new(),
        );
        scheduler.slots.insert(active_id.clone(), slot);

        let err = scheduler
            .submit_task(
                "cdc-job".to_string(),
                job_config(Some(SyncMode::Incremental)),
                Schedule::Cron("*/5 * * * *".to_string()),
            )
            .expect_err("active CDC should conflict");

        let scheduler_error = err
            .downcast_ref::<SchedulerError>()
            .expect("scheduler error");
        assert_eq!(
            scheduler_error,
            &SchedulerError::JobAlreadyExists { job_id: active_id }
        );
    }

    #[test]
    fn running_job_name_cannot_be_submitted_again() {
        let mut scheduler = scheduler();
        let active_id = "daily-job-active".to_string();
        let slot = TaskSlot::new(
            active_id.clone(),
            "daily-job".to_string(),
            false,
            None,
            CancellationToken::new(),
        );
        scheduler.slots.insert(active_id.clone(), slot);

        let err = scheduler
            .submit_task(
                "daily-job".to_string(),
                job_config(Some(SyncMode::Fullsnapshot)),
                Schedule::Cron("*/5 * * * *".to_string()),
            )
            .expect_err("running job should conflict");

        let scheduler_error = err
            .downcast_ref::<SchedulerError>()
            .expect("scheduler error");
        assert_eq!(
            scheduler_error,
            &SchedulerError::JobAlreadyExists { job_id: active_id }
        );
    }
}
