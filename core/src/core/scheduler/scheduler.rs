use super::checkpoint::CheckpointStore;
use super::cmd::{Schedule, TaskDoneEvent, TaskDoneResult, TaskInfo};
use super::cron::CronTracker;
use super::repl::{ReplCommand, ReplLoop};
use super::task_slot::TaskSlot;
use crate::core::runner::{run_sync, RunStatus};
use anyhow::Result;
use relus_common::job_config::{JobConfig, SyncMode};
use std::collections::HashMap;
use std::path::PathBuf;
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
    cmd_rx: mpsc::Receiver<ReplCommand>,
    cmd_tx: mpsc::Sender<ReplCommand>,
    cron_tracker: CronTracker,
    #[allow(dead_code)]
    checkpoint: CheckpointStore,
    shutdown_token: CancellationToken,
    repl_cancel: CancellationToken,
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
        })
    }

    /// 启动 REPL 交互循环（在独立线程中运行，不阻塞 tokio runtime）
    pub fn start_repl(&self) {
        let repl = Arc::new(ReplLoop::new(self.cmd_tx.clone(), self.repl_cancel.clone()));
        std::thread::spawn(move || {
            repl.run();
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

                // REPL 命令
                cmd = self.cmd_rx.recv() => {
                    if let Some(cmd) = cmd {
                        if self.handle_repl_cmd(cmd).await {
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

    /// 处理 REPL 命令，返回 true 表示应该退出循环
    async fn handle_repl_cmd(&mut self, cmd: ReplCommand) -> bool {
        match cmd {
            ReplCommand::Status { job_id } => {
                self.cmd_status(job_id);
            }
            ReplCommand::Submit { path } => {
                self.cmd_submit(&path);
            }
            ReplCommand::Cancel { job_id } => {
                self.cmd_cancel(&job_id);
            }
            ReplCommand::Exit => {
                self.repl_cancel.cancel();
                info!("[TaskScheduler] exit command received, shutting down");
                self.graceful_shutdown().await;
                return true;
            }
            ReplCommand::Unknown(input) => {
                if input.starts_with("Usage:") {
                    println!("{}", input);
                } else {
                    println!("Unknown command: {}. Type 'exit' to quit.", input);
                }
            }
        }
        false
    }

    /// status 命令：格式化表格输出任务状态
    fn cmd_status(&self, job_id: Option<String>) {
        let tasks = self.list_tasks();
        let filtered: Vec<&TaskInfo> = match &job_id {
            Some(id) => tasks.iter().filter(|t| &t.job_id == id).collect(),
            None => tasks.iter().collect(),
        };

        if filtered.is_empty() {
            match job_id {
                Some(id) => println!("Job '{}' not found.", id),
                None => println!("No tasks."),
            }
            return;
        }

        println!(
            "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
            "job_id", "phase", "cdc", "read", "written", "elapsed"
        );
        println!("{}", "-".repeat(74));
        for t in &filtered {
            let (read, written, elapsed) = match &t.stats {
                Some(s) => (
                    s.records_read.to_string(),
                    s.records_written.to_string(),
                    format!("{:.1}s", s.elapsed_secs),
                ),
                None => ("-".to_string(), "-".to_string(), "-".to_string()),
            };
            println!(
                "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
                t.job_id, t.phase, t.is_cdc, read, written, elapsed
            );
        }
    }

    /// submit 命令：加载配置并提交任务
    fn cmd_submit(&mut self, path: &str) {
        let path = PathBuf::from(path);
        let data = match std::fs::read_to_string(&path) {
            Ok(d) => d,
            Err(e) => {
                println!("File not found: {} ({})", path.display(), e);
                return;
            }
        };

        let config: JobConfig = match serde_json::from_str(&data) {
            Ok(c) => c,
            Err(e) => {
                println!("Parse failed: {}", e);
                return;
            }
        };

        let job_id = config
            .job_id
            .clone()
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "unknown".to_string());

        let schedule = config
            .schedule
            .as_ref()
            .map(|s| Schedule::from_cli_str(&format!("{:?}", s)))
            .unwrap_or_default();

        match self.submit_task(job_id.clone(), Arc::new(config), schedule) {
            Ok(id) => println!("Job '{}' submitted.", id),
            Err(e) => println!("Submit failed: {}", e),
        }
    }

    /// cancel 命令：取消运行中的任务
    fn cmd_cancel(&mut self, job_id: &str) {
        match self.slots.get(job_id) {
            Some(slot) => {
                slot.cancel_token.cancel();
                println!("Job '{}' cancel signal sent.", job_id);
            }
            None => println!("Job '{}' not found.", job_id),
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
            anyhow::bail!("job '{}' already exists", job_id);
        }

        let running = self
            .slots
            .values()
            .filter(|s| matches!(s.phase, super::task_slot::TaskPhase::Running))
            .count();
        if running >= MAX_CONCURRENCY {
            anyhow::bail!("max concurrency reached ({}/{})", running, MAX_CONCURRENCY);
        }

        let is_cdc = config.sync_mode == Some(SyncMode::Cdc);
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

        let handle = tokio::spawn(async move {
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

        let slot = TaskSlot::new(job_id.clone(), is_cdc, None, cancel_token, handle);
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

            // Batch 任务完成 → 终端打印结果摘要（格式与 sync 命令一致）
            if was_running && !is_cdc {
                match &slot.phase {
                    super::task_slot::TaskPhase::Completed(stats) => {
                        let tp = if stats.elapsed_secs > 0.0 {
                            stats.records_written as f64 / stats.elapsed_secs
                        } else {
                            0.0
                        };
                        println!(
                            "[{}] complete:\n 任务读出 {} records\n 任务写入 {} records\n 耗时 {:.2}s\n TP {:.0} rec/s",
                            job_id, stats.records_read, stats.records_written, stats.elapsed_secs, tp
                        );
                    }
                    super::task_slot::TaskPhase::Failed(msg) => {
                        println!("[{}] failed: {}", job_id, msg);
                    }
                    super::task_slot::TaskPhase::Cancelled => {
                        println!("[{}] cancelled", job_id);
                    }
                    _ => {}
                }

                // Cron Batch 完成后重新注册
                if let Some(schedule) = self.cron_tracker.get_schedule(job_id).cloned() {
                    if matches!(schedule, Schedule::Cron(_)) {
                        self.cron_tracker.register(job_id.clone(), schedule);
                    }
                }
            }

            // CDC 任务退出 → 终端打印
            if was_running && is_cdc {
                match &slot.phase {
                    super::task_slot::TaskPhase::Completed(stats) => {
                        println!(
                            "[{}] CDC 任务已停止\n 已读出 {} records\n 已写入 {} records\n 耗时 {:.2}s",
                            job_id, stats.records_read, stats.records_written, stats.elapsed_secs
                        );
                    }
                    super::task_slot::TaskPhase::Failed(msg) => {
                        println!("[{}] CDC 任务异常退出: {}", job_id, msg);
                    }
                    super::task_slot::TaskPhase::Cancelled => {
                        println!("[{}] CDC 任务已停止", job_id);
                    }
                    _ => {}
                }
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
