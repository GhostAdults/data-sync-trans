use crate::core::scheduler::cmd::{Schedule, TaskDoneResult, TaskInfo, TaskStats};
use std::time::{Instant, SystemTime};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 任务运行阶段
#[derive(Debug)]
pub enum TaskPhase {
    Running,
    Completed(TaskStats),
    Failed(String),
    Cancelled,
}

impl TaskPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskPhase::Running => "running",
            TaskPhase::Completed(_) => "completed",
            TaskPhase::Failed(_) => "failed",
            TaskPhase::Cancelled => "cancelled",
        }
    }
}

/// 任务运行时槽位
pub struct TaskSlot {
    pub job_id: String,
    pub phase: TaskPhase,
    pub cancel_token: CancellationToken,
    pub handle: JoinHandle<()>,
    pub started_at: Instant,
    pub is_cdc: bool,
    pub schedule: Option<Schedule>,
}

impl TaskSlot {
    pub fn new(
        job_id: String,
        is_cdc: bool,
        schedule: Option<Schedule>,
        cancel_token: CancellationToken,
        handle: JoinHandle<()>,
    ) -> Self {
        Self {
            job_id,
            phase: TaskPhase::Running,
            cancel_token,
            handle,
            started_at: Instant::now(),
            is_cdc,
            schedule,
        }
    }

    pub fn to_info(&self) -> TaskInfo {
        TaskInfo {
            job_id: self.job_id.clone(),
            phase: self.phase.as_str().to_string(),
            is_cdc: self.is_cdc,
            started_at: Some({
                let duration = self.started_at.elapsed();
                let now = SystemTime::now();
                let start_time = now.checked_sub(duration).unwrap_or(now);
                chrono::DateTime::<chrono::Utc>::from(start_time).to_rfc3339()
            }),
            schedule: self.schedule.as_ref().map(|s| match s {
                Schedule::Immediate => "immediate".to_string(),
                Schedule::Once(dt) => dt.to_rfc3339(),
                Schedule::Cron(expr) => format!("cron({})", expr),
            }),
            stats: match &self.phase {
                TaskPhase::Completed(stats) => Some(stats.clone()),
                _ => None,
            },
            error: match &self.phase {
                TaskPhase::Failed(msg) => Some(msg.clone()),
                _ => None,
            },
        }
    }

    pub fn update_from_done(&mut self, done: TaskDoneResult) {
        self.phase = match done {
            TaskDoneResult::Success { records_read, records_written, elapsed_secs } => {
                TaskPhase::Completed(TaskStats { records_read, records_written, records_failed: 0, elapsed_secs })
            }
            TaskDoneResult::Failed(_) if self.cancel_token.is_cancelled() => TaskPhase::Cancelled,
            TaskDoneResult::Failed(msg) => TaskPhase::Failed(msg),
            TaskDoneResult::Cancelled => TaskPhase::Cancelled,
        };
    }
}
