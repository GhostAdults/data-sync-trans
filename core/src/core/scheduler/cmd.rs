use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 调度策略
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Schedule {
    Immediate,
    Once(DateTime<Utc>),
    Cron(String),
}

impl Default for Schedule {
    fn default() -> Self {
        Schedule::Immediate
    }
}

impl Schedule {
    pub fn from_cli_str(s: &str) -> Self {
        if let Ok(dt) = s.parse::<DateTime<Utc>>() {
            return Schedule::Once(dt);
        }
        if s.split_whitespace().count() == 5 {
            return Schedule::Cron(s.to_string());
        }
        Schedule::Immediate
    }
}

/// 任务状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub job_id: String,
    pub phase: String,
    pub is_cdc: bool,
    pub started_at: Option<String>,
    pub schedule: Option<String>,
    pub stats: Option<TaskStats>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStats {
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub elapsed_secs: f64,
}

/// 任务完成事件（spawned task → scheduler）
#[derive(Debug)]
pub struct TaskDoneEvent {
    pub job_id: String,
    pub is_cdc: bool,
    pub result: TaskDoneResult,
}

#[derive(Debug)]
pub enum TaskDoneResult {
    Success { records_read: usize, records_written: usize, elapsed_secs: f64 },
    Failed(String),
    Cancelled,
}
