use chrono::{DateTime, Utc};
use relus_common::job_config::ScheduleConfig;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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

    pub fn from_config(config: &ScheduleConfig) -> Result<Self, String> {
        match config {
            ScheduleConfig::Cron(value) => Self::parse_schedule_value(value),
            ScheduleConfig::Typed { r#type, value } => {
                let schedule_type = r#type.to_ascii_lowercase();
                match schedule_type.as_str() {
                    "immediate" => Ok(Schedule::Immediate),
                    "once" => {
                        let value = value
                            .as_deref()
                            .ok_or_else(|| "schedule once requires a value".to_string())?;
                        value
                            .parse::<DateTime<Utc>>()
                            .map(Schedule::Once)
                            .map_err(|e| format!("invalid once schedule '{}': {}", value, e))
                    }
                    "cron" => {
                        let value = value
                            .as_deref()
                            .ok_or_else(|| "schedule cron requires a value".to_string())?;
                        Self::parse_cron(value)
                    }
                    other => Err(format!("unsupported schedule type '{}'", other)),
                }
            }
        }
    }

    fn parse_schedule_value(value: &str) -> Result<Self, String> {
        if let Ok(dt) = value.parse::<DateTime<Utc>>() {
            return Ok(Schedule::Once(dt));
        }
        Self::parse_cron(value)
    }

    fn parse_cron(value: &str) -> Result<Self, String> {
        let expr = value.trim();
        if expr.is_empty() {
            return Err("cron schedule cannot be empty".to_string());
        }

        let full_expr = if expr.split_whitespace().count() == 5 {
            format!("0 {}", expr)
        } else {
            expr.to_string()
        };
        cron::Schedule::from_str(&full_expr)
            .map(|_| Schedule::Cron(expr.to_string()))
            .map_err(|e| format!("invalid cron schedule '{}': {}", expr, e))
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
    Success {
        records_read: usize,
        records_written: usize,
        elapsed_secs: f64,
    },
    Failed(String),
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_config_cron_string_maps_to_cron() {
        let schedule = Schedule::from_config(&ScheduleConfig::Cron("*/5 * * * *".to_string()))
            .expect("cron schedule");

        assert!(matches!(schedule, Schedule::Cron(expr) if expr == "*/5 * * * *"));
    }

    #[test]
    fn schedule_config_typed_once_maps_to_once() {
        let schedule = Schedule::from_config(&ScheduleConfig::Typed {
            r#type: "once".to_string(),
            value: Some("2026-04-30T12:00:00Z".to_string()),
        })
        .expect("once schedule");

        assert!(matches!(schedule, Schedule::Once(_)));
    }

    #[test]
    fn schedule_config_rejects_invalid_cron() {
        let err = Schedule::from_config(&ScheduleConfig::Cron("not a cron".to_string()))
            .expect_err("invalid cron should fail");

        assert!(err.contains("invalid cron schedule"));
    }
}
