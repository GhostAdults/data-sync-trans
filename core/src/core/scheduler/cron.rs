use crate::core::scheduler::cmd::Schedule;
use chrono::Utc;
use cron::Schedule as CronSchedule;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::warn;

/// 定时任务跟踪器
pub struct CronTracker {
    entries: HashMap<String, ScheduleEntry>,
}

struct ScheduleEntry {
    schedule: Schedule,
    next_fire: Option<Instant>,
}

impl CronTracker {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// 注册定时任务
    pub fn register(&mut self, job_id: String, schedule: Schedule) {
        let next_fire = Self::calc_next_fire(&schedule);
        self.entries.insert(job_id, ScheduleEntry { schedule, next_fire });
    }

    /// 取消注册
    pub fn unregister(&mut self, job_id: &str) {
        self.entries.remove(job_id);
    }

    /// 计算最近的下一个到期时间
    pub fn next_due(&self) -> Option<Instant> {
        self.entries.values().filter_map(|e| e.next_fire).min()
    }

    /// 取出到期任务 ID 列表，并重新计算下次触发时��
    pub fn pop_due(&mut self, now: Instant) -> Vec<String> {
        let due: Vec<String> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                entry.next_fire.map_or(false, |f| f <= now).then(|| id.clone())
            })
            .collect();

        for job_id in &due {
            // Immediate / Once: 触发后移除
            if let Some(entry) = self.entries.get(job_id) {
                if matches!(entry.schedule, Schedule::Immediate | Schedule::Once(_)) {
                    self.entries.remove(job_id);
                    continue;
                }
            }
            // Cron: 计算下次触发时间
            if let Some(entry) = self.entries.get_mut(job_id) {
                entry.next_fire = Self::next_cron_fire(&entry.schedule);
            }
        }

        due
    }

    /// 获取指定任务的 schedule
    pub fn get_schedule(&self, job_id: &str) -> Option<&Schedule> {
        self.entries.get(job_id).map(|e| &e.schedule)
    }

    /// 遍历所有已注册的定时任务
    pub fn iter_entries(&self) -> impl Iterator<Item = (&String, &Schedule)> {
        self.entries.iter().map(|(id, e)| (id, &e.schedule))
    }

    fn calc_next_fire(schedule: &Schedule) -> Option<Instant> {
        match schedule {
            Schedule::Immediate => Some(Instant::now()),
            Schedule::Once(dt) => {
                let now = Utc::now();
                let duration = dt.signed_duration_since(now).to_std().ok()?;
                Some(Instant::now() + duration)
            }
            Schedule::Cron(_) => Self::next_cron_fire(schedule),
        }
    }

    fn next_cron_fire(schedule: &Schedule) -> Option<Instant> {
        let expr = match schedule {
            Schedule::Cron(expr) => expr,
            _ => return None,
        };

        // cron crate 默认 7 字段 (秒 分 时 日 月 周 年)，补 0 前缀支持 5 字段
        let full_expr = if expr.split_whitespace().count() == 5 {
            format!("0 {}", expr)
        } else {
            expr.clone()
        };

        let cron_schedule = match CronSchedule::from_str(&full_expr) {
            Ok(s) => s,
            Err(e) => {
                warn!("Invalid cron expression '{}': {}", expr, e);
                return None;
            }
        };

        let next = cron_schedule.upcoming(Utc).next()?;
        let duration = (next - Utc::now()).to_std().ok()?;
        Some(Instant::now() + duration)
    }
}

/// 默认 sleep 时长（无定时任务时，1 小时检查一次）
pub const IDLE_SLEEP: Duration = Duration::from_secs(3600);
