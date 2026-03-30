//! Writer 相关类型定义

use std::sync::Arc;

use data_trans_common::JobConfig;

/// 一个writer job分裂成多个writer task
#[derive(Debug, Clone)]
pub struct WriteTask {
    pub task_id: usize,
    pub config: Arc<JobConfig>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// 写入模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Insert,
    Upsert,
}

impl WriteMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "upsert" => WriteMode::Upsert,
            _ => WriteMode::Insert,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::Insert => "insert",
            WriteMode::Upsert => "upsert",
        }
    }
}

/// Job 切分结果
pub struct SplitResult {
    pub tasks: Vec<WriteTask>,
}
