//! `Writer` 提供向数据目标写入数据的核心抽象Trait。
//!
//! # 核心特征
//!
//! ## [`WriterJob`]
//! 数据写入任务的顶层 trait，所有数据目标均需实现此接口：
//! - [`WriterJob::split`] — 根据 writer_threads 将写入任务切分为多个 [`WriteTask`]
//! - [`WriterJob::execute_task`] — 执行单个写入任务，从 Channel 接收数据并写入目标
//! - [`WriterJob::description`] — 返回数据目标的描述信息，用于日志输出
//!
//! ## [`WriterTask`]
//! 单个分片的底层写入接口，由具体数据目标实现，供 [`WriterJob::execute_task`] 调用。
//!
//! # 数据结构
//! - [`WriteTask`] — 描述一个写入任务（task_id、配置、模式等）
//! - [`SplitResult`] — [`WriterJob::split`] 的返回值，包含分片列表
//!
//! # 示例实现
//! `database_writer` 是内置的参考实现，对应关系型数据库写入目标。
//!
use crate::job_config::JobConfig;
use crate::pipeline;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 一个writer job分裂成多个writer task
#[derive(Debug, Clone)]
pub struct WriteTask {
    pub task_id: usize,
    pub config: Arc<JobConfig>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// Job 切分结果
pub struct SplitWriterResult {
    pub tasks: Vec<WriteTask>,
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

#[async_trait::async_trait]
pub trait WriterJob<M>: Send + Sync
where
    M: Send + 'static,
{
    /// 切分 Writer 任务，返回 writer_threads 个 WriteTask
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult>;

    /// 执行单个 Writer 任务，从 Channel 接收数据并写入
    async fn execute_task(&self, task: WriteTask, rx: mpsc::Receiver<M>) -> Result<usize>;

    /// 返回描述信息
    fn description(&self) -> String;
}

/// WriterTask trait
#[async_trait::async_trait]
pub trait WriterTask: Send + Sync {
    async fn write_batch(&self, rows: &[pipeline::Record]) -> Result<usize>;
}
