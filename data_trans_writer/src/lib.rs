//! `data_trans_writer` 提供向数据目标写入数据的核心抽象与实现。
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

pub mod database_writer;
pub mod rdbms_writer_util;
pub mod types;

use anyhow::Result;
use tokio::sync::mpsc;

pub use data_trans_common::pipeline;
pub use database_writer::DatabaseJob;
pub use rdbms_writer_util::rdbms_writer::{RdbmsConfig, RdbmsJob, RowWriter};
pub use types::{SplitResult, WriteMode, WriteTask};

#[async_trait::async_trait]
pub trait WriterJob<M>: Send + Sync
where
    M: Send + 'static,
{
    /// 切分 Writer 任务，返回 writer_threads 个 WriteTask
    async fn split(&self, writer_threads: usize) -> Result<SplitResult>;

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
