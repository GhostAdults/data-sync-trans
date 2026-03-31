//! `Reader` 提供从数据源读取数据的核心抽象Trait。
//!
//! # 核心特征
//!
//! ## [`ReaderJob`]
//! 数据读取任务的顶层 trait，所有数据源均需实现此接口：
//! - [`ReaderJob::split`] — 根据总记录数与并发度将数据源切分为多个 [`ReadTask`]，
//!   切分阶段**不持有**连接池，仅用于计算分片范围
//! - [`ReaderJob::execute_task`] — 执行单个分片任务，内部按需创建连接池并通过
//!   `mpsc::Sender` 将数据批量推送到下游管道
//! - [`ReaderJob::description`] — 返回数据源的描述信息，用于日志输出
//!
//! ## [`ReaderTask`]
//! 单个分片的底层读取接口，由具体数据源实现，供 [`ReaderJob::execute_task`] 调用。
//!
//! # 数据结构
//! - [`ReadTask`] — 描述一个分片任务（task_id、offset、limit、可选的自定义 SQL）
//! - [`SplitResult`] — [`ReaderJob::split`] 的返回值，包含总记录数与分片列表
//!
//! # 示例实现
//! `database_reader` 和 `api_reader` 是两个内置的参考实现，分别对应关系型数据库
//!  HTTP API 数据源，可作为自定义数据源实现的参考。
//!

use crate::pipeline::PipelineMessage;
use anyhow::Result;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

/// 一个 reader job 分裂成多个 reader task
#[derive(Debug, Clone)]
pub struct ReadTask {
    pub task_id: usize,
    pub conn: JsonValue,
    pub query_sql: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

/// 分片结果
pub struct SplitReaderResult {
    pub total_records: usize,
    pub tasks: Vec<ReadTask>,
}

/// Reader Job trait
///
/// 所有数据源读取器 trait
/// 统一输出 PipelineMessage 类型
#[async_trait::async_trait]
pub trait ReaderJob: Send + Sync {
    /// 切分任务
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult>;
    /// 执行单个分片任务
    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize>;
    /// 返回描述信息
    fn description(&self) -> String;
}

/// Reader Task trait
///
/// 底层读取接口，由具体数据源实现
#[async_trait::async_trait]
pub trait ReaderTask: Send + Sync {
    /// 读取数据并发送到 Channel
    async fn read_data(&self, task: &ReadTask, tx: &mpsc::Sender<PipelineMessage>)
        -> Result<usize>;
}
