//! API Reader - HTTP API 数据源的 Reader 实现
//!
//! `ApiReader` 持有 `ApiJob`，`ReaderJob` trait 实现在 Reader 上。
//! `ApiJob` 负责业务逻辑（配置解析），
//! `ApiReader` 负责生命周期管理和数据读取。

use anyhow::Result;
use futures::stream;
use crate::{JsonStream, ReadTask, DataReaderJob, DataReaderTask, SplitReaderResult, StreamMode};
use relus_common::JobConfig;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tracing::info;

use crate::rdbms_reader_util::util;

pub struct ApiReader {
    job: ApiJob,
}

/// API 数据源的 Job 业务逻辑
pub struct ApiJob {
    config: Arc<JobConfig>,
}

impl ApiJob {
    pub fn new(config: Arc<JobConfig>) -> Self {
        Self { config }
    }
}

impl ApiReader {
    pub fn init(config: Arc<JobConfig>) -> Result<Self> {
        let job = ApiJob::new(config);
        Ok(Self { job })
    }
}

#[async_trait::async_trait]
impl DataReaderJob for ApiReader {
    async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
        Ok(SplitReaderResult {
            total_records: 0,
            stream_mode: StreamMode::Batch,
            tasks: vec![ReadTask {
                task_id: 0,
                conn: JsonValue::Null,
                query_sql: None,
                offset: 0,
                limit: 0,
            }],
        })
    }
    fn description(&self) -> String {
        format!("ApiReader (source: {})", self.job.config.source.name)
    }
}

#[async_trait::async_trait]
impl DataReaderTask for ApiReader {
    async fn read_data(&self, task: &ReadTask) -> Result<JsonStream> {
        info!("Reader-{} 获取 (API 数据源)", task.task_id);
        let items = util::client_tool::fetch_from_api(&self.job.config).await?;
        info!("Reader-{} 获取了 {} 条数据", task.task_id, items.len());

        // 将一次性获取的 Vec<JsonValue> 包装为流
        let json_stream: JsonStream = Box::pin(stream::iter(
            items.into_iter().map(|v| Ok(v)),
        ));
        Ok(json_stream)
    }
}
