//! API Reader - HTTP API 数据源的 Job 实现

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc;

use data_trans_common::pipeline::{PipelineMessage, RecordBuilder};
use data_trans_common::JobConfig;

use crate::rdbms_reader_util::util;
use data_trans_common::interface::{ReadTask, ReaderJob, SplitReaderResult};

/// API Job 实现
pub struct ApiJob {
    config: Arc<JobConfig>,
    builder: RecordBuilder,
}

impl ApiJob {
    pub fn new(config: Arc<JobConfig>) -> Self {
        let builder =
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone());
        Self { config, builder }
    }
}

#[async_trait::async_trait]
impl ReaderJob for ApiJob {
    async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
        Ok(SplitReaderResult {
            total_records: 0,
            tasks: vec![ReadTask {
                task_id: 0,
                conn: JsonValue::Null,
                query_sql: None,
                offset: 0,
                limit: 0,
            }],
        })
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        println!("Reader-{} 获取 (API 数据源)", task.task_id);
        let items = util::client_tool::fetch_from_api(&self.config).await?;
        println!("Reader-{} 获取了 {} 条数据", task.task_id, items.len());

        let mut sent = 0;
        for chunk in items.chunks(100) {
            let message = self.builder.build_message(chunk)?;
            tx.send(message).await?;
            sent += chunk.len();
        }

        println!("Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("ApiJob (source: {})", self.config.input.name)
    }
}
