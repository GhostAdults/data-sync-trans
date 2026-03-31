//! Database Writer 包装层
//!
//! 包装 RdbmsJob 提供统一的 DatabaseJob 接口

use std::sync::Arc;

use anyhow::Result;
use data_trans_common::pipeline::PipelineMessage;
use data_trans_common::JobConfig;
use tokio::sync::mpsc;

use crate::rdbms_writer_util::rdbms_writer::{PipelineRowWriter, RdbmsConfig, RdbmsJob};
use data_trans_common::interface::{SplitWriterResult, WriteMode, WriteTask, WriterJob};

/// Database Writer Job
pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
}

impl DatabaseJob {
    pub fn new(original_config: Arc<JobConfig>) -> Result<Self> {
        Ok(Self { original_config })
    }

    fn build_rdbms_config(&self) -> Result<RdbmsConfig> {
        let db_config = JobConfig::parse_database_config(&self.original_config.output)?;

        let mode = self
            .original_config
            .mode
            .as_deref()
            .map(WriteMode::from_str)
            .unwrap_or(WriteMode::Insert);

        let batch_size = self.original_config.batch_size.unwrap_or(100);
        let use_transaction = db_config.use_transaction.unwrap_or(false);

        Ok(RdbmsConfig {
            table: db_config.table,
            key_columns: db_config.key_columns.unwrap_or_default(),
            mode,
            use_transaction,
            batch_size,
        })
    }

    fn build_rdbms_job(&self) -> Result<RdbmsJob<PipelineMessage>> {
        let config = self.build_rdbms_config()?;
        let writer = Arc::new(PipelineRowWriter);

        Ok(RdbmsJob::new(
            Arc::clone(&self.original_config),
            config,
            writer,
        ))
    }
}

#[async_trait::async_trait]
impl WriterJob<PipelineMessage> for DatabaseJob {
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult> {
        let rdbms_job = self.build_rdbms_job()?;
        rdbms_job.split(writer_threads).await
    }

    async fn execute_task(
        &self,
        task: WriteTask,
        rx: mpsc::Receiver<PipelineMessage>,
    ) -> Result<usize> {
        let rdbms_job = self.build_rdbms_job()?;
        rdbms_job.execute_task(task, rx).await
    }

    fn description(&self) -> String {
        format!("DatabaseJob (target: {})", self.original_config.output.name)
    }
}
