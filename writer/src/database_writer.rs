//! Database Writer 包装层
//!
//! `DatabaseWriter` 持有 `DatabaseJob`，`WriterJob` trait 实现在 Writer 上。
//! `DatabaseJob` 负责业务逻辑（配置构建），
//! `DatabaseWriter` 负责生命周期管理和 pipeline 对接。

use std::sync::Arc;

use anyhow::Result;
use relus_common::pipeline::PipelineMessage;
use relus_common::JobConfig;
use tokio::sync::mpsc;

use crate::rdbms_writer_util::rdbms_writer::{
    PipelineRowWriter, RdbmsConfig, RdbmsJob, RdbmsWriter,
};
use relus_common::constant::pipeline::DEFAULT_BATCH_SIZE;
use crate::{SplitWriterResult, WriteMode, WriteTask, DataWriterJob, DataWriterTask};

pub struct DatabaseWriter {
    job: DatabaseJob,
}

pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
}

impl DatabaseWriter {
    pub fn init(config: Arc<JobConfig>) -> Result<Self> {
        let job = DatabaseJob::new(config)?;
        Ok(Self { job })
    }
}

impl DatabaseJob {
    pub fn new(original_config: Arc<JobConfig>) -> Result<Self> {
        Ok(Self { original_config })
    }

    fn build_rdbms_config(&self) -> Result<RdbmsConfig> {
        let db_config = self.original_config.target.parse_database_config()?;

        let mode = self
            .original_config
            .target
            .writer_mode
            .as_deref()
            .map(WriteMode::from_str)
            .unwrap_or(WriteMode::Insert);

        let batch_size = self
            .original_config
            .batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let use_transaction = db_config.use_transaction.unwrap_or(false);

        Ok(RdbmsConfig {
            table: db_config.table,
            key_columns: db_config.key_columns.unwrap_or_default(),
            mode,
            use_transaction,
            batch_size,
        })
    }

    fn build_rdbms_writer(&self) -> Result<RdbmsWriter> {
        let config = self.build_rdbms_config()?;
        let writer = Arc::new(PipelineRowWriter);

        let job = RdbmsJob::new(Arc::clone(&self.original_config), config, writer);

        Ok(RdbmsWriter::from_job(job))
    }
}

#[async_trait::async_trait]
impl DataWriterJob for DatabaseWriter {
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult> {
        let rdbms_writer = self.job.build_rdbms_writer()?;
        rdbms_writer.split(writer_threads).await
    }
    fn description(&self) -> String {
        format!("{}", self.job.original_config.target.name)
    }
}

#[async_trait::async_trait]
impl DataWriterTask for DatabaseWriter {
    async fn write_data(
        &self,
        task: WriteTask,
        rx: mpsc::Receiver<PipelineMessage>,
    ) -> Result<usize> {
        let rdbms_writer = self.job.build_rdbms_writer()?;
        rdbms_writer.write_data(task, rx).await
    }
}
