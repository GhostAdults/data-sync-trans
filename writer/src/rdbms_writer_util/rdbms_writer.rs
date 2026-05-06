//! RDBMS Writer 核心实现
//!
//! Writer 只负责：接收 PipelineMessage → 提取数据 → 调用 connector-rdbms 的 SQL builder → 执行写入

use std::sync::Arc;

use anyhow::{bail, Result};
use relus_connector_rdbms::pool::{DbKind, RdbmsPool};
use relus_connector_rdbms::sql_builder::{
    execute_db_write, prepare_write_batch, validate_upsert_keys,
};
use relus_connector_rdbms::util::get_pool_from_output;

use relus_common::pipeline::PipelineMessage;
use relus_common::types::UnifiedValue;
use relus_common::JobConfig;
use relus_common::MappingRow;
use tokio::sync::mpsc;
use tracing::info;

use crate::rdbms_writer_util::util::writer_split_util;
use relus_common::job_config::WriteMode;

use crate::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};

/// RDBMS 写入配置
#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub key_columns: Vec<String>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// RDBMS Writer
pub struct RdbmsWriter {
    job: RdbmsJob,
}

impl RdbmsWriter {
    pub fn init(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter + Send + Sync>,
    ) -> Result<Self> {
        let job = RdbmsJob::new(original_config, config, writer);
        Ok(Self { job })
    }

    pub fn from_job(job: RdbmsJob) -> Self {
        Self { job }
    }
}

#[async_trait::async_trait]
impl DataWriterJob for RdbmsWriter {
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult> {
        let result = writer_split_util::do_split(&self.job.original_config, writer_threads);
        Ok(result)
    }

    fn description(&self) -> String {
        format!("RdbmsWriter (table: {})", self.job.config.table)
    }
}

/// RDBMS Writer Job 业务逻辑
pub struct RdbmsJob {
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub writer: Arc<dyn RowWriter>,
}

impl RdbmsJob {
    pub fn new(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter>,
    ) -> Self {
        Self {
            original_config,
            config,
            writer,
        }
    }
}

/// Row Writer trait - 处理消息并写入
#[async_trait::async_trait]
pub trait RowWriter: Send + Sync {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<RdbmsPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize>;
}

/// 从 MappingRow 提取列名和行值
fn extract_rows_from_mapping(mapped_rows: &[MappingRow]) -> (Vec<String>, Vec<Vec<UnifiedValue>>) {
    let columns: Vec<String> = mapped_rows[0].field_names().cloned().collect();
    let mut rows = Vec::with_capacity(mapped_rows.len());
    for mapped_row in mapped_rows {
        let mut values = Vec::with_capacity(columns.len());
        for col in &columns {
            let val = mapped_row
                .get_value(col)
                .cloned()
                .unwrap_or(UnifiedValue::String(String::new()));
            values.push(val);
        }
        rows.push(values);
    }
    (columns, rows)
}

/// PipelineMessage 的 RowWriter 实现
pub struct PipelineRowWriter;

#[async_trait::async_trait]
impl RowWriter for PipelineRowWriter {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<RdbmsPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize> {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                if rows.is_empty() {
                    return Ok(0);
                }
                let (columns, row_values) = extract_rows_from_mapping(rows);
                let batch = prepare_write_batch(
                    &columns,
                    &row_values,
                    &config.table,
                    &config.key_columns,
                    config.mode,
                    db_kind,
                )?;
                execute_db_write(&batch, pool, task.batch_size).await
            }
            PipelineMessage::ReaderFinished => {
                info!("Writer-{} 收到 Reader 完成信号", task.task_id);
                Ok(0)
            }
            PipelineMessage::Error(err) => {
                bail!("收到错误信号: {}", err)
            }
        }
    }
}

#[async_trait::async_trait]
impl DataWriterTask for RdbmsWriter {
    async fn write_data(
        &self,
        task: WriteTask,
        mut rx: mpsc::Receiver<PipelineMessage>,
    ) -> Result<usize> {
        let pool = get_pool_from_output(&self.job.original_config).await?;
        let db_kind = match pool.as_ref() {
            RdbmsPool::Postgres(_) => DbKind::Postgres,
            RdbmsPool::Mysql(_) => DbKind::Mysql,
        };

        if self.job.config.mode == WriteMode::Upsert {
            validate_upsert_keys(&pool, &self.job.config.table, &self.job.config.key_columns)
                .await?;
        }

        let mut written = 0;

        while let Some(msg) = rx.recv().await {
            match self
                .job
                .writer
                .process_message(&msg, &pool, &self.job.config, db_kind, &task)
                .await
            {
                Ok(count) => {
                    written += count;
                    if count > 0 {
                        info!(
                            "Writer-{} 写入中 {} 条数据（累计：{}）",
                            task.task_id, count, written
                        );
                    }
                }
                Err(e) => bail!("Writer-{} 写入失败: {}", task.task_id, e),
            }
        }
        info!("Writer-{} 完成，共写入 {} 条数据", task.task_id, written);
        Ok(written)
    }
}
