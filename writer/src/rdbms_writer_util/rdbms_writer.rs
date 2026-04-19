//! RDBMS Writer 核心实现
//!
//! 提供关系型数据库写入功能，支持 PostgreSQL 和 MySQL

use std::sync::Arc;

use anyhow::{bail, Result};
use relus_connector_rdbms::pool::{DbKind, RdbmsPool};
use relus_connector_rdbms::util::get_pool_from_output;

use relus_common::pipeline::{DbBatch, PipelineMessage};
use relus_common::types::UnifiedValue;
use relus_common::JobConfig;
use relus_common::MappingRow;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::rdbms_writer_util::util::writer_split_util;
use crate::{SplitWriterResult, WriteMode, WriteTask, DataWriterJob, DataWriterTask};

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

/// PipelineMessage 的 RowWriter 实现
pub struct PipelineRowWriter;

#[async_trait::async_trait]
impl RowWriter for PipelineRowWriter {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<RdbmsPool>,
        config: &RdbmsConfig,
        _db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize> {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                let executor =
                    RdbmsWriterExecutor::new(Arc::clone(pool), config.clone(), task.batch_size);
                executor.write_batch(rows).await
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

/// 准备数据库批次数据
pub fn prepare_db_batch(
    mapped_rows: &[MappingRow],
    table_name: &str,
    key_columns: &[String],
    mode: WriteMode,
    db_kind: DbKind,
) -> Result<DbBatch> {
    if mapped_rows.is_empty() {
        bail!("没有数据需要同步");
    }
    let columns: Vec<String> = mapped_rows[0].field_names().cloned().collect();
    let (keys, nonkeys) = split_keys_nonkeys(&columns, key_columns);
    let base_sql = build_base_sql(table_name, &columns, &keys, &nonkeys, mode, db_kind)?;

    let mut rows = Vec::new();
    for mapped_row in mapped_rows {
        let mut values = Vec::new();
        for col in &columns {
            let val = mapped_row
                .get_value(col)
                .cloned()
                .unwrap_or(UnifiedValue::String(String::new()));
            values.push(val);
        }
        rows.push(values);
    }
    Ok(DbBatch {
        base_sql,
        table_name: table_name.to_string(),
        rows,
        columns,
    })
}

/// 分离主键列和非主键列
fn split_keys_nonkeys(all_cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut nonkeys = Vec::new();

    for col in all_cols {
        if key_cols.contains(col) {
            keys.push(col.clone());
        } else {
            nonkeys.push(col.clone());
        }
    }

    (keys, nonkeys)
}

/// 构建 SQL 基础部分（不含 VALUES）
pub fn build_base_sql(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
    mode: WriteMode,
    kind: DbKind,
) -> Result<String> {
    let mut sql = String::new();

    let quoted_columns: Vec<String> = match kind {
        DbKind::Postgres => columns.iter().map(|c| format!("\"{}\"", c)).collect(),
        DbKind::Mysql => columns.to_vec(),
    };

    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    sql.push_str(&quoted_columns.join(", "));
    sql.push(')');

    if mode == WriteMode::Upsert {
        match kind {
            DbKind::Postgres => {
                if !keys.is_empty() {
                    let quoted_keys: Vec<String> =
                        keys.iter().map(|c| format!("\"{}\"", c)).collect();
                    sql.push_str(" ON CONFLICT (");
                    sql.push_str(&quoted_keys.join(", "));
                    sql.push_str(") DO UPDATE SET ");
                    let sets: Vec<String> = nonkeys
                        .iter()
                        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
                        .collect();
                    sql.push_str(&sets.join(", "));
                } else {
                    bail!("upsert 模式需要指定 key_columns");
                }
            }
            DbKind::Mysql => {
                sql.push_str(" ON DUPLICATE KEY UPDATE ");
                let sets: Vec<String> = nonkeys
                    .iter()
                    .map(|c| format!("{} = VALUES({})", c, c))
                    .collect();
                sql.push_str(&sets.join(", "))
            }
        }
    }

    Ok(sql)
}

/// 执行数据库写入（批量模式)
pub async fn execute_db_write(
    batch: &DbBatch,
    pool: &Arc<RdbmsPool>,
    batch_size: usize,
) -> Result<usize> {
    let total_rows = batch.rows.len();
    if total_rows == 0 {
        return Ok(0);
    }

    let executor = pool.executor();
    let mut processed = 0usize;
    // 这里写入会按照 batch_size 分批执行，避免一次性写入过多数据导致数据库压力过大
    for chunk in batch.rows.chunks(batch_size) {
        executor.execute_batch(&batch.base_sql, chunk).await?;
        processed += chunk.len();
    }

    Ok(processed)
}

/// WriterTaskExecutor 实现
pub struct RdbmsWriterExecutor {
    pool: Arc<RdbmsPool>,
    config: RdbmsConfig,
    db_kind: DbKind,
    batch_size: usize,
}

impl RdbmsWriterExecutor {
    pub fn new(pool: Arc<RdbmsPool>, config: RdbmsConfig, batch_size: usize) -> Self {
        let db_kind = match pool.as_ref() {
            RdbmsPool::Postgres(_) => DbKind::Postgres,
            RdbmsPool::Mysql(_) => DbKind::Mysql,
        };
        Self {
            pool,
            config,
            db_kind,
            batch_size,
        }
    }
    async fn write_batch(&self, rows: &[MappingRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let batch = prepare_db_batch(
            rows,
            &self.config.table,
            &self.config.key_columns,
            self.config.mode,
            self.db_kind,
        )?;
        execute_db_write(&batch, &self.pool, self.batch_size).await
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
                Err(e) => {
                    error!("Writer-{} 写入失败: {}", task.task_id, e);
                    bail!("Writer-{} 写入失败: {}", task.task_id, e);
                }
            }
        }
        info!("Writer-{} 完成，共写入 {} 条数据", task.task_id, written);
        Ok(written)
    }
}
