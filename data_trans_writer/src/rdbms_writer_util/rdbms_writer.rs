//! RDBMS Writer 核心实现
//!
//! 提��关系型数据库写入功能，支持 PostgreSQL 和 MySQL

use std::sync::Arc;

use anyhow::{bail, Result};
use data_trans_common::db::{get_pool_from_config, DbKind, DbPool};
use data_trans_common::pipeline::{DbBatch, PipelineMessage};
use data_trans_common::types::UnifiedValue;
use data_trans_common::JobConfig;
use data_trans_common::MappingRow;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::rdbms_writer_util::util::writer_split_util;
use crate::types::{SplitResult, WriteMode, WriteTask};
use crate::{WriterJob, WriterTask};

/// RDBMS 写入配置
#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub key_columns: Vec<String>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// RDBMS Writer Job
pub struct RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub writer: Arc<dyn RowWriter<M> + Send + Sync>,
}

impl<M> RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub fn new(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter<M> + Send + Sync>,
    ) -> Self {
        Self {
            original_config,
            config,
            writer,
        }
    }
}

/// Job 实现
#[async_trait::async_trait]
impl<M> WriterJob<M> for RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    async fn split(&self, writer_threads: usize) -> Result<SplitResult> {
        let result = writer_split_util::do_split(&self.original_config, writer_threads);
        Ok(result)
    }

    async fn execute_task(&self, task: WriteTask, mut rx: mpsc::Receiver<M>) -> Result<usize> {
        println!("Writer-{} 启动", task.task_id);

        let pool = get_pool_from_config(&self.original_config).await?;
        let db_kind = match pool.as_ref() {
            DbPool::Postgres(_) => DbKind::Postgres,
            DbPool::Mysql(_) => DbKind::Mysql,
        };
        let mut written = 0;

        while let Some(msg) = rx.recv().await {
            match self
                .writer
                .process_message(&msg, &pool, &self.config, db_kind, &task)
                .await
            {
                Ok(count) => {
                    written += count;
                    if count > 0 {
                        println!(
                            "Writer-{} 写入 {} 条数据（累计：{}）",
                            task.task_id, count, written
                        );
                    }
                }
                Err(e) => {
                    error!("Writer-{} 写入失败: {}", task.task_id, e);
                    // 直接返回错误，停止写入
                    bail!("Writer-{} 写入失败: {}", task.task_id, e);
                }
            }
        }

        info!("Writer-{} 完成，共写入 {} 条数据", task.task_id, written);
        Ok(written)
    }

    fn description(&self) -> String {
        format!("RdbmsJob (table: {})", self.config.table)
    }
}

/// Row Writer trait - 处理消息并写入
#[async_trait::async_trait]
pub trait RowWriter<M>: Send + Sync {
    async fn process_message(
        &self,
        msg: &M,
        pool: &Arc<DbPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize>;
}

/// PipelineMessage 的 RowWriter 实现
pub struct PipelineRowWriter;

#[async_trait::async_trait]
impl RowWriter<PipelineMessage> for PipelineRowWriter {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<DbPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize> {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                if rows.is_empty() {
                    return Ok(0);
                }

                let batch = prepare_db_batch(
                    rows,
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
    println!("数据 {:#?}", mapped_rows);

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

    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    sql.push_str(&columns.join(", "));
    sql.push(')');

    if mode == WriteMode::Upsert {
        match kind {
            DbKind::Postgres => {
                if !keys.is_empty() {
                    sql.push_str(" ON CONFLICT (");
                    sql.push_str(&keys.join(", "));
                    sql.push_str(") DO UPDATE SET ");
                    let sets: Vec<String> = nonkeys
                        .iter()
                        .map(|c| format!("{} = EXCLUDED.{}", c, c))
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
    pool: &Arc<DbPool>,
    batch_size: usize,
) -> Result<usize> {
    let total_rows = batch.rows.len();
    if total_rows == 0 {
        return Ok(0);
    }

    let executor = pool.executor();
    let mut processed = 0usize;

    for chunk in batch.rows.chunks(batch_size) {
        executor.execute_batch(&batch.base_sql, chunk).await?;
        processed += chunk.len();
    }

    Ok(processed)
}

/// WriterTaskExecutor 实现
pub struct RdbmsWriterExecutor {
    pool: Arc<DbPool>,
    config: RdbmsConfig,
    db_kind: DbKind,
    batch_size: usize,
}

impl RdbmsWriterExecutor {
    pub fn new(pool: Arc<DbPool>, config: RdbmsConfig, batch_size: usize) -> Self {
        let db_kind = match pool.as_ref() {
            DbPool::Postgres(_) => DbKind::Postgres,
            DbPool::Mysql(_) => DbKind::Mysql,
        };
        Self {
            pool,
            config,
            db_kind,
            batch_size,
        }
    }
}

#[async_trait::async_trait]
impl WriterTask for RdbmsWriterExecutor {
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
