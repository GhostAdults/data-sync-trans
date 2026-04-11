//! RDBMS Reader 核心实现
//!
//! 提供关系型数据库读取功能，支持 PostgreSQL 和 MySQL

use crate::rdbms_reader_util::util::*;
use anyhow::Result;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::usize;
use tokio::sync::mpsc;
use tracing::info;

use data_trans_common::constant::pipeline::DEFAULT_BATCH_SIZE;
use data_trans_common::interface::{ReadTask, ReaderJob, ReaderTask, SplitReaderResult};
use data_trans_common::pipeline::RecordBuilder;
use data_trans_common::schema::{MetadataDiscoverer, RdbmsDiscoverer, TableSchema};
use data_trans_common::JobConfig;
use data_trans_common::PipelineMessage;

/// RDBMS 读取配置
#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub table_count: usize,
    pub is_table_mode: bool,
    pub query_sql: Option<Vec<String>>,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub split_pk: Option<String>,
    pub split_factor: Option<usize>,
    pub where_clause: Option<String>,
    pub columns: String,
}

/// RDBMS Reader Job
pub struct RdbmsJob {
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub builder: RecordBuilder,
    /// 发现的表结构（可选）
    pub table_schema: Option<TableSchema>,
}

impl RdbmsJob {
    pub fn init(original_config: Arc<JobConfig>, config: RdbmsConfig) -> Self {
        let builder =
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone());

        Self {
            original_config,
            config,
            builder,
            table_schema: None,
        }
    }

    pub async fn discover(&mut self) -> Result<()> {
        let pool = get_pool_from_config(&self.original_config).await?;
        let discoverer = RdbmsDiscoverer::new(pool, self.config.table.clone());

        info!("Discovering schema for table: {}", self.config.table);
        let schema = discoverer.discover().await?;
        info!(
            "Schema discovered: {} columns, {} primary keys",
            schema.columns.len(),
            schema.primary_keys.len()
        );

        self.table_schema = Some(schema);
        Ok(())
    }

    pub fn schema(&self) -> Option<&TableSchema> {
        self.table_schema.as_ref()
    }

    /// 发送批量数据到下游 Channel
    /// 返回实际发送的数据行数
    async fn send_batch(
        &self,
        rows: &[JsonValue],
        tx: &mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let row_count = rows.len();
        let message = self.builder.build_message(rows)?;

        tx.send(message)
            .await
            .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
        Ok(row_count)
    }
}

/// Job
#[async_trait::async_trait]
impl ReaderJob for RdbmsJob {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult> {
        let result = reader_split_util::do_split(&self, reader_threads).await;
        Ok(result)
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let sent: usize = self.read_data(&task, &tx).await?;
        info!("Reader-{} 已发送 {} 条", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("RdbmsJob (table: {})", self.config.table)
    }
}

/// Task
#[async_trait::async_trait]
impl ReaderTask for RdbmsJob {
    async fn read_data(
        &self,
        slice_task: &ReadTask,
        tx: &mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let pool = get_pool_from_config(&self.original_config).await?;

        let sql = match &slice_task.query_sql {
            Some(query) => query.clone(),
            None => {
                let query_str = self
                    .config
                    .query_sql
                    .as_ref()
                    .and_then(|v| v.first())
                    .map(|s| s.as_str());
                build_query_sql(
                    &self.config.columns,
                    self.config.table.as_str(),
                    query_str,
                    slice_task.limit,
                    slice_task.offset,
                )
            }
        };
        let row_stream = execute_query_stream(pool.as_ref(), &sql)?;

        let batch_size = self
            .original_config
            .batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let mut sent = 0;
        let mut buffer = Vec::with_capacity(batch_size);

        let mut stream = row_stream.into_inner();

        while let Some(row_result) = stream.next().await {
            let json_row = row_result?;
            buffer.push(json_row);

            if buffer.len() >= batch_size {
                sent += self.send_batch(&buffer, tx).await?;
                buffer.clear();
            }
        }

        if !buffer.is_empty() {
            sent += self.send_batch(&buffer, tx).await?;
        }

        Ok(sent)
    }
}

// ============================================
// 数据库流式查询
// ============================================

type JsonStream<'a> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>;

/// 数据库行流
pub struct DbRowStream<'a> {
    stream: JsonStream<'a>,
}

impl<'a> DbRowStream<'a> {
    pub fn into_inner(self) -> JsonStream<'a> {
        self.stream
    }
}

/// 执行流式查询
pub fn execute_query_stream<'a>(pool: &'a DbPool, sql: &'a str) -> Result<DbRowStream<'a>> {
    use sqlx::{Column, Row};

    let stream: JsonStream<'a> = match pool {
        DbPool::Postgres(pg_pool) => Box::pin(sqlx::query(sql).fetch(pg_pool).map(|result| {
            result.map(|row: sqlx::postgres::PgRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let col_name = col.name();
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col_name.to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
        DbPool::Mysql(my_pool) => Box::pin(sqlx::query(sql).fetch(my_pool).map(|result| {
            result.map(|row: sqlx::mysql::MySqlRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let col_name = col.name();
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col_name.to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
    };

    Ok(DbRowStream { stream })
}

// 获取当前数据count
pub async fn count_total_records(pool: &DbPool, table: &str, query: Option<&str>) -> Result<usize> {
    let sql = if let Some(custom_query) = query {
        format!("SELECT COUNT(*) FROM ({}) AS subquery", custom_query)
    } else {
        format!("SELECT COUNT(*) FROM {}", table)
    };
    let result = pool.executor().fetch_column_pair(&sql).await?;
    match result.0 {
        Some(cv) => cv
            .into_string()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| anyhow::anyhow!("解析记录数失败")),
        None => Ok(0),
    }
}
