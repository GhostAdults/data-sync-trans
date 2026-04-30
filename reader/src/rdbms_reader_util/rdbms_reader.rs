//! RDBMS Reader 核心实现
//!
//! `RdbmsReader` 持有 `RdbmsJob`，`ReaderJob` trait 实现在 Reader 上。
//! `RdbmsJob` 负责业务逻辑（配置管理、schema discovery），
//! `RdbmsReader` 负责生命周期管理和数据读取（返回 JsonStream）。

use crate::rdbms_reader_util::util::*;
use crate::{DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult};
use anyhow::Result;
use futures::{stream, StreamExt};
use relus_common::JobConfig;
use relus_connector_rdbms::pool::RdbmsPool;
use relus_connector_rdbms::schema::{MetadataDiscoverer, RdbmsDiscoverer, TableSchema};
use relus_connector_rdbms::util::{build_query_sql, get_pool_from_config};
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

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

/// RDBMS Reader
pub struct RdbmsReader {
    job: RdbmsJob,
}

impl RdbmsReader {
    pub fn new(config: Arc<JobConfig>, rdbms_config: RdbmsConfig) -> Result<Self> {
        let job = RdbmsJob::new(config, rdbms_config);
        Ok(Self { job })
    }

    pub fn from_job(job: RdbmsJob) -> Self {
        Self { job }
    }
}

#[async_trait::async_trait]
impl DataReaderJob for RdbmsReader {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult> {
        let result = reader_split_util::do_split(&self.job, reader_threads).await;
        Ok(result)
    }
    fn description(&self) -> String {
        format!("RdbmsReader (table: {})", self.job.config.table)
    }
}

/// RDBMS Reader Job 业务逻辑
pub struct RdbmsJob {
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub table_schema: Option<TableSchema>,
}

impl RdbmsJob {
    pub fn new(original_config: Arc<JobConfig>, config: RdbmsConfig) -> Self {
        Self {
            original_config,
            config,
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
}

/// Task: 执行查询并返回原始数据流
///
/// 每个 task 在 split 阶段已被切分为 offset/limit 分片，
/// 在 async 上下文中 collect 行后包装为 `'static` JsonStream。
#[async_trait::async_trait]
impl DataReaderTask for RdbmsReader {
    async fn read_data(&self, slice_task: &ReadTask) -> Result<JsonStream> {
        let pool = get_pool_from_config(&self.job.original_config).await?;

        let sql = match &slice_task.query_sql {
            Some(query) => query.clone(),
            None => {
                let query_str = self
                    .job
                    .config
                    .query_sql
                    .as_ref()
                    .and_then(|v| v.first())
                    .map(|s| s.as_str());
                build_query_sql(
                    &self.job.config.columns,
                    self.job.config.table.as_str(),
                    query_str,
                    slice_task.limit,
                    slice_task.offset,
                )
            }
        };

        let rows = collect_query_rows(&pool, &sql).await?;
        info!("Reader-{} 查询到 {} 条数据", slice_task.task_id, rows.len());

        Ok(Box::pin(stream::iter(rows.into_iter().map(Ok))))
    }
}

/// 从连接池执行查询并收集所有行为 JsonValue
async fn collect_query_rows(pool: &RdbmsPool, sql: &str) -> Result<Vec<JsonValue>> {
    match pool {
        RdbmsPool::Postgres(pg_pool) => {
            let mut rows = Vec::new();
            let mut stream = sqlx::query(sql).fetch(pg_pool);
            while let Some(result) = stream.next().await {
                let row: sqlx::postgres::PgRow = result?;
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col.name().to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                rows.push(JsonValue::Object(obj));
            }
            Ok(rows)
        }
        RdbmsPool::Mysql(my_pool) => {
            let mut rows = Vec::new();
            let mut stream = sqlx::query(sql).fetch(my_pool);
            while let Some(result) = stream.next().await {
                let row: sqlx::mysql::MySqlRow = result?;
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col.name().to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                rows.push(JsonValue::Object(obj));
            }
            Ok(rows)
        }
    }
}

type JsonStreamInternal<'a> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>;

/// 执行流式查询（借用 供 split 阶段 count 等场景使用）
pub fn execute_query_stream<'a>(pool: &'a RdbmsPool, sql: &'a str) -> Result<DbRowStream<'a>> {
    let stream: JsonStreamInternal<'a> = match pool {
        RdbmsPool::Postgres(pg_pool) => Box::pin(sqlx::query(sql).fetch(pg_pool).map(|result| {
            result.map(|row: sqlx::postgres::PgRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col.name().to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
        RdbmsPool::Mysql(my_pool) => Box::pin(sqlx::query(sql).fetch(my_pool).map(|result| {
            result.map(|row: sqlx::mysql::MySqlRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col.name().to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
    };

    Ok(DbRowStream { stream })
}

/// 数据库行流（借用）
pub struct DbRowStream<'a> {
    stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>,
}

impl<'a> DbRowStream<'a> {
    pub fn into_inner(
        self,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>
    {
        self.stream
    }
}

/// 获取当前表数据 count
pub async fn count_total_records(
    pool: &RdbmsPool,
    table: &str,
    query: Option<&str>,
) -> Result<usize> {
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
