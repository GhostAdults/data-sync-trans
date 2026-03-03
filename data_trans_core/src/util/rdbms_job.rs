use anyhow::{Result, Context};
use async_trait::async_trait;
use data_trans_reader::{Job, JobSplitResult, Task};
use futures::StreamExt;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::util::dbpool::DbPool;
use crate::util::dbutil::execute_query_stream;
use crate::util::splitutil::split_by_total;

#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub query: Option<String>,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
}

pub struct RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    config: RdbmsConfig,
    pool: Arc<DbPool>,
    mapper: Arc<dyn RowMapper<M> + Send + Sync>,
}

impl<M> RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub fn new(
        config: RdbmsConfig,
        pool: Arc<DbPool>,
        mapper: Arc<dyn RowMapper<M> + Send + Sync>,
    ) -> Self {
        Self {
            config,
            pool,
            mapper,
        }
    }

    fn build_query_sql(&self, limit: usize, offset: usize) -> String {
        if let Some(ref custom_query) = self.config.query {
            format!("{} LIMIT {} OFFSET {}", custom_query, limit, offset)
        } else {
            format!(
                "SELECT * FROM {} LIMIT {} OFFSET {}",
                self.config.table, limit, offset
            )
        }
    }

    async fn count_total_records(&self) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) as count FROM {}", self.config.table);

        let total = match self.pool.as_ref() {
            DbPool::Postgres(pg_pool) => {
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(pg_pool)
                    .await
                    .context("PostgreSQL COUNT 查询失败")? as usize
            }
            DbPool::Mysql(my_pool) => {
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(my_pool)
                    .await
                    .context("MySQL COUNT 查询失败")? as usize
            }
        };

        Ok(total)
    }

    async fn send_batch(
        &self,
        buffer: &[JsonValue],
        tx: &mpsc::Sender<M>,
    ) -> Result<usize> {
        let messages = self.mapper.map_rows(buffer)?;
        let count = buffer.len();
        for msg in messages {
            tx.send(msg).await?;
        }
        Ok(count)
    }

    async fn stream_data(
        &self,
        task: &Task,
        tx: &mpsc::Sender<M>,
    ) -> Result<usize> {
        let sql = self.build_query_sql(task.limit, task.offset);
        let row_stream = execute_query_stream(self.pool.as_ref(), &sql)?;

        let mut sent = 0;
        let mut buffer = Vec::with_capacity(100);

        let mut stream = row_stream.into_inner();

        while let Some(row_result) = stream.next().await {
            let json_row = row_result?;
            buffer.push(json_row);

            if buffer.len() >= 100 {
                sent += self.send_batch(&buffer, tx).await?;
                buffer.clear();

                if sent % 1000 == 0 {
                    println!("Reader-{} 已发送 {} 条", task.task_id, sent);
                }
            }
        }

        if !buffer.is_empty() {
            sent += self.send_batch(&buffer, tx).await?;
        }

        Ok(sent)
    }
}

#[async_trait]
impl<M> Job<M> for RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
        let total = self.count_total_records().await?;
        Ok(split_by_total(total, reader_threads))
    }

    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<M>,
    ) -> Result<usize> {
        println!(
            "Reader-{} 启动 (OFFSET={}, LIMIT={})",
            task.task_id, task.offset, task.limit
        );

        let sent = self.stream_data(&task, &tx).await?;

        println!("Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("RdbmsJob (table: {})", self.config.table)
    }
}

pub trait RowMapper<M>: Send + Sync {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<M>>;
}

pub struct JsonRowMapper;

impl RowMapper<JsonValue> for JsonRowMapper {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<JsonValue>> {
        Ok(rows.to_vec())
    }
}
