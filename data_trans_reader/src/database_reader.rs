use anyhow::Result;
/// Database Reader - 数据库数据源的 Job 实现
use data_trans_common::interface::{ReadTask, ReaderJob, SplitReaderResult};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::rdbms_reader_util::rdbms_reader::{RdbmsConfig, RdbmsJob};
use data_trans_common::schema::SchemaDiscoveryConfig;
use data_trans_common::JobConfig;
use data_trans_common::PipelineMessage;

pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
    discovery_config: SchemaDiscoveryConfig,
}

impl DatabaseJob {
    pub fn new(original_config: Arc<JobConfig>) -> Result<Self> {
        Ok(Self {
            original_config,
            discovery_config: SchemaDiscoveryConfig::default(),
        })
    }

    pub fn with_discovery_config(mut self, config: SchemaDiscoveryConfig) -> Self {
        self.discovery_config = config;
        self
    }

    fn build_rdbms_job(&self) -> RdbmsJob {
        let db_config = JobConfig::parse_database_config(&self.original_config.input).unwrap();

        let input_config = &self.original_config.input.config;
        let split_pk = input_config
            .get("split_pk")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let where_clause = input_config
            .get("where")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let columns = input_config
            .get("columns")
            .and_then(|v| v.as_str())
            .unwrap_or("*")
            .to_string();
        let split_factor = input_config
            .get("split_factor")
            .and_then(|v| v.as_u64().map(|n| n as usize));

        let rdbms_config = RdbmsConfig {
            table: db_config.table,
            table_count: 1,
            is_table_mode: self.original_config.input.is_table_mode,
            query_sql: self.original_config.input.query_sql.clone(),
            column_mapping: self.original_config.column_mapping.clone(),
            column_types: self.original_config.column_types.clone(),
            split_pk,
            split_factor,
            where_clause,
            columns,
        };

        RdbmsJob::init(Arc::clone(&self.original_config), rdbms_config)
    }

    pub async fn discover(&self) -> Result<RdbmsJob> {
        let mut job = self.build_rdbms_job();

        if self.discovery_config.enabled {
            info!("Schema discovery enabled, discovering table schema...");
            job.discover().await?;
        }

        Ok(job)
    }
}

#[async_trait::async_trait]
impl ReaderJob for DatabaseJob {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult> {
        let rdbms_job = self.discover().await?;
        rdbms_job.split(reader_threads).await
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let rdbms_job = self.discover().await?;
        rdbms_job.execute_task(task, tx).await
    }

    fn description(&self) -> String {
        format!("DatabaseJob (source: {})", self.original_config.input.name)
    }
}
