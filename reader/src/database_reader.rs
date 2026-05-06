//! Database Reader - 数据库数据源的 Reader 实现
//!
//! `DatabaseReader` 持有 `DatabaseJob`，`ReaderJob` trait 实现在 Reader 上。
//! `DatabaseJob` 负责业务逻辑（配置构建、schema discovery），
//! `DatabaseReader` 负责生命周期管理和数据读取。

use crate::{DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult};
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use crate::rdbms_reader_util::rdbms_reader::{RdbmsConfig, RdbmsJob, RdbmsReader};
use relus_common::JobConfig;
use relus_connector_rdbms::schema::SchemaDiscoveryConfig;

pub struct DatabaseReader {
    job: DatabaseJob,
}

pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
    discovery_config: SchemaDiscoveryConfig,
}

impl DatabaseReader {
    pub fn init(config: Arc<JobConfig>) -> Result<Self> {
        let job = DatabaseJob::new(config)?;
        Ok(Self { job })
    }
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

    fn build_rdbms_job(&self) -> Result<RdbmsJob> {
        let db_config = self.original_config.source.parse_database_config()?;

        let input = &self.original_config.source;
        let split_pk = input.config_str("split_pk");
        let where_clause = input.config_str("where");
        let columns = input.config_str("columns").unwrap_or_else(|| {
            self.original_config
                .column_mapping
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        });
        let split_factor = input.config_u64("split_factor").map(|n| n as usize);

        let rdbms_config = RdbmsConfig {
            table: db_config.table,
            table_count: 1,
            is_table_mode: self.original_config.source.is_table_mode,
            query_sql: self.original_config.source.query_sql.clone(),
            column_mapping: self.original_config.column_mapping.clone(),
            column_types: self.original_config.column_types.clone(),
            split_pk,
            split_factor,
            where_clause,
            columns,
        };

        Ok(RdbmsJob::new(
            Arc::clone(&self.original_config),
            rdbms_config,
        ))
    }

    pub async fn discover(&self) -> Result<RdbmsReader> {
        let mut rdbms_job = self.build_rdbms_job()?;

        if self.discovery_config.enabled {
            info!("Schema discovery enabled, discovering table schema...");
            rdbms_job.discover().await?;
        }

        Ok(RdbmsReader::from_job(rdbms_job))
    }
}

#[async_trait::async_trait]
impl DataReaderJob for DatabaseReader {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult> {
        let rdbms_reader: RdbmsReader = self.job.discover().await?;
        rdbms_reader.split(reader_threads).await
    }
    fn description(&self) -> String {
        self.job.original_config.source.name.to_string()
    }
}

#[async_trait::async_trait]
impl DataReaderTask for DatabaseReader {
    async fn read_data(&self, task: &ReadTask) -> Result<JsonStream> {
        let rdbms_reader: RdbmsReader = self.job.discover().await?;
        rdbms_reader.read_data(task).await
    }
}
