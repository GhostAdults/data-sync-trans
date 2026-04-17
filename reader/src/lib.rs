pub mod api_reader;
pub mod binlog_reader;
pub mod database_reader;
pub mod rdbms_reader_util;

pub use api_reader::{ApiJob, ApiReader};
pub use binlog_reader::{BinlogConfig, BinlogReader, CdcOp};
pub use database_reader::{DatabaseJob, DatabaseReader};
pub use rdbms_reader_util::rdbms_reader::{
    count_total_records, execute_query_stream, DbRowStream, RdbmsConfig, RdbmsReader,
};

use anyhow::Result;
use futures::stream::Stream;
use relus_common::job_config::JobConfig;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};

// ==========================================
// Reader trait 定义
// ==========================================

/// Reader 返回的原始数据流
pub type JsonStream = Pin<Box<dyn Stream<Item = Result<JsonValue>> + Send + 'static>>;

/// Stream 模式：Finite(全量，有终点) / Infinite(CDC，永不结束)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    Finite,
    Infinite,
}

impl Default for StreamMode {
    fn default() -> Self {
        StreamMode::Finite
    }
}

/// 一个 reader job 分裂成多个 reader task
#[derive(Debug, Clone)]
pub struct ReadTask {
    pub task_id: usize,
    pub conn: JsonValue,
    pub query_sql: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

/// 分片结果
pub struct SplitReaderResult {
    pub total_records: usize,
    pub tasks: Vec<ReadTask>,
    /// 声明 stream 模式：Finite(全量) / Infinite(CDC)
    pub stream_mode: StreamMode,
}

/// Reader Job trait
#[async_trait::async_trait]
pub trait ReaderJob: Send + Sync {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult>;
    fn description(&self) -> String;
}

/// Reader Task trait
#[async_trait::async_trait]
pub trait ReaderTask: Send + Sync {
    async fn read_data(&self, task: &ReadTask) -> Result<JsonStream>;

    fn shutdown(&self) {}
}

/// Reader = ReaderJob + ReaderTask
#[async_trait::async_trait]
pub trait Reader: ReaderJob + ReaderTask {}

#[async_trait::async_trait]
impl<T: ReaderJob + ReaderTask> Reader for T {}

// ==========================================
// Reader 全局注册表
// ==========================================

/// Reader 创建函数类型
type ReaderCreator = fn(Arc<JobConfig>) -> Result<Box<dyn Reader>>;

/// Reader 插件（由各 reader 实现通过 inventory::submit! 注册）
pub struct ReaderPlugin {
    pub source_type: &'static str,
    pub create: ReaderCreator,
}

inventory::collect!(ReaderPlugin);

/// Reader 全局注册表
pub struct ReaderRegistry {
    creators: RwLock<HashMap<String, ReaderCreator>>,
}

impl ReaderRegistry {
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceLock<ReaderRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| ReaderRegistry {
            creators: RwLock::new(HashMap::new()),
        })
    }

    /// 从 inventory 收集所有已链接的 Reader 插件并注册
    pub fn collect_and_register() {
        let registry = Self::instance();
        for plugin in inventory::iter::<ReaderPlugin> {
            registry
                .creators
                .write()
                .unwrap()
                .insert(plugin.source_type.to_string(), plugin.create);
        }
    }

    pub fn prepare_reader(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Box<dyn Reader>> {
        let creators = self.creators.read().unwrap();
        let creator = creators.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Reader 类型: '{}'. 已注册: {:?}",
                source_type,
                creators.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    pub fn list_readers(&self) -> Vec<String> {
        self.creators.read().unwrap().keys().cloned().collect()
    }
}

// ==========================================
// 注册本 crate 的 Reader 实现
// ==========================================

inventory::submit! {
    ReaderPlugin {
        source_type: "api",
        create: |config| {
            let reader = ApiReader::init(config)?;
            Ok(Box::new(reader))
        },
    }
}

inventory::submit! {
    ReaderPlugin {
        source_type: "database",
        create: |config| {
            let reader = DatabaseReader::init(config)?;
            Ok(Box::new(reader))
        },
    }
}

inventory::submit! {
    ReaderPlugin {
        source_type: "mysql_binlog",
        create: |config| {
            let reader = BinlogReader::init(config)?;
            Ok(Box::new(reader))
        },
    }
}
