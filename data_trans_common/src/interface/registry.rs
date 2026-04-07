//! 数据源注册表
//!
//! 使用函数指针模式注册 Reader/Writer 创建器，支持动态扩展。
//! GlobalRegistry 定义在 common crate 中，供 Reader/Writer crate 自注册。

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::interface::{ReaderJob, WriterJob};
use crate::job_config::JobConfig;
use crate::pipeline::PipelineMessage;
use anyhow::Result;

/// Reader 创建函数类型
pub type ReaderCreator = fn(Arc<JobConfig>) -> Result<Box<dyn ReaderJob>>;

/// Writer 创建函数类型
pub type WriterCreator = fn(Arc<JobConfig>) -> Result<Box<dyn WriterJob<PipelineMessage>>>;

/// 全局注册表
pub struct GlobalRegistry {
    readers: RwLock<HashMap<String, ReaderCreator>>,
    writers: RwLock<HashMap<String, WriterCreator>>,
}

impl GlobalRegistry {
    /// 获取全局单例（空注册表，需由外部调用 register 初始化）
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceLock<GlobalRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| GlobalRegistry {
            readers: RwLock::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
        })
    }

    /// 注册 Reader 创建器
    pub fn register_reader(&self, source_type: &str, creator: ReaderCreator) {
        self.readers
            .write()
            .unwrap()
            .insert(source_type.to_string(), creator);
    }

    /// 注册 Writer 创建器
    pub fn register_writer(&self, source_type: &str, creator: WriterCreator) {
        self.writers
            .write()
            .unwrap()
            .insert(source_type.to_string(), creator);
    }

    /// 创建 Reader 实例
    pub fn create_reader(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Box<dyn ReaderJob>> {
        let readers = self.readers.read().unwrap();
        let creator = readers.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Reader 类型: '{}'. 已注册: {:?}",
                source_type,
                readers.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    /// 创建 Writer 实例
    pub fn create_writer(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Box<dyn WriterJob<PipelineMessage>>> {
        let writers = self.writers.read().unwrap();
        let creator = writers.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Writer 类型: '{}'. 已注册: {:?}",
                source_type,
                writers.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    /// 列出所有已注册的 Reader 类型
    pub fn list_readers(&self) -> Vec<String> {
        self.readers.read().unwrap().keys().cloned().collect()
    }

    /// 列出所有已注册的 Writer 类型
    pub fn list_writers(&self) -> Vec<String> {
        self.writers.read().unwrap().keys().cloned().collect()
    }
}
