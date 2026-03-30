//! 数据源注册表
//!
//! 使用函数指针模式注册 Reader/Writer 创建器，支持动态扩展。

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::Result;
use data_trans_common::{JobConfig, PipelineMessage};
use data_trans_reader::ReaderJob;
use data_trans_writer::WriterJob;

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
    /// 获取全局单例
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceLock<GlobalRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let registry = GlobalRegistry {
                readers: RwLock::new(HashMap::new()),
                writers: RwLock::new(HashMap::new()),
            };
            register_builtin(&registry);
            registry
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
            anyhow::anyhow!("未找到 Reader 类型: '{}'. 已注册: {:?}", source_type, readers.keys().collect::<Vec<_>>())
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
            anyhow::anyhow!("未找到 Writer 类型: '{}'. 已注册: {:?}", source_type, writers.keys().collect::<Vec<_>>())
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

/// 注册内置数据源
fn register_builtin(registry: &GlobalRegistry) {
    // Reader: API
    registry.register_reader("api", |config| {
        Ok(Box::new(data_trans_reader::ApiJob::new(config)))
    });

    // Reader: Database
    registry.register_reader("database", |config| {
        let job = data_trans_reader::DatabaseJob::new(config)?;
        Ok(Box::new(job))
    });

    // Writer: Database
    registry.register_writer("database", |config| {
        let job = data_trans_writer::DatabaseJob::new(config)?;
        Ok(Box::new(job))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_registered_types() {
        let registry = GlobalRegistry::instance();
        let readers = registry.list_readers();
        let writers = registry.list_writers();

        assert!(readers.contains(&"api".to_string()));
        assert!(readers.contains(&"database".to_string()));
        assert!(writers.contains(&"database".to_string()));
    }
}
