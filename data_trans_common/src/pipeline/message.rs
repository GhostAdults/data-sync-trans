//! Pipeline 消息定义
//!
//! 定义 Reader 到 Writer 的消息类型和管道配置

use crate::types::{MappingRow, SourceType, UnifiedValue};

/// Reader 到 Writer 的消息
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    DataBatch(Vec<MappingRow>),
    ReaderFinished,
    Error(String),
}

/// 数据库批次
#[derive(Debug)]
pub struct DbBatch {
    pub base_sql: String,
    pub table_name: String,
    pub rows: Vec<Vec<UnifiedValue>>,
    pub columns: Vec<String>,
}

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub reader_threads: usize,
    pub writer_threads: usize,
    pub channel_buffer_size: usize,
    pub batch_size: usize,
    pub use_transaction: bool,
    pub data_source: SourceType,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: 4,
            writer_threads: 4,
            channel_buffer_size: 1000,
            batch_size: 100,
            use_transaction: true,
            data_source: SourceType::Other("unknown".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.reader_threads, 4);
        assert_eq!(config.writer_threads, 4);
        assert_eq!(config.batch_size, 100);
    }
}
