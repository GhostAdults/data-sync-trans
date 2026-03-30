//! Pipeline 模块
//!
//! 提供数据管道相关的数据结构和中间类型系统构建器
//!
//! # 核心类型
//!
//! - `Record`: MappingRow 的类型别名，全流程唯一数据载体
//! - `PipelineMessage`: Reader 到 Writer 的消息
//! - `RecordBuilder`: 数据转换构建器
//! - `DbBatch`: 数据库批量写入容器
//! - `PipelineConfig`: 管道配置

mod message;
mod record;
mod record_builder;

pub use message::*;
pub use record::*;
pub use record_builder::*;

// 重新导出 MappingRow 相关类型（来自 types 模块）
pub use crate::types::{BatchMetadata, MappingBatch, MappingRow};
