//! 数据记录定义
//!
//! Record 是 MappingRow 的类型别名，作为全流程唯一数据载体

// Re-export MappingRow as Record for backward compatibility
pub use crate::types::MappingRow as Record;

// Also re-export related types for convenience
pub use crate::types::{MappingField, MappingSchema, UnifiedValue};
