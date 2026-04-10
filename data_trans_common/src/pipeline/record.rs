//! 数据记录行定义rows
//!
//! Record 是 MappingRow 的类型别名，作为系统内唯一数据记录类型，方便在不同模块间传递和使用。

// Re-export MappingRow as Record for backward compatibility
pub use crate::types::MappingRow as Record;

// Also re-export related types for convenience
pub use crate::types::{MappingField, MappingSchema, UnifiedValue};
