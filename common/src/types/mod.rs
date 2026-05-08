//! 类型系统模块
//!
//! 提供统一的中间类型表示和类型转换机制
//! 以及 MappingRow 架构规范的完整类型定义
//!
//! # 类型系统架构
//!
//! - `UnifiedValue`: 统一值类型
//! - `TypeKind`: 类型标识枚举
//! - `SourceType`: 数据源类型
//! - `OriginalTypeInfo`: 原始类型元信息
//! - `MappingField`: 单字段封装（值 + 元信息）
//! - `MappingSchema`: 表结构定义
//! - `MappingRow`: 核心数据载体
//! - `TypeConverter` / `TypeConverterRegistry`: 类型转换器

mod converter;
mod mapping_field;
mod mapping_row;
mod mapping_schema;
mod original_type_info;
mod source_type;
mod unified_value;

// === 核心类型导出 ===

pub use converter::{TypeConverter, TypeConverterRegistry};
pub use mapping_field::MappingField;
pub use mapping_row::{BatchMetadata, MappingBatch, MappingRow};
pub use mapping_schema::MappingSchema;
pub use original_type_info::OriginalTypeInfo;
pub use source_type::SourceType;
pub use unified_value::{TypeKind, UnifiedValue};
