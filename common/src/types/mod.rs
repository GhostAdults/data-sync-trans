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

// === 向后兼容类型别名 ===

/// 向后兼容：TypedVal 是 UnifiedValue 的别名
#[deprecated(since = "0.2.0", note = "请使用 UnifiedValue")]
pub type TypedVal = UnifiedValue;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_value_types() {
        let val = UnifiedValue::Int(42);
        assert_eq!(val.type_name(), "int");
        assert_eq!(val.as_int(), Some(42));
    }

    #[test]
    fn test_source_type() {
        let source = SourceType::MySQL;
        assert!(source.is_rdbms());
        assert_eq!(source.as_str(), "mysql");
    }

    #[test]
    fn test_mapping_row() {
        let mut row = MappingRow::simple();
        row.insert_simple("id", UnifiedValue::Int(1), "int");
        row.insert_simple("name", UnifiedValue::String("Alice".into()), "varchar");

        assert_eq!(row.len(), 2);
        assert!(row.contains("id"));
    }

    #[test]
    fn test_type_kind_from_str() {
        let parse = |value: &str| value.parse::<TypeKind>().unwrap_or_else(|err| match err {});

        assert_eq!(parse("int"), TypeKind::Int);
        assert_eq!(parse("float"), TypeKind::Float);
        assert_eq!(parse("timestamp"), TypeKind::Timestamp);
    }
}
