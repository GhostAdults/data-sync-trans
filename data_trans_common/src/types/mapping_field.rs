//! MappingField - 单字段封装
//!
//! 值与原始类型元信息绑定，确保类型信息永不丢失

use serde::{Deserialize, Serialize};

use super::{OriginalTypeInfo, UnifiedValue};

/// 单个字段的统一表示（值 + 原始类型元信息绑定）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MappingField {
    /// 统一值
    pub value: UnifiedValue,
    /// 原始类型元信息
    pub original_info: OriginalTypeInfo,
}

impl MappingField {
    pub fn new(value: UnifiedValue, original_info: OriginalTypeInfo) -> Self {
        Self {
            value,
            original_info,
        }
    }

    pub fn null(original_info: OriginalTypeInfo) -> Self {
        Self {
            value: UnifiedValue::Null,
            original_info,
        }
    }

    pub fn simple(value: UnifiedValue, type_name: &str) -> Self {
        Self {
            value,
            original_info: OriginalTypeInfo::simple(type_name.to_string()),
        }
    }

    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }

    pub fn type_name(&self) -> &str {
        self.value.type_name()
    }

    pub fn original_type_name(&self) -> &str {
        &self.original_info.original_type_name
    }

    pub fn source_type(&self) -> &super::SourceType {
        &self.original_info.source_type
    }

    pub fn is_nullable(&self) -> bool {
        self.original_info.nullable
    }

    pub fn with_value(mut self, value: UnifiedValue) -> Self {
        self.value = value;
        self
    }

    pub fn map_value<F>(self, f: F) -> Self
    where
        F: FnOnce(UnifiedValue) -> UnifiedValue,
    {
        MappingField::new(f(self.value), self.original_info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SourceType;

    #[test]
    fn test_mapping_field_creation() {
        let info = OriginalTypeInfo::new(SourceType::MySQL, "int".to_string());
        let field = MappingField::new(UnifiedValue::Int(42), info.clone());

        assert_eq!(field.type_name(), "int");
        assert_eq!(field.original_type_name(), "int");
        assert!(!field.is_null());
    }

    #[test]
    fn test_null_field() {
        let info = OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string());
        let field = MappingField::null(info);

        assert!(field.is_null());
        assert!(field.is_nullable());
    }

    #[test]
    fn test_simple_field() {
        let field = MappingField::simple(UnifiedValue::String("hello".to_string()), "varchar");
        assert_eq!(field.original_type_name(), "varchar");
    }

    #[test]
    fn test_map_value() {
        let field = MappingField::simple(UnifiedValue::Int(1), "int");
        let field = field.map_value(|v| {
            if let UnifiedValue::Int(n) = v {
                UnifiedValue::Int(n * 2)
            } else {
                v
            }
        });
        assert_eq!(field.value.as_int(), Some(2));
    }
}
