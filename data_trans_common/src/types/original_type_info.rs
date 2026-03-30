//! OriginalTypeInfo - 原始类型元信息
//!
//! 精准保留数据源原生类型属性，支撑跨库类型信息不丢失

use serde::{Deserialize, Serialize};
use std::fmt;

use super::SourceType;

/// 原始字段类型信息（必须保留）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OriginalTypeInfo {
    /// 数据源类型
    pub source_type: SourceType,
    /// 原始类型名如 "decimal(18,2)"
    pub original_type_name: String,
    /// 精度
    pub precision: Option<u8>,
    /// 刻度
    pub scale: Option<u8>,
    /// 是否无符号
    pub unsigned: bool,
    /// 是否可空
    pub nullable: bool,
}

impl OriginalTypeInfo {
    pub fn new(source_type: SourceType, original_type_name: String) -> Self {
        Self {
            source_type,
            original_type_name,
            precision: None,
            scale: None,
            unsigned: false,
            nullable: true,
        }
    }

    pub fn simple(original_type_name: String) -> Self {
        Self {
            source_type: SourceType::default(),
            original_type_name,
            precision: None,
            scale: None,
            unsigned: false,
            nullable: true,
        }
    }

    pub fn with_source_type(mut self, source_type: SourceType) -> Self {
        self.source_type = source_type;
        self
    }

    pub fn with_precision(mut self, precision: u8) -> Self {
        self.precision = Some(precision);
        self
    }

    pub fn with_scale(mut self, scale: u8) -> Self {
        self.scale = Some(scale);
        self
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn full_type_name(&self) -> String {
        let mut result = self.original_type_name.clone();
        if let Some(p) = self.precision {
            result = format!("{}({}", result, p);
            if let Some(s) = self.scale {
                result = format!("{}, {}", result, s);
            }
            result.push(')');
        }
        if self.unsigned {
            result = format!("{} unsigned", result);
        }
        result
    }

    pub fn is_numeric(&self) -> bool {
        let lower = self.original_type_name.to_lowercase();
        matches!(
            lower.as_str(),
            "int"
                | "integer"
                | "bigint"
                | "smallint"
                | "tinyint"
                | "mediumint"
                | "float"
                | "double"
                | "decimal"
                | "numeric"
                | "real"
                | "number"
        ) || lower.starts_with("int")
            || lower.starts_with("bigint")
            || lower.starts_with("smallint")
            || lower.starts_with("tinyint")
            || lower.starts_with("float")
            || lower.starts_with("double")
            || lower.starts_with("decimal")
            || lower.starts_with("numeric")
    }

    pub fn is_string(&self) -> bool {
        let lower = self.original_type_name.to_lowercase();
        matches!(
            lower.as_str(),
            "varchar" | "char" | "text" | "string" | "nvarchar" | "nchar" | "clob"
        ) || lower.starts_with("varchar")
            || lower.starts_with("char")
            || lower.starts_with("text")
    }

    pub fn is_datetime(&self) -> bool {
        let lower = self.original_type_name.to_lowercase();
        matches!(
            lower.as_str(),
            "datetime" | "timestamp" | "date" | "time" | "timestamptz"
        ) || lower.starts_with("datetime")
            || lower.starts_with("timestamp")
    }

    pub fn is_boolean(&self) -> bool {
        let lower = self.original_type_name.to_lowercase();
        matches!(lower.as_str(), "bool" | "boolean") || lower.starts_with("tinyint(1)")
    }

    pub fn is_binary(&self) -> bool {
        let lower = self.original_type_name.to_lowercase();
        matches!(lower.as_str(), "blob" | "binary" | "varbinary" | "bytea")
            || lower.starts_with("blob")
            || lower.starts_with("binary")
    }
}

impl fmt::Display for OriginalTypeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_type_name())
    }
}

impl Default for OriginalTypeInfo {
    fn default() -> Self {
        Self {
            source_type: SourceType::default(),
            original_type_name: "unknown".to_string(),
            precision: None,
            scale: None,
            unsigned: false,
            nullable: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_original_type_info_builder() {
        let info = OriginalTypeInfo::new(SourceType::MySQL, "decimal".to_string())
            .with_precision(18)
            .with_scale(2)
            .with_unsigned(false)
            .with_nullable(false);

        assert_eq!(info.precision, Some(18));
        assert_eq!(info.scale, Some(2));
        assert!(!info.nullable);
        assert!(info.is_numeric());
    }

    #[test]
    fn test_full_type_name() {
        let info = OriginalTypeInfo::new(SourceType::MySQL, "decimal".to_string())
            .with_precision(18)
            .with_scale(2);
        assert_eq!(info.full_type_name(), "decimal(18, 2)");

        let info = OriginalTypeInfo::new(SourceType::MySQL, "int".to_string()).with_unsigned(true);
        assert_eq!(info.full_type_name(), "int unsigned");
    }

    #[test]
    fn test_type_detection() {
        let int_info = OriginalTypeInfo::new(SourceType::MySQL, "int".to_string());
        assert!(int_info.is_numeric());
        assert!(!int_info.is_string());

        let varchar_info = OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string());
        assert!(varchar_info.is_string());
        assert!(!varchar_info.is_numeric());

        let dt_info = OriginalTypeInfo::new(SourceType::MySQL, "datetime".to_string());
        assert!(dt_info.is_datetime());

        let blob_info = OriginalTypeInfo::new(SourceType::MySQL, "blob".to_string());
        assert!(blob_info.is_binary());
    }
}
