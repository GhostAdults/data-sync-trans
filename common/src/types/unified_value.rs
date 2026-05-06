//! 统一值类型定义
//!
//! 整合 TypedVal 和 UnifiedValue，提供全数据源兼容的类型系统
//! 保留 Option 变体以兼容现有代码，同时支持完整的类型表示

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

/// 类型标识 - 用于类型提示和映射配置
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TypeKind {
    Int,
    Float,
    Bool,
    Decimal,
    Timestamp,
    Date,
    Time,
    Json,
    #[default]
    Text,
    Bytes,
    Array,
}

impl TypeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TypeKind::Int => "int",
            TypeKind::Float => "float",
            TypeKind::Bool => "bool",
            TypeKind::Decimal => "decimal",
            TypeKind::Timestamp => "timestamp",
            TypeKind::Date => "date",
            TypeKind::Time => "time",
            TypeKind::Json => "json",
            TypeKind::Text => "text",
            TypeKind::Bytes => "bytes",
            TypeKind::Array => "array",
        }
    }
}

impl FromStr for TypeKind {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s.to_lowercase().as_str() {
            "int" | "integer" | "i64" | "i32" => TypeKind::Int,
            "float" | "double" | "f64" => TypeKind::Float,
            "bool" | "boolean" => TypeKind::Bool,
            "decimal" | "numeric" => TypeKind::Decimal,
            "timestamp" | "datetime" => TypeKind::Timestamp,
            "date" => TypeKind::Date,
            "time" => TypeKind::Time,
            "json" => TypeKind::Json,
            "bytes" | "binary" | "blob" => TypeKind::Bytes,
            "array" => TypeKind::Array,
            _ => TypeKind::Text,
        };

        Ok(kind)
    }
}

impl fmt::Display for TypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 系统内部统一值类型（全数据源兼容，无类型丢失）
///
/// 整合了原 TypedVal 和 UnifiedValue 的所有变体：
/// - 基础类型：I64, F64, Bool, Decimal, Text
/// - 时间类型：Date, Time, DateTime
/// - 复合类型：Json, Bytes, Array
/// - Option 变体：OptI64, OptF64, OptBool, OptDecimal, OptDateTime（兼容现有代码）
/// - 空值：Null
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnifiedValue {
    /// 空值
    Null,
    /// 布尔值
    Bool(bool),
    /// 整数类型
    Int(i64),
    /// 浮点数类型
    Float(f64),
    /// 高精度小数
    Decimal(Decimal),
    /// 字符串类型
    String(String),
    /// 字节数组
    Bytes(Vec<u8>),
    /// 日期类型
    Date(NaiveDate),
    /// 时间类型
    Time(NaiveTime),
    /// 日期时间类型
    DateTime(NaiveDateTime),
    /// JSON
    Json(serde_json::Value),
    /// 数组
    Array(Vec<UnifiedValue>),

    // Option 变体（兼容现有 TypedVal 代码）
    /// 可空整数
    OptI64(Option<i64>),
    /// 可空浮点数
    OptF64(Option<f64>),
    /// 可空布尔值
    OptBool(Option<bool>),
    /// 可空高精度小数
    OptDecimal(Option<Decimal>),
    /// 可空日期时间
    OptDateTime(Option<NaiveDateTime>),
}

impl UnifiedValue {
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            UnifiedValue::Null
                | UnifiedValue::OptI64(None)
                | UnifiedValue::OptF64(None)
                | UnifiedValue::OptBool(None)
                | UnifiedValue::OptDecimal(None)
                | UnifiedValue::OptDateTime(None)
        )
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            UnifiedValue::Null => "null",
            UnifiedValue::Bool(_) => "bool",
            UnifiedValue::Int(_) => "int",
            UnifiedValue::Float(_) => "float",
            UnifiedValue::Decimal(_) => "decimal",
            UnifiedValue::String(_) => "string",
            UnifiedValue::Bytes(_) => "bytes",
            UnifiedValue::Date(_) => "date",
            UnifiedValue::Time(_) => "time",
            UnifiedValue::DateTime(_) => "datetime",
            UnifiedValue::Json(_) => "json",
            UnifiedValue::Array(_) => "array",
            UnifiedValue::OptI64(Some(_)) => "option<int>",
            UnifiedValue::OptI64(None) => "null",
            UnifiedValue::OptF64(Some(_)) => "option<float>",
            UnifiedValue::OptF64(None) => "null",
            UnifiedValue::OptBool(Some(_)) => "option<bool>",
            UnifiedValue::OptBool(None) => "null",
            UnifiedValue::OptDecimal(Some(_)) => "option<decimal>",
            UnifiedValue::OptDecimal(None) => "null",
            UnifiedValue::OptDateTime(Some(_)) => "option<datetime>",
            UnifiedValue::OptDateTime(None) => "null",
        }
    }

    pub fn type_kind(&self) -> TypeKind {
        match self {
            UnifiedValue::Bool(_) | UnifiedValue::OptBool(_) => TypeKind::Bool,
            UnifiedValue::Int(_) | UnifiedValue::OptI64(_) => TypeKind::Int,
            UnifiedValue::Float(_) | UnifiedValue::OptF64(_) => TypeKind::Float,
            UnifiedValue::Decimal(_) | UnifiedValue::OptDecimal(_) => TypeKind::Decimal,
            UnifiedValue::Date(_) => TypeKind::Date,
            UnifiedValue::Time(_) => TypeKind::Time,
            UnifiedValue::DateTime(_) | UnifiedValue::OptDateTime(_) => TypeKind::Timestamp,
            UnifiedValue::Json(_) => TypeKind::Json,
            UnifiedValue::Bytes(_) => TypeKind::Bytes,
            UnifiedValue::Array(_) => TypeKind::Array,
            _ => TypeKind::Text,
        }
    }

    // === as_* 方法 ===

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            UnifiedValue::Bool(b) => Some(*b),
            UnifiedValue::OptBool(Some(b)) => Some(*b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            UnifiedValue::Int(i) => Some(*i),
            UnifiedValue::OptI64(Some(i)) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            UnifiedValue::Float(f) => Some(*f),
            UnifiedValue::OptF64(Some(f)) => Some(*f),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<&Decimal> {
        match self {
            UnifiedValue::Decimal(d) => Some(d),
            UnifiedValue::OptDecimal(Some(d)) => Some(d),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            UnifiedValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            UnifiedValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<&NaiveDate> {
        match self {
            UnifiedValue::Date(d) => Some(d),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<&NaiveTime> {
        match self {
            UnifiedValue::Time(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<&NaiveDateTime> {
        match self {
            UnifiedValue::DateTime(dt) => Some(dt),
            UnifiedValue::OptDateTime(Some(dt)) => Some(dt),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            UnifiedValue::Json(j) => Some(j),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<UnifiedValue>> {
        match self {
            UnifiedValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    // === 转换为非 Option 变体 ===

    /// 将 Option 变体转换为标准变体（Null 或具体值）
    pub fn to_canonical(&self) -> UnifiedValue {
        match self {
            UnifiedValue::OptI64(Some(v)) => UnifiedValue::Int(*v),
            UnifiedValue::OptI64(None) => UnifiedValue::Null,
            UnifiedValue::OptF64(Some(v)) => UnifiedValue::Float(*v),
            UnifiedValue::OptF64(None) => UnifiedValue::Null,
            UnifiedValue::OptBool(Some(v)) => UnifiedValue::Bool(*v),
            UnifiedValue::OptBool(None) => UnifiedValue::Null,
            UnifiedValue::OptDecimal(Some(v)) => UnifiedValue::Decimal(*v),
            UnifiedValue::OptDecimal(None) => UnifiedValue::Null,
            UnifiedValue::OptDateTime(Some(v)) => UnifiedValue::DateTime(*v),
            UnifiedValue::OptDateTime(None) => UnifiedValue::Null,
            other => other.clone(),
        }
    }
}

impl Default for UnifiedValue {
    fn default() -> Self {
        UnifiedValue::Null
    }
}

// === From 实现 ===

impl From<bool> for UnifiedValue {
    fn from(v: bool) -> Self {
        UnifiedValue::Bool(v)
    }
}

impl From<i64> for UnifiedValue {
    fn from(v: i64) -> Self {
        UnifiedValue::Int(v)
    }
}

impl From<i32> for UnifiedValue {
    fn from(v: i32) -> Self {
        UnifiedValue::Int(v as i64)
    }
}

impl From<f64> for UnifiedValue {
    fn from(v: f64) -> Self {
        UnifiedValue::Float(v)
    }
}

impl From<Decimal> for UnifiedValue {
    fn from(v: Decimal) -> Self {
        UnifiedValue::Decimal(v)
    }
}

impl From<String> for UnifiedValue {
    fn from(v: String) -> Self {
        UnifiedValue::String(v)
    }
}

impl From<&str> for UnifiedValue {
    fn from(v: &str) -> Self {
        UnifiedValue::String(v.to_string())
    }
}

impl From<Vec<u8>> for UnifiedValue {
    fn from(v: Vec<u8>) -> Self {
        UnifiedValue::Bytes(v)
    }
}

impl From<NaiveDate> for UnifiedValue {
    fn from(v: NaiveDate) -> Self {
        UnifiedValue::Date(v)
    }
}

impl From<NaiveTime> for UnifiedValue {
    fn from(v: NaiveTime) -> Self {
        UnifiedValue::Time(v)
    }
}

impl From<NaiveDateTime> for UnifiedValue {
    fn from(v: NaiveDateTime) -> Self {
        UnifiedValue::DateTime(v)
    }
}

impl From<serde_json::Value> for UnifiedValue {
    fn from(v: serde_json::Value) -> Self {
        UnifiedValue::Json(v)
    }
}

impl From<Option<i64>> for UnifiedValue {
    fn from(v: Option<i64>) -> Self {
        UnifiedValue::OptI64(v)
    }
}

impl From<Option<f64>> for UnifiedValue {
    fn from(v: Option<f64>) -> Self {
        UnifiedValue::OptF64(v)
    }
}

impl From<Option<bool>> for UnifiedValue {
    fn from(v: Option<bool>) -> Self {
        UnifiedValue::OptBool(v)
    }
}

impl From<Option<Decimal>> for UnifiedValue {
    fn from(v: Option<Decimal>) -> Self {
        UnifiedValue::OptDecimal(v)
    }
}

impl From<Option<NaiveDateTime>> for UnifiedValue {
    fn from(v: Option<NaiveDateTime>) -> Self {
        UnifiedValue::OptDateTime(v)
    }
}

// === 向后兼容函数 ===
// 旧 TypedVal 使用 I64/F64/Text，新代码应使用 Int/Float/String

#[allow(non_snake_case)]
impl UnifiedValue {
    /// 向后兼容：创建 I64 类型值
    #[deprecated(since = "0.2.0", note = "请使用 UnifiedValue::Int")]
    pub fn I64(v: i64) -> Self {
        UnifiedValue::Int(v)
    }

    /// 向后兼容：创建 F64 类型值
    #[deprecated(since = "0.2.0", note = "请使用 UnifiedValue::Float")]
    pub fn F64(v: f64) -> Self {
        UnifiedValue::Float(v)
    }

    /// 向后兼容：创建 Text 类型值
    #[deprecated(since = "0.2.0", note = "请使用 UnifiedValue::String")]
    pub fn Text(v: String) -> Self {
        UnifiedValue::String(v)
    }

    /// 向后兼容：创建 OptNaiveTs 类型值
    #[deprecated(since = "0.2.0", note = "请使用 UnifiedValue::OptDateTime")]
    pub fn OptNaiveTs(v: Option<NaiveDateTime>) -> Self {
        UnifiedValue::OptDateTime(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_kind() {
        let parse = |value: &str| value.parse::<TypeKind>().unwrap_or_else(|err| match err {});

        assert_eq!(parse("int"), TypeKind::Int);
        assert_eq!(parse("float"), TypeKind::Float);
        assert_eq!(parse("timestamp"), TypeKind::Timestamp);
        assert_eq!(parse("unknown"), TypeKind::Text);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(UnifiedValue::Null.type_name(), "null");
        assert_eq!(UnifiedValue::Bool(true).type_name(), "bool");
        assert_eq!(UnifiedValue::Int(42).type_name(), "int");
        assert_eq!(UnifiedValue::OptI64(None).type_name(), "null");
        assert_eq!(UnifiedValue::OptI64(Some(42)).type_name(), "option<int>");
    }

    #[test]
    fn test_is_null() {
        assert!(UnifiedValue::Null.is_null());
        assert!(UnifiedValue::OptI64(None).is_null());
        assert!(!UnifiedValue::OptI64(Some(42)).is_null());
        assert!(!UnifiedValue::Int(42).is_null());
    }

    #[test]
    fn test_as_methods() {
        let val = UnifiedValue::Int(42);
        assert_eq!(val.as_int(), Some(42));
        assert_eq!(val.as_str(), None);

        let val = UnifiedValue::OptI64(Some(42));
        assert_eq!(val.as_int(), Some(42));

        let val = UnifiedValue::OptI64(None);
        assert_eq!(val.as_int(), None);
        assert!(val.is_null());
    }

    #[test]
    fn test_to_canonical() {
        let val = UnifiedValue::OptI64(Some(42));
        assert_eq!(val.to_canonical(), UnifiedValue::Int(42));

        let val = UnifiedValue::OptI64(None);
        assert_eq!(val.to_canonical(), UnifiedValue::Null);

        let val = UnifiedValue::Int(42);
        assert_eq!(val.to_canonical(), UnifiedValue::Int(42));
    }

    #[test]
    fn test_from_impls() {
        let val: UnifiedValue = 42i64.into();
        assert_eq!(val.as_int(), Some(42));

        let val: UnifiedValue = "hello".into();
        assert_eq!(val.as_str(), Some("hello"));

        let val: UnifiedValue = true.into();
        assert_eq!(val.as_bool(), Some(true));

        let val: UnifiedValue = Some(42i64).into();
        assert!(matches!(val, UnifiedValue::OptI64(Some(42))));

        let val: UnifiedValue = Option::<i64>::None.into();
        assert!(matches!(val, UnifiedValue::OptI64(None)));
    }
}
