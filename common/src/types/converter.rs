//! 类型转换器 trait 和注册表
//!
//! 支持 UnifiedValue 的类型转换

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

use super::{TypeKind, UnifiedValue};

/// 类型转换器 trait - 策略模式
pub trait TypeConverter: Send + Sync {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue>;
    fn name(&self) -> &'static str;
}

// === 内置转换器实现 ===

/// 整数转换器
pub struct IntConverter;

impl TypeConverter for IntConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        if v.is_null() {
            return Ok(UnifiedValue::OptI64(None));
        }
        if let Some(n) = v.as_i64() {
            return Ok(UnifiedValue::Int(n));
        }
        if let Some(s) = v.as_str() {
            return s
                .parse::<i64>()
                .map(UnifiedValue::Int)
                .map_err(|_| anyhow::anyhow!("无法转换为 int: {}", s));
        }
        anyhow::bail!("无法转换为 int: {}", v)
    }

    fn name(&self) -> &'static str {
        "int"
    }
}

/// 浮点数转换器
pub struct FloatConverter;

impl TypeConverter for FloatConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        if v.is_null() {
            return Ok(UnifiedValue::OptF64(None));
        }
        if let Some(n) = v.as_f64() {
            return Ok(UnifiedValue::Float(n));
        }
        if let Some(s) = v.as_str() {
            return s
                .parse::<f64>()
                .map(UnifiedValue::Float)
                .map_err(|_| anyhow::anyhow!("无法转换为 float: {}", s));
        }
        anyhow::bail!("无法转换为 float: {}", v)
    }

    fn name(&self) -> &'static str {
        "float"
    }
}

/// 布尔转换器
pub struct BoolConverter;

impl TypeConverter for BoolConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        if v.is_null() {
            return Ok(UnifiedValue::OptBool(None));
        }
        if let Some(b) = v.as_bool() {
            return Ok(UnifiedValue::Bool(b));
        }
        if let Some(s) = v.as_str() {
            match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => return Ok(UnifiedValue::Bool(true)),
                "false" | "0" | "no" | "off" => return Ok(UnifiedValue::Bool(false)),
                _ => {}
            }
        }
        anyhow::bail!("无法转换为 bool: {}", v)
    }

    fn name(&self) -> &'static str {
        "bool"
    }
}

/// 高精度小数转换器
pub struct DecimalConverter;

impl TypeConverter for DecimalConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        use rust_decimal::Decimal;

        if v.is_null() {
            return Ok(UnifiedValue::OptDecimal(None));
        }
        if let Some(n) = v.as_f64() {
            return Ok(UnifiedValue::Decimal(
                Decimal::from_f64_retain(n).unwrap_or_default(),
            ));
        }
        if let Some(s) = v.as_str() {
            return s
                .parse::<Decimal>()
                .map(UnifiedValue::Decimal)
                .map_err(|_| anyhow::anyhow!("无法转换为 decimal: {}", s));
        }
        anyhow::bail!("无法转换为 decimal: {}", v)
    }

    fn name(&self) -> &'static str {
        "decimal"
    }
}

/// 时间戳转换器
pub struct TimestampConverter;

impl TypeConverter for TimestampConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        use chrono::NaiveDateTime;

        if v.is_null() {
            return Ok(UnifiedValue::OptDateTime(None));
        }
        if let Some(s) = v.as_str() {
            let formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%.fZ",
            ];

            for fmt in &formats {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                    return Ok(UnifiedValue::DateTime(dt));
                }
            }

            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Ok(UnifiedValue::DateTime(dt.naive_utc()));
            }

            anyhow::bail!("无法解析时间戳: {}", s)
        }
        anyhow::bail!("无法转换为 timestamp: {}", v)
    }

    fn name(&self) -> &'static str {
        "timestamp"
    }
}

/// JSON 转换器
pub struct JsonConverter;

impl TypeConverter for JsonConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        Ok(UnifiedValue::Json(v.clone()))
    }

    fn name(&self) -> &'static str {
        "json"
    }
}

/// 文本转换器（默认）
pub struct TextConverter;

impl TypeConverter for TextConverter {
    fn convert(&self, v: &JsonValue) -> Result<UnifiedValue> {
        if v.is_null() {
            return Ok(UnifiedValue::String(String::new()));
        }
        match v {
            JsonValue::String(s) => Ok(UnifiedValue::String(s.clone())),
            JsonValue::Number(n) => Ok(UnifiedValue::String(n.to_string())),
            JsonValue::Bool(b) => Ok(UnifiedValue::String(b.to_string())),
            _ => Ok(UnifiedValue::String(v.to_string())),
        }
    }

    fn name(&self) -> &'static str {
        "text"
    }
}

/// 转换器注册表
pub struct TypeConverterRegistry {
    converters: HashMap<TypeKind, Arc<dyn TypeConverter>>,
    default: Arc<dyn TypeConverter>,
}

impl Default for TypeConverterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeConverterRegistry {
    pub fn new() -> Self {
        let mut converters: HashMap<TypeKind, Arc<dyn TypeConverter>> = HashMap::new();

        converters.insert(TypeKind::Int, Arc::new(IntConverter));
        converters.insert(TypeKind::Float, Arc::new(FloatConverter));
        converters.insert(TypeKind::Bool, Arc::new(BoolConverter));
        converters.insert(TypeKind::Decimal, Arc::new(DecimalConverter));
        converters.insert(TypeKind::Timestamp, Arc::new(TimestampConverter));
        converters.insert(TypeKind::Json, Arc::new(JsonConverter));

        Self {
            converters,
            default: Arc::new(TextConverter),
        }
    }

    pub fn convert(&self, v: &JsonValue, type_hint: Option<&str>) -> Result<UnifiedValue> {
        let kind = type_hint
            .map(|hint| hint.parse::<TypeKind>().unwrap_or_else(|err| match err {}))
            .unwrap_or(TypeKind::Text);

        let converter = self.converters.get(&kind).unwrap_or(&self.default);

        converter.convert(v)
    }

    pub fn register(&mut self, kind: TypeKind, converter: Arc<dyn TypeConverter>) {
        self.converters.insert(kind, converter);
    }

    pub fn get(&self, kind: &TypeKind) -> Option<&Arc<dyn TypeConverter>> {
        self.converters.get(kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_int_converter() {
        let converter = IntConverter;
        let result = converter.convert(&json!(42)).unwrap();
        assert_eq!(result.as_int(), Some(42));

        let result = converter.convert(&json!(null)).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_float_converter() {
        let converter = FloatConverter;
        let result = converter.convert(&json!(3.14)).unwrap();
        assert_eq!(result.as_float(), Some(3.14));
    }

    #[test]
    fn test_bool_converter() {
        let converter = BoolConverter;
        let result = converter.convert(&json!(true)).unwrap();
        assert_eq!(result.as_bool(), Some(true));

        let result = converter.convert(&json!("yes")).unwrap();
        assert_eq!(result.as_bool(), Some(true));
    }

    #[test]
    fn test_text_converter() {
        let converter = TextConverter;
        let result = converter.convert(&json!("hello")).unwrap();
        assert_eq!(result.as_str(), Some("hello"));

        let result = converter.convert(&json!(42)).unwrap();
        assert_eq!(result.as_str(), Some("42"));
    }

    #[test]
    fn test_registry() {
        let registry = TypeConverterRegistry::new();

        let result = registry.convert(&json!(42), Some("int")).unwrap();
        assert_eq!(result.as_int(), Some(42));

        let result = registry.convert(&json!("hello"), None).unwrap();
        assert_eq!(result.as_str(), Some("hello"));
    }
}
