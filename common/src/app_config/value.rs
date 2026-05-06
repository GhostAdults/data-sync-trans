use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<ConfigValue>),
    Object(HashMap<String, ConfigValue>),
}

impl ConfigValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ConfigValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConfigValue::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_value(&self) -> serde_json::Value {
        match self {
            ConfigValue::Bool(v) => serde_json::Value::Bool(*v),
            ConfigValue::Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            ConfigValue::Float(v) => serde_json::Value::Number(
                serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)),
            ),
            ConfigValue::String(v) => serde_json::Value::String(v.clone()),
            ConfigValue::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.as_value()).collect())
            }
            ConfigValue::Object(map) => {
                let obj: serde_json::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.as_value())).collect();
                serde_json::Value::Object(obj)
            }
        }
    }
}
impl From<serde_json::Value> for ConfigValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Bool(v) => ConfigValue::Bool(v),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ConfigValue::Int(i)
                } else {
                    ConfigValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(v) => ConfigValue::String(v),
            serde_json::Value::Array(arr) => {
                ConfigValue::Array(arr.into_iter().map(ConfigValue::from).collect())
            }
            serde_json::Value::Object(map) => ConfigValue::Object(
                map.into_iter()
                    .map(|(k, v)| (k, ConfigValue::from(v)))
                    .collect(),
            ),
            serde_json::Value::Null => {
                // 通常 ConfigValue 不包含 Null 变体，
                // 这里如果遇到 Null，映射为空字符串
                ConfigValue::String(String::new())
            }
        }
    }
}

impl From<&serde_json::Value> for ConfigValue {
    fn from(value: &serde_json::Value) -> Self {
        ConfigValue::from(value.clone())
    }
}
