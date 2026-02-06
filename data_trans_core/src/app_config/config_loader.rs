use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use config::{Config, File};
use serde_json::Value;
use crate::app_config::manager::ConfigManager;
use crate::app_config::schema::ConfigSchema;
use crate::app_config::value::ConfigValue;

pub fn load_user_config(path: &Path) -> Result<HashMap<String, ConfigValue>> {
    let settings = Config::builder()
        .add_source(File::from(path).required(false))
        .build()?;

    let root: HashMap<String, ConfigValue> = settings.try_deserialize()?;
    
    let mut result = HashMap::new();
    flatten("", &ConfigValue::Object(root), &mut result);
    Ok(result)
}

pub fn flatten(prefix: &str, value: &ConfigValue, out: &mut HashMap<String, ConfigValue>) {
    match value {
        ConfigValue::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                flatten(&key, v, out);
            }
        }
        _ => {
            out.insert(prefix.to_string(), value.clone());
        }
    }
}
pub fn apply_json_defaults(
    mgr: &mut ConfigManager,
    prefix: &str,
    value: &Value,
) {
    match value {
        // 1️⃣ 对象：继续递归
        Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.to_string()
                } else {
                    format!("{}.{}", prefix, k)
                };

                apply_json_defaults(mgr, &key, v);
            }
        }

        // 2️⃣ 数组：作为一个整体注册（不要拆 index）
        Value::Array(arr) => {
            let default = ConfigValue::Array(
                arr.iter().map(ConfigValue::from).collect()
            );

            mgr.register(
                prefix,
                ConfigSchema {
                    default,
                    description: "",
                },
            );
        }

        // 3️⃣ 基本类型：注册 schema
        _ => {
            mgr.register(
                prefix,
                ConfigSchema {
                    default: ConfigValue::from(value),
                    description: "",
                },
            );
        }
    }
}

pub fn unflatten(flat: &HashMap<String, ConfigValue>) -> serde_json::Value {
    let mut root = serde_json::Map::new();

    for (key, value) in flat {
        let parts: Vec<&str> = key.split('.').collect();
        let mut current = &mut root;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                current.insert(part.to_string(), serde_json::to_value(value).unwrap());
            } else {
                current = current
                    .entry(part.to_string())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                    .as_object_mut()
                    .unwrap();
            }
        }
    }

    serde_json::Value::Object(root)
}

