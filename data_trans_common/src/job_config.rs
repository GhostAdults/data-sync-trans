use crate::app_config::manager::ConfigManager;
use crate::app_config::value::ConfigValue;
use anyhow::{bail, Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobConfig {
    pub id: String,
    pub input: DataSourceConfig,
    pub output: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
    /// Reader 线程数
    #[serde(default = "default_reader_threads")]
    pub reader_threads: usize,
    /// Writer 线程数
    #[serde(default = "default_writer_threads")]
    pub writer_threads: usize,
    /// Channel 缓冲区大小
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    /// 是否使用事务
    #[serde(default = "default_use_transaction")]
    pub use_transaction: bool,
}

fn default_reader_threads() -> usize {
    4
}

fn default_writer_threads() -> usize {
    4
}

fn default_channel_buffer_size() -> usize {
    1000
}

fn default_use_transaction() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateConfigReq {
    pub task_id: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UpdateConfigReq {
    pub task_id: String,
    pub updates: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataSourceConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default = "default_is_table_mode")]
    pub is_table_mode: bool,
    /// 是否是表模式，如果是表模式
    pub query_sql: Option<Vec<String>>,
    pub config: Value,
}

fn default_is_table_mode() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
pub struct MappingConfig {
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: BTreeMap<String, String>,
    pub key_columns: Option<Vec<String>>,
    pub mode: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiConfig {
    pub url: String,
    pub method: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub body: Option<Value>,
    pub items_json_path: Option<String>,
    pub timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DbConfig {
    pub url: String,
    pub table: String,
    pub key_columns: Option<Vec<String>>,
    pub max_connections: Option<u32>,
    pub acquire_timeout_secs: Option<u64>,
    pub use_transaction: Option<bool>,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: "".to_string(),
            table: "".to_string(),
            key_columns: Some(Vec::<String>::new()),
            max_connections: Some(10),
            acquire_timeout_secs: Some(30),
            use_transaction: Some(false),
        }
    }
}

// 针对config 配置的函数
impl JobConfig {
    pub fn from_manager(mgr: &ConfigManager, task_id: &str) -> anyhow::Result<Self> {
        let tasks = mgr
            .get("tasks")
            .and_then(|v| {
                if let ConfigValue::Array(arr) = v {
                    Some(arr)
                } else {
                    None
                }
            })
            .context("未找到任务配置列表")?;

        let task_val = tasks
            .iter()
            .find(|v| {
                if let ConfigValue::Object(map) = v {
                    map.get("id").and_then(|id| id.as_str()) == Some(task_id)
                } else {
                    false
                }
            })
            .context(format!("未找到 ID 为 {} 的任务配置", task_id))?;

        let map = if let ConfigValue::Object(m) = task_val {
            m
        } else {
            bail!("配置格式错误")
        };

        let get_str = |k: &str| map.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
        let get_i64 = |k: &str| map.get(k).and_then(|v| v.as_i64());

        let input: &HashMap<String, ConfigValue> = map
            .get("input")
            .and_then(|v| match v {
                ConfigValue::Object(obj) => Some(obj),
                _ => None,
            })
            .context("未找到 input 配置")?;

        let output: &HashMap<String, ConfigValue> = map
            .get("output")
            .and_then(|v| match v {
                ConfigValue::Object(obj) => Some(obj),
                _ => None,
            })
            .context("未找到 output 配置")?;

        let input_config = DataSourceConfig {
            name: input
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("input")
                .to_string(),
            source_type: input
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("api")
                .to_string(),
            is_table_mode: input
                .get("is_table_mode")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            config: input
                .get("config")
                .map(|v| v.as_value())
                .unwrap_or(serde_json::Value::Null),
            query_sql: input.get("query_sql").and_then(|v| {
                if let ConfigValue::Array(arr) = v {
                    Some(
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect(),
                    )
                } else {
                    None
                }
            }),
        };

        let output_config = DataSourceConfig {
            name: output
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("output")
                .to_string(),
            source_type: output
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("database")
                .to_string(),
            is_table_mode: output
                .get("is_table_mode")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            config: output
                .get("config")
                .map(|v| v.as_value())
                .unwrap_or(serde_json::Value::Null),
            query_sql: output.get("query_sql").and_then(|v| {
                if let ConfigValue::Array(arr) = v {
                    Some(
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect(),
                    )
                } else {
                    None
                }
            }),
        };

        let batch_size = get_i64("batch_size").map(|i| i as usize);
        let mode = get_str("mode");

        let mut column_mapping = BTreeMap::new();
        if let Some(ConfigValue::Object(m)) = map.get("column_mapping") {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    column_mapping.insert(k.clone(), s.to_string());
                }
            }
        }

        let mut column_types = BTreeMap::new();
        if let Some(ConfigValue::Object(m)) = map.get("column_types") {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    column_types.insert(k.clone(), s.to_string());
                }
            }
        }
        let column_types = if column_types.is_empty() {
            None
        } else {
            Some(column_types)
        };

        Ok(Self {
            id: task_id.to_string(),
            input: input_config,
            output: output_config,
            column_mapping,
            column_types,
            mode,
            batch_size,
            reader_threads: get_i64("reader_threads").map(|i| i as usize).unwrap_or_else(default_reader_threads),
            writer_threads: get_i64("writer_threads").map(|i| i as usize).unwrap_or_else(default_writer_threads),
            channel_buffer_size: get_i64("channel_buffer_size").map(|i| i as usize).unwrap_or_else(default_channel_buffer_size),
            use_transaction: map.get("use_transaction").and_then(|v| v.as_bool()).unwrap_or_else(default_use_transaction),
        })
    }

    pub fn parse_api_config(source: &DataSourceConfig) -> Result<ApiConfig> {
        let cfg: &Value = &source.config;
        let url = cfg
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let method = cfg
            .get("method")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let items_json_path = cfg
            .get("items_json_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let timeout_secs = cfg.get("timeout_secs").and_then(|v| v.as_u64());
        let headers = cfg.get("headers").and_then(|v| {
            if let serde_json::Value::Object(h) = v {
                let mut hm = BTreeMap::new();
                for (k, v) in h {
                    if let Some(s) = v.as_str() {
                        hm.insert(k.clone(), s.to_string());
                    }
                }
                Some(hm)
            } else {
                None
            }
        });

        Ok(ApiConfig {
            url,
            method,
            headers,
            body: None,
            items_json_path,
            timeout_secs,
        })
    }

    // 解析输入类型为 database 的数据源配置
    pub fn parse_database_config(source: &DataSourceConfig) -> Result<DbConfig> {
        let cfg = &source.config;
        let url = cfg
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table = cfg
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let key_columns = cfg.get("key_columns").and_then(|v| {
            if let serde_json::Value::Array(arr) = v {
                Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                )
            } else {
                None
            }
        });

        let max_connections = cfg
            .get("max_connections")
            .and_then(|v| v.as_u64())
            .map(|i| i as u32);
        let acquire_timeout_secs = cfg.get("acquire_timeout_secs").and_then(|v| v.as_u64());
        let use_transaction = cfg.get("use_transaction").and_then(|v| v.as_bool());

        Ok(DbConfig {
            url,
            table,
            key_columns,
            max_connections,
            acquire_timeout_secs,
            use_transaction,
        })
    }

    pub fn get_source_db_type(source: &DataSourceConfig) -> Option<String> {
        source
            .config
            .get("db_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    pub fn default_with_id(id: String) -> Self {
        Self {
            id,
            input: DataSourceConfig {
                name: "api_source".to_string(),
                source_type: "api".to_string(),
                is_table_mode: true,
                query_sql: None,
                config: serde_json::json!({
                    "url": "",
                    "method": "GET",
                    "headers": {},
                    "items_json_path": null,
                    "timeout_secs": 30
                }),
            },
            output: DataSourceConfig {
                name: "db_target".to_string(),
                source_type: "database".to_string(),
                is_table_mode: true,
                query_sql: None,
                config: serde_json::json!({
                    "db_type": "postgres",
                    "url": "",
                    "table": "",
                    "key_columns": [],
                    "max_connections": 10,
                    "acquire_timeout_secs": 30,
                    "use_transaction": true
                }),
            },
            column_mapping: BTreeMap::new(),
            column_types: None,
            mode: Some("insert".to_string()),
            batch_size: Some(100),
            reader_threads: default_reader_threads(),
            writer_threads: default_writer_threads(),
            channel_buffer_size: default_channel_buffer_size(),
            use_transaction: default_use_transaction(),
        }
    }
}
