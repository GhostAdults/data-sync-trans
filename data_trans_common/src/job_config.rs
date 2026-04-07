use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobConfig {
    pub input: DataSourceConfig,
    pub output: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
    pub channel_buffer_size: Option<usize>,
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
    /// 支持两种格式:
    /// - 数组: `{ "connections": [{ ... }] }` — 取第一个元素
    /// - 对象: `{ "connection": { ... } }` — 直接使用
    fn extract_connection(source: &DataSourceConfig) -> Result<&Value> {
        let cfg = &source.config;

        if let Some(arr) = cfg.get("connections").and_then(|v| v.as_array()) {
            return arr
                .first()
                .ok_or_else(|| anyhow::anyhow!("config.connections 数组为空"));
        }

        if let Some(obj) = cfg.get("connection") {
            return Ok(obj);
        }

        Err(anyhow::anyhow!(
            "config 中缺少 connections 或 connection 字段"
        ))
    }

    /// 解析 database 数据源配置（取 connections[0]）
    pub fn parse_database_config(source: &DataSourceConfig) -> Result<DbConfig> {
        let conn = Self::extract_connection(source)?;

        let url = conn
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table = conn
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let key_columns = conn.get("key_columns").and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
        });

        let max_connections = conn
            .get("max_connections")
            .and_then(|v| v.as_u64())
            .map(|i| i as u32);
        let acquire_timeout_secs = conn.get("acquire_timeout_secs").and_then(|v| v.as_u64());
        let use_transaction = conn.get("use_transaction").and_then(|v| v.as_bool());

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
        if source.source_type != "database" {
            return None;
        }
        Self::extract_connection(source)
            .ok()
            .and_then(|conn| conn.get("db_type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }
    pub fn default_test() -> Self {
        Self {
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
                    "connections": [{
                        "db_type": "postgres",
                        "url": "",
                        "table": "",
                        "key_columns": [],
                        "max_connections": 10,
                        "acquire_timeout_secs": 30,
                        "use_transaction": true
                    }]
                }),
            },
            column_mapping: BTreeMap::new(),
            column_types: None,
            mode: Some("insert".to_string()),
            batch_size: Some(100),
            channel_buffer_size: None,
        }
    }
}
