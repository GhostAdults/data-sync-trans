pub mod serve;
pub mod axum_api;
pub mod cli;
pub mod client_tool;
pub mod pipeline;
use axum::{Router, routing::{get, post}};
use axum::{extract::Query, Json};
use serde::Deserialize;
use std::collections::BTreeMap;
use serde_json::Value;
use serde::Serialize;
use chrono::NaiveDateTime;
use tokio::runtime::Runtime;
use tokio::net::TcpListener;
use axum_api::*;
use crate::app_config::manager::ConfigManager;
use crate::app_config::value::ConfigValue;
use anyhow::{Result, bail, Context as _};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataSourceConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub config: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub id: String,
    pub input: DataSourceConfig,
    pub output: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LegacyConfig {
    pub id: String,
    pub db_type: Option<String>,
    pub db: DbConfig,
    pub api: ApiConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}

impl Config {
    pub fn from_manager(mgr: &ConfigManager, task_id: &str) -> anyhow::Result<Self> {
        let tasks = mgr.get("tasks").and_then(|v| {
            if let ConfigValue::Array(arr) = v {
                Some(arr)
            } else {
                None
            }
        }).context("未找到任务配置列表")?;

        let task_val = tasks.iter().find(|v| {
            if let ConfigValue::Object(map) = v {
                map.get("id").and_then(|id| id.as_str()) == Some(task_id)
            } else {
                false
            }
        }).context(format!("未找到 ID 为 {} 的任务配置", task_id))?;

        let map = if let ConfigValue::Object(m) = task_val { m } else { bail!("配置格式错误") };

        let get_str = |k: &str| map.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
        let get_i64 = |k: &str| map.get(k).and_then(|v| v.as_i64());

        let input: &std::collections::HashMap<String, ConfigValue> = map.get("input")
            .and_then(|v| match v {
                ConfigValue::Object(obj) => Some(obj),
                _ => None,
            })
            .context("未找到 input 配置")?;

        let output: &std::collections::HashMap<String, ConfigValue> = map.get("output")
            .and_then(|v| match v {
                ConfigValue::Object(obj) => Some(obj),
                _ => None,
            })
            .context("未找到 output 配置")?;

        let input_config = DataSourceConfig {
            name: input.get("name").and_then(|v| v.as_str()).unwrap_or("input").to_string(),
            source_type: input.get("type").and_then(|v| v.as_str()).unwrap_or("api").to_string(),
            config: input.get("config").map(|v| v.as_value()).unwrap_or(serde_json::Value::Null),
        };

        let output_config = DataSourceConfig {
            name: output.get("name").and_then(|v| v.as_str()).unwrap_or("output").to_string(),
            source_type: output.get("type").and_then(|v| v.as_str()).unwrap_or("database").to_string(),
            config: output.get("config").map(|v| v.as_value()).unwrap_or(serde_json::Value::Null),
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
        let column_types = if column_types.is_empty() { None } else { Some(column_types) };

        Ok(Self {
            id: task_id.to_string(),
            input: input_config,
            output: output_config,
            column_mapping,
            column_types,
            mode,
            batch_size,
        })
    }

    pub fn parse_api_config(source: &DataSourceConfig) -> Result<ApiConfig> {
        let cfg: &Value = &source.config;
        let url = cfg.get("url").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let method = cfg.get("method").and_then(|v| v.as_str()).map(|s| s.to_string());
        let items_json_path = cfg.get("items_json_path").and_then(|v| v.as_str()).map(|s| s.to_string());
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

    pub fn parse_db_config(source: &DataSourceConfig) -> Result<DbConfig> {
        let cfg = &source.config;
        let url = cfg.get("url").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let table = cfg.get("table").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        let key_columns = cfg.get("key_columns").and_then(|v| {
            if let serde_json::Value::Array(arr) = v {
                Some(arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            } else {
                None
            }
        });

        let max_connections = cfg.get("max_connections").and_then(|v| v.as_u64()).map(|i| i as u32);
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
        source.config.get("db_type").and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    pub fn default_with_id(id: String) -> Self {
        Self {
            id,
            input: DataSourceConfig {
                name: "api_source".to_string(),
                source_type: "api".to_string(),
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
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UpdateConfigReq {
    pub task_id: String,
    pub updates: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateConfigReq {
    pub task_id: String,
    pub config: serde_json::Value,
}

#[derive(Serialize,Debug)]
pub struct ApiResp<T> {
    pub ok: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}
/// 通用的数据库查询参数
#[derive(Deserialize)]
pub struct BaseDbQuery {
    pub db_url: Option<String>,
    pub db_type: Option<String>,
    pub task_id: Option<String>,
}

impl BaseDbQuery {
    pub fn resolve_url(&self) -> Result<String> {
        if let Some(url) = self.db_url.as_ref().filter(|s| !s.is_empty()) {
            Ok(url.clone())
        } else if let Some(cfg) = crate::get_system_config(self.task_id.as_deref()) {
            let db_config = Config::parse_db_config(&cfg.output)?;
            if !db_config.url.is_empty() {
                Ok(db_config.url)
            } else {
                bail!("missing db.url in query or config")
            }
        } else {
            bail!("missing db.url in query or config")
        }
    }

    pub fn resolve_type(&self) -> Option<String> {
        if let Some(t) = &self.db_type {
            if !t.is_empty() {
                return Some(t.clone());
            }
        }
        if let Some(cfg) = crate::get_system_config(self.task_id.as_deref()) {
            return Config::get_source_db_type(&cfg.output);
        }
        None
    }
}
#[derive(Deserialize)]
pub struct TablesQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
}
#[derive(Deserialize)]
pub struct DescribeQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
    pub table: String,
}
#[derive(Deserialize)]
pub struct GenMapQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
    pub table: String,
}
#[derive(Clone)]
pub struct ColInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

pub type OptionalString = Option<String>;

#[derive(Debug, Clone)]
pub enum TypedVal {
    I64(i64),
    F64(f64),
    Bool(bool),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptNaiveTs(Option<NaiveDateTime>),
    Text(String),
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

#[derive(Deserialize)]
pub struct SyncReq {
    pub task_id: String,
    pub mapping: MappingConfig,
}
pub fn run_serve(host: String, port: u16) -> anyhow::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {serve_http(host, port).await })?;
    Ok(())
}

pub async fn serve_http(host: String, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/tables", get(|q: Query<TablesQuery>| async move { h_list_tables(q).await }))
        .route("/describe", get(|q: Query<DescribeQuery>| async move { h_describe(q).await }))
        .route("/gen-mapping", get(|q: Query<GenMapQuery>| async move { h_gen_mapping(q).await }))
        .route("/sync", post(|body: Json<SyncReq>| async move {h_sync(body).await }));
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
