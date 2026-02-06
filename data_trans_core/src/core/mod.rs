pub mod server;
pub mod axum_api;
pub mod cli;
pub mod db;
pub mod client_tool;
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


#[derive(Clone, Debug, Deserialize,Serialize)]
pub struct Config {
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
        let get_bool = |k: &str| map.get(k).and_then(|v| v.as_bool());
        let _ = get_bool; // 防止未使用警告

        let db_type = get_str("db_type");
        
        let db_val = map.get("db").and_then(|v| if let ConfigValue::Object(m) = v { Some(m) } else { None });
        let db_config = if let Some(db_map) = db_val {
            DbConfig {
                url: db_map.get("url").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                table: db_map.get("table").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                key_columns: None, 
                max_connections: db_map.get("max_connections").and_then(|v| v.as_i64()).map(|i| i as u32),
                acquire_timeout_secs: db_map.get("acquire_timeout_secs").and_then(|v| v.as_i64()).map(|i| i as u64),
                use_transaction: db_map.get("use_transaction").and_then(|v| v.as_bool()),
            }
        } else {
            DbConfig::default()
        };

        let api_val = map.get("api").and_then(|v| if let ConfigValue::Object(m) = v { Some(m) } else { None });
        let api_config = if let Some(api_map) = api_val {
            let headers = api_map.get("headers").and_then(|v| {
                if let ConfigValue::Object(h) = v {
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

            ApiConfig {
                url: api_map.get("url").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                method: api_map.get("method").and_then(|v| v.as_str()).map(|s| s.to_string()),
                headers,
                body: None,
                items_json_path: api_map.get("items_json_path").and_then(|v| v.as_str()).map(|s| s.to_string()),
                timeout_secs: api_map.get("timeout_secs").and_then(|v| v.as_i64()).map(|i| i as u64),
            }
        } else {
            // 提供一个默认的空 API 配置，避免 panic
            ApiConfig {
                url: "".to_string(),
                method: None,
                headers: None,
                body: None,
                items_json_path: None,
                timeout_secs: None,
            }
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
            db_type,
            db: db_config,
            api: api_config,
            column_mapping,
            column_types,
            mode,
            batch_size,
        })
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
        } else if let Some(url) = crate::get_system_config(self.task_id.as_deref()).and_then(|c| if c.db.url.is_empty() { None } else { Some(c.db.url) }) {
            Ok(url)
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
        // Fallback to system config
        if let Some(cfg) = crate::get_system_config(self.task_id.as_deref()) {
            return cfg.db_type;
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

pub enum TypedVal {
    I64(i64),
    F64(f64),
    Bool(bool),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptNaiveTs(Option<NaiveDateTime>),
    Text(OptionalString),
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
