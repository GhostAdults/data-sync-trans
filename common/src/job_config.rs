use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::{fmt, str::FromStr};

use crate::data_source_config::DataSourceConfig;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobConfig {
    #[serde(alias = "input")]
    pub source: DataSourceConfig,
    #[serde(alias = "output")]
    pub target: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub sync_mode: Option<SyncMode>,
    pub batch_size: Option<usize>,
    pub channel_buffer_size: Option<usize>,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub schedule: Option<ScheduleConfig>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    #[default]
    Fullsnapshot,
    Incremental,
    Mix,
}

/// 调度策略配置（可从 JSON 反序列化）
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ScheduleConfig {
    /// cron 表达式
    Cron(String),
    /// 带类型标签
    Typed {
        r#type: String,
        value: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Copy)]
#[serde(rename_all = "lowercase")]
pub enum WriteMode {
    #[default]
    Insert,
    Upsert,
    Update,
    Delete,
}

impl WriteMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::Insert => "insert",
            WriteMode::Upsert => "upsert",
            WriteMode::Update => "update",
            WriteMode::Delete => "delete",
        }
    }

    pub fn from_config(config: &JobConfig) -> Self {
        config
            .target
            .writer_mode
            .as_deref()
            .and_then(|mode| mode.parse().ok())
            .unwrap_or_default()
    }
}

impl FromStr for WriteMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "insert" => Ok(WriteMode::Insert),
            "upsert" => Ok(WriteMode::Upsert),
            "update" => Ok(WriteMode::Update),
            "delete" => Ok(WriteMode::Delete),
            _ => Err(()),
        }
    }
}

impl fmt::Display for JobConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = serde_json::to_string_pretty(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", json)
    }
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

#[derive(Clone, Debug, Deserialize)]
pub struct MappingConfig {
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: BTreeMap<String, String>,
    pub key_columns: Option<Vec<String>>,
    pub mode: Option<String>,
}

// impl JobConfig {
//     pub fn default_test() -> Self {
//         Self {
//             input: DataSourceConfig {
//                 name: "api_source".to_string(),
//                 source_type: "api".to_string(),
//                 is_table_mode: true,
//                 query_sql: None,
//                 config: serde_json::json!({
//                     "url": "",
//                     "method": "GET",
//                     "headers": {},
//                     "items_json_path": null,
//                     "timeout_secs": 30
//                 }),
//             },
//             output: DataSourceConfig {
//                 name: "db_target".to_string(),
//                 source_type: "database".to_string(),
//                 is_table_mode: true,
//                 query_sql: None,
//                 config: serde_json::json!({
//                     "connections": [{
//                         "db_type": "postgres",
//                         "url": "",
//                         "table": "",
//                         "key_columns": [],
//                         "max_connections": 10,
//                         "acquire_timeout_secs": 30,
//                         "use_transaction": true
//                     }]
//                 }),
//             },
//             column_mapping: BTreeMap::new(),
//             column_types: None,
//             mode: Some("insert".to_string()),
//             batch_size: Some(100),
//             channel_buffer_size: None,
//         }
//     }
// }
