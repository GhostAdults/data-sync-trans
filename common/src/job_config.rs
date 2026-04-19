use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

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
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    Full,
    Cdc,
    Hybrid,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Full
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
