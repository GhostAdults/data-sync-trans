use data_trans_core::{init_system_config, core::serve::sync_command, };
use data_trans_common::job_config::{JobConfig, MappingConfig};
use std::collections::BTreeMap;

#[tokio::main]
async fn main() {
    init_system_config();

    let mut mapping = BTreeMap::new();
    mapping.insert("id".to_string(), "id".to_string());
    mapping.insert("name".to_string(), "name".to_string());

    let mut types = BTreeMap::new();
    types.insert("id".to_string(), "int".to_string());
    types.insert("name".to_string(), "text".to_string());

    let mapping_config = MappingConfig {
        column_mapping: mapping,
        column_types: types,
        key_columns: Some(vec!["id".to_string()]),
        mode: Some("insert".to_string()),
    };

    let cfg = JobConfig::default_test();
    println!("调用 sync_command");
    let (status, result) = sync_command(cfg, Some(mapping_config)).await;

    println!("\nHTTP Status: {:?}", status);
    println!("\n响应结果:");
    println!("{}", serde_json::to_string_pretty(&result.0).unwrap());
}
