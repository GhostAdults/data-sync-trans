use relus_common::job_config::{JobConfig, MappingConfig};
use relus_core::{core::serve::start_job, init_system_config};
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

    // TODO: 替换为实际的 JobConfig 配置文件加载
    let cfg: JobConfig = serde_json::from_str("{}").expect("请提供有效的 JobConfig 配置");
    println!("调用 start_job");
    let mut cfg = cfg;
    cfg.column_mapping = mapping_config.column_mapping;
    cfg.column_types = Some(mapping_config.column_types);
    if let Some(mode) = mapping_config.mode {
        cfg.target.writer_mode = Some(mode);
    }
    if let Some(key_columns) = mapping_config.key_columns {
        if let Some(obj) = cfg.target.config.as_object_mut() {
            obj.insert("key_columns".to_string(), serde_json::json!(key_columns));
        }
    }

    println!("\n响应结果:");
    match start_job(cfg).await {
        Ok(result) => println!("{}", serde_json::to_string_pretty(&result).unwrap()),
        Err(error) => println!("数据同步错误: {}", error),
    }
}
