use data_trans_core::{init_system_config, core::{Config, DataSourceConfig, serve::sync}, util::dbutil::get_pool_from_config};
use std::collections::BTreeMap;
use serde_json::json;

#[tokio::main]
async fn main() {
    //当前先定义固定配置，后续可以改为从文件或命令行参数加载
    let input = DataSourceConfig {
        name: "mysql_source".to_string(),
        source_type: "database".to_string(),
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data",
            "key_columns": ["id"]
        }),
    };

    let output = DataSourceConfig {
        name: "pg_target".to_string(),
        source_type: "database".to_string(),
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data",
            "key_columns": ["id"]
        }),
    };
    // 配置映射字段
    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("type".to_string(), "type".to_string());
    column_mapping.insert("site_number".to_string(), "site_number".to_string());

    let mut column_types = BTreeMap::new();
    column_types.insert("id".to_string(), "int".to_string());
    column_types.insert("type".to_string(), "text".to_string());
    column_types.insert("site_number".to_string(), "text".to_string());

    let config = Config {
        id: "test_sync".to_string(),
        input,
        output,
        column_mapping,
        column_types: Some(column_types),
        mode: Some("insert".to_string()),
        batch_size: Some(100),
    };

    match get_pool_from_config(&config).await {
        Ok(pool) => {
            println!("开始同步...");
            match sync(&config, &pool).await {
                Ok(_) => println!("✓ 同步成功"),
                Err(e) => println!("✗ 同步失败: {}", e),
            }
        }
        Err(e) => println!("✗ 连接数据库失败: {}", e),
    }
}
