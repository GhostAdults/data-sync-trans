use data_trans_core::init_system_config;
use data_trans_core::core::server::{create_config, sync_command};
use data_trans_core::core::{CreateConfigReq, MappingConfig, ApiResp};
use axum::Json;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() {
    println!("--- Starting Sync Command Test ---");

    // 1. Init
    init_system_config();

    // 2. Create Task Config
    let task_id = "test_sync_task";
    let create_req = CreateConfigReq {
        task_id: task_id.to_string(),
        config: json!({
            "mode": "insert",
            "batch_size": 10,
            "db": {
                "url": "mysql://root:123456@localhost:3306/my_db", // Mock URL, likely to fail connection but logic runs
                "table": "test_table"
            },
            "api": {
                "url": "http://example.com/api",
                "method": "GET"
            }
        }),
    };
     let (status, resp) = create_config(Json(create_req)).await;
    println!("Create Config Status: {:?}", status);

    // 3. Call sync_command with just task_id and mapping
    let mapping = MappingConfig {
        column_mapping: BTreeMap::new(),
        column_types: BTreeMap::new(),
        key_columns: None,
        mode: Some("overwrite".to_string()),
    };

    println!("Calling sync_command...");
    let (status, Json(resp)) = sync_command(task_id.to_string(), mapping).await;
    
    println!("Status: {:?}", status);
    // Expect error because DB connection fails, but verify it reached that point
    if let Some(err) = resp.error {
        println!("Error (expected): {}", err);
        if err.contains("无法识别数据库类型") || err.contains("connection refused") || err.contains("Access denied") {
            println!("SUCCESS: Logic reached DB connection phase.");
        }
    } else {
        println!("Unexpected success (did you have a real DB?)");
    }
}
