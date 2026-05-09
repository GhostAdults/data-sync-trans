use relus_common::job_config::{CreateConfigReq, UpdateConfigReq};
use relus_core::core::serve::{create_config, update_config};
use relus_core::init_system_config;

use serde_json::json;

#[tokio::main]
async fn main() {
    println!("Starting update_config test...");

    // 1. 初始化配置系统
    init_system_config();
    println!("System config initialized.");

    // 2. 创建系统配置 (Create Config)
    println!("\n--- Step 1: Create System Config ---");
    let create_req = CreateConfigReq {
        task_id: String::new(),
        config: json!({
            "server": {
                "host": "127.0.0.1",
                "port": 30001
            },
            "pipeline": {
                "batch_size": 100,
                "use_transaction": true
            }
        }),
    };

    let (status, resp) = create_config(create_req).await;
    println!("Create Status: {:?}", status);
    println!("Create Response: {:?}", resp);

    if !resp.ok {
        println!("Create failed: {:?}", resp.error);
    }

    // 3. 更新系统配置 (Update Config)
    println!("\n--- Step 2: Update System Config ---");
    let update_req = UpdateConfigReq {
        task_id: String::new(),
        updates: json!({
            "server": {
                "port": 30002
            },
            "pipeline": {
                "batch_size": 500
            }
        }),
    };

    let (status, resp) = update_config(update_req).await;
    println!("Update Status: {:?}", status);
    println!("Update Response: {:?}", resp);

    if resp.ok {
        println!("SUCCESS: Task updated successfully.");
    } else {
        println!("FAILURE: Task update failed: {:?}", resp.error);
    }

    println!("\nTest finished.");
}
