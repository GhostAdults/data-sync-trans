use data_trans_core::init_system_config;
use data_trans_core::core::server::{create_config, update_config};
use data_trans_core::core::{CreateConfigReq, UpdateConfigReq};
use axum::Json;
use serde_json::json;

#[tokio::main]
async fn main() {

    println!("Starting update_config test...");

    // 1. 初始化配置系统
    init_system_config();
    println!("System config initialized.");

    let task_id = "example_task_001";

    // 2. 创建一个任务 (Create Config)
    println!("\n--- Step 1: Create Task ---");
    let create_req = CreateConfigReq {
        task_id: task_id.to_string(),
        config: json!({
            "mode": "insert",
            "batch_size": 100,
            "db": {
                "url": "mysql://localhost/test",
                "table": "test_table"
            }
        }),
    };

    let (status, Json(resp)) = create_config(Json(create_req)).await;
    println!("Create Status: {:?}", status);
    println!("Create Response: {:?}", resp);

    if !resp.ok {
        // 如果任务已存在，可能也是一种预期（比如多次运行），但为了测试更新，我们需要它存在
        println!("Note: Create failed (maybe already exists): {:?}", resp.error);
    }

    // 3. 更新该任务 (Update Config)
    println!("\n--- Step 2: Update Task ---");
    let update_req = UpdateConfigReq {
        task_id: task_id.to_string(),
        updates: json!({
            "mode": "upsert", // 修改模式
            "batch_size": 500, // 修改 batch_size
            "new_field": "test_val" // 添加新字段 (取决于是否允许额外字段，这里是 merge_json_value)
        }),
    };

    let (status, Json(resp)) = update_config(Json(update_req)).await;
    println!("Update Status: {:?}", status);
    println!("Update Response: {:?}", resp);

    if resp.ok {
        println!("SUCCESS: Task updated successfully.");
    } else {
        println!("FAILURE: Task update failed: {:?}", resp.error);
    }
    
    // 4. (Optional) 再次创建以验证配置是否真的变了？ 
    // 由于我们不能直接在该 example 中方便地读取内部 ConfigManager 的状态（除非它是 pub 的），
    // 我们主要依赖 update_config 的返回状态。
    // 但我们可以尝试再次调用 update，或者假设它生效了。
    // 在实际的集成测试中，我们会去 assert 状态。
    
    println!("\nTest finished.");
}
