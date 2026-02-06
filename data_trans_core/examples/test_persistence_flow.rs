use data_trans_core::init_system_config;
use data_trans_core::core::server::create_config;
use data_trans_core::core::CreateConfigReq;
use data_trans_core::get_system_config;
use data_trans_core::app_config::manager::ConfigManager;
use axum::Json;
use serde_json::json;

#[tokio::main]
async fn main() {
    println!("--- Starting Persistence Test ---");

    // 1. Initialize system config
    // This loads the existing config or creates defaults
    init_system_config();
    println!("System config initialized.");

    // Verify the path
    // We create a temporary manager just to check the path expected by for_os("app_trans")
    let mgr_path = ConfigManager::for_os("app_trans").unwrap().path;
    println!("Config file path: {}", mgr_path.display());

    // 2. Define a new task (Config)
    let task_id = "test_persistence_task_003";
    let create_req = CreateConfigReq {
        task_id: task_id.to_string(),
        config: json!({
            "mode": "overwrite",
            "batch_size": 789,
            "db": {
                "url": "mysql://localhost/test_db",
                "table": "test_table"
            }
        }),
    };

    println!("Creating config for task_id: {}", task_id);

    // 3. Call create_config to persist it
    let (status, Json(resp)) = create_config(Json(create_req)).await;
    
    println!("Create Response Status: {:?}", status);
    if !resp.ok {
        println!("Create failed: {:?}", resp.error);
        // If it failed because it exists, that's fine for this test, we can try to read it
    } else {
        println!("Config created successfully.");
    }

    // 4. Verify persistence to file
    // We check if the file exists and contains the task_id
    if mgr_path.exists() {
        let content = std::fs::read_to_string(&mgr_path).unwrap();
        if content.contains(task_id) {
            println!("SUCCESS: Config file contains the new task ID.");
        } else {
            println!("FAILURE: Config file does not contain the new task ID.");
        }
    } else {
        println!("FAILURE: Config file does not exist at {}", mgr_path.display());
    }

    // 5. Simulate "next time" read by ID
    // We use get_system_config which retrieves from the loaded manager.
    // In a real "next run", we would call init_system_config() again, but in a single process 
    // we rely on the manager state which should have been updated by create_config.
    
    // To strictly verify it loads from file, we could create a NEW ConfigManager instance manually
    // loading from that path.
    println!("Verifying read from file via new ConfigManager...");
    match ConfigManager::new(&mgr_path) {
        Ok(mgr) => {
             // ConfigManager loads 'user' config from file in new()
             // We need to 'build' it to get merged view if we relied on defaults, 
             // but here we just check if 'tasks' has our task.
             if let Some(tasks_val) = mgr.user.get("tasks") {
                 let tasks_str = serde_json::to_string(tasks_val).unwrap();
                 if tasks_str.contains(task_id) {
                     println!("SUCCESS: Newly created ConfigManager loaded the task from file.");
                 } else {
                     println!("FAILURE: Newly created ConfigManager did not find the task.");
                 }
             } else {
                 println!("FAILURE: No 'tasks' key found in loaded config.");
             }
        },
        Err(e) => println!("Failed to load ConfigManager from file: {}", e),
    }

    // Also check the global system config
    match get_system_config(Some(task_id)) {
        Some(cfg) => {
            println!("SUCCESS: get_system_config retrieved the task configuration.");
            println!("Batch size: {:?}", cfg.batch_size);
        },
        None => println!("FAILURE: get_system_config returned None for task_id: {}", task_id),
    }
}
