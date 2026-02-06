use data_trans_core::{init_and_watch_config, get_config_manager};
use std::thread;
use std::time::Duration;
use std::fs;
use serde_json::Value;

fn main() {
    println!("Starting config watch test...");

    // 1. Initialize and start watching
    init_and_watch_config();
    println!("Config initialized and watcher started.");

    let mgr_arc = get_config_manager().expect("Config manager should be initialized");
    let config_path = mgr_arc.read().path.clone();
    println!("Config path: {:?}", config_path);

    // Ensure the directory exists
    if let Some(parent) = config_path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    // Helper function to update config file safely without overwriting existing keys
    let update_config_file = |path: &std::path::PathBuf, key: &str, value: i64| {
        let content = if path.exists() {
            fs::read_to_string(path).unwrap_or_else(|_| "{}".to_string())
        } else {
            "{}".to_string()
        };
        
        let mut json_val: Value = serde_json::from_str(&content).unwrap_or(serde_json::json!({}));
        
        if let Value::Object(ref mut map) = json_val {
            map.insert(key.to_string(), serde_json::json!(value));
        } else {
            // If the root is not an object (e.g. array or null), force it to be an object
            json_val = serde_json::json!({
                key: value
            });
        }
        
        let new_content = serde_json::to_string_pretty(&json_val).expect("Failed to serialize config");
        fs::write(path, new_content).expect("Failed to write config file");
    };

    // 2. Write initial config (safely)
    println!("Updating config with initial value...");
    update_config_file(&config_path, "test_watch_key", 1);

    // Wait a bit for the watcher
    thread::sleep(Duration::from_secs(1));

    // 3. Update config file (safely)
    println!("Updating config file with new value...");
    update_config_file(&config_path, "test_watch_key", 2);

    // 4. Wait for watcher to detect change
    println!("Waiting for watcher to detect change...");
    
    let mut detected = false;
    for i in 0..10 {
        thread::sleep(Duration::from_millis(500));
        let mgr = mgr_arc.read();
        if let Some(val) = mgr.get("test_watch_key") {
            if let Some(int_val) = val.as_i64() {
                println!("Current value check #{}: {}", i + 1, int_val);
                if int_val == 2 {
                    detected = true;
                    break;
                }
            }
        }
        drop(mgr);
    }

    if detected {
        println!("SUCCESS: Config change detected!");
    } else {
        println!("FAILURE: Config change NOT detected within timeout.");
    }
}
