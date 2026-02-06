pub use std::path::PathBuf;
pub use data_trans_core::read_config;
use flutter_rust_bridge::frb;
use data_trans_core::app_config::path::default_config_path;
pub use data_trans_core::core::Config;
pub use data_trans_core::core::{TablesQuery,BaseDbQuery};
pub use data_trans_core::core::server::list_tables;
use serde_json::json;

#[frb(sync)] // Synchronous mode for simplicity of the demo
pub fn greet(name: String) -> String {
    format!("Hello, {name}!")
}
#[frb]  // 这是 FRB 宏
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
#[frb(sync)]
pub fn init_and_watch_config() {
    data_trans_core::init_and_watch_config();
}

#[frb(sync)]
pub fn get_config(path: Option<PathBuf>, id: Option<String>) -> Result<String, String> {
    let p: PathBuf = path.unwrap_or_else(|| default_config_path("app_trans"));
    let config: Config = match read_config(p, id) {
        Ok(c) => c,
        Err(e) => return Err(e.to_string()),
    };
    println!("配置刷新了");
    serde_json::to_string(&config).map_err(|e| e.to_string())
}

pub async fn get_tables(_name: String) -> String {
    // 获取数据库数据
    let query = TablesQuery {
        base: BaseDbQuery {
            db_url: None,
            db_type: None,
            task_id: None,
        }
    };
    let (_, resp) = list_tables(query).await;
    // 直接序列化 resp.data (Option<Vec<String>> or similar)
    // 假设 resp.data 是我们需要的数据列表
    serde_json::to_string(&resp.data).unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
}

#[frb(init)]
pub fn init_app() {
    // Default utilities - feel free to customize
    flutter_rust_bridge::setup_default_user_utils();
    init_and_watch_config();
}

#[frb(sync)]
pub fn config_to_json(config: &Config) -> String {
    serde_json::to_string(config).unwrap_or_else(|e| e.to_string())
}
