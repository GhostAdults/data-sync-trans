pub mod core;
pub mod dsl_engine;
use crate::core::serve_http;
use anyhow::Result;
use data_trans_common::app_config::config_loader::{
    apply_json_defaults, get_config_manager, CONFIG_MANAGER, WATCHER_HOLDER,
};
use data_trans_common::app_config::manager::ConfigManager;
use data_trans_common::app_config::schema::ConfigSchema;
use data_trans_common::app_config::value::ConfigValue;
use data_trans_common::app_config::watcher;
use data_trans_common::job_config::JobConfig;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

pub fn init_system_config() -> Option<Arc<RwLock<ConfigManager>>> {
    let mgr_arc = CONFIG_MANAGER.get_or_init(|| {
        //  创建 ConfigManager (OS 路径)
        let mut mgr = match ConfigManager::for_os("app_trans") {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to create ConfigManager: {}", e);
                // 暂时构建一个空的
                return Arc::new(RwLock::new(ConfigManager::new(PathBuf::from("")).unwrap()));
            }
        };

        //  读取 defaults.config.json
        let defaults_content = include_str!("../../data_trans_cli/user_config/default.config.json");
        let defaults_json: serde_json::Value = match serde_json::from_str(defaults_content) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to parse defaults.json: {}", e);
                serde_json::json!({})
            }
        };

        // 调用 apply_json_defaults
        apply_json_defaults(&mut mgr, "", &defaults_json);

        //  加载用户配置文件（已在 ConfigManager::for_desktop 内部完成）

        // build / merge
        mgr.build();

        // 6. 如果用户配置不存在或为空 → 写出默认配置
        let should_write_defaults = if !mgr.path.exists() {
            true
        } else {
            // 检查配置json是否合法（仅作为测试）
            match std::fs::read_to_string(&mgr.path) {
                Ok(content) => match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(v) => v.get("tasks").is_none(),
                    Err(_) => true,
                },
                Err(_) => false,
            }
        };

        if should_write_defaults && !mgr.path.as_os_str().is_empty() {
            // 将 defaults 复制到 user 中，以便首次生成完整的配置文件
            mgr.user = mgr.defaults.clone();
            mgr.build(); // 重新 build 确保 merged 正确
            if let Err(e) = mgr.persist() {
                eprintln!("Failed to persist defaults: {}", e);
            }
        }

        Arc::new(RwLock::new(mgr))
    });

    // 启动 watcher
    if !mgr_arc.read().path.as_os_str().is_empty() {
        let _config_path = mgr_arc.read().path.clone();
    }

    Some(mgr_arc.clone())
}

pub fn init_and_watch_config() {
    if let Some(mgr) = init_system_config() {
        // 注册插件默认配置
        {
            let mut w = mgr.write();
            w.register(
                "plugin.todo.enabled",
                ConfigSchema {
                    default: ConfigValue::Bool(true),
                    description: "Todo plugin",
                },
            );
            w.build(); // 重新构建以应用新注册的默认值
        }

        let config_path = mgr.read().path.clone();

        // 启动 watcher 并持有句柄
        match watcher::watch(config_path, mgr.clone()) {
            Ok(w) => {
                let _ = WATCHER_HOLDER.set(w);
            }
            Err(e) => eprintln!("Failed to start config watcher: {}", e),
        }
    }
}

pub fn read_config(path: PathBuf) -> Result<JobConfig> {
    let data = std::fs::read_to_string(&path)
        .map_err(|_| anyhow::anyhow!("读取配置文件失败: {}", path.display()))?;
    let cfg: JobConfig = serde_json::from_str(&data)?;
    Ok(cfg)
}

// 启动 HTTP 服务
pub fn run_serve(host: String, port: u16) -> Result<()> {
    init_system_config();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { serve_http(host, port).await })?;
    Ok(())
}
