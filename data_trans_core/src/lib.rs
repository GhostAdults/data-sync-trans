pub mod core;
pub mod dsl_engine;
pub mod app_config;
use crate::core::db::DbKind;
use anyhow::{Result, bail};
use crate::core::{Config,serve_http};
use regex::Regex;
use std::sync::{Arc, OnceLock};
use std::path::PathBuf;
use parking_lot::RwLock;
use crate::app_config::watcher;
use crate::app_config::schema::ConfigSchema;
use crate::app_config::value::ConfigValue;
use crate::app_config::manager::ConfigManager;
use crate::app_config::config_loader::apply_json_defaults;

static SYSTEM_CONFIG: OnceLock<Option<Config>> = OnceLock::new();
static CONFIG_MANAGER: OnceLock<Arc<RwLock<ConfigManager>>> = OnceLock::new();
static WATCHER_HOLDER: OnceLock<notify::RecommendedWatcher> = OnceLock::new();

pub fn init_system_config() -> Option<Arc<RwLock<ConfigManager>>> {
    let mgr_arc = CONFIG_MANAGER.get_or_init(|| {
        // 1. 创建 ConfigManager (OS 路径)
        let mut mgr = match ConfigManager::for_os("app_trans") {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to create ConfigManager: {}", e);
                // 暂时构建一个空的
                return Arc::new(RwLock::new(ConfigManager::new(PathBuf::from("")).unwrap())); 
            }
        };

        // 2. 读取 defaults.json
        let defaults_content = include_str!("../defaults.json");
        let defaults_json: serde_json::Value = match serde_json::from_str(defaults_content) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to parse defaults.json: {}", e);
                serde_json::json!({})
            }
        };

        // 3. 调用 apply_json_defaults
        apply_json_defaults(&mut mgr, "", &defaults_json);

        // 4. 加载用户配置文件（已在 ConfigManager::for_desktop 内部完成）

        // 5. build / merge
        mgr.build();

        // 6. 如果用户配置不存在或为空 → 写出默认配置
        let should_write_defaults = if !mgr.path.exists() {
            true
        } else {
            // 检查配置json是否合法（仅作为测试） 
            match std::fs::read_to_string(&mgr.path) {
                Ok(content) => {
                     match serde_json::from_str::<serde_json::Value>(&content) {
                        Ok(v) => v.get("tasks").is_none(),
                        Err(_) => true,
                     }
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

    // 初始化 SYSTEM_CONFIG (保持兼容性，默认加载 id="default")
    SYSTEM_CONFIG.get_or_init(|| {
        let mgr = mgr_arc.read();
        match Config::from_manager(&mgr, "default") {
             Ok(cfg) => Some(cfg),
             Err(e) => {
                 eprintln!("Failed to construct default Config: {}", e);
                 None
             }
        }
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
            },
            Err(e) => eprintln!("Failed to start config watcher: {}", e),
        }
    }
}

pub fn get_config_manager() -> Option<Arc<RwLock<ConfigManager>>> {
    CONFIG_MANAGER.get().cloned()
}

// 获取当前的系统配置
pub fn get_system_config(task_id: Option<&str>) -> Option<Config> {
    if let Some(mgr_arc) = CONFIG_MANAGER.get() {
        let mgr = mgr_arc.read();
        let id = task_id.unwrap_or("default");
        match Config::from_manager(&mgr, id) {
             Ok(cfg) => Some(cfg),
             Err(_) => None
        }
    } else {
        None
    }
}
pub fn get_default_system_config() -> Option<&'static Config> {
    SYSTEM_CONFIG.get().and_then(|c| c.as_ref())
}

pub fn sanitize_identifier(s: &str) -> Result<()> {
    let re = Regex::new(r"^[A-Za-z0-9_]+$").unwrap();
    if !re.is_match(s) {
        bail!("标识符仅允许字母、数字和下划线: {}", s);
    }
    Ok(())
}
pub fn read_config(path: PathBuf, task_id: Option<String>) -> Result<Config> {
    // 优先从内存中获取最新的系统配置（由 Watcher 维护）
    if let Some(cfg) = crate::get_system_config(task_id.as_deref()) {
        return Ok(cfg);
    }
    // 尝试初始化一下，如果还没初始化。
    if init_system_config().is_some() {
         if let Some(cfg) = crate::get_system_config(task_id.as_deref()) {
            return Ok(cfg);
        }
    }

    bail!(format!("读取配置文件失败: {}", path.display()))
}

pub fn read_defaluts_config(path: PathBuf) -> Result<Config> {
         if let Some(cfg) = crate::get_default_system_config() {    
            return Ok(cfg.clone());
        }
        bail!(format!("读取默认配置文件失败: {}", path.display()))
}

pub fn detect_db_kind(url: &str, explicit: Option<DbKind>) -> Result<DbKind> {
    if let Some(k) = explicit {
        return Ok(k);
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(DbKind::Postgres)
    } else if url.starts_with("mysql://") {
        Ok(DbKind::Mysql)
    } else {
        bail!("无法识别数据库类型，需提供 db_type 或使用以 postgres:// 或 mysql:// 开头的连接串")
    }
}

// 启动 HTTP 服务
pub fn run_serve(host: String, port: u16) -> Result<()> {
    init_system_config();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {serve_http(host, port).await })?;
    Ok(())
}
