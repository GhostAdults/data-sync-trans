pub mod core;
pub mod dsl_engine;
pub mod pipeline;

// 确保 inventory::submit! 被 core 链接
use relus_reader as _;
use relus_writer as _;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use relus_common::app_config::config_loader::{
    apply_json_defaults, get_config_manager, CONFIG_MANAGER, WATCHER_HOLDER,
};
use relus_common::app_config::manager::ConfigManager;
use relus_common::app_config::schema::ConfigSchema;
use relus_common::app_config::value::ConfigValue;
use relus_common::app_config::watcher;
use relus_common::job_config::JobConfig;
use std::path::PathBuf;
use std::sync::Arc;

fn load_embedded_defaults() -> serde_json::Value {
    let defaults_content = include_str!("../../cli/user_config/default.config.json");
    match serde_json::from_str(defaults_content) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Failed to parse embedded defaults: {}", e);
            serde_json::json!({})
        }
    }
}

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

        // 优先读取用户目录下的 .config.json，不存在时 fallback 到内嵌默认值
        let user_config_dir = relus_common::app_config::path::default_config_path("app_trans")
            .parent()
            .map(|p| p.join(".config.json"))
            .unwrap_or_default();

        let defaults_json = if user_config_dir.exists() {
            match std::fs::read_to_string(&user_config_dir) {
                Ok(content) => match serde_json::from_str(&content) {
                    Ok(v) => {
                        eprintln!("使用用户配置: {}", user_config_dir.display());
                        v
                    }
                    Err(e) => {
                        eprintln!("用户配置解析失败({}), 使用内嵌默认值", e);
                        load_embedded_defaults()
                    }
                },
                Err(e) => {
                    eprintln!("用户配置读取失败({}), 使用内嵌默认值", e);
                    load_embedded_defaults()
                }
            }
        } else {
            load_embedded_defaults()
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
        .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
    let cfg: JobConfig = serde_json::from_str(&data)
        .with_context(|| format!("配置文件解析失败: {}", path.display()))?;
    Ok(cfg)
}

// 启动 HTTP 服务
pub async fn run_serve(host: String, port: u16) -> Result<()> {
    init_system_config();
    core::serve::serve_http(host, port, None).await?;
    Ok(())
}

fn scheduler_server_addr(host: Option<String>, port: Option<u16>) -> (String, u16) {
    let Some(mgr) = init_system_config() else {
        return (
            host.unwrap_or_else(|| "127.0.0.1".to_string()),
            port.unwrap_or(30001),
        );
    };

    let mgr = mgr.read();
    let host = host.unwrap_or_else(|| {
        mgr.get("server.host")
            .and_then(ConfigValue::as_str)
            .filter(|host| !host.trim().is_empty())
            .unwrap_or("127.0.0.1")
            .to_string()
    });

    let port = port.unwrap_or_else(|| {
        mgr.get("server.port")
            .and_then(ConfigValue::as_i64)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
            .unwrap_or(30001)
    });

    (host, port)
}

/// 启动 TaskScheduler 常驻运行
pub async fn run_scheduler(
    configs: Vec<(String, relus_common::job_config::JobConfig)>,
    enable_repl: bool,
    host: Option<String>,
    port: Option<u16>,
) -> Result<()> {
    let checkpoint_dir = relus_common::app_config::path::default_config_path("app_trans")
        .parent()
        .map(|p| p.join("checkpoints.redb"))
        .unwrap_or_else(|| PathBuf::from("checkpoints.redb"));

    let mut scheduler = crate::core::scheduler::TaskScheduler::new(checkpoint_dir)?;
    let scheduler_handle = scheduler.control_handle();

    if configs.is_empty() {
        tracing::info!("[Run] no jobs loaded at startup; waiting for REPL/API submissions");
        println!("No jobs loaded at startup; waiting for REPL/API submissions.");
    }

    for (job_id, config) in configs {
        let schedule = config
            .schedule
            .as_ref()
            .map(crate::core::scheduler::Schedule::from_config)
            .transpose()
            .map_err(|message| anyhow::anyhow!("job '{}' schedule invalid: {}", job_id, message))?
            .unwrap_or_default();
        match scheduler.submit_task(job_id.clone(), std::sync::Arc::new(config), schedule) {
            Ok(id) => tracing::info!("[Run] job '{}' submitted", id),
            Err(e) => tracing::error!("[Run] job '{}' submit failed: {}", job_id, e),
        }
    }

    if enable_repl {
        scheduler.start_repl();
    } else {
        tracing::info!("[Run] REPL disabled by --no-repl");
    }

    let (host, port) = scheduler_server_addr(host, port);
    let server = tokio::spawn(crate::core::serve::serve_http(
        host.clone(),
        port,
        Some(scheduler_handle),
    ));
    tracing::info!("[Run] scheduler control API listening on {}:{}", host, port);
    println!("Scheduler control API listening on {}:{}", host, port);
    tracing::info!("[Run] scheduler starting, waiting for tasks...");
    scheduler.run().await;
    server.abort();
    let _ = server.await;
    tracing::info!("[Run] scheduler stopped");

    Ok(())
}
