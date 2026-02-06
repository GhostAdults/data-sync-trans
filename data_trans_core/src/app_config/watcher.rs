use crate::app_config::manager::SharedConfig;
use crate::app_config::config_loader::load_user_config;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;

pub fn watch(path: PathBuf, config: Arc<SharedConfig>) -> notify::Result<RecommendedWatcher> {
    let config_file_path = path.clone();
    // 监听父目录，因为原子写入（Atomic Writes）可能会替换文件，导致针对单文件的监听失效
    let parent_dir = path.parent().unwrap_or(&path).to_path_buf();
    
    println!("Starting watcher on directory: {:?}", parent_dir);
    println!("Target config file: {:?}", config_file_path);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        match res {
            Ok(event) => {
                println!("Watcher event: {:?}", event); // 打印所有事件以便调试
                
                // 检查事件是否与我们的配置文件相关
                let is_relevant = event.paths.iter().any(|p| {
                    // 需要处理可能的路径规范化差异，简单起见先比较文件名或绝对路径
                    p.file_name() == config_file_path.file_name()
                });

                if is_relevant {
                    if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() || event.kind.is_access() {
                         // 注意：is_access 通常不需要重载，但在某些平台上可能需要宽容处理
                         // 为了稳健，我们尝试重载
                         println!("Detected change in config file, reloading...");
                         let mut cfg = config.write();
                         match load_user_config(&config_file_path) {
                            Ok(user_cfg) => {
                                cfg.user = user_cfg;
                                cfg.build();
                                println!("Config reloaded successfully. New keys: {:?}", cfg.user.keys());
                            },
                            Err(e) => {
                                eprintln!("Failed to reload config in watcher: {}", e);
                            }
                         }
                    }
                }
            },
            Err(e) => eprintln!("watch error: {:?}", e),
        }
    })?;
    
    // 监听父目录
    watcher.watch(&parent_dir, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}
