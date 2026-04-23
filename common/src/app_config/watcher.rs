use crate::app_config::config_loader::load_user_config;
use crate::app_config::manager::SharedConfig;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info};

pub fn watch(path: PathBuf, config: Arc<SharedConfig>) -> notify::Result<RecommendedWatcher> {
    let config_file_path = path.clone();
    let parent_dir = path.parent().unwrap_or(&path).to_path_buf();

    info!("[Watcher] watching directory: {:?}", parent_dir);

    let mut watcher =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(event) => {
                let is_relevant = event
                    .paths
                    .iter()
                    .any(|p| p.file_name() == config_file_path.file_name());

                if is_relevant {
                    if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() {
                        info!("[Watcher] config file changed, reloading...");
                        let mut cfg = config.write();
                        match load_user_config(&config_file_path) {
                            Ok(user_cfg) => {
                                cfg.user = user_cfg;
                                cfg.build();
                                debug!("[Watcher] reloaded, keys: {:?}", cfg.user.keys());
                            }
                            Err(e) => {
                                error!("[Watcher] reload failed: {}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => error!("[Watcher] error: {:?}", e),
        })?;

    watcher.watch(&parent_dir, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}
