use std::path::PathBuf;

pub fn default_config_path(app_name: &str) -> PathBuf {
    let mut dir = dirs_next::config_dir()
        .expect("Cannot determine OS config directory");

    dir.push(app_name);
    std::fs::create_dir_all(&dir).ok();
    dir.push("config.json");
    dir
}
