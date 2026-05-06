use std::path::PathBuf;

pub fn default_config_path(app_name: &str) -> PathBuf {
    let mut dir = dirs_next::config_dir().unwrap_or_else(|| PathBuf::from("."));
    // 生成 ./{app_name}/.config.json
    dir.push(app_name);
    std::fs::create_dir_all(&dir).ok();
    dir.push(".config.json");
    dir
}
