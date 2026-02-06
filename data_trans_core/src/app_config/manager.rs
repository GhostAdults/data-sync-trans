use crate::app_config::config_loader::{load_user_config,unflatten};
use crate::app_config::path::default_config_path;
use crate::app_config::schema::ConfigSchema;
use crate::app_config::value::ConfigValue;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct ConfigManager {
    pub schema: HashMap<String, ConfigSchema>,
    pub defaults: HashMap<String, ConfigValue>,
    pub user: HashMap<String, ConfigValue>,
    pub merged: HashMap<String, ConfigValue>,
    pub path: PathBuf,
}

impl ConfigManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        Ok(Self {
            schema: HashMap::new(),
            defaults: HashMap::new(),
            user: load_user_config(&path)?,
            merged: HashMap::new(),
            path,
        })
    }

    pub fn for_os(app_name: &str) -> anyhow::Result<Self> {
        let path = default_config_path(app_name);
        Self::new(path)
    }

    pub fn register(&mut self, key: &str, schema: ConfigSchema) {
        self.defaults.insert(key.to_string(), schema.default.clone());
        self.schema.insert(key.to_string(), schema);
    }

    pub fn build(&mut self) {
        self.merged = self.defaults.clone();
        for (k, v) in &self.user {
            self.merged.insert(k.clone(), v.clone());
        }
    }

    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.merged.get(key)
    }

    pub fn get_by_prefix(&self, prefix: &str) -> HashMap<String, ConfigValue> {
        let mut result = HashMap::new();
        let prefix_dot = format!("{}.", prefix);
        for (k, v) in &self.merged {
            if k.starts_with(&prefix_dot) {
                let sub_key = k.trim_start_matches(&prefix_dot).to_string();
                result.insert(sub_key, v.clone());
            }
        }
        result
    }

    pub fn set(&mut self, key: &str, value: ConfigValue) -> Result<()> {
        self.user.insert(key.to_string(), value);
        self.build();
        self.persist()
    }

    pub fn persist(&self) -> Result<()> {
        let root = unflatten(&self.user);
        let content = serde_json::to_string_pretty(&root)?;
        std::fs::write(&self.path, content)?;
        Ok(())
    }
}


pub type SharedConfig = RwLock<ConfigManager>;
