use crate::app_config::value::ConfigValue;

#[derive(Debug, Clone)]
pub struct ConfigSchema {
    pub default: ConfigValue,
    pub description: &'static str,
}