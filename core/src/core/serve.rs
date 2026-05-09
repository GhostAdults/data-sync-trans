use relus_common::app_config::config_loader::flatten;
use relus_common::app_config::value::ConfigValue;
use relus_common::job_config::JobConfig;
use relus_common::resp::ApiResp;
use relus_common::{CreateConfigReq, UpdateConfigReq};
use relus_api::server::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::core::runner::{start_task, RunResult};

/// Starts an immediate task.
///
/// Scheduler callers that need a custom cancellation token should call
/// `runner::start_task` directly.
pub async fn start_job(cfg: JobConfig) -> anyhow::Result<RunResult> {
    start_task(Arc::new(cfg), CancellationToken::new()).await
}

pub async fn create_config(req: CreateConfigReq) -> (StatusCode, ApiResp<Value>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        let mut config = match default_system_config() {
            Ok(config) => config,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                );
            }
        };

        if !req.config.is_object() {
            return (
                StatusCode::BAD_REQUEST,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some("config must be a JSON object".to_string()),
                },
            );
        }

        merge_json_object(&mut config, &req.config);
        let mut user = HashMap::new();
        flatten("", &ConfigValue::from(&config), &mut user);
        mgr.user = user;
        mgr.build();

        match mgr.persist() {
            Ok(_) => (
                StatusCode::OK,
                ApiResp {
                    ok: true,
                    data: Some(config),
                    error: None,
                },
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            },
        )
    }
}

fn default_system_config() -> anyhow::Result<Value> {
    let content = include_str!("../../../cli/user_config/default.config.json");
    Ok(serde_json::from_str(content)?)
}

pub async fn update_config(req: UpdateConfigReq) -> (StatusCode, ApiResp<Value>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        if !req.updates.is_object() {
            return (
                StatusCode::BAD_REQUEST,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some("updates must be a JSON object".to_string()),
                },
            );
        }

        let mut config = relus_common::app_config::config_loader::unflatten(&mgr.user)
            .unwrap_or_else(|_| serde_json::json!({}));
        merge_json_object(&mut config, &req.updates);

        let mut user = HashMap::new();
        flatten("", &ConfigValue::from(&config), &mut user);
        mgr.user = user;
        mgr.build();

        match mgr.persist() {
            Ok(_) => (
                StatusCode::OK,
                ApiResp {
                    ok: true,
                    data: Some(config),
                    error: None,
                },
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            },
        )
    }
}

fn merge_json_object(target: &mut Value, updates: &Value) {
    let (Some(target_map), Some(update_map)) = (target.as_object_mut(), updates.as_object()) else {
        return;
    };

    for (key, value) in update_map {
        if value.is_null() {
            target_map.remove(key);
            continue;
        }

        match (target_map.get_mut(key), value) {
            (Some(target_value), Value::Object(_)) if target_value.is_object() => {
                merge_json_object(target_value, value);
            }
            _ => {
                target_map.insert(key.clone(), value.clone());
            }
        }
    }
}
