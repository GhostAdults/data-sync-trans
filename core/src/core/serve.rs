use axum::extract::Query;
use axum::http::StatusCode;
use axum::Json;
use axum::Router;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::result::Result::{Err, Ok};
use std::sync::Arc;

use crate::core::axum_api::{h_describe, h_gen_mapping, h_list_tables, h_sync};
use crate::core::runner::{run_sync, RunResult};
use anyhow::Result;
use axum::routing::{get, post};
use relus_common::app_config::value::ConfigValue;
use relus_common::data_source_config::{ApiConfig, DbConfig};
use relus_common::job_config::{JobConfig, MappingConfig};
use relus_common::resp::{ApiResp, ColInfo};
use relus_common::resp::{DescribeQuery, GenMapQuery, TablesQuery};
use relus_common::{CreateConfigReq, UpdateConfigReq};
use relus_connector_rdbms::pool::{get_pool_from_query, RdbmsPool};
use sqlx::{MySqlPool, PgPool, Row};
use tokio::net::TcpListener;

use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LegacyConfig {
    pub id: String,
    pub db_type: Option<String>,
    pub db: DbConfig,
    pub api: ApiConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Deserialize)]
pub struct SyncReq {
    pub config: JobConfig,
    pub mapping: Option<MappingConfig>,
}

pub async fn serve_http(host: String, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/tables",
            get(|q: Query<TablesQuery>| async move { h_list_tables(q).await }),
        )
        .route(
            "/describe",
            get(|q: Query<DescribeQuery>| async move { h_describe(q).await }),
        )
        .route(
            "/gen-mapping",
            get(|q: Query<GenMapQuery>| async move { h_gen_mapping(q).await }),
        )
        .route(
            "/sync",
            post(|body: Json<SyncReq>| async move { h_sync(body).await }),
        );
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn list_tables_postgres(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query("select tablename from pg_catalog.pg_tables where schemaname not in ('pg_catalog','information_schema')").fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("tablename")?;
        out.push(name);
    }
    Ok(out)
}

pub async fn list_tables_mysql(pool: &MySqlPool) -> Result<Vec<String>> {
    let rows = sqlx::query("show tables").fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let v: String = r.try_get(0)?;
        out.push(v);
    }
    Ok(out)
}

pub async fn fetch_columns_postgres(pool: &PgPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select column_name, data_type, is_nullable from information_schema.columns where table_schema='public' and table_name=$1 order by ordinal_position").bind(table).fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("column_name")?;
        let ty: String = r.try_get("data_type")?;
        let is_nullable: String = r.try_get("is_nullable")?;
        out.push(ColInfo {
            name,
            data_type: ty,
            is_nullable: is_nullable == "YES",
        });
    }
    Ok(out)
}

pub async fn fetch_columns_mysql(pool: &MySqlPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select COLUMN_NAME, DATA_TYPE, IS_NULLABLE from information_schema.columns where table_schema = database() and table_name = ? order by ORDINAL_POSITION").bind(table).fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("COLUMN_NAME")?;
        let ty: String = r.try_get("DATA_TYPE")?;
        let is_nullable: String = r.try_get("IS_NULLABLE")?;
        out.push(ColInfo {
            name,
            data_type: ty,
            is_nullable: is_nullable == "YES",
        });
    }
    Ok(out)
}

fn guess_type(sql_type: &str) -> &'static str {
    let t = sql_type.to_ascii_lowercase();
    if t.contains("int") {
        "int"
    } else if t.contains("numeric")
        || t.contains("decimal")
        || t.contains("double")
        || t.contains("real")
        || t.contains("float")
    {
        "float"
    } else if t.contains("bool") {
        "bool"
    } else if t.contains("timestamp") || t == "datetime" || t == "date" || t == "time" {
        "timestamp"
    } else {
        "text"
    }
}

/// 同步数据函数
pub async fn sync(cfg: JobConfig) -> Result<RunResult> {
    run_sync(Arc::new(cfg)).await
}

/// CLI 同步
pub async fn sync_cli(cfg: JobConfig) -> Result<RunResult> {
    run_sync(Arc::new(cfg)).await
}

pub async fn list_tables(q: TablesQuery) -> (StatusCode, Json<ApiResp<Vec<String>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => match pool {
            RdbmsPool::Postgres(p) => match list_tables_postgres(&p).await {
                Ok(tabs) => (
                    StatusCode::OK,
                    Json(ApiResp {
                        ok: true,
                        data: Some(tabs),
                        error: None,
                    }),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    }),
                ),
            },
            RdbmsPool::Mysql(p) => match list_tables_mysql(&p).await {
                Ok(tabs) => (
                    StatusCode::OK,
                    Json(ApiResp {
                        ok: true,
                        data: Some(tabs),
                        error: None,
                    }),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    }),
                ),
            },
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub async fn describe(q: DescribeQuery) -> (StatusCode, Json<ApiResp<Vec<Value>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => match pool {
            RdbmsPool::Postgres(p) => match fetch_columns_postgres(&p, &q.table).await {
                Ok(cols) => {
                    let v = cols.into_iter().map(|c| serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})).collect::<Vec<_>>();
                    (
                        StatusCode::OK,
                        Json(ApiResp {
                            ok: true,
                            data: Some(v),
                            error: None,
                        }),
                    )
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    }),
                ),
            },
            RdbmsPool::Mysql(p) => match fetch_columns_mysql(&p, &q.table).await {
                Ok(cols) => {
                    let v = cols.into_iter().map(|c| serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})).collect::<Vec<_>>();
                    (
                        StatusCode::OK,
                        Json(ApiResp {
                            ok: true,
                            data: Some(v),
                            error: None,
                        }),
                    )
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    }),
                ),
            },
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub async fn gen_mapping(q: GenMapQuery) -> (StatusCode, Json<ApiResp<Value>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => {
            let cols_res = match pool {
                RdbmsPool::Postgres(p) => fetch_columns_postgres(&p, &q.table).await,
                RdbmsPool::Mysql(p) => fetch_columns_mysql(&p, &q.table).await,
            };
            match cols_res {
                Ok(cols) => {
                    let mut mapping = BTreeMap::new();
                    let mut types = BTreeMap::new();
                    for c in cols {
                        mapping.insert(c.name.clone(), "".to_string());
                        types.insert(c.name.clone(), guess_type(&c.data_type).to_string());
                    }
                    let obj = serde_json::json!({
                        "table": q.table,
                        "key_columns": [],
                        "column_mapping": mapping,
                        "column_types": types
                    });
                    (
                        StatusCode::OK,
                        Json(ApiResp {
                            ok: true,
                            data: Some(obj),
                            error: None,
                        }),
                    )
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    }),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub async fn sync_command(
    mut cfg: JobConfig,
    mapping: Option<MappingConfig>,
) -> (StatusCode, Json<ApiResp<Value>>) {
    if let Some(mapping) = mapping {
        cfg.column_mapping = mapping.column_mapping;
        cfg.column_types = Some(mapping.column_types);
        if let Some(m) = mapping.mode {
            cfg.target.writer_mode = Some(m);
        }
        if let Some(k) = mapping.key_columns {
            if let Some(obj) = cfg.target.config.as_object_mut() {
                obj.insert("key_columns".to_string(), serde_json::json!(k));
            }
        }
    }

    match sync(cfg).await {
        Ok(result) => (
            StatusCode::OK,
            Json(ApiResp {
                ok: result.status == crate::core::runner::RunStatus::Success,
                data: Some(serde_json::to_value(&result).unwrap_or_default()),
                error: result.error,
            }),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some(format!("数据同步错误: {}", e)),
            }),
        ),
    }
}

pub async fn create_config(Json(req): Json<CreateConfigReq>) -> (StatusCode, Json<ApiResp<Value>>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        let mut tasks = match mgr.get("tasks") {
            Some(ConfigValue::Array(arr)) => arr.clone(),
            _ => Vec::new(),
        };

        // Check if task ID already exists
        for task in &tasks {
            if let ConfigValue::Object(map) = task {
                if let Some(ConfigValue::String(id)) = map.get("id") {
                    if id == &req.task_id {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ApiResp {
                                ok: false,
                                data: None,
                                error: Some(format!("Task {} already exists", req.task_id)),
                            }),
                        );
                    }
                }
            }
        }

        // Construct new task with defaults
        let mut new_task_map = HashMap::new();
        new_task_map.insert("id".to_string(), ConfigValue::String(req.task_id.clone()));

        // Apply defaults first
        apply_default_task_values(&mut new_task_map);

        // Merge user provided config
        merge_json_value(&mut new_task_map, &req.config);

        // Ensure ID is correct (in case user config overwrote it)
        new_task_map.insert("id".to_string(), ConfigValue::String(req.task_id.clone()));

        tasks.push(ConfigValue::Object(new_task_map));

        match mgr.set("tasks", ConfigValue::Array(tasks)) {
            Ok(_) => (
                StatusCode::OK,
                Json(ApiResp {
                    ok: true,
                    data: None,
                    error: None,
                }),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                }),
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            }),
        )
    }
}

pub async fn update_config(Json(req): Json<UpdateConfigReq>) -> (StatusCode, Json<ApiResp<Value>>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        let mut tasks = match mgr.get("tasks") {
            Some(ConfigValue::Array(arr)) => arr.clone(),
            _ => Vec::new(),
        };

        let mut found = false;
        for task in &mut tasks {
            if let ConfigValue::Object(map) = task {
                if let Some(ConfigValue::String(id)) = map.get("id") {
                    if id == &req.task_id {
                        // Direct merge using the recursive helper
                        merge_json_value(map, &req.updates);

                        // Protect ID from being changed via updates
                        map.insert("id".to_string(), ConfigValue::String(req.task_id.clone()));

                        found = true;
                        break;
                    }
                }
            }
        }

        if !found {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResp {
                    ok: false,
                    data: None,
                    error: Some(format!("Task {} not found", req.task_id)),
                }),
            );
        }

        match mgr.set("tasks", ConfigValue::Array(tasks)) {
            Ok(_) => (
                StatusCode::OK,
                Json(ApiResp {
                    ok: true,
                    data: None,
                    error: None,
                }),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                }),
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            }),
        )
    }
}

// 辅助函数：递归合并 JSON Value 到 ConfigValue::Object
fn merge_json_value(target: &mut HashMap<String, ConfigValue>, updates: &serde_json::Value) {
    if let serde_json::Value::Object(update_map) = updates {
        for (k, v) in update_map {
            // 如果 v 是 null，表示删除该字段
            if v.is_null() {
                target.remove(k);
                continue;
            }

            // 如果 target 中已存在且也是 Object，则递归合并
            // 否则直接覆盖
            let target_val = target
                .entry(k.clone())
                .or_insert_with(|| ConfigValue::String("".to_string()));

            match (target_val, v) {
                (ConfigValue::Object(t_map), serde_json::Value::Object(_)) => {
                    merge_json_value(t_map, v);
                }
                (t_val, new_val) => {
                    *t_val = ConfigValue::from(new_val);
                }
            }
        }
    }
}

fn apply_default_task_values(map: &mut HashMap<String, ConfigValue>) {
    // Top level defaults
    map.entry("db_type".to_string())
        .or_insert(ConfigValue::String("".to_string()));
    map.entry("mode".to_string())
        .or_insert(ConfigValue::String("insert".to_string()));
    map.entry("batch_size".to_string())
        .or_insert(ConfigValue::Int(100));
    map.entry("column_mapping".to_string())
        .or_insert_with(|| ConfigValue::Object(HashMap::new()));
    map.entry("column_types".to_string())
        .or_insert_with(|| ConfigValue::Object(HashMap::new()));

    // DB defaults
    let db = map
        .entry("db".to_string())
        .or_insert_with(|| ConfigValue::Object(HashMap::new()));
    if let ConfigValue::Object(db_map) = db {
        db_map
            .entry("url".to_string())
            .or_insert(ConfigValue::String("".to_string()));
        db_map
            .entry("table".to_string())
            .or_insert(ConfigValue::String("".to_string()));
        db_map
            .entry("key_columns".to_string())
            .or_insert_with(|| ConfigValue::Array(Vec::new()));
        db_map
            .entry("max_connections".to_string())
            .or_insert(ConfigValue::Int(20));
        db_map
            .entry("acquire_timeout_secs".to_string())
            .or_insert(ConfigValue::Int(60));
        db_map
            .entry("use_transaction".to_string())
            .or_insert(ConfigValue::Bool(true));
    }

    // API defaults
    let api = map
        .entry("api".to_string())
        .or_insert_with(|| ConfigValue::Object(HashMap::new()));
    if let ConfigValue::Object(api_map) = api {
        api_map
            .entry("url".to_string())
            .or_insert(ConfigValue::String("".to_string()));
        api_map
            .entry("method".to_string())
            .or_insert(ConfigValue::String("GET".to_string()));
        api_map
            .entry("headers".to_string())
            .or_insert_with(|| ConfigValue::Object(HashMap::new()));
    }
}
