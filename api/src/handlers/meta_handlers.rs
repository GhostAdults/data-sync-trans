use axum::http::StatusCode;
use axum::{extract::Path, extract::Query, extract::State, Json};
use relus_common::job_config::{JobConfig, MappingConfig};
use relus_common::resp::{ApiResp, ColInfo};
use relus_common::{DescribeQuery, GenMapQuery, TablesQuery};
use relus_connector_rdbms::pool::{get_pool_from_query, RdbmsPool};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{MySqlPool, PgPool, Row};
use std::collections::BTreeMap;

use crate::server::SharedState;

#[derive(Deserialize)]
pub struct SyncReq {
    pub config: JobConfig,
    pub mapping: Option<MappingConfig>,
}

#[derive(Deserialize)]
pub struct SchedulerTasksQuery {
    pub job_id: Option<String>,
}

#[derive(Deserialize)]
pub struct SchedulerSubmitReq {
    pub path: String,
}

pub async fn h_list_tables(
    Query(q): Query<TablesQuery>,
) -> (StatusCode, Json<ApiResp<Vec<String>>>) {
    list_tables(q).await
}

pub async fn h_sync(
    State(state): State<SharedState>,
    Json(body): Json<SyncReq>,
) -> (StatusCode, Json<ApiResp<Value>>) {
    sync_command(state, body.config, body.mapping).await
}

pub async fn h_describe(Query(q): Query<DescribeQuery>) -> (StatusCode, Json<ApiResp<Vec<Value>>>) {
    describe(q).await
}

pub async fn h_gen_mapping(Query(q): Query<GenMapQuery>) -> (StatusCode, Json<ApiResp<Value>>) {
    gen_mapping(q).await
}

pub async fn list_tables(q: TablesQuery) -> (StatusCode, Json<ApiResp<Vec<String>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => match pool {
            RdbmsPool::Postgres(p) => match list_tables_postgres(&p).await {
                Ok(tabs) => api_ok(tabs),
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            },
            RdbmsPool::Mysql(p) => match list_tables_mysql(&p).await {
                Ok(tabs) => api_ok(tabs),
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            },
        },
        Err(e) => api_error(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

pub async fn describe(q: DescribeQuery) -> (StatusCode, Json<ApiResp<Vec<Value>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => {
            let cols_res = match pool {
                RdbmsPool::Postgres(p) => fetch_columns_postgres(&p, &q.table).await,
                RdbmsPool::Mysql(p) => fetch_columns_mysql(&p, &q.table).await,
            };

            match cols_res {
                Ok(cols) => {
                    let v = cols
                        .into_iter()
                        .map(|c| {
                            serde_json::json!({
                                "name": c.name,
                                "data_type": c.data_type,
                                "nullable": c.is_nullable
                            })
                        })
                        .collect::<Vec<_>>();
                    api_ok(v)
                }
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        Err(e) => api_error(StatusCode::BAD_REQUEST, e.to_string()),
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
                    api_ok(obj)
                }
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
        Err(e) => api_error(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

pub async fn sync_command(
    state: SharedState,
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

    let Some(executor) = state.sync_executor.as_ref() else {
        return api_value_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Sync executor is unavailable".to_string(),
        );
    };

    let (status, resp) = executor.execute_sync(cfg).await;
    (status, Json(resp))
}

pub async fn get_scheduler_tasks(
    State(state): State<SharedState>,
    Query(query): Query<SchedulerTasksQuery>,
) -> (StatusCode, Json<ApiResp<Value>>) {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return scheduler_unavailable();
    };

    let (status, resp) = scheduler.query_tasks(query.job_id).await;
    (status, Json(resp))
}

pub async fn post_scheduler_task(
    State(state): State<SharedState>,
    Json(body): Json<SchedulerSubmitReq>,
) -> (StatusCode, Json<ApiResp<Value>>) {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return scheduler_unavailable();
    };

    let (status, resp) = scheduler.submit_task(body.path).await;
    (status, Json(resp))
}

pub async fn post_scheduler_cancel(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<ApiResp<Value>>) {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return scheduler_unavailable();
    };

    let (status, resp) = scheduler.cancel_task(job_id).await;
    (status, Json(resp))
}

pub async fn list_tables_postgres(pool: &PgPool) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query("select tablename from pg_catalog.pg_tables where schemaname not in ('pg_catalog','information_schema')").fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("tablename")?;
        out.push(name);
    }
    Ok(out)
}

pub async fn list_tables_mysql(pool: &MySqlPool) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query("show tables").fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let v: String = r.try_get(0)?;
        out.push(v);
    }
    Ok(out)
}

pub async fn fetch_columns_postgres(pool: &PgPool, table: &str) -> anyhow::Result<Vec<ColInfo>> {
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

pub async fn fetch_columns_mysql(pool: &MySqlPool, table: &str) -> anyhow::Result<Vec<ColInfo>> {
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

pub fn guess_type(sql_type: &str) -> &'static str {
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

fn scheduler_unavailable() -> (StatusCode, Json<ApiResp<Value>>) {
    api_value_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "Scheduler control plane is unavailable".to_string(),
    )
}

fn api_ok<T>(data: T) -> (StatusCode, Json<ApiResp<T>>) {
    (
        StatusCode::OK,
        Json(ApiResp {
            ok: true,
            data: Some(data),
            error: None,
        }),
    )
}

fn api_error<T>(status: StatusCode, error: String) -> (StatusCode, Json<ApiResp<T>>) {
    (
        status,
        Json(ApiResp {
            ok: false,
            data: None,
            error: Some(error),
        }),
    )
}

fn api_value_error(status: StatusCode, error: String) -> (StatusCode, Json<ApiResp<Value>>) {
    api_error(status, error)
}
