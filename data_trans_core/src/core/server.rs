use axum::{Json};
use axum::http::StatusCode;
use std::result::Result::{Ok, Err};
use serde_json::Value;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use chrono::NaiveDateTime;

use crate::core::{ApiResp};
use crate::core::db::{DbKind, DbPool, get_db_pool, get_pool_from_query};
use crate::core::{Config, ColInfo, TypedVal};
use crate::core::client_tool::fetch_json;
use anyhow::{Context, Result};
use anyhow::*;
use crate::{detect_db_kind,get_config_manager};
use sqlx::{Row, PgPool, MySqlPool};
use super::{TablesQuery, DescribeQuery, GenMapQuery, SyncReq, BaseDbQuery,CreateConfigReq,UpdateConfigReq, MappingConfig};
use crate::app_config::value::ConfigValue;

use std::collections::HashMap;

pub trait DbParams {
    fn db_url_opt(&self) -> Option<&str>;
    fn db_type_opt(&self) -> Option<&str>;
    fn task_id_opt(&self) -> Option<&str> { None }

    fn resolve_url(&self) -> Result<String> {
        if let Some(s) = self.db_url_opt() {
            if !s.is_empty() { return Ok(s.to_string()); }
        }
        
        let task_id = self.task_id_opt().unwrap_or("default");

        if let Some(mgr) = get_config_manager() {
            let mgr = mgr.read();
            if let Ok(cfg) = Config::from_manager(&mgr, task_id) {
                if !cfg.db.url.is_empty() {
                     return Ok(cfg.db.url);
                }
            }
        }
        bail!("missing db.url in query or config")
    }

    fn resolve_type(&self) -> Option<String> {
        if let Some(s) = self.db_type_opt() {
            if !s.is_empty() { return Some(s.to_string()); }
        }
        
        let task_id = self.task_id_opt().unwrap_or("default");

        if let Some(mgr) = crate::get_config_manager() {
             let mgr = mgr.read();
             if let Ok(cfg) = Config::from_manager(&mgr, task_id) {
                 return cfg.db_type;
             }
        }
        None
    }
}

impl DbParams for BaseDbQuery {
    fn db_url_opt(&self) -> Option<&str> { self.db_url.as_deref() }
    fn db_type_opt(&self) -> Option<&str> { self.db_type.as_deref() }
    fn task_id_opt(&self) -> Option<&str> { self.task_id.as_deref() }
}

pub fn dbkind_from_opt_str(s: &Option<String>) -> Option<DbKind> {
    s.as_ref().and_then(|v| {
        if v.eq_ignore_ascii_case("postgres") {
            Some(DbKind::Postgres)
        } else if v.eq_ignore_ascii_case("mysql") {
            Some(DbKind::Mysql)
        } else {
            None
        }
    })
}


fn extract_by_path<'a>(root: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path == "/" || path.is_empty() {
        return Some(root);
    }
    if path.starts_with('/') {
        return root.pointer(path);
    }
    let mut cur = root;
    for seg in path.split('.') {
        match cur {
            JsonValue::Object(map) => {
                cur = map.get(seg)?;
            }
            JsonValue::Array(arr) => {
                if let Ok(idx) = seg.parse::<usize>() {
                    cur = arr.get(idx)?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn to_typed_value(v: &JsonValue, ty: Option<&str>) -> Result<TypedVal> {
    match ty.unwrap_or("text") {
        "int" => {
            if v.is_null() {
                return Ok(TypedVal::OptI64(None));
            }
            if let Some(n) = v.as_i64() {
                Ok(TypedVal::I64(n))
            } else if let Some(s) = v.as_str() {
                let n = s.parse::<i64>()?;
                Ok(TypedVal::I64(n))
            } else {
                bail!("无法转换为 int: {}", v)
            }
        }
        "float" => {
            if v.is_null() {
                return Ok(TypedVal::OptF64(None));
            }
            if let Some(n) = v.as_f64() {
                Ok(TypedVal::F64(n))
            } else if let Some(s) = v.as_str() {
                let n = s.parse::<f64>()?;
                Ok(TypedVal::F64(n))
            } else {
                bail!("无法转换为 float: {}", v)
            }
        }
        "bool" => {
            if v.is_null() {
                Ok(TypedVal::OptBool(None))
            } else if let Some(b) = v.as_bool() {
                Ok(TypedVal::Bool(b))
            } else if let Some(s) = v.as_str() {
                let b = match s {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    _ => bail!("无法转换为 bool: {}", s),
                };
                Ok(TypedVal::Bool(b))
            } else {
                bail!("无法转换为 bool: {}", v)
            }
        }
        "json" => {
            let s = serde_json::to_string(v)?;
            Ok(TypedVal::Text(Some(s)))
        }
        "timestamp" => {
            if v.is_null() {
                return Ok(TypedVal::OptNaiveTs(None));
            }
            if let Some(s) = v.as_str() {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    Ok(TypedVal::OptNaiveTs(Some(dt)))
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                    Ok(TypedVal::OptNaiveTs(Some(dt)))
                } else {
                    bail!("无法解析时间: {}", s)
                }
            } else {
                bail!("无法转换为 timestamp: {}", v)
            }
        }
        _ => {
            if v.is_null() {
                Ok(TypedVal::Text(None))
            } else if let Some(s) = v.as_str() {
                Ok(TypedVal::Text(Some(s.to_string())))
            } else {
                Ok(TypedVal::Text(Some(v.to_string())))
            }
        }
    }
}

fn build_placeholders(kind: DbKind, count: usize, start: usize) -> String {
    match kind {
        DbKind::Postgres => {
            let mut v = Vec::with_capacity(count);
            for i in 0..count {
                v.push(format!("${}", start + i));
            }
            v.join(", ")
        }
        DbKind::Mysql => vec!["?"; count].join(", "),
    }
}

fn split_keys_nonkeys(cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut nonkeys = Vec::new();
    for c in cols {
        if key_cols.contains(c) {
            keys.push(c.clone());
        } else {
            nonkeys.push(c.clone());
        }
    }
    (keys, nonkeys)
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
        out.push(ColInfo { name, data_type: ty, is_nullable: is_nullable == "YES" });
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
        out.push(ColInfo { name, data_type: ty, is_nullable: is_nullable == "YES" });
    }
    Ok(out)
}

fn guess_type(sql_type: &str) -> &'static str {
    let t = sql_type.to_ascii_lowercase();
    if t.contains("int") {
        "int"
    } else if t.contains("numeric") || t.contains("decimal") || t.contains("double") || t.contains("real") || t.contains("float") {
        "float"
    } else if t.contains("bool") {
        "bool"
    } else if t.contains("timestamp") || t == "datetime" || t == "date" || t == "time" {
        "timestamp"
    } else {
        "text"
    }
}

pub async fn get_pool_from_config(cfg: &Config) -> Result<DbPool> {
    let kind = detect_db_kind(&cfg.db.url, cfg.db_type.as_ref().and_then(|s| {
        if s.eq_ignore_ascii_case("postgres") { Some(DbKind::Postgres) }
        else if s.eq_ignore_ascii_case("mysql") { Some(DbKind::Mysql) }
        else { None }
    })).context("无法识别数据库类型")?;
    let max_conns = cfg.db.max_connections.unwrap_or(20);
    let acq_timeout = cfg.db.acquire_timeout_secs.unwrap_or(60);
    get_db_pool(&cfg.db.url, kind, max_conns, Some(acq_timeout)).await
}

// 同步数据函数
pub async fn sync(cfg: &Config, pool: &DbPool) -> Result<()> {
    let kind = match pool {
        DbPool::Postgres(_) => DbKind::Postgres,
        DbPool::Mysql(_) => DbKind::Mysql,
    };
    let use_tx = cfg.db.use_transaction.unwrap_or(true);
    let (is_pg, pg_pool, my_pool) = match pool {
        DbPool::Postgres(p) => (true, Some(p), None),
        DbPool::Mysql(p) => (false, None, Some(p)),
    };
    let resp = fetch_json(&cfg.api).await?;
    let items = if let Some(p) = &cfg.api.items_json_path {
        let v = extract_by_path(&resp, p).context("未找到 items_json_path 指定的数据")?;
        match v {
            JsonValue::Array(a) => a.clone(),
            _ => bail!("items_json_path 需指向数组"),
        }
    } else {
        match &resp {
            JsonValue::Array(a) => a.clone(),
            JsonValue::Object(o) => {
                let mut arr = None;
                for (_k, v) in o {
                    if let JsonValue::Array(a) = v {
                        arr = Some(a.clone());
                        break;
                    }
                }
                arr.context("响应顶层不是数组，且未提供 items_json_path")?
            }
            _ => bail!("响应不是数组或对象"),
        }
    };
    let cols: Vec<String> = cfg.column_mapping.keys().cloned().collect();
    let key_cols = cfg.db.key_columns.clone().unwrap_or_default();
    let (keys, nonkeys) = split_keys_nonkeys(&cols, &key_cols);
    let mode = cfg.mode.as_deref().unwrap_or("insert");
    let batch_size = cfg.batch_size.unwrap_or(10);
    if use_tx && is_pg {
        let pg_pool = pg_pool.unwrap();
        let mut start = 0usize;
        while start < items.len() {
            let end = (start + batch_size).min(items.len());
            let mut tx = pg_pool.begin().await?;
            for item in items[start..end].iter() {
                let mut values = Vec::<TypedVal>::new();
                for c in &cols {
                    let p = cfg.column_mapping.get(c).unwrap();
                    let v = extract_by_path(item, p).unwrap_or(&JsonValue::Null);
                    let t = cfg.column_types.as_ref().and_then(|m| m.get(c)).map(|s| s.as_str());
                    let tv = to_typed_value(v, t)?;
                    values.push(tv);
                }
                let mut sql = String::new();
                sql.push_str("insert into ");
                sql.push_str(&cfg.db.table);
                sql.push_str(" (");
                sql.push_str(&cols.join(", "));
                sql.push_str(") values (");
                sql.push_str(&build_placeholders(kind, cols.len(), 1));
                sql.push(')');
                if mode == "upsert" {
                    if !keys.is_empty() {
                        sql.push_str(" on conflict (");
                        sql.push_str(&keys.join(", "));
                        sql.push_str(") do update set ");
                        let mut sets = Vec::new();
                        for c in &nonkeys {
                            sets.push(format!("{} = excluded.{}", c, c));
                        }
                        sql.push_str(&sets.join(", "));
                    }
                }
                let mut q = sqlx::query(&sql);
                for v in values {
                    match v {
                        TypedVal::I64(n) => q = q.bind(n),
                        TypedVal::F64(n) => q = q.bind(n),
                        TypedVal::Bool(b) => q = q.bind(b),
                        TypedVal::OptI64(n) => q = q.bind(n),
                        TypedVal::OptF64(n) => q = q.bind(n),
                        TypedVal::OptBool(n) => q = q.bind(n),
                        TypedVal::OptNaiveTs(n) => q = q.bind(n),
                        TypedVal::Text(s) => q = q.bind(s),
                    }
                }
                q.execute(&mut *tx).await?;
            }
            tx.commit().await?;
            start = end;
            println!("{:?}", format!("已同步 {} 条数据", end));
        }
    } else if use_tx && !is_pg {
        let my_pool = my_pool.unwrap();
        let mut start = 0usize;
        while start < items.len() {
            let end = (start + batch_size).min(items.len());
            let mut tx = my_pool.begin().await?;
            for item in items[start..end].iter() {
                let mut values = Vec::<TypedVal>::new();
                for c in &cols {
                    let p = cfg.column_mapping.get(c).unwrap();
                    let v = extract_by_path(item, p).unwrap_or(&JsonValue::Null);
                    let t = cfg.column_types.as_ref().and_then(|m| m.get(c)).map(|s| s.as_str());
                    let tv = to_typed_value(v, t)?;
                    values.push(tv);
                }
                let mut sql = String::new();
                sql.push_str("insert into ");
                sql.push_str(&cfg.db.table);
                sql.push_str(" (");
                sql.push_str(&cols.join(", "));
                sql.push_str(") values (");
                sql.push_str(&build_placeholders(kind, cols.len(), 1));
                sql.push(')');
                if mode == "upsert" {
                    sql.push_str(" on duplicate key update ");
                    let mut sets = Vec::new();
                    for c in &nonkeys {
                        sets.push(format!("{} = values({})", c, c));
                    }
                    sql.push_str(&sets.join(", "));
                }
                let mut q = sqlx::query(&sql);
                for v in values {
                    match v {
                        TypedVal::I64(n) => q = q.bind(n),
                        TypedVal::F64(n) => q = q.bind(n),
                        TypedVal::Bool(b) => q = q.bind(b),
                        TypedVal::OptI64(n) => q = q.bind(n),
                        TypedVal::OptF64(n) => q = q.bind(n),
                        TypedVal::OptBool(n) => q = q.bind(n),
                        TypedVal::OptNaiveTs(n) => q = q.bind(n),
                        TypedVal::Text(s) => q = q.bind(s),
                    }
                }
                q.execute(&mut *tx).await?;
            }
            tx.commit().await?;
            start = end;
        }
    } 
    Ok(())
}

pub async fn list_tables(q: TablesQuery) -> (StatusCode, Json<ApiResp<Vec<String>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => match pool {
            DbPool::Postgres(p) => match list_tables_postgres(&p).await {
                Ok(tabs) => (StatusCode::OK, Json(ApiResp { ok: true, data: Some(tabs), error: None})),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
            },
            DbPool::Mysql(p) => match list_tables_mysql(&p).await {
                Ok(tabs) => (StatusCode::OK, Json(ApiResp { ok: true, data: Some(tabs), error: None})),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
            },
        },
        Err(e) => (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
    }
}

pub async fn describe(q: DescribeQuery) -> (StatusCode, Json<ApiResp<Vec<Value>>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => match pool {
            DbPool::Postgres(p) => match fetch_columns_postgres(&p, &q.table).await {
                Ok(cols) => {
                    let v = cols.into_iter().map(|c| serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})).collect::<Vec<_>>();
                    (StatusCode::OK, Json(ApiResp { ok: true, data: Some(v), error: None }))
                }
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
            },
            DbPool::Mysql(p) => match fetch_columns_mysql(&p, &q.table).await {
                Ok(cols) => {
                    let v = cols.into_iter().map(|c| serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})).collect::<Vec<_>>();
                    (StatusCode::OK, Json(ApiResp { ok: true, data: Some(v), error: None }))
                }
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
            },
        },
        Err(e) => (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
    }
}

pub async fn gen_mapping(q: GenMapQuery) -> (StatusCode, Json<ApiResp<Value>>) {
    match get_pool_from_query(&q.base).await {
        Ok(pool) => {
            let cols_res = match pool {
                DbPool::Postgres(p) => fetch_columns_postgres(&p, &q.table).await,
                DbPool::Mysql(p) => fetch_columns_mysql(&p, &q.table).await,
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
                    (StatusCode::OK, Json(ApiResp { ok: true, data: Some(obj), error: None }))
                }
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
    }
}

pub async fn sync_command(task_id: String, mapping: MappingConfig) -> (StatusCode, Json<ApiResp<Value>>) {
    let mgr_arc = match crate::get_config_manager() {
        Some(m) => m,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some("ConfigManager not initialized".to_string()) })),
    };

    let mut cfg = match Config::from_manager(&mgr_arc.read(), &task_id) {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
    };

    // Override with mapping
    cfg.column_mapping = mapping.column_mapping;
    cfg.column_types = Some(mapping.column_types);
    if let Some(m) = mapping.mode {
        cfg.mode = Some(m);
    }
    if let Some(k) = mapping.key_columns {
        cfg.db.key_columns = Some(k);
    }
    
    // Get pool from config
    let pool = match get_pool_from_config(&cfg).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
    };

    match sync(&cfg, &pool).await {
        Ok(()) => (StatusCode::OK, Json(ApiResp { ok: true, data: Some(serde_json::json!({})), error: None })),
        Err(e) => (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(format!("数据同步错误: {}", e)) })),
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
                         return (StatusCode::BAD_REQUEST, Json(ApiResp { ok: false, data: None, error: Some(format!("Task {} already exists", req.task_id)) }));
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
            Ok(_) => (StatusCode::OK, Json(ApiResp { ok: true, data: None, error: None })),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
        }
    } else {
         (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some("ConfigManager not initialized".to_string()) }))
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
             return (StatusCode::NOT_FOUND, Json(ApiResp { ok: false, data: None, error: Some(format!("Task {} not found", req.task_id)) }));
        }

        match mgr.set("tasks", ConfigValue::Array(tasks)) {
            Ok(_) => (StatusCode::OK, Json(ApiResp { ok: true, data: None, error: None })),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some(e.to_string()) })),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResp { ok: false, data: None, error: Some("ConfigManager not initialized".to_string()) }))
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
            let target_val = target.entry(k.clone()).or_insert_with(|| ConfigValue::String("".to_string()));
            
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
    map.entry("db_type".to_string()).or_insert(ConfigValue::String("".to_string()));
    map.entry("mode".to_string()).or_insert(ConfigValue::String("insert".to_string()));
    map.entry("batch_size".to_string()).or_insert(ConfigValue::Int(100));
    map.entry("column_mapping".to_string()).or_insert_with(|| ConfigValue::Object(HashMap::new()));
    map.entry("column_types".to_string()).or_insert_with(|| ConfigValue::Object(HashMap::new()));

    // DB defaults
    let db = map.entry("db".to_string()).or_insert_with(|| ConfigValue::Object(HashMap::new()));
    if let ConfigValue::Object(db_map) = db {
        db_map.entry("url".to_string()).or_insert(ConfigValue::String("".to_string()));
        db_map.entry("table".to_string()).or_insert(ConfigValue::String("".to_string()));
        db_map.entry("key_columns".to_string()).or_insert_with(|| ConfigValue::Array(Vec::new()));
        db_map.entry("max_connections".to_string()).or_insert(ConfigValue::Int(20));
        db_map.entry("acquire_timeout_secs".to_string()).or_insert(ConfigValue::Int(60));
        db_map.entry("use_transaction".to_string()).or_insert(ConfigValue::Bool(true));
    }

    // API defaults
    let api = map.entry("api".to_string()).or_insert_with(|| ConfigValue::Object(HashMap::new()));
    if let ConfigValue::Object(api_map) = api {
        api_map.entry("url".to_string()).or_insert(ConfigValue::String("".to_string()));
        api_map.entry("method".to_string()).or_insert(ConfigValue::String("GET".to_string()));
        api_map.entry("headers".to_string()).or_insert_with(|| ConfigValue::Object(HashMap::new()));
    }
}
