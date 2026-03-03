use anyhow::{Context, Result};
use futures::stream::Stream;
use serde_json::Value as JsonValue;
use std::pin::Pin;

use crate::util::dbpool::{DbKind, DbPool, get_db_pool};
use crate::core::Config;
use crate::detect_db_kind;

pub async fn get_pool_from_config(cfg: &Config) -> Result<DbPool> {
    let db_config = Config::parse_db_config(&cfg.output)?;
    let db_type_str = Config::get_source_db_type(&cfg.output);

    let kind = detect_db_kind(&db_config.url, db_type_str.as_ref().and_then(|s: &String| {
        if s.eq_ignore_ascii_case("postgres") { Some(DbKind::Postgres) }
        else if s.eq_ignore_ascii_case("mysql") { Some(DbKind::Mysql) }
        else { None }
    })).context("无法识别数据库类型")?;
    let max_conns = db_config.max_connections.unwrap_or(20);
    let acq_timeout = db_config.acquire_timeout_secs.unwrap_or(60);
    get_db_pool(&db_config.url, kind, max_conns, Some(acq_timeout)).await
}

type JsonStream<'a> = Pin<Box<dyn Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>;

pub struct DbRowStream<'a> {
    stream: JsonStream<'a>,
}

impl<'a> DbRowStream<'a> {
    pub fn into_inner(self) -> JsonStream<'a> {
        self.stream
    }
}

pub fn execute_query_stream<'a>(pool: &'a DbPool, sql: &'a str) -> Result<DbRowStream<'a>> {
    use futures::StreamExt;
    use sqlx::{Row, Column};

    let stream: JsonStream<'a> = match pool {
        DbPool::Postgres(pg_pool) => {
            Box::pin(sqlx::query(sql)
                .fetch(pg_pool)
                .map(|result| {
                    result.map(|row: sqlx::postgres::PgRow| {
                        let mut obj = serde_json::Map::new();
                        for (idx, col) in row.columns().iter().enumerate() {
                            let col_name = col.name();
                            let val: Option<String> = row.try_get(idx).ok();
                            obj.insert(
                                col_name.to_string(),
                                val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                            );
                        }
                        JsonValue::Object(obj)
                    })
                }))
        }
        DbPool::Mysql(my_pool) => {
            Box::pin(sqlx::query(sql)
                .fetch(my_pool)
                .map(|result| {
                    result.map(|row: sqlx::mysql::MySqlRow| {
                        let mut obj = serde_json::Map::new();
                        for (idx, col) in row.columns().iter().enumerate() {
                            let col_name = col.name();
                            let val: Option<String> = row.try_get(idx).ok();
                            obj.insert(
                                col_name.to_string(),
                                val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                            );
                        }
                        JsonValue::Object(obj)
                    })
                }))
        }
    };

    Ok(DbRowStream { stream })
}
