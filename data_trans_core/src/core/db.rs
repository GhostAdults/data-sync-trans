use sqlx::{PgPool, MySqlPool};
use clap::{ValueEnum};
use sqlx::postgres::PgPoolOptions;
use sqlx::mysql::MySqlPoolOptions;
use std::sync::OnceLock;
use anyhow::Result;
use dashmap::DashMap;
use std::time::Duration;

// 数据库连接池包装枚举
#[derive(Clone, Debug)]
pub enum DbPool {
    Postgres(PgPool),
    Mysql(MySqlPool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Hash)]
pub enum DbKind {
    Postgres,
    Mysql,
}

#[derive(Clone, Hash, PartialEq, Eq,Debug)]
pub struct DbConfig {
    pub kind: DbKind,
    pub url: String,
    pub max_conns: u32,
    pub timeout_secs: Option<u64>,
}

use crate::core::server::{DbParams, dbkind_from_opt_str};
use crate::detect_db_kind;

// 全局连接池管理器
static DB_POOLS: OnceLock<DashMap<DbConfig, DbPool>> = OnceLock::new();

async fn create_pool(cfg: &DbConfig) -> Result<DbPool> {
    let timeout = Duration::from_secs(cfg.timeout_secs.unwrap_or(30));
    match cfg.kind {
        DbKind::Postgres => {
            let pool = PgPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect(&cfg.url)
                .await?;
            Ok(DbPool::Postgres(pool))
        }
        DbKind::Mysql => {
            let pool = MySqlPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect(&cfg.url)
                .await?;
            Ok(DbPool::Mysql(pool))
        }
    }
}

pub async fn get_db_pool(url: &str, kind: DbKind, max_conns: u32, timeout_secs: Option<u64>) -> Result<DbPool> {
    let pools = DB_POOLS.get_or_init(|| DashMap::new());
    let key = DbConfig {
        kind,
        url: url.to_string(),
        max_conns,
        timeout_secs,
    };
    if let Some(pool) = pools.get(&key) {
        println!("Current DB Pools count: {}", pools.len());
        return Ok(pool.clone());
    }
    
    // 如果不存在，创建新连接池
    let pool = create_pool(&key).await?;
    println!("create db pool: {:?}", key);
    pools.insert(key, pool.clone());
    Ok(pool)
}

pub async fn get_pool_from_query<T: DbParams>(q: &T) -> Result<DbPool> {
    let db_url = q.resolve_url()?;
    let kind = detect_db_kind(&db_url, dbkind_from_opt_str(&q.resolve_type()))?;
    // 默认连接数5，超时时间默认
    get_db_pool(&db_url, kind, 5, None).await
}


