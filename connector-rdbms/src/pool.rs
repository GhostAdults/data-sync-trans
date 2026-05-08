/// 统一的 RDBMS 连接池
//Database connection pool management
//Provides connection pooling for PostgreSQL and MySQL with caching support.
///
use anyhow::Result;

use async_trait::async_trait;
use chrono::{Local, Offset};
use clap::ValueEnum;
use dashmap::DashMap;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{MySqlPool, PgPool};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use super::util::{database_kind_from_opt_str, DbParams};
use relus_common::types::UnifiedValue;

/// Database type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Hash)]
pub enum DatabaseKind {
    Postgres,
    Mysql,
}

/// Database connection pool wrapper
#[derive(Debug, Clone)]
pub enum RdbmsPool {
    Postgres(PgPool),
    Mysql(MySqlPool),
}

/// Database pool configuration
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct PoolConfig {
    pub kind: DatabaseKind,
    pub url: String,
    pub max_conns: u32,
    pub timeout_secs: Option<u64>,
    pub timezone: String,
}

static DB_POOLS: OnceLock<DashMap<PoolConfig, RdbmsPool>> = OnceLock::new();

async fn create_pool(cfg: &PoolConfig) -> Result<RdbmsPool> {
    let timeout = Duration::from_secs(cfg.timeout_secs.unwrap_or(30));
    match cfg.kind {
        DatabaseKind::Postgres => {
            let connect_options =
                PgConnectOptions::from_str(&cfg.url)?.options([("timezone", cfg.timezone.clone())]);
            let pool = PgPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect_with(connect_options)
                .await?;
            Ok(RdbmsPool::Postgres(pool))
        }
        DatabaseKind::Mysql => {
            let connect_options = MySqlConnectOptions::from_str(&cfg.url)?
                // sqlx 的 MySQL 默认会设置 time_zone='+00:00'。
                // 这里显式使用配置时区；未配置时使用当前系统时区，避免本地时间被按 UTC 解释。
                .timezone(cfg.timezone.clone());
            let pool = MySqlPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect_with(connect_options)
                .await?;
            Ok(RdbmsPool::Mysql(pool))
        }
    }
}

fn current_system_timezone() -> String {
    timezone_offset_string(Local::now().offset().fix().local_minus_utc())
}

fn timezone_offset_string(offset_seconds: i32) -> String {
    let sign = if offset_seconds >= 0 { '+' } else { '-' };
    let abs_seconds = offset_seconds.abs();
    let hours = abs_seconds / 3600;
    let minutes = (abs_seconds % 3600) / 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

/// Get or create a database connection pool
/// 如果存在相同配置的连接池则返回，否则创建新的连接池并缓存
pub async fn get_db_pool(
    url: &str,
    kind: DatabaseKind,
    max_conns: u32,
    timeout_secs: Option<u64>,
    timezone: Option<String>,
) -> Result<RdbmsPool> {
    let pools: &DashMap<PoolConfig, RdbmsPool> = DB_POOLS.get_or_init(DashMap::new);
    let timezone = timezone.unwrap_or_else(current_system_timezone);
    let key = PoolConfig {
        kind,
        url: url.to_string(),
        max_conns,
        timeout_secs,
        timezone,
    };
    if let Some(pool) = pools.get(&key) {
        return Ok(pool.clone());
    }

    let pool = create_pool(&key).await?;
    pools.insert(key, pool.clone());
    Ok(pool)
}

/// Detect database type from URL or explicit option
pub fn detect_database_kind(url: &str, explicit: Option<DatabaseKind>) -> Result<DatabaseKind> {
    if let Some(k) = explicit {
        return Ok(k);
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(DatabaseKind::Postgres)
    } else if url.starts_with("mysql://") {
        Ok(DatabaseKind::Mysql)
    } else {
        anyhow::bail!(
            "无法识别数据库类型，需提供 db_type 或使用以 postgres:// 或 mysql:// 开头的连接串"
        )
    }
}

/// Get pool from DbParams query
pub async fn get_pool_from_query<T: DbParams>(q: &T) -> Result<RdbmsPool> {
    let db_url = q.resolve_url()?;
    let kind = detect_database_kind(&db_url, database_kind_from_opt_str(&q.resolve_type()))?;
    get_db_pool(&db_url, kind, 5, None, None).await
}

/// 查询列值的动态类型承载
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int(i64),
    Float(f64),
    Text(String),
    Null,
}

impl ColumnValue {
    pub fn into_string(self) -> Option<String> {
        match self {
            ColumnValue::Int(n) => Some(n.to_string()),
            ColumnValue::Float(n) => Some(n.to_string()),
            ColumnValue::Text(s) => Some(s),
            ColumnValue::Null => None,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, ColumnValue::Int(_) | ColumnValue::Float(_))
    }
}

/// Database executor trait for common operations
#[async_trait]
pub trait DatabaseExecutor: Send + Sync {
    // 返回两列字符串的查询结果
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)>;
    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>>;

    /// 泛型查询：返回两列的动态类型值
    async fn fetch_column_pair(
        &self,
        sql: &str,
    ) -> Result<(Option<ColumnValue>, Option<ColumnValue>)>;

    async fn execute(&self, sql: &str) -> Result<u64>;

    /// 执行带参数的 SQL
    async fn execute_with_params(&self, sql: &str, params: &[UnifiedValue]) -> Result<u64>;
}

/// PostgreSQL executor reference
pub struct PgExecutorRef {
    pool: sqlx::PgPool,
}

#[async_trait]
impl DatabaseExecutor for PgExecutorRef {
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)> {
        let row: (Option<String>, Option<String>) =
            sqlx::query_as(sql).fetch_one(&self.pool).await?;
        Ok(row)
    }

    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>> {
        let result: Option<(String,)> = sqlx::query_as(sql).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn fetch_column_pair(
        &self,
        sql: &str,
    ) -> Result<(Option<ColumnValue>, Option<ColumnValue>)> {
        let row = sqlx::query(sql).fetch_one(&self.pool).await?;
        Ok((pg_column_value(&row, 0), pg_column_value(&row, 1)))
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    async fn execute_with_params(&self, sql: &str, params: &[UnifiedValue]) -> Result<u64> {
        let mut query = sqlx::query(sql);
        for p in params {
            query = bind_typed_val_pg(query, p);
        }
        let result = query.execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

fn pg_column_value(row: &sqlx::postgres::PgRow, idx: usize) -> Option<ColumnValue> {
    use sqlx::Row;
    if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
        return Some(ColumnValue::Int(v));
    }
    if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
        return Some(ColumnValue::Float(v));
    }
    if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
        return Some(ColumnValue::Text(v));
    }
    None
}

fn mysql_column_value(row: &sqlx::mysql::MySqlRow, idx: usize) -> Option<ColumnValue> {
    use sqlx::Row;
    if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
        return Some(ColumnValue::Int(v));
    }
    if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
        return Some(ColumnValue::Float(v));
    }
    if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
        return Some(ColumnValue::Text(v));
    }
    None
}

fn bind_typed_val_pg<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: &UnifiedValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        UnifiedValue::Int(n) => q = q.bind(*n),
        UnifiedValue::Float(n) => q = q.bind(*n),
        UnifiedValue::Bool(b) => q = q.bind(*b),
        UnifiedValue::Decimal(d) => q = q.bind(*d),
        UnifiedValue::OptI64(n) => q = q.bind(*n),
        UnifiedValue::OptF64(n) => q = q.bind(*n),
        UnifiedValue::OptBool(n) => q = q.bind(*n),
        UnifiedValue::OptDecimal(n) => q = q.bind(*n),
        UnifiedValue::OptDateTime(n) => q = q.bind(*n),
        UnifiedValue::DateTime(n) => q = q.bind(*n),
        UnifiedValue::String(s) => q = q.bind(s.clone()),
        UnifiedValue::Null => q = q.bind(Option::<String>::None),
        UnifiedValue::Bytes(b) => q = q.bind(b.clone()),
        UnifiedValue::Date(d) => q = q.bind(*d),
        UnifiedValue::Time(t) => q = q.bind(*t),
        UnifiedValue::Json(j) => q = q.bind(j.to_string()),
        UnifiedValue::Array(_) => q = q.bind(String::new()), // Arrays not directly supported
    }
    q
}

/// MySQL executor reference
pub struct MySqlExecutorRef {
    pool: sqlx::MySqlPool,
}

#[async_trait]
impl DatabaseExecutor for MySqlExecutorRef {
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)> {
        let row: (Option<String>, Option<String>) =
            sqlx::query_as(sql).fetch_one(&self.pool).await?;
        Ok(row)
    }

    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>> {
        let result: Option<(String,)> = sqlx::query_as(sql).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn fetch_column_pair(
        &self,
        sql: &str,
    ) -> Result<(Option<ColumnValue>, Option<ColumnValue>)> {
        let row = sqlx::query(sql).fetch_one(&self.pool).await?;
        Ok((mysql_column_value(&row, 0), mysql_column_value(&row, 1)))
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    async fn execute_with_params(&self, sql: &str, params: &[UnifiedValue]) -> Result<u64> {
        let mut query = sqlx::query(sql);
        for p in params {
            query = bind_typed_val_my(query, p);
        }
        let result = query.execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

fn bind_typed_val_my<'q>(
    mut q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    v: &UnifiedValue,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match v {
        UnifiedValue::Int(n) => q = q.bind(*n),
        UnifiedValue::Float(n) => q = q.bind(*n),
        UnifiedValue::Bool(b) => q = q.bind(*b),
        UnifiedValue::Decimal(d) => q = q.bind(*d),
        UnifiedValue::OptI64(n) => q = q.bind(*n),
        UnifiedValue::OptF64(n) => q = q.bind(*n),
        UnifiedValue::OptBool(n) => q = q.bind(*n),
        UnifiedValue::OptDecimal(n) => q = q.bind(*n),
        UnifiedValue::OptDateTime(n) => q = q.bind(*n),
        UnifiedValue::DateTime(n) => q = q.bind(*n),
        UnifiedValue::String(s) => q = q.bind(s.clone()),
        UnifiedValue::Null => q = q.bind(Option::<String>::None),
        UnifiedValue::Bytes(b) => q = q.bind(b.clone()),
        UnifiedValue::Date(d) => q = q.bind(*d),
        UnifiedValue::Time(t) => q = q.bind(*t),
        UnifiedValue::Json(j) => q = q.bind(j.to_string()),
        UnifiedValue::Array(_) => q = q.bind(String::new()), // Arrays not directly supported
    }
    q
}

impl RdbmsPool {
    /// Get executor for database operations
    pub fn executor(&self) -> Box<dyn DatabaseExecutor> {
        match self {
            RdbmsPool::Postgres(p) => Box::new(PgExecutorRef { pool: p.clone() }),
            RdbmsPool::Mysql(p) => Box::new(MySqlExecutorRef { pool: p.clone() }),
        }
    }
    pub fn db_type(&self) -> &str {
        match self {
            Self::Postgres(_) => "postgres",
            Self::Mysql(_) => "mysql",
        }
    }

    pub async fn from_url(url: &str) -> Result<Self> {
        if url.starts_with("postgres") {
            let pool = PgPool::connect(url).await?;
            Ok(Self::Postgres(pool))
        } else if url.starts_with("mysql") {
            let pool = MySqlPool::connect(url).await?;
            Ok(Self::Mysql(pool))
        } else {
            Err(anyhow::anyhow!("不支持的数据库协议: {}", url))
        }
    }
}
