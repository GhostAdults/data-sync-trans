//! Database utilities
//!
//! Provides common database utilities including configuration parsing and SQL building.

use anyhow::{bail, Context, Result};
use std::sync::Arc;

use super::pool::{detect_db_kind, get_db_pool, DbKind, RdbmsPool};
use relus_common::data_source_config::DataSourceConfig;
use relus_common::job_config::JobConfig;
use relus_common::resp::BaseDbQuery;

/// Database parameters trait for resolving connection info
pub trait DbParams {
    fn db_url_opt(&self) -> Option<&str>;
    fn db_type_opt(&self) -> Option<&str>;

    fn resolve_url(&self) -> Result<String> {
        if let Some(s) = self.db_url_opt() {
            if !s.is_empty() {
                return Ok(s.to_string());
            }
        }
        bail!("missing db.url in query or config")
    }

    fn resolve_type(&self) -> Option<String> {
        if let Some(s) = self.db_type_opt() {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
        None
    }
}

impl DbParams for BaseDbQuery {
    fn db_url_opt(&self) -> Option<&str> {
        self.db_url.as_deref()
    }
    fn db_type_opt(&self) -> Option<&str> {
        self.db_type.as_deref()
    }
}

/// Get database pool from DataSourceConfig (input or output)
pub async fn get_pool_for(ds: &DataSourceConfig) -> Result<Arc<RdbmsPool>> {
    let db_config = ds.parse_database_config()?;
    let db_type_str = ds.get_source_db_type();

    let kind = detect_db_kind(
        &db_config.to_url(),
        db_type_str.as_ref().and_then(|s: &String| {
            if s.eq_ignore_ascii_case("postgres") {
                Some(DbKind::Postgres)
            } else if s.eq_ignore_ascii_case("mysql") {
                Some(DbKind::Mysql)
            } else {
                None
            }
        }),
    )
    .context("无法识别数据库类型")?;
    let max_conns = db_config.max_connections.unwrap_or(20);
    let acq_timeout = db_config.acquire_timeout_secs.unwrap_or(60);
    Ok(Arc::new(
        get_db_pool(&db_config.to_url(), kind, max_conns, Some(acq_timeout)).await?,
    ))
}

/// Get database pool from JobConfig source
pub async fn get_pool_from_config(cfg: &JobConfig) -> Result<Arc<RdbmsPool>> {
    get_pool_for(&cfg.source).await
}

/// Get database pool from JobConfig target
pub async fn get_pool_from_output(cfg: &JobConfig) -> Result<Arc<RdbmsPool>> {
    get_pool_for(&cfg.target).await
}

/// Build query SQL with LIMIT and OFFSET
pub fn build_query_sql(
    columns: &str,
    table: &str,
    query: Option<&str>,
    limit: usize,
    offset: usize,
) -> String {
    let query_str = query.filter(|s| !s.trim().is_empty());
    if let Some(custom_query) = query_str {
        format!("{} LIMIT {} OFFSET {}", custom_query, limit, offset)
    } else if limit > 0 || offset > 0 {
        format!(
            "SELECT {} FROM {} LIMIT {} OFFSET {}",
            columns, table, limit, offset
        )
    } else {
        format!("SELECT {} FROM {}", columns, table)
    }
}

pub fn build_select_query(columns: &str, table: &str) -> String {
    format!("SELECT {} FROM {}", columns, table)
}

/// PostgreSQL 列名加双引号，防止保留字冲突
pub fn quote_pg_columns(columns: &str) -> String {
    columns
        .split(',')
        .map(|c| format!("\"{}\"", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn build_select_query_for(columns: &str, table: &str, db_kind: DbKind) -> String {
    let cols = match db_kind {
        DbKind::Postgres => quote_pg_columns(columns),
        DbKind::Mysql => columns.to_string(),
    };
    format!("SELECT {} FROM {}", cols, table)
}

/// Convert option string to DbKind
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

/// Resolve database query trait
pub trait ResolveDbQuery {
    fn resolve_url(&self) -> Result<String>;
    fn resolve_type(&self) -> Option<String>;
}

impl ResolveDbQuery for BaseDbQuery {
    fn resolve_url(&self) -> Result<String> {
        if let Some(url) = self.db_url.as_ref().filter(|s| !s.is_empty()) {
            Ok(url.clone())
        } else {
            bail!("missing db.url in query or config")
        }
    }

    fn resolve_type(&self) -> Option<String> {
        if let Some(t) = &self.db_type {
            if !t.is_empty() {
                return Some(t.clone());
            }
        }
        None
    }
}
