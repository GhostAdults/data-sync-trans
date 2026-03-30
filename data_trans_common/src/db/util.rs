//! Database utilities
//!
//! Provides common database utilities including configuration parsing and SQL building.

use anyhow::{bail, Context, Result};
use std::sync::Arc;

use super::pool::{detect_db_kind, get_db_pool, DbKind, DbPool};
use crate::app_config::config_loader::get_config_manager;
use crate::job_config::JobConfig;
use crate::resp::BaseDbQuery;

/// Database parameters trait for resolving connection info
pub trait DbParams {
    fn db_url_opt(&self) -> Option<&str>;
    fn db_type_opt(&self) -> Option<&str>;
    fn task_id_opt(&self) -> Option<&str> {
        None
    }

    fn resolve_url(&self) -> Result<String> {
        if let Some(s) = self.db_url_opt() {
            if !s.is_empty() {
                return Ok(s.to_string());
            }
        }

        let task_id = self.task_id_opt().unwrap_or("default");

        if let Some(mgr) = get_config_manager() {
            let mgr = mgr.read();
            if let Ok(cfg) = JobConfig::from_manager(&mgr, task_id) {
                if let Ok(db_config) = JobConfig::parse_database_config(&cfg.output) {
                    if !db_config.url.is_empty() {
                        return Ok(db_config.url);
                    }
                }
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

        let task_id = self.task_id_opt().unwrap_or("default");

        if let Some(mgr) = get_config_manager() {
            let mgr = mgr.read();
            if let Ok(cfg) = JobConfig::from_manager(&mgr, task_id) {
                return JobConfig::get_source_db_type(&cfg.output);
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
    fn task_id_opt(&self) -> Option<&str> {
        self.task_id.as_deref()
    }
}

/// Get database pool from JobConfig
pub async fn get_pool_from_config(cfg: &JobConfig) -> Result<Arc<DbPool>> {
    let db_config = JobConfig::parse_database_config(&cfg.input)?;
    let db_type_str = JobConfig::get_source_db_type(&cfg.input);

    let kind = detect_db_kind(
        &db_config.url,
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
        get_db_pool(&db_config.url, kind, max_conns, Some(acq_timeout)).await?,
    ))
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
