//! RDBMS 写入 SQL 构建与执行
//!
//! 提供按 WriteMode + DbKind 生成正确 SQL 的函数组，
//! 以及批量数据准备和执行写入的能力。

use anyhow::{bail, Result};
use std::sync::Arc;

use relus_common::job_config::WriteMode;
use relus_common::types::UnifiedValue;

use super::pool::{DbKind, RdbmsPool};

/// 写入批次数据
pub struct WriteBatch {
    /// SQL 前缀（INSERT INTO t (cols) / UPDATE t SET / DELETE FROM t）
    pub sql: String,
    /// upsert 模式的 VALUES 后缀，其他模式为 None
    pub upsert_suffix: Option<String>,
    pub table_name: String,
    pub rows: Vec<Vec<UnifiedValue>>,
    pub columns: Vec<String>,
}

/// 分离主键列和非主键列
pub fn split_keys_nonkeys(all_cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut nonkeys = Vec::new();
    for col in all_cols {
        if key_cols.contains(col) {
            keys.push(col.clone());
        } else {
            nonkeys.push(col.clone());
        }
    }
    (keys, nonkeys)
}

fn quote_columns(columns: &[String], kind: DbKind) -> Vec<String> {
    match kind {
        DbKind::Postgres => columns.iter().map(|c| format!("\"{}\"", c)).collect(),
        DbKind::Mysql => columns.to_vec(),
    }
}

/// 构建 INSERT SQL（不含 VALUES，由 execute_batch 拼接）
pub fn build_insert_sql(table: &str, columns: &[String], kind: DbKind) -> String {
    let quoted = quote_columns(columns, kind);
    format!("INSERT INTO {} ({})", table, quoted.join(", "))
}

/// Upsert SQL 拆分结果
pub struct UpsertParts {
    /// `INSERT INTO table (cols)`
    pub prefix: String,
    /// `ON DUPLICATE KEY UPDATE ...` 或 `ON CONFLICT (...) DO UPDATE SET ...`
    pub suffix: String,
}

/// 构建 upsert SQL，拆为 prefix + suffix
///
/// execute_batch 拼接顺序：prefix + VALUES (?,?,?) + suffix
pub fn build_upsert_sql(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
    kind: DbKind,
) -> Result<UpsertParts> {
    let quoted = quote_columns(columns, kind);
    let prefix = format!("INSERT INTO {} ({})", table, quoted.join(", "));

    let suffix = match kind {
        DbKind::Postgres => {
            if keys.is_empty() {
                bail!("upsert 模式需要指定 key_columns");
            }
            let quoted_keys: Vec<String> = keys.iter().map(|c| format!("\"{}\"", c)).collect();
            let mut s = format!(" ON CONFLICT ({}) DO UPDATE SET ", quoted_keys.join(", "));
            let sets: Vec<String> = nonkeys
                .iter()
                .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
                .collect();
            s.push_str(&sets.join(", "));
            s
        }
        DbKind::Mysql => {
            let mut s = String::from(" ON DUPLICATE KEY UPDATE ");
            let sets: Vec<String> = nonkeys
                .iter()
                .map(|c| format!("{} = VALUES({})", c, c))
                .collect();
            s.push_str(&sets.join(", "));
            s
        }
    };
    Ok(UpsertParts { prefix, suffix })
}

/// 构建 UPDATE SQL（完整带占位符，走 execute_with_params 逐行执行）
pub fn build_update_sql(
    table: &str,
    nonkeys: &[String],
    keys: &[String],
    kind: DbKind,
) -> Result<String> {
    if keys.is_empty() {
        bail!("update 模式需要指定 key_columns");
    }
    let sets: Vec<String> = match kind {
        DbKind::Postgres => nonkeys
            .iter()
            .map(|c| format!("\"{}\" = {}", c, "?"))
            .collect(),
        DbKind::Mysql => nonkeys
            .iter()
            .map(|c: &String| format!("{} = {}", c, "?"))
            .collect(),
    };
    let wheres: Vec<String> = match kind {
        DbKind::Postgres => keys
            .iter()
            .map(|c| format!("\"{}\" = {}", c, "?"))
            .collect(),
        DbKind::Mysql => keys.iter().map(|c| format!("{} = {}", c, "?")).collect(),
    };
    Ok(format!(
        "UPDATE {} SET {} WHERE {}",
        table,
        sets.join(", "),
        wheres.join(" AND ")
    ))
}

/// 构建 DELETE SQL（完整带占位符，走 execute_with_params 逐行执行）
pub fn build_delete_sql(table: &str, keys: &[String], kind: DbKind) -> Result<String> {
    if keys.is_empty() {
        bail!("delete 模式需要指定 key_columns");
    }
    let wheres: Vec<String> = match kind {
        DbKind::Postgres => keys
            .iter()
            .map(|c| format!("\"{}\" = {}", c, "?"))
            .collect(),
        DbKind::Mysql => keys.iter().map(|c| format!("{} = {}", c, "?")).collect(),
    };
    Ok(format!(
        "DELETE FROM {} WHERE {}",
        table,
        wheres.join(" AND ")
    ))
}

/// 准备写入批次
pub fn prepare_write_batch(
    columns: &[String],
    rows_data: &[Vec<UnifiedValue>],
    table_name: &str,
    key_columns: &[String],
    mode: WriteMode,
    kind: DbKind,
) -> Result<WriteBatch> {
    if rows_data.is_empty() {
        bail!("没有数据需要同步");
    }
    let (keys, nonkeys) = split_keys_nonkeys(columns, key_columns);

    let (sql, upsert_suffix) = match mode {
        WriteMode::Insert => (build_insert_sql(table_name, columns, kind), None),
        WriteMode::Upsert => {
            let parts = build_upsert_sql(table_name, columns, &keys, &nonkeys, kind)?;
            (parts.prefix, Some(parts.suffix))
        }
        WriteMode::Update => (build_update_sql(table_name, &nonkeys, &keys, kind)?, None),
        WriteMode::Delete => (build_delete_sql(table_name, &keys, kind)?, None),
    };

    Ok(WriteBatch {
        sql,
        upsert_suffix,
        table_name: table_name.to_string(),
        rows: rows_data.to_vec(),
        columns: columns.to_vec(),
    })
}

/// 执行批量写入
///
/// Insert/Upsert 走 execute_batch（参数化批量），Update/Delete 走 execute_with_params（逐行）
pub async fn execute_db_write(
    batch: &WriteBatch,
    pool: &Arc<RdbmsPool>,
    batch_size: usize,
) -> Result<usize> {
    if batch.rows.is_empty() {
        return Ok(0);
    }

    let executor = pool.executor();
    let mut processed = 0usize;

    match &batch.upsert_suffix {
        Some(suffix) => {
            // Upsert: execute_batch(base_sql, rows, suffix) → INSERT ... VALUES (?,?,?) ON DUPLICATE KEY UPDATE ...
            for chunk in batch.rows.chunks(batch_size) {
                executor.execute_batch(&batch.sql, chunk, suffix).await?;
                processed += chunk.len();
            }
        }
        None if is_insert_style(&batch.sql) => {
            // Insert: execute_batch(base_sql, rows, "") → INSERT ... VALUES (?,?,?)
            for chunk in batch.rows.chunks(batch_size) {
                executor.execute_batch(&batch.sql, chunk, "").await?;
                processed += chunk.len();
            }
        }
        None => {
            // Update/Delete: 逐行参数化执行
            for r in &batch.rows {
                executor.execute_with_params(&batch.sql, r).await?;
                processed += 1;
            }
        }
    }

    Ok(processed)
}

fn is_insert_style(sql: &str) -> bool {
    sql.trim_start().to_uppercase().starts_with("INSERT")
}

/// Upsert 前置校验：key_columns 对应的列在目标表上必须有 UNIQUE 索引（含 PRIMARY KEY）
///
/// MySQL ON DUPLICATE KEY UPDATE 和 PostgreSQL ON CONFLICT 都要求
/// 冲突列存在唯一约束，否则永远不会触发 UPDATE，导致数据重复。
pub async fn validate_upsert_keys(
    pool: &RdbmsPool,
    table: &str,
    key_columns: &[String],
) -> Result<()> {
    if key_columns.is_empty() {
        bail!("upsert 模式需要指定 key_columns");
    }

    let unique_columns = query_unique_columns(pool, table).await?;
    if unique_columns.is_empty() {
        bail!(
            "目标表 '{}' 没有任何唯一索引或主键，无法执行 upsert。\n\
             请为 key_columns {:?} 中的列创建 UNIQUE 索引，例如：\n\
             ALTER TABLE {} ADD UNIQUE INDEX idx_upsert ({});",
            table,
            key_columns,
            table,
            key_columns.join(", ")
        );
    }

    for kc in key_columns {
        if !unique_columns.contains(kc) {
            bail!(
                "目标表 '{}' 的 key_column '{}' 没有唯一索引。\n\
                 当前的唯一索引列: {:?}\n\
                 请为该列创建 UNIQUE 索引，例如：\n\
                 ALTER TABLE {} ADD UNIQUE INDEX idx_{} ({});",
                table,
                kc,
                unique_columns,
                table,
                kc,
                kc
            );
        }
    }

    Ok(())
}

/// 查询目标表上所有 UNIQUE 约束覆盖的列（含 PRIMARY KEY）
async fn query_unique_columns(pool: &RdbmsPool, table: &str) -> Result<Vec<String>> {
    match pool {
        RdbmsPool::Mysql(p) => {
            let sql = format!(
                "SELECT DISTINCT kcu.COLUMN_NAME \
                 FROM information_schema.TABLE_CONSTRAINTS tc \
                 JOIN information_schema.KEY_COLUMN_USAGE kcu \
                     ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                     AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA \
                     AND tc.TABLE_NAME = kcu.TABLE_NAME \
                 WHERE tc.TABLE_SCHEMA = DATABASE() \
                     AND tc.TABLE_NAME = '{}' \
                     AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')",
                table
            );
            let rows = sqlx::query(&sql).fetch_all(p).await?;
            use sqlx::Row;
            Ok(rows
                .iter()
                .filter_map(|r| r.try_get("COLUMN_NAME").ok())
                .collect())
        }
        RdbmsPool::Postgres(p) => {
            let sql = format!(
                "SELECT DISTINCT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                     ON tc.constraint_name = kcu.constraint_name \
                     AND tc.table_schema = kcu.table_schema \
                 WHERE tc.table_schema = 'public' \
                     AND tc.table_name = '{}' \
                     AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
                table
            );
            let rows = sqlx::query(&sql).fetch_all(p).await?;
            use sqlx::Row;
            Ok(rows
                .iter()
                .filter_map(|r| r.try_get("column_name").ok())
                .collect())
        }
    }
}
