use crate::rdbms_reader_util::rdbms_reader::count_total_records;
use crate::rdbms_reader_util::rdbms_reader::RdbmsJob;
use anyhow::Result;
use relus_common::constant::key::SPLIT_FACTOR;
use crate::{ReadTask, SplitReaderResult, StreamMode};
use relus_connector_rdbms::pool::{ColumnValue, DbKind, RdbmsPool};
use relus_connector_rdbms::util::{build_select_query_for, get_pool_from_config};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

/// Reader 切分工具
/// 如果need_split_table为true，优先尝试主键切分；如果失败则回退到LIMIT/OFFSET切分；如果不需要切分，则构建单个任务。
/// has_split_pk: 是否存在可用的主键用于切分
/// each_table_should_split: 每个表建议切分的片数 (advice_number / table_number)
pub async fn do_split(rdbms_job: &RdbmsJob, advice_number: usize) -> SplitReaderResult {
    let config = &rdbms_job.config;

    let pool = match get_pool_from_config(&rdbms_job.original_config).await {
        Ok(p) => p,
        Err(e) => {
            warn!("获取数据库连接池失败: {:?}", e);
            return SplitReaderResult {
                total_records: 0,
                stream_mode: StreamMode::Batch,
                tasks: vec![],
            };
        }
    };
    // 计算总行数
    let total_records = match count_total_records(
        &pool,
        &config.table,
        config
            .query_sql
            .as_ref()
            .and_then(|v| v.first())
            .map(|s| s.as_str()),
    )
    .await
    {
        Ok(count) => count,
        Err(_) => 0,
    };
    let data_source_config = &rdbms_job.original_config.source.config;
    let conns: Vec<JsonValue> = data_source_config
        .get("connections")
        .and_then(|v| v.as_array())
        .cloned()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| JsonValue::String(s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let db_kind = match pool.as_ref() {
        RdbmsPool::Postgres(_) => DbKind::Postgres,
        RdbmsPool::Mysql(_) => DbKind::Mysql,
    };

    let mut tasks = Vec::new();

    if config.is_table_mode {
        let table_number = config.table_count.max(1);
        let mut each_table_should_split =
            calculate_each_table_split_number(advice_number, table_number);

        let has_split_pk = config
            .split_pk
            .as_ref()
            .map(|pk| !pk.trim().is_empty())
            .unwrap_or(false);

        let need_split_table = each_table_should_split > 1 && has_split_pk;

        if need_split_table {
            if config.table_count == 1usize {
                // split_factor是切分倍数,如果只有一张表，且需要切分，则可以适当增加切分片数以提高并行度
                let split_factor = config.split_factor.unwrap_or(SPLIT_FACTOR);
                each_table_should_split = each_table_should_split * split_factor;
            }
            // 进行单表的切分
            match split_by_pk(
                &pool,
                db_kind,
                config,
                config.split_pk.as_ref().unwrap(),
                each_table_should_split,
                &conns,
            )
            .await
            {
                Ok(split_tasks) => tasks.extend(split_tasks),
                Err(e) => {
                    warn!("主键切分失败，回退到 LIMIT/OFFSET 模式: {:?}", e);
                    tasks = split_by_limit_offset(
                        config,
                        db_kind,
                        each_table_should_split,
                        total_records,
                        &conns,
                    );
                }
            }
        } else {
            tasks = build_single_task(config, db_kind, &conns);
        }
        // 直接使用sql 构建
    } else if let Some(sqls) = &config.query_sql {
        for (i, sql) in sqls.iter().enumerate() {
            if sql.trim().is_empty() {
                continue;
            }
            let conn = conns.get(i).cloned().unwrap_or_else(|| JsonValue::Null);
            tasks.push(ReadTask {
                task_id: i,
                conn,
                query_sql: Some(sql.clone()),
                offset: 0,
                limit: 0,
            });
        }
    }
    info!(
        "sql{}",
        tasks
            .iter()
            .map(|t| format!(
                "[task_id: {}, sql: {}]",
                t.task_id,
                t.query_sql.as_deref().unwrap_or("")
            ))
            .collect::<Vec<String>>()
            .join(", ")
    );
    SplitReaderResult {
        total_records,
        tasks,
        stream_mode: StreamMode::Batch,
    }
}

/// 计算每个表应该分多少片
/// 公式: eachTableShouldSplittedNumber = adviceNumber / tableNumber
fn calculate_each_table_split_number(advice_number: usize, table_number: usize) -> usize {
    if table_number == 0 {
        return advice_number;
    }
    (advice_number as f64 / table_number as f64).ceil() as usize
}

fn build_single_task(
    config: &crate::RdbmsConfig,
    db_kind: DbKind,
    conns: &[JsonValue],
) -> Vec<ReadTask> {
    let query_sql = match &config.where_clause {
        Some(w) => format!(
            "SELECT {} FROM {} WHERE {}",
            config.columns, config.table, w
        ),
        None => build_select_query_for(config.columns.as_str(), config.table.as_str(), db_kind),
    };

    let conn = conns.first().cloned().unwrap_or_else(|| JsonValue::Null);
    vec![ReadTask {
        task_id: 0,
        conn,
        query_sql: Some(query_sql),
        offset: 0,
        limit: 0,
    }]
}

async fn split_by_pk(
    pool: &RdbmsPool,
    db_kind: DbKind,
    config: &crate::RdbmsConfig,
    split_pk: &str,
    shard_count: usize,
    conns: &[JsonValue],
) -> Result<Vec<ReadTask>> {
    let (min_cv, max_cv) = get_pk_range(
        pool,
        &config.table,
        split_pk,
        config.where_clause.as_deref(),
    )
    .await?;

    let is_numeric = min_cv.as_ref().map_or(false, |v| v.is_numeric())
        || max_cv.as_ref().map_or(false, |v| v.is_numeric());

    let min_str = min_cv.and_then(|v| v.into_string());
    let max_str = max_cv.and_then(|v| v.into_string());

    let mut tasks = Vec::new();

    let ranges = if is_numeric {
        split_by_numeric_pk(
            min_str.as_deref(),
            max_str.as_deref(),
            shard_count,
            split_pk,
        )
    } else {
        split_by_string_pk(
            min_str.as_deref(),
            max_str.as_deref(),
            shard_count,
            split_pk,
        )
    };

    for (i, range) in ranges.iter().enumerate() {
        let query_sql = build_range_query_sql(
            &config.table,
            &config.columns,
            config.where_clause.as_deref(),
            range,
            db_kind,
        );
        let conn = conns
            .get(i % conns.len().max(1))
            .cloned()
            .unwrap_or_else(|| JsonValue::Null);
        tasks.push(ReadTask {
            task_id: i,
            conn,
            query_sql: Some(query_sql),
            offset: 0,
            limit: 0,
        });
    }

    let null_sql = build_null_pk_query_sql(
        &config.table,
        &config.columns,
        config.where_clause.as_deref(),
        split_pk,
        db_kind,
    );
    let conn = conns.first().cloned().unwrap_or_else(|| JsonValue::Null);
    tasks.push(ReadTask {
        task_id: tasks.len(),
        conn,
        query_sql: Some(null_sql),
        offset: 0,
        limit: 0,
    });

    Ok(tasks)
}

fn split_by_limit_offset(
    config: &crate::RdbmsConfig,
    db_kind: DbKind,
    shard_count: usize,
    total_records: usize,
    conns: &[JsonValue],
) -> Vec<ReadTask> {
    if total_records == 0 || shard_count == 0 {
        return vec![];
    }

    let shard_size = (total_records + shard_count - 1) / shard_count;
    let actual_count = (total_records + shard_size - 1) / shard_size;

    let mut tasks = Vec::with_capacity(actual_count);
    for i in 0..actual_count {
        let offset = i * shard_size;
        let remaining = total_records.saturating_sub(offset);
        let limit = shard_size.min(remaining);
        let query_sql = build_limit_offset_sql(
            &config.table,
            &config.columns,
            config.where_clause.as_deref(),
            limit,
            offset,
            db_kind,
        );
        let conn = conns
            .get(i % conns.len().max(1))
            .cloned()
            .unwrap_or_else(|| JsonValue::Null);
        tasks.push(ReadTask {
            task_id: i,
            conn,
            query_sql: Some(query_sql),
            offset,
            limit,
        });
    }
    tasks
}

#[derive(Debug, Clone)]
pub enum PkRange {
    Range {
        pk: String,
        min: String,
        max: String,
    },
    Inclusive {
        pk: String,
        min: String,
        max: String,
    },
}

async fn get_pk_range(
    pool: &RdbmsPool,
    table: &str,
    split_pk: &str,
    where_clause: Option<&str>,
) -> Result<(Option<ColumnValue>, Option<ColumnValue>)> {
    let where_cond = match where_clause {
        Some(w) => format!(" WHERE ({}) AND {} IS NOT NULL", w, split_pk),
        None => format!(" WHERE {} IS NOT NULL", split_pk),
    };
    let sql = format!(
        "SELECT MIN({}), MAX({}) FROM {}{}",
        split_pk, split_pk, table, where_cond
    );

    pool.executor().fetch_column_pair(&sql).await
}

fn split_by_numeric_pk(
    min: Option<&str>,
    max: Option<&str>,
    count: usize,
    pk: &str,
) -> Vec<PkRange> {
    let min_val: i64 = min.and_then(|s| s.parse().ok()).unwrap_or(0);
    let max_val: i64 = max.and_then(|s| s.parse().ok()).unwrap_or(0);

    if min_val >= max_val || count <= 1 {
        return vec![PkRange::Inclusive {
            pk: pk.to_string(),
            min: min_val.to_string(),
            max: max_val.to_string(),
        }];
    }

    let step = (max_val - min_val + count as i64) / count as i64;
    (0..count)
        .map(|i| {
            let start = min_val + step * i as i64;
            let end = if i == count - 1 {
                max_val + 1
            } else {
                start + step
            };
            PkRange::Range {
                pk: pk.to_string(),
                min: start.to_string(),
                max: end.to_string(),
            }
        })
        .collect()
}

fn split_by_string_pk(
    min: Option<&str>,
    max: Option<&str>,
    count: usize,
    pk: &str,
) -> Vec<PkRange> {
    let min_val = min.unwrap_or("");
    let max_val = max.unwrap_or("");

    if count > 1 {
        // super::range_split_util::do_ascii_string_split(min_val, max_val, pk, count, 128)
        // 这个方法通过ascii 所以redix必须固定128
        super::range_split_util::do_hex_string_split(min_val, max_val, pk, count, 128)
    } else {
        vec![PkRange::Range {
            pk: pk.to_string(),
            min: min_val.to_string(),
            max: max_val.to_string(),
        }]
    }
}

fn build_range_query_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    range: &PkRange,
    db_kind: DbKind,
) -> String {
    let base = build_select_query_for(columns, table, db_kind);
    let range_cond = match range {
        PkRange::Range { pk, min, max } => {
            let min_q = quote_string_value(min);
            let max_q = quote_string_value(max);
            format!("{} >= {} AND {} < {}", pk, min_q, pk, max_q)
        }
        PkRange::Inclusive { pk, min, max } => {
            let min_q = quote_string_value(min);
            let max_q = quote_string_value(max);
            format!("{} >= {} AND {} <= {}", pk, min_q, pk, max_q)
        }
    };

    match where_clause {
        Some(w) => format!("{} WHERE ({}) AND ({})", base, w, range_cond),
        None => format!("{} WHERE {}", base, range_cond),
    }
}

// todo 防止出现单引号导致的SQL语法错误，应该对字符串进行转义处理
fn quote_string_value(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{}'", escaped)
}

fn build_null_pk_query_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    split_pk: &str,
    db_kind: DbKind,
) -> String {
    let base = build_select_query_for(columns, table, db_kind);
    let null_cond = format!("{} IS NULL", split_pk);

    match where_clause {
        Some(w) => format!("{} WHERE ({}) AND ({})", base, w, null_cond),
        None => format!("{} WHERE {}", base, null_cond),
    }
}

fn build_limit_offset_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    limit: usize,
    offset: usize,
    db_kind: DbKind,
) -> String {
    let base = build_select_query_for(columns, table, db_kind);

    match where_clause {
        Some(w) => format!("{} WHERE {} LIMIT {} OFFSET {}", base, w, limit, offset),
        None => format!("{} LIMIT {} OFFSET {}", base, limit, offset),
    }
}
