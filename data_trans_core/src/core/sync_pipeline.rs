/// DataX 风格的数据同步管道
/// Reader → Channel → Writer 架构，支持多线程并发

use anyhow::{Result, bail, Context};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::sync::Arc;
use std::time::Instant;
use std::collections::{HashMap, BTreeMap};
use sqlx::{Row, Column};

use crate::core::{Config, TypedVal};
use crate::core::db::{DbKind, DbPool};
use crate::core::client_tool::fetch_json;

// ==========================================
// 数据源抽象
// ==========================================

/// 数据源类型
#[derive(Debug, Clone)]
pub enum DataSourceType {
    Api,
    Database {
        query: String,
        limit: Option<usize>,
        offset: Option<usize>,
    },
}

/// 任务（Job Split 切分后的子任务）
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: usize,
    pub offset: usize,
    pub limit: usize,
}

/// 任务切分结果
pub struct JobSplitResult {
    pub total_records: usize,
    pub tasks: Vec<Task>,
}

/// Job trait - 定义数据读取任务的通用接口
#[async_trait::async_trait]
pub trait Job: Send + Sync {
    /// 切分任务为多个子任务
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult>;

    /// 执行单个任务，读取数据并发送到 Channel
    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize>;

    /// 获取任务描述（用于日志）
    fn description(&self) -> String;
}

/// 数据库 Job 实现
pub struct DatabaseJob {
    config: Arc<Config>,
    pool: Arc<DbPool>,
}

impl DatabaseJob {
    pub fn new(config: Arc<Config>, pool: Arc<DbPool>) -> Self {
        Self { config, pool }
    }
}

#[async_trait::async_trait]
impl Job for DatabaseJob {
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
        let db_config = Config::parse_db_config(&self.config.input)?;
        let table = &db_config.table;

        let sql = format!("SELECT COUNT(*) as count FROM {}", table);

        let total = match self.pool.as_ref() {
            DbPool::Postgres(pg_pool) => {
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(pg_pool)
                    .await? as usize
            }
            DbPool::Mysql(my_pool) => {
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(my_pool)
                    .await? as usize
            }
        };

        let task_size = if total == 0 {
            0
        } else {
            (total + reader_threads - 1) / reader_threads
        };

        let mut tasks = Vec::new();
        for i in 0..reader_threads {
            let offset = i * task_size;
            if offset >= total {
                break;
            }
            let limit = task_size.min(total - offset);

            tasks.push(Task {
                task_id: i,
                offset,
                limit,
            });
        }

        Ok(JobSplitResult {
            total_records: total,
            tasks,
        })
    }

    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        use futures::StreamExt;

        println!("📖 Reader-{} 启动 (OFFSET={}, LIMIT={})",
                 task.task_id, task.offset, task.limit);

        let db_config = Config::parse_db_config(&self.config.input)?;
        let table = &db_config.table;

        let sql = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            table, task.limit, task.offset
        );

        let mut sent = 0;
        let mut buffer = Vec::with_capacity(100);

        match self.pool.as_ref() {
            DbPool::Postgres(pg_pool) => {
                let mut stream = sqlx::query(&sql).fetch(pg_pool);

                while let Some(row_result) = stream.next().await {
                    let row = row_result?;

                    let mut obj = serde_json::Map::new();
                    for (idx, col) in row.columns().iter().enumerate() {
                        let col_name = col.name();
                        let val: Option<String> = row.try_get(idx).ok();
                        obj.insert(
                            col_name.to_string(),
                            val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                        );
                    }

                    buffer.push(JsonValue::Object(obj));

                    if buffer.len() >= 100 {
                        let mapped = apply_mapping(
                            &buffer,
                            &self.config.column_mapping,
                            &self.config.column_types,
                        )?;

                        tx.send(PipelineMessage::DataBatch(mapped)).await?;
                        sent += buffer.len();
                        buffer.clear();

                        println!("📖 Reader-{} 已发送 {} 条", task.task_id, sent);
                    }
                }

                if !buffer.is_empty() {
                    let mapped = apply_mapping(&buffer, &self.config.column_mapping, &self.config.column_types)?;
                    tx.send(PipelineMessage::DataBatch(mapped)).await?;
                    sent += buffer.len();
                }
            }

            DbPool::Mysql(my_pool) => {
                let mut stream = sqlx::query(&sql).fetch(my_pool);

                while let Some(row_result) = stream.next().await {
                    let row = row_result?;

                    let mut obj = serde_json::Map::new();
                    for (idx, col) in row.columns().iter().enumerate() {
                        let col_name = col.name();
                        let val: Option<String> = row.try_get(idx).ok();
                        obj.insert(
                            col_name.to_string(),
                            val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                        );
                    }

                    buffer.push(JsonValue::Object(obj));

                    if buffer.len() >= 100 {
                        let mapped = apply_mapping(
                            &buffer,
                            &self.config.column_mapping,
                            &self.config.column_types,
                        )?;

                        tx.send(PipelineMessage::DataBatch(mapped)).await?;
                        sent += buffer.len();
                        buffer.clear();

                        println!("📖 Reader-{} 已发送 {} 条", task.task_id, sent);
                    }
                }

                if !buffer.is_empty() {
                    let mapped = apply_mapping(&buffer, &self.config.column_mapping, &self.config.column_types)?;
                    tx.send(PipelineMessage::DataBatch(mapped)).await?;
                    sent += buffer.len();
                }
            }
        }

        println!("✅ Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("DatabaseJob (source: {})", self.config.input.name)
    }
}

/// API Job 实现
pub struct ApiJob {
    config: Arc<Config>,
}

impl ApiJob {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Job for ApiJob {
    async fn split(&self, _reader_threads: usize) -> Result<JobSplitResult> {
        Ok(JobSplitResult {
            total_records: 0,
            tasks: vec![Task {
                task_id: 0,
                offset: 0,
                limit: 0,
            }],
        })
    }

    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        println!("📖 Reader-{} 启动 (API 数据源)", task.task_id);

        let items = fetch_from_api(&self.config).await?;
        println!("📖 Reader-{} 获取了 {} 条数据", task.task_id, items.len());

        let mut sent = 0;
        for chunk in items.chunks(100) {
            let mapped = apply_mapping(chunk, &self.config.column_mapping, &self.config.column_types)?;
            tx.send(PipelineMessage::DataBatch(mapped)).await?;
            sent += chunk.len();
        }

        println!("✅ Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("ApiJob (source: {})", self.config.input.name)
    }
}

// ==========================================
// 配置
// ==========================================

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（分片读取任务数）
    pub reader_threads: usize,

    /// Writer 线程数（用于并发写入）
    pub writer_threads: usize,

    /// Channel 缓冲区大小
    pub channel_buffer_size: usize,

    /// 每个批次的大小
    pub batch_size: usize,

    /// 是否使用事务
    pub use_transaction: bool,

    /// 数据源类型
    pub data_source: DataSourceType,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: 1,       // 默认单 Reader（API 通常只需要一次请求）
            writer_threads: 4,       // 默认 4 个 Writer
            channel_buffer_size: 1000, // 缓冲 1000 条数据
            batch_size: 100,         // 每批 100 条
            use_transaction: true,
            data_source: DataSourceType::Api,  // 默认使用 API 数据源
        }
    }
}

// ==========================================
// 数据流中的消息类型
// ==========================================

/// Reader 到 Writer 的消息
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    /// 数据批次
    DataBatch(Vec<MappedRow>),

    /// Reader 完成信号
    ReaderFinished,

    /// 错误信号
    Error(String),
}

// ==========================================
// 统计信息
// ==========================================

/// 管道统计信息
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// 读取的记录数
    pub records_read: usize,

    /// 写入的记录数
    pub records_written: usize,

    /// 失败的记录数
    pub records_failed: usize,

    /// 总耗时（秒）
    pub elapsed_secs: f64,

    /// 吞吐量（记录/秒）
    pub throughput: f64,
}

impl PipelineStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }
}

// ==========================================
// 数据结构
// ==========================================

/// 映射后的数据行
#[derive(Debug, Clone)]
pub struct MappedRow {
    pub values: HashMap<String, TypedVal>,
    pub source: JsonValue,
}

/// 数据库批次
#[derive(Debug)]
pub struct DbBatch {
    pub sql: String,
    pub rows: Vec<Vec<TypedVal>>,
    pub columns: Vec<String>,
}

// ==========================================
// Reader 任务
// ==========================================

/// 从 API 获取数据
async fn fetch_from_api(config: &Config) -> Result<Vec<JsonValue>> {
    let api_config = Config::parse_api_config(&config.input)?;
    let resp = fetch_json(&api_config).await?;
    let items = extract_items(&resp, &api_config.items_json_path)?;
    Ok(items)
}

/// 从响应中提取数据项数组
fn extract_items(resp: &JsonValue, items_path: &Option<String>) -> Result<Vec<JsonValue>> {
    if let Some(path) = items_path {
        let v = extract_by_path(resp, path)
            .context("未找到 items_json_path 指定的数据")?;
        match v {
            JsonValue::Array(a) => Ok(a.clone()),
            _ => bail!("items_json_path 需指向数组"),
        }
    } else {
        match resp {
            JsonValue::Array(a) => Ok(a.clone()),
            JsonValue::Object(o) => {
                for (_k, v) in o {
                    if let JsonValue::Array(a) = v {
                        return Ok(a.clone());
                    }
                }
                bail!("响应顶层不是数组，且未提供 items_json_path")
            }
            _ => bail!("响应不是数组或对象"),
        }
    }
}

/// 提取 JSON 路径
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

/// 应用字段映射
fn apply_mapping(
    items: &[JsonValue],
    column_mapping: &BTreeMap<String, String>,
    column_types: &Option<BTreeMap<String, String>>,
) -> Result<Vec<MappedRow>> {
    let mut mapped_rows = Vec::new();

    for item in items {
        let mut row = HashMap::new();

        for (target_col, source_path) in column_mapping {
            let source_val = extract_by_path(item, source_path)
                .unwrap_or(&JsonValue::Null);

            let type_hint = column_types
                .as_ref()
                .and_then(|m| m.get(target_col))
                .map(|s| s.as_str());

            let typed_val = to_typed_value(source_val, type_hint)?;
            row.insert(target_col.clone(), typed_val);
        }

        mapped_rows.push(MappedRow {
            values: row,
            source: item.clone(),
        });
    }

    Ok(mapped_rows)
}

/// 将 JSON 值转换为类型化的 SQL 值
fn to_typed_value(v: &JsonValue, type_hint: Option<&str>) -> Result<TypedVal> {
    match type_hint {
        Some("int") => {
            if v.is_null() {
                Ok(TypedVal::OptI64(None))
            } else {
                let n = v.as_i64().context("无法转换为 int")?;
                Ok(TypedVal::I64(n))
            }
        }
        Some("float") => {
            if v.is_null() {
                Ok(TypedVal::OptF64(None))
            } else {
                let n = v.as_f64().context("无法转换为 float")?;
                Ok(TypedVal::F64(n))
            }
        }
        Some("bool") => {
            if v.is_null() {
                Ok(TypedVal::OptBool(None))
            } else {
                let b = v.as_bool().context("无法转换为 bool")?;
                Ok(TypedVal::Bool(b))
            }
        }
        Some("timestamp") => {
            if v.is_null() {
                Ok(TypedVal::OptNaiveTs(None))
            } else {
                let s = v.as_str().context("timestamp 需要字符串")?;
                let ts = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .context("无法解析时间戳")?;
                Ok(TypedVal::OptNaiveTs(Some(ts)))
            }
        }
        _ => {
            if v.is_null() {
                Ok(TypedVal::Text("".to_string()))
            } else if let Some(s) = v.as_str() {
                Ok(TypedVal::Text(s.to_string()))
            } else {
                Ok(TypedVal::Text(v.to_string()))
            }
        }
    }
}

// ==========================================
// Writer 任务辅助函数
// ==========================================

/// 准备数据库批次数据
fn prepare_db_batch(
    mapped_rows: &[MappedRow],
    table_name: &str,
    key_columns: &[String],
    mode: &str,
    db_kind: DbKind,
) -> Result<DbBatch> {
    if mapped_rows.is_empty() {
        bail!("没有数据需要同步");
    }

    let columns: Vec<String> = mapped_rows[0].values.keys().cloned().collect();
    let (keys, nonkeys) = split_keys_nonkeys(&columns, key_columns);
    let sql = build_sql(table_name, &columns, &keys, &nonkeys, mode, db_kind)?;

    let mut rows = Vec::new();
    for mapped_row in mapped_rows {
        let mut values = Vec::new();
        for col in &columns {
            let val = mapped_row.values.get(col)
                .cloned()
                .unwrap_or(TypedVal::Text("".to_string()));
            values.push(val);
        }
        rows.push(values);
    }

    Ok(DbBatch {
        sql,
        rows,
        columns,
    })
}

/// 分离主键列和非主键列
fn split_keys_nonkeys(all_cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
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

/// 构建 SQL 语句
fn build_sql(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
    mode: &str,
    kind: DbKind,
) -> Result<String> {
    let mut sql = String::new();

    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    sql.push_str(&columns.join(", "));
    sql.push_str(") VALUES (");
    sql.push_str(&build_placeholders(kind, columns.len(), 1));
    sql.push(')');

    if mode == "upsert" {
        match kind {
            DbKind::Postgres => {
                if !keys.is_empty() {
                    sql.push_str(" ON CONFLICT (");
                    sql.push_str(&keys.join(", "));
                    sql.push_str(") DO UPDATE SET ");
                    let sets: Vec<String> = nonkeys
                        .iter()
                        .map(|c| format!("{} = EXCLUDED.{}", c, c))
                        .collect();
                    sql.push_str(&sets.join(", "));
                } else {
                    bail!("upsert 模式需要指定 key_columns");
                }
            }
            DbKind::Mysql => {
                sql.push_str(" ON DUPLICATE KEY UPDATE ");
                let sets: Vec<String> = nonkeys
                    .iter()
                    .map(|c| format!("{} = VALUES({})", c, c))
                    .collect();
                sql.push_str(&sets.join(", "));
            }
        }
    }

    Ok(sql)
}

/// 构建占位符
fn build_placeholders(kind: DbKind, count: usize, _offset: usize) -> String {
    match kind {
        DbKind::Postgres => {
            (1..=count).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
        }
        DbKind::Mysql => {
            vec!["?"; count].join(", ")
        }
    }
}

/// 执行数据库写入
async fn execute_db_write(
    batch: &DbBatch,
    pool: &DbPool,
    use_transaction: bool,
    batch_size: usize,
) -> Result<usize> {
    let total_rows = batch.rows.len();
    let mut processed = 0;

    match pool {
        DbPool::Postgres(pg_pool) => {
            let mut start = 0;
            while start < total_rows {
                let end = (start + batch_size).min(total_rows);

                if use_transaction {
                    let mut tx = pg_pool.begin().await?;
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_pg(&batch.sql, row_values, &mut *tx).await?;
                    }
                    tx.commit().await?;
                } else {
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_pg(&batch.sql, row_values, pg_pool).await?;
                    }
                }

                processed += end - start;
                start = end;
            }
        }
        DbPool::Mysql(my_pool) => {
            let mut start = 0;
            while start < total_rows {
                let end = (start + batch_size).min(total_rows);

                if use_transaction {
                    let mut tx = my_pool.begin().await?;
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_my(&batch.sql, row_values, &mut *tx).await?;
                    }
                    tx.commit().await?;
                } else {
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_my(&batch.sql, row_values, my_pool).await?;
                    }
                }

                processed += end - start;
                start = end;
            }
        }
    }

    Ok(processed)
}

/// 执行单行 SQL（PostgreSQL）
async fn execute_single_row_pg(
    sql: &str,
    values: &[TypedVal],
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<()> {
    let mut q = sqlx::query(sql);

    for v in values {
        match v {
            TypedVal::I64(n) => q = q.bind(n),
            TypedVal::F64(n) => q = q.bind(n),
            TypedVal::Bool(b) => q = q.bind(b),
            TypedVal::OptI64(n) => q = q.bind(n),
            TypedVal::OptF64(n) => q = q.bind(n),
            TypedVal::OptBool(n) => q = q.bind(n),
            TypedVal::OptNaiveTs(n) => q = q.bind(n),
            TypedVal::Text(s) => q = q.bind(s.clone()),
        }
    }

    q.execute(executor).await?;
    Ok(())
}

/// 执行单行 SQL（MySQL）
async fn execute_single_row_my(
    sql: &str,
    values: &[TypedVal],
    executor: impl sqlx::Executor<'_, Database = sqlx::MySql>,
) -> Result<()> {
    let mut q = sqlx::query(sql);

    for v in values {
        match v {
            TypedVal::I64(n) => q = q.bind(n),
            TypedVal::F64(n) => q = q.bind(n),
            TypedVal::Bool(b) => q = q.bind(b),
            TypedVal::OptI64(n) => q = q.bind(n),
            TypedVal::OptF64(n) => q = q.bind(n),
            TypedVal::OptBool(n) => q = q.bind(n),
            TypedVal::OptNaiveTs(n) => q = q.bind(n),
            TypedVal::Text(s) => q = q.bind(s.clone()),
        }
    }

    q.execute(executor).await?;
    Ok(())
}

/// 流式 Reader 任务（数据库）
// ==========================================
// Writer 任务
// ==========================================

/// Writer 任务：从 Channel 接收数据并写入数据库
async fn writer_task(
    config: Arc<Config>,
    pool: Arc<DbPool>,
    mut rx: mpsc::Receiver<PipelineMessage>,
    writer_id: usize,
    pipeline_config: Arc<PipelineConfig>,
) -> Result<usize> {
    println!("✍️  Writer-{} 启动", writer_id);

    let mut written = 0;
    let db_kind = match pool.as_ref() {
        DbPool::Postgres(_) => DbKind::Postgres,
        DbPool::Mysql(_) => DbKind::Mysql,
    };

    let db_config = crate::core::Config::parse_db_config(&config.output).ok().unwrap_or_default();
    let key_cols = db_config.key_columns.clone().unwrap_or_default();
    let mode = config.mode.as_deref().unwrap_or("insert");

    // 持续从 Channel 接收数据
    while let Some(msg) = rx.recv().await {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                let _batch_size = rows.len();

                // 准备数据库批次
                let batch = prepare_db_batch(
                    &rows,
                    &db_config.table,
                    &key_cols,
                    mode,
                    db_kind,
                )?;

                // 执行写入
                let result = execute_db_write(
                    &batch,
                    &pool,
                    pipeline_config.use_transaction,
                    pipeline_config.batch_size,
                ).await;

                match result {
                    Ok(count) => {
                        written += count;
                        println!("✍️  Writer-{} 写入了 {} 条数据（累计：{}）",
                                 writer_id, count, written);
                    }
                    Err(e) => {
                        eprintln!("❌ Writer-{} 写入失败: {}", writer_id, e);
                        // 继续处理其他批次，不中断
                    }
                }
            }
            PipelineMessage::ReaderFinished => {
                println!("📭 Writer-{} 收到 Reader 完成信号", writer_id);
                // 注意：不立即退出，继续处理队列中剩余的数据
            }
            PipelineMessage::Error(err) => {
                eprintln!("❌ Writer-{} 收到错误: {}", writer_id, err);
                break;
            }
        }
    }

    println!("✅ Writer-{} 完成，共写入 {} 条数据", writer_id, written);
    Ok(written)
}

// ==========================================
// 管道执行器
// ==========================================

/// 管道执行器：协调 Reader 和 Writer
pub struct Pipeline {
    config: Arc<Config>,
    pool: Arc<DbPool>,
    pipeline_config: Arc<PipelineConfig>,
}

impl Pipeline {
    pub fn new(config: Config, pool: DbPool, pipeline_config: PipelineConfig) -> Self {
        Self {
            config: Arc::new(config),
            pool: Arc::new(pool),
            pipeline_config: Arc::new(pipeline_config),
        }
    }

    /// 执行管道
    pub async fn execute(&self) -> Result<PipelineStats> {
        let start_time = Instant::now();
        println!("\n🚀 启动数据同步管道");
        println!("📊 配置: {} Reader, {} Writer, Channel 缓冲 {}",
                 self.pipeline_config.reader_threads,
                 self.pipeline_config.writer_threads,
                 self.pipeline_config.channel_buffer_size);

        // 创建 Channel
        let (tx, rx) = mpsc::channel::<PipelineMessage>(
            self.pipeline_config.channel_buffer_size
        );

        let reader_handles = match &self.pipeline_config.data_source {
            DataSourceType::Database { .. } => {
                println!("📊 数据库数据源，启动任务切分...");

                let job: Arc<dyn Job> = Arc::new(DatabaseJob::new(
                    Arc::clone(&self.config),
                    Arc::clone(&self.pool),
                ));

                println!("📋 Job: {}", job.description());

                let split_result = job.split(self.pipeline_config.reader_threads).await?;
                println!("📊 总记录数: {}", split_result.total_records);
                println!("📊 切分为 {} 个任务", split_result.tasks.len());

                let mut handles = Vec::new();
                for task in split_result.tasks {
                    let job = Arc::clone(&job);
                    let tx = tx.clone();

                    let handle = tokio::spawn(async move {
                        job.execute_task(task, tx).await
                    });

                    handles.push(handle);
                }

                handles
            }

            DataSourceType::Api => {
                println!("📊 API 数据源，使用单 Reader");

                let job: Arc<dyn Job> = Arc::new(ApiJob::new(Arc::clone(&self.config)));
                println!("📋 Job: {}", job.description());

                let split_result = job.split(1).await?;
                let task = split_result.tasks[0].clone();

                let tx = tx.clone();
                let handle = tokio::spawn(async move {
                    job.execute_task(task, tx).await
                });

                vec![handle]
            }
        };

        // 释放原始 tx，确保所有 Reader 完成后 Channel 能正确关闭
        drop(tx);

        // 创建多个 Writer 共享同一个 Receiver（使用广播模式）
        // 注意：mpsc::Receiver 不能直接 clone，需要使用 broadcast 或其他方式
        // 这里我们使用一个分发器
        let writer_handles = self.start_writers(rx).await;

        // 等待所有 Reader 完成
        let mut total_read = 0;
        for (i, handle) in reader_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_read += count;
                    println!("✅ Reader-{} 完成，读取 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    eprintln!("❌ Reader-{} 失败: {}", i, e);
                }
                Err(e) => {
                    eprintln!("❌ Reader-{} 任务崩溃: {}", i, e);
                }
            }
        }

        // 等待所有 Writer 完成
        let mut total_written = 0;
        for (i, handle) in writer_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_written += count;
                    println!("✅ Writer-{} 完成，写入 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    eprintln!("❌ Writer-{} 失败: {}", i, e);
                }
                Err(e) => {
                    eprintln!("❌ Writer-{} 任务崩溃: {}", i, e);
                }
            }
        }

        let elapsed = start_time.elapsed();
        let mut stats = PipelineStats {
            records_read: total_read,
            records_written: total_written,
            records_failed: total_read.saturating_sub(total_written),
            elapsed_secs: elapsed.as_secs_f64(),
            throughput: 0.0,
        };
        stats.calculate_throughput();

        println!("\n📊 同步完成");
        println!("   读取: {} 条", stats.records_read);
        println!("   写入: {} 条", stats.records_written);
        println!("   失败: {} 条", stats.records_failed);
        println!("   耗时: {:.2} 秒", stats.elapsed_secs);
        println!("   吞吐: {:.2} 条/秒", stats.throughput);

        Ok(stats)
    }

    /// 启动多个 Writer（使用分发策略）
    async fn start_writers(
        &self,
        mut rx: mpsc::Receiver<PipelineMessage>,
    ) -> Vec<JoinHandle<Result<usize>>> {
        let writer_count = self.pipeline_config.writer_threads;

        // 创建每个 Writer 的 Channel
        let mut writer_txs = Vec::new();
        let mut writer_handles = Vec::new();

        for i in 0..writer_count {
            let (tx, rx) = mpsc::channel::<PipelineMessage>(100);
            writer_txs.push(tx);

            let config = Arc::clone(&self.config);
            let pool = Arc::clone(&self.pool);
            let pipeline_config = Arc::clone(&self.pipeline_config);

            let handle = tokio::spawn(async move {
                writer_task(config, pool, rx, i, pipeline_config).await
            });
            writer_handles.push(handle);
        }

        // 启动分发器任务：从主 Channel 读取，轮询分发到各 Writer
        let reader_total = self.pipeline_config.reader_threads;
        tokio::spawn(async move {
            let mut current_writer = 0;
            let mut reader_finished_count = 0;

            while let Some(msg) = rx.recv().await {
                match &msg {
                    PipelineMessage::DataBatch(_) => {
                        // 轮询分发到 Writer
                        let tx = &writer_txs[current_writer];
                        if tx.send(msg).await.is_err() {
                            eprintln!("⚠️  分发器: Writer-{} Channel 已关闭", current_writer);
                        }
                        current_writer = (current_writer + 1) % writer_count;
                    }
                    PipelineMessage::ReaderFinished => {
                        reader_finished_count += 1;
                        println!("📭 分发器: 收到 Reader 完成信号 ({}/{})",
                                 reader_finished_count, reader_total);

                        // 所有 Reader 完成后，通知所有 Writer
                        if reader_finished_count >= reader_total {
                            println!("📭 分发器: 所有 Reader 完成，通知所有 Writer");
                            for tx in &writer_txs {
                                let _ = tx.send(PipelineMessage::ReaderFinished).await;
                            }
                            break;
                        }
                    }
                    PipelineMessage::Error(err) => {
                        eprintln!("❌ 分发器: 收到错误 - {}", err);
                        // 通知所有 Writer 停止
                        for tx in &writer_txs {
                            let _ = tx.send(msg.clone()).await;
                        }
                        break;
                    }
                }
            }

            // 关闭所有 Writer 的 Channel
            drop(writer_txs);
            println!("📭 分发器: 已关闭所有 Writer Channel");
        });

        writer_handles
    }
}

// ==========================================
// 便捷入口函数
// ==========================================

/// 使用管道执行同步（默认配置）
pub async fn sync_with_pipeline(config: Config, pool: DbPool) -> Result<PipelineStats> {
    let pipeline_config = PipelineConfig::default();
    let pipeline = Pipeline::new(config, pool, pipeline_config);
    pipeline.execute().await
}

/// 使用管道执行同步（自定义配置）
pub async fn sync_with_pipeline_custom(
    config: Config,
    pool: DbPool,
    pipeline_config: PipelineConfig,
) -> Result<PipelineStats> {
    let pipeline = Pipeline::new(config, pool, pipeline_config);
    pipeline.execute().await
}
