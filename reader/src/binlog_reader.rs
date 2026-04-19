//! MySQL Binlog CDC Reader
//!
//! 通过 MySQL replication protocol 消费 ROW 格式 Binlog，
//! 实时捕获 INSERT/UPDATE/DELETE 变更事件，
//! 转换为 JsonStream 输出。
//!
//! 使用 `mysql_cdc` crate 同步消费 Binlog 事件，
//! 通过 `tokio::task::spawn_blocking` + `mpsc` 桥接到 async stream。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value as JsonValue;

use crate::{JsonStream, ReadTask, DataReaderJob, DataReaderTask, SplitReaderResult, StreamMode};
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::row_events::mysql_value::MySqlValue;
use mysql_cdc::events::row_events::row_data::RowData;
use relus_common::data_source_config::DataSourceConfig;
use relus_common::job_config::JobConfig;

// ==========================================
// BinlogConfig
// ==========================================

/// Binlog Reader 配置
#[derive(Debug, Clone)]
pub struct BinlogConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub server_id: u32,
    pub binlog_file: Option<String>,
    pub binlog_position: Option<u32>,
    pub gtid: Option<String>,
    pub table_filter: Option<String>,
    pub checkpoint_dir: PathBuf,
}

impl BinlogConfig {
    pub fn from_data_source_config(input: &DataSourceConfig) -> Result<Self> {
        let conn = input.config.get("connection").cloned().unwrap_or_default();
        let conns = input.config.get("connections").and_then(|v| v.as_array());
        let source = conns.and_then(|a| a.first()).unwrap_or_else(|| &conn);

        Ok(Self {
            hostname: source
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1")
                .to_string(),
            port: source
                .get("port")
                .and_then(|v| v.as_u64())
                .unwrap_or(3306) as u16,
            username: source
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("root")
                .to_string(),
            password: source
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            server_id: source
                .get("server_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(1001) as u32,
            binlog_file: source
                .get("binlog_file")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            binlog_position: source
                .get("binlog_position")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32),
            gtid: source
                .get("gtid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            table_filter: source
                .get("table_filter")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            checkpoint_dir: source
                .get("checkpoint_dir")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./checkpoints")),
        })
    }
}

// ==========================================
// CdcOp & 事件转换
// ==========================================

/// 变更事件操作类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Insert => "insert",
            CdcOp::Update => "update",
            CdcOp::Delete => "delete",
        }
    }
}

/// 将变更事件转换为 JsonValue
///
/// 输出格式包含元数据字段（`_op`, `_table`, `_timestamp`）和业务列值。
/// 列名来自 TableMapEvent 的 column metadata（当可用时用 col_0, col_1...）。
pub fn cdc_event_to_json(
    op: CdcOp,
    table: &str,
    timestamp: u32,
    columns: &[(String, JsonValue)],
) -> JsonValue {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "_op".to_string(),
        JsonValue::String(op.as_str().to_string()),
    );
    obj.insert("_table".to_string(), JsonValue::String(table.to_string()));
    obj.insert(
        "_timestamp".to_string(),
        JsonValue::Number(timestamp.into()),
    );
    for (col, val) in columns {
        obj.insert(col.clone(), val.clone());
    }
    JsonValue::Object(obj)
}

fn mysql_value_to_json(val: &MySqlValue) -> JsonValue {
    match val {
        MySqlValue::TinyInt(n) => JsonValue::Number((*n).into()),
        MySqlValue::SmallInt(n) => JsonValue::Number((*n).into()),
        MySqlValue::MediumInt(n) => JsonValue::Number((*n).into()),
        MySqlValue::Int(n) => JsonValue::Number((*n).into()),
        MySqlValue::BigInt(n) => {
            let n = *n as i64;
            JsonValue::Number(n.into())
        }
        MySqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        MySqlValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        MySqlValue::Decimal(s) => JsonValue::String(s.clone()),
        MySqlValue::String(s) => JsonValue::String(s.clone()),
        MySqlValue::Blob(b) => {
            let s = String::from_utf8_lossy(b).to_string();
            JsonValue::String(s)
        }
        MySqlValue::Year(y) => JsonValue::Number((*y).into()),
        MySqlValue::Date(d) => JsonValue::String(format!("{}-{:02}-{:02}", d.year, d.month, d.day)),
        MySqlValue::Time(t) => JsonValue::String(format!(
            "{}:{:02}:{:02}.{:03}",
            t.hour, t.minute, t.second, t.millis
        )),
        MySqlValue::DateTime(dt) => JsonValue::String(format!(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.millis
        )),
        MySqlValue::Timestamp(millis) => JsonValue::Number((*millis as i64).into()),
        MySqlValue::Bit(bits) => {
            let val: u64 =
                bits.iter().rev().enumerate().fold(
                    0u64,
                    |acc, (i, &b)| {
                        if b {
                            acc | (1 << i)
                        } else {
                            acc
                        }
                    },
                );
            JsonValue::Number((val as i64).into())
        }
        MySqlValue::Enum(n) => JsonValue::Number((*n as i64).into()),
        MySqlValue::Set(n) => JsonValue::Number((*n as i64).into()),
    }
}

fn row_data_to_columns(row: &RowData, column_names: &[String]) -> Vec<(String, JsonValue)> {
    row.cells
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let name = column_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{}", i));
            let val = cell
                .as_ref()
                .map(mysql_value_to_json)
                .unwrap_or(JsonValue::Null);
            (name, val)
        })
        .collect()
}

/// 简化的表映射信息（提取自 TableMapEvent）
#[derive(Debug, Clone)]
struct TableMapInfo {
    database_name: String,
    table_name: String,
}

// ==========================================
// BinlogReader
// ==========================================

/// Binlog CDC Reader
pub struct BinlogReader {
    job: BinlogJob,
}

/// Binlog Job 业务逻辑
pub struct BinlogJob {
    original_config: Arc<JobConfig>,
    binlog_config: BinlogConfig,
    column_names: Vec<String>,
    shutdown: Arc<AtomicBool>,
}

impl BinlogJob {
    pub fn new(original_config: Arc<JobConfig>, binlog_config: BinlogConfig) -> Self {
        let column_names: Vec<String> = original_config.column_mapping.keys().cloned().collect();
        Self {
            original_config,
            binlog_config,
            column_names,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl BinlogReader {
    pub fn init(config: Arc<JobConfig>) -> Result<Self> {
        let binlog_config = BinlogConfig::from_data_source_config(&config.source)?;
        let job = BinlogJob::new(config, binlog_config);
        Ok(Self { job })
    }
}

#[async_trait::async_trait]
impl DataReaderJob for BinlogReader {
    /// Binlog 是单流有序消费，返回单个 task
    async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
        Ok(SplitReaderResult {
            total_records: 1, // 由于是流式消费，无法预知总记录数，暂时返回 1
            stream_mode: StreamMode::Infinite,
            tasks: vec![ReadTask {
                task_id: 0,
                conn: JsonValue::Null,
                query_sql: None,
                offset: 0,
                limit: 0,
            }],
        })
    }

    fn description(&self) -> String {
        format!(
            "BinlogReader (table: {})",
            self.job
                .binlog_config
                .table_filter
                .as_deref()
                .unwrap_or("*")
        )
    }
}

#[async_trait::async_trait]
impl DataReaderTask for BinlogReader {
    async fn read_data(&self, _task: &ReadTask) -> Result<JsonStream> {
        let config = self.job.binlog_config.clone();
        let column_names = self.job.column_names.clone();
        let shutdown = Arc::clone(&self.job.shutdown);

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<JsonValue>>(1024);

        tokio::task::spawn_blocking(move || {
            if let Err(e) = run_binlog_stream(config, column_names, tx, shutdown) {
                tracing::error!("Binlog stream ended: {:?}", e);
            }
        });

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        Ok(Box::pin(stream))
    }

    fn shutdown(&self) {
        self.job.shutdown.store(true, Ordering::Relaxed);
        tracing::info!("[BinlogReader] shutdown");
    }
}

// ==========================================
// Binlog 消费循环（阻塞线程）
// ==========================================

fn run_binlog_stream(
    config: BinlogConfig,
    column_names: Vec<String>,
    tx: tokio::sync::mpsc::Sender<Result<JsonValue>>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    use mysql_cdc::binlog_client::BinlogClient;
    use mysql_cdc::binlog_options::BinlogOptions;
    use mysql_cdc::replica_options::ReplicaOptions;
    use mysql_cdc::ssl_mode::SslMode;
    use std::time::Duration;

    println!(
        "[BinlogReader] 正在连接 {}:{} (server_id={})...",
        config.hostname, config.port, config.server_id
    );
    tracing::info!(
        "[BinlogReader] 连接中: {}:{} (server_id={})",
        config.hostname,
        config.port,
        config.server_id
    );

    let binlog_opts = match (&config.binlog_file, config.binlog_position) {
        (Some(file), Some(pos)) => {
            println!("[BinlogReader] 从指定位点恢复: {}:{}", file, pos);
            BinlogOptions::from_position(file.clone(), pos)
        }
        _ => {
            println!("[BinlogReader] 从最新位点开始消费");
            BinlogOptions::from_end()
        }
    };

    let options = ReplicaOptions {
        hostname: config.hostname.clone(),
        port: config.port,
        username: config.username.clone(),
        password: config.password.clone(),
        server_id: config.server_id,
        binlog: binlog_opts,
        ssl_mode: SslMode::Disabled, // todo 是否可以支持ssl？
        blocking: true,
        heartbeat_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);

    let replicate_result = client.replicate().map_err(|e| {
        let msg = format!("Binlog 连接失败: {:?}", e);
        tracing::error!("[BinlogReader] {}", msg);
        let _ = tx.blocking_send(Err(anyhow::anyhow!("{}", msg)));
        anyhow::anyhow!("{}", msg)
    })?;

    let filter = config.table_filter.as_deref().unwrap_or("*");
    println!("[BinlogReader] 已连接，开始监听 Binlog (filter={})", filter);
    tracing::info!(
        "[BinlogReader] 连接成功，开始消费 Binlog (filter={})",
        filter
    );

    let mut table_map: HashMap<u64, TableMapInfo> = HashMap::new();
    let mut event_counter: usize = 0;

    for result in replicate_result {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let (header, event) = result.map_err(|e| {
            let msg = format!("Binlog 事件解析失败: {:?}", e);
            tracing::error!("[BinlogReader] {}", msg);
            let _ = tx.blocking_send(Err(anyhow::anyhow!("{}", msg)));
            anyhow::anyhow!("{}", msg)
        })?;
        let timestamp = header.timestamp;

        match event {
            BinlogEvent::TableMapEvent(e) => {
                table_map.insert(
                    e.table_id,
                    TableMapInfo {
                        database_name: e.database_name.clone(),
                        table_name: e.table_name.clone(),
                    },
                );
            }
            BinlogEvent::WriteRowsEvent(e) => {
                let table_name = table_map
                    .get(&e.table_id)
                    .map(|t| format_table(&t.database_name, &t.table_name))
                    .unwrap_or_else(|| format!("unknown_{}", e.table_id));

                if !matches_table_filter(&config.table_filter, &table_name) {
                    continue;
                }

                for row in &e.rows {
                    let columns = row_data_to_columns(row, &column_names);
                    let json = cdc_event_to_json(CdcOp::Insert, &table_name, timestamp, &columns);
                    if tx.blocking_send(Ok(json)).is_err() {
                        return Ok(());
                    }
                    event_counter += 1;
                }
                log_event_sample("INSERT", &table_name, e.rows.len(), &mut event_counter);
            }
            BinlogEvent::UpdateRowsEvent(e) => {
                let table_name = table_map
                    .get(&e.table_id)
                    .map(|t| format_table(&t.database_name, &t.table_name))
                    .unwrap_or_else(|| format!("unknown_{}", e.table_id));

                if !matches_table_filter(&config.table_filter, &table_name) {
                    continue;
                }

                for row in &e.rows {
                    let columns = row_data_to_columns(&row.after_update, &column_names);
                    let json = cdc_event_to_json(CdcOp::Update, &table_name, timestamp, &columns);
                    if tx.blocking_send(Ok(json)).is_err() {
                        return Ok(());
                    }
                }
                log_event_sample("UPDATE", &table_name, e.rows.len(), &mut event_counter);
            }
            BinlogEvent::DeleteRowsEvent(e) => {
                let table_name = table_map
                    .get(&e.table_id)
                    .map(|t| format_table(&t.database_name, &t.table_name))
                    .unwrap_or_else(|| format!("unknown_{}", e.table_id));

                if !matches_table_filter(&config.table_filter, &table_name) {
                    continue;
                }

                for row in &e.rows {
                    let columns = row_data_to_columns(row, &column_names);
                    let json = cdc_event_to_json(CdcOp::Delete, &table_name, timestamp, &columns);
                    if tx.blocking_send(Ok(json)).is_err() {
                        return Ok(());
                    }
                }
                log_event_sample("DELETE", &table_name, e.rows.len(), &mut event_counter);
            }
            BinlogEvent::HeartbeatEvent(e) => {
                tracing::info!("[BinlogReader] heartbeat at {}", e.binlog_filename);
            }
            BinlogEvent::XidEvent(_) => {}
            _ => {}
        }
    }

    tracing::info!("[BinlogReader] Binlog stream 正常结束");
    Ok(())
}

/// 检查 table_name 是否匹配 table_filter
fn matches_table_filter(filter: &Option<String>, table_name: &str) -> bool {
    match filter {
        None => true,
        Some(f) => table_name == f,
    }
}

/// 采样日志：每 1000 条事件输出一次汇总
fn log_event_sample(op: &str, table: &str, row_count: usize, total: &mut usize) {
    *total += row_count;
    if *total % 1000 < row_count {
        tracing::info!(
            "[BinlogReader] {} events on {} (累计 {} 条)",
            op,
            table,
            total
        );
    }
}

fn format_table(database: &str, table: &str) -> String {
    format!("{}.{}", database, table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cdc_event_to_json_insert() {
        let columns = vec![
            ("id".to_string(), json!(1)),
            ("name".to_string(), json!("Alice")),
        ];
        let result = cdc_event_to_json(CdcOp::Insert, "test.users", 1710000000, &columns);

        assert_eq!(result["_op"], "insert");
        assert_eq!(result["_table"], "test.users");
        assert_eq!(result["_timestamp"], 1710000000);
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
    }

    #[test]
    fn test_cdc_event_to_json_update() {
        let columns = vec![("id".to_string(), json!(2))];
        let result = cdc_event_to_json(CdcOp::Update, "test.orders", 1710000001, &columns);

        assert_eq!(result["_op"], "update");
        assert_eq!(result["_table"], "test.orders");
    }

    #[test]
    fn test_cdc_event_to_json_delete() {
        let columns = vec![("id".to_string(), json!(3))];
        let result = cdc_event_to_json(CdcOp::Delete, "test.logs", 1710000002, &columns);

        assert_eq!(result["_op"], "delete");
    }

    #[test]
    fn test_binlog_config_parse() {
        let config = serde_json::json!({
            "connection": {
                "url": "mysql://root:pass@192.168.1.100:3307/mydb",
                "username": "repl_user",
                "password": "secret",
                "server_id": 2001,
                "binlog_file": "mysql-bin.000003",
                "binlog_position": 194,
                "table_filter": "mydb.users"
            }
        });

        let ds = relus_common::data_source_config::DataSourceConfig {
            name: "binlog_test".to_string(),
            source_type: "mysql_binlog".to_string(),
            is_table_mode: true,
            query_sql: None,
            writer_mode: None,
            config,
        };

        let binlog_cfg = BinlogConfig::from_data_source_config(&ds).unwrap();

        assert_eq!(binlog_cfg.hostname, "192.168.1.100");
        assert_eq!(binlog_cfg.port, 3307);
        assert_eq!(binlog_cfg.username, "repl_user");
        assert_eq!(binlog_cfg.server_id, 2001);
        assert_eq!(binlog_cfg.binlog_file, Some("mysql-bin.000003".to_string()));
        assert_eq!(binlog_cfg.binlog_position, Some(194));
    }

    #[test]
    fn test_format_table() {
        assert_eq!(format_table("mydb", "users"), "mydb.users");
    }
}
