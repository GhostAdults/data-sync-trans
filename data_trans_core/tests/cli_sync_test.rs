//! CLI sync 命令集成测试
//!
//! 通过注�� Mock Reader / Mock Writer 验证 `sync` 完整流程：
//! 配置解析 → Reader split → Reader execute_task → Writer split → Writer execute_task → 结果统计

use std::collections::BTreeMap;
use std::sync::Arc;

use data_trans_common::interface::{
    GlobalRegistry, ReadTask, ReaderJob, SplitReaderResult, SplitWriterResult, WriteMode,
    WriteTask, WriterJob,
};
use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::PipelineMessage;
use data_trans_common::types::{MappingRow, UnifiedValue};
use data_trans_core::core::runner::RunStatus;
use data_trans_core::core::serve::sync;
use serde_json::json;
use tokio::sync::mpsc;

// ==========================================
// Mock Reader
// ==========================================

struct MockReader;

#[async_trait::async_trait]
impl ReaderJob for MockReader {
    async fn split(&self, reader_threads: usize) -> anyhow::Result<SplitReaderResult> {
        // 使用默认值 50 行
        let row_count: usize = 50;
        let chunk = (row_count + reader_threads - 1) / reader_threads.max(1);
        let tasks = (0..reader_threads)
            .map(|i| ReadTask {
                task_id: i,
                conn: json!(null),
                query_sql: None,
                offset: i * chunk,
                limit: chunk,
            })
            .collect();
        Ok(SplitReaderResult {
            total_records: row_count,
            tasks,
        })
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> anyhow::Result<usize> {
        let row_count: usize = 50;
        let count = task.limit.min(row_count.saturating_sub(task.offset));
        if count > 0 {
            let rows: Vec<MappingRow> = (0..count)
                .map(|i| {
                    let mut row = MappingRow::simple();
                    row.insert_simple("id", UnifiedValue::Int((task.offset + i) as i64), "int");
                    row.insert_simple(
                        "name",
                        UnifiedValue::String(format!("user_{}", task.offset + i)),
                        "text",
                    );
                    row
                })
                .collect();
            tx.send(PipelineMessage::DataBatch(rows)).await?;
        }
        tx.send(PipelineMessage::ReaderFinished).await?;
        Ok(count)
    }

    fn description(&self) -> String {
        "MockReader(50 rows)".to_string()
    }
}

// ==========================================
// Mock Writer
// ==========================================

struct MockWriter;

#[async_trait::async_trait]
impl WriterJob<PipelineMessage> for MockWriter {
    async fn split(&self, writer_threads: usize) -> anyhow::Result<SplitWriterResult> {
        let tasks = (0..writer_threads)
            .map(|i| WriteTask {
                task_id: i,
                config: Arc::new(JobConfig::default_test()),
                mode: WriteMode::Insert,
                use_transaction: false,
                batch_size: 100,
            })
            .collect();
        Ok(SplitWriterResult { tasks })
    }

    async fn execute_task(
        &self,
        _task: WriteTask,
        mut rx: mpsc::Receiver<PipelineMessage>,
    ) -> anyhow::Result<usize> {
        let mut written = 0usize;
        while let Some(msg) = rx.recv().await {
            match msg {
                PipelineMessage::DataBatch(rows) => written += rows.len(),
                PipelineMessage::ReaderFinished => break,
                PipelineMessage::Error(e) => return Err(anyhow::anyhow!("Reader error: {}", e)),
            }
        }
        Ok(written)
    }

    fn description(&self) -> String {
        "MockWriter".to_string()
    }
}

// ==========================================
// Helper
// ==========================================

fn ensure_mock_registered() {
    let registry = GlobalRegistry::instance();
    // 可重复调用，后注册覆盖前次
    registry.register_reader("mock", |_config: Arc<JobConfig>| Ok(Box::new(MockReader)));
    registry.register_writer("mock", |_config: Arc<JobConfig>| Ok(Box::new(MockWriter)));
}

fn make_config(reader_type: &str, writer_type: &str) -> JobConfig {
    JobConfig {
        input: DataSourceConfig {
            name: "test_input".to_string(),
            source_type: reader_type.to_string(),
            is_table_mode: true,
            query_sql: None,
            config: json!({}),
        },
        output: DataSourceConfig {
            name: "test_output".to_string(),
            source_type: writer_type.to_string(),
            is_table_mode: true,
            query_sql: None,
            config: json!({}),
        },
        column_mapping: {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), "id".to_string());
            m.insert("name".to_string(), "name".to_string());
            m
        },
        column_types: None,
        mode: Some("insert".to_string()),
        batch_size: Some(50),
        channel_buffer_size: None,
    }
}

// ==========================================
// Tests
// ==========================================

#[tokio::test]
async fn test_sync_basic() {
    ensure_mock_registered();

    let cfg = make_config("mock", "mock");
    let result = sync(cfg).await;

    assert!(result.is_ok(), "sync 应成功执行，实际: {:?}", result.err());
    let run_result = result.unwrap();
    assert_eq!(run_result.status, RunStatus::Success);
    assert_eq!(run_result.stats.records_read, 50, "应读取 50 条");
    assert_eq!(run_result.stats.records_written, 50, "应写入 50 条");
    assert_eq!(run_result.stats.records_failed, 0, "不应有失败记录");
}

#[tokio::test]
async fn test_sync_single_thread() {
    ensure_mock_registered();

    let cfg = make_config("mock", "mock");

    let result = sync(cfg).await;
    assert!(result.is_ok());
    let run_result = result.unwrap();
    assert_eq!(run_result.stats.records_read, 50);
    assert_eq!(run_result.stats.records_written, 50);
}

#[tokio::test]
async fn test_sync_unregistered_source_type() {
    let cfg = make_config("nonexistent_type", "nonexistent_type");

    let result = sync(cfg).await;
    assert!(result.is_err(), "未注册的 source_type 应返回错误");
}
