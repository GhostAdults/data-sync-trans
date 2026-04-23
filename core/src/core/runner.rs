//! Pipeline Runner 模块
//!
//! 职责：调起数据同步任务
//! - 解析配置
//! - 通过 Registry 动态创建 Reader/Writer
//! - 根据 StreamMode 选择 BatchRunner 或 CdcRunner 策略
//! - 委托给 Pipeline 执行
//! - 返回结果

use anyhow::Result;
use relus_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_NUMBER, DEFAULT_PER_GROUP_CHANNEL,
    DEFAULT_READER_THREADS,
};
use relus_common::job_config::JobConfig;
use relus_reader::{DataReader, StreamMode};
use relus_writer::DataWriter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::container::{run_pipeline, PipelineConfig, PipelineStats};
use relus_reader::ReaderRegistry;
use relus_writer::WriterRegistry;

// ==========================================
// 配置类型
// ==========================================

/// Runner 配置
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub buffer_size: usize,
    pub reader_threads: usize,
    pub channel_number: usize,
    pub per_group_channel: usize,
    pub batch_size: usize,
    pub use_transaction: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            reader_threads: DEFAULT_READER_THREADS,
            channel_number: DEFAULT_CHANNEL_NUMBER,
            per_group_channel: DEFAULT_PER_GROUP_CHANNEL,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl RunnerConfig {
    pub fn from_job_config(config: &JobConfig) -> Self {
        let pipeline_config = PipelineConfig::from_system_config(config);
        Self {
            buffer_size: pipeline_config.buffer_size,
            reader_threads: pipeline_config.reader_threads,
            channel_number: pipeline_config.channel_number,
            per_group_channel: pipeline_config.per_group_channel,
            batch_size: pipeline_config.batch_size,
            use_transaction: pipeline_config.use_transaction,
        }
    }

    fn to_pipeline_config(&self) -> PipelineConfig {
        PipelineConfig {
            reader_threads: self.reader_threads,
            buffer_size: self.buffer_size,
            channel_number: self.channel_number,
            per_group_channel: self.per_group_channel,
            batch_size: self.batch_size,
            use_transaction: self.use_transaction,
        }
    }
}

// ==========================================
// 结果类型
// ==========================================

/// 执行结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub stats: RunnerStats,
    pub status: RunStatus,
    pub duration: Duration,
    pub error: Option<String>,
}

/// 执行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Success,
    Failed,
    Partial,
    Shutdown,
}

/// 统计信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunnerStats {
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub elapsed_secs: f64,
    pub throughput: f64,
}

impl RunnerStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }

    pub fn from_pipeline(stats: PipelineStats) -> Self {
        Self {
            records_read: stats.records_read,
            records_written: stats.records_written,
            records_failed: stats.records_failed,
            elapsed_secs: stats.elapsed_secs,
            throughput: stats.throughput,
        }
    }
}

// ==========================================
// TaskRunner trait
// ==========================================

/// Runner 策略 trait — Batch 和 Cdc 的生命周期编排接口
#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync {
    async fn run(
        &self,
        reader: Arc<dyn DataReader>,
        writer: Arc<dyn DataWriter>,
        job_config: Arc<JobConfig>,
        cancel_token: CancellationToken,
    ) -> Result<RunResult>;
}

// ==========================================
// BatchRunner
// ==========================================

/// Batch 策略：Reader stream 自然读到 None(EndOfInput)，进程自动退出
struct BatchRunner {
    config: RunnerConfig,
}

impl BatchRunner {
    fn new(config: RunnerConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl TaskRunner for BatchRunner {
    async fn run(
        &self,
        reader: Arc<dyn DataReader>,
        writer: Arc<dyn DataWriter>,
        job_config: Arc<JobConfig>,
        cancel_token: CancellationToken,
    ) -> Result<RunResult> {
        let start_time = Instant::now();
        let pipeline_config = self.config.to_pipeline_config();

        let pipeline_stats =
            run_pipeline(pipeline_config, reader, writer, job_config, cancel_token).await?;

        let elapsed = start_time.elapsed();
        let mut stats = RunnerStats::from_pipeline(pipeline_stats);
        stats.elapsed_secs = elapsed.as_secs_f64();
        stats.calculate_throughput();

        let status = if stats.records_failed > 0 {
            RunStatus::Partial
        } else {
            RunStatus::Success
        };

        Ok(RunResult {
            stats,
            status,
            duration: elapsed,
            error: None,
        })
    }
}

// ==========================================
// CdcRunner
// ==========================================

/// Cdc 策略：Reader stream 永远不会 None，进程常驻等待新事件
/// 收到 ctrl+c 后 cancel token -> reader.shutdown() -> stream 被迫 None -> 级联退出
struct CdcRunner {
    config: RunnerConfig,
}

impl CdcRunner {
    fn new(config: RunnerConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl TaskRunner for CdcRunner {
    async fn run(
        &self,
        reader: Arc<dyn DataReader>,
        writer: Arc<dyn DataWriter>,
        job_config: Arc<JobConfig>,
        cancel_token: CancellationToken,
    ) -> Result<RunResult> {
        let start_time = Instant::now();
        let pipeline_config = self.config.to_pipeline_config();

        let pipeline_token = cancel_token.clone();
        let pipeline_future = run_pipeline(pipeline_config, reader, writer, job_config, pipeline_token);
        let cancelled = cancel_token.cancelled();

        tokio::pin!(pipeline_future);

        tokio::select! {
            _ = cancelled => {
                info!("[CdcRunner] 收到停止信号，开始关闭");
                let result = (&mut pipeline_future).await?;

                let elapsed = start_time.elapsed();
                let mut stats = RunnerStats::from_pipeline(result);
                stats.elapsed_secs = elapsed.as_secs_f64();
                stats.calculate_throughput();
                Ok(RunResult {
                    stats,
                    status: RunStatus::Shutdown,
                    duration: elapsed,
                    error: None,
                })
            }
            result = &mut pipeline_future => {
                let pipeline_result = result?;

                let elapsed = start_time.elapsed();
                let mut stats = RunnerStats::from_pipeline(pipeline_result);
                stats.elapsed_secs = elapsed.as_secs_f64();
                stats.calculate_throughput();

                let status = if stats.records_failed > 0 {
                    RunStatus::Partial
                } else {
                    RunStatus::Failed
                };

                warn!("[CdcRunner] Pipeline 非预期退出");
                Ok(RunResult {
                    stats,
                    status,
                    duration: elapsed,
                    error: Some("CDC pipeline 非预期退出".to_string()),
                })
            }
        }
    }
}

// ==========================================
// 分发入口
// ==========================================

/// 根据 StreamMode 选择对应的 TaskRunner 策略执行同步
async fn dispatch_runner(
    stream_mode: StreamMode,
    config: RunnerConfig,
    reader: Arc<dyn DataReader>,
    writer: Arc<dyn DataWriter>,
    job_config: Arc<JobConfig>,
    cancel_token: CancellationToken,
) -> Result<RunResult> {
    let runner: Box<dyn TaskRunner> = match stream_mode {
        StreamMode::Finite => Box::new(BatchRunner::new(config)),
        StreamMode::Infinite => Box::new(CdcRunner::new(config)),
    };
    runner.run(reader, writer, job_config, cancel_token).await
}

/// 根据配置动态创建 Reader/Writer 并执行同步
///
/// 通过 ReaderRegistry / WriterRegistry 根据 source_type 动态创建对应的数据源实例，
/// 然后根据 Reader 返回的 StreamMode 选择 BatchRunner 或 CdcRunner。
pub async fn run_sync(config: Arc<JobConfig>, cancel_token: CancellationToken) -> Result<RunResult> {
    super::registry::ensure_initialized();

    let reader_registry = ReaderRegistry::instance();
    let writer_registry = WriterRegistry::instance();

    let reader: Arc<dyn DataReader> =
        Arc::from(reader_registry.prepare_reader(&config.source.source_type, Arc::clone(&config))?);
    let writer: Arc<dyn DataWriter> =
        Arc::from(writer_registry.prepare_writer(&config.target.source_type, Arc::clone(&config))?);

    // 先 split 获取 StreamMode，用于选择策略
    let runner_config = RunnerConfig::from_job_config(&config);
    let split_result = reader.split(runner_config.reader_threads).await?;
    let stream_mode = split_result.stream_mode;

    info!(
        "[run_sync] {} 模式, {} 个任务, 总记录数 {}",
        match stream_mode {
            StreamMode::Finite => "Batch",
            StreamMode::Infinite => "Cdc",
        },
        split_result.tasks.len(),
        split_result.total_records
    );

    dispatch_runner(stream_mode, runner_config, reader, writer, config, cancel_token).await
}
