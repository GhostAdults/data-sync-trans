//! Pipeline Runner 模块
//!
//! 职责：调起数据同步任务
//! - 解析配置
//! - 通过 Registry 动态创建 Reader/Writer
//! - 委托给 Pipeline 执行
//! - 返回结果

use anyhow::Result;
use data_trans_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_READER_THREADS, DEFAULT_WRITER_THREADS,
};
use data_trans_common::interface::{ReaderJob, WriterJob};
use data_trans_common::job_config::JobConfig;
use data_trans_common::pipeline::PipelineMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::pipeline::{run_dynamic_pipeline, run_processor, PipelineConfig, PipelineStats};
use super::registry::GlobalRegistry;

// ==========================================
// 配置类型
// ==========================================

/// Runner 配置
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub buffer_size: usize,
    pub reader_threads: usize,
    pub writer_threads: usize,
    pub batch_size: usize,
    pub use_transaction: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            reader_threads: DEFAULT_READER_THREADS,
            writer_threads: DEFAULT_WRITER_THREADS,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl RunnerConfig {
    pub fn from_job_config(config: &JobConfig) -> Self {
        Self {
            buffer_size: config.channel_buffer_size,
            reader_threads: config.reader_threads,
            writer_threads: config.writer_threads,
            batch_size: config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            use_transaction: config.use_transaction,
        }
    }

    fn to_pipeline_config(&self) -> PipelineConfig {
        PipelineConfig {
            reader_threads: self.reader_threads,
            writer_threads: self.writer_threads,
            buffer_size: self.buffer_size,
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
    pub task_id: String,
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
// Runner 启动器
// ==========================================

/// Runner
pub struct Runner {
    config: RunnerConfig,
}

impl Runner {
    pub fn new(config: RunnerConfig) -> Self {
        Self { config }
    }

    pub fn from_config(job_config: &JobConfig) -> Self {
        let config = RunnerConfig::from_job_config(job_config);
        Self::new(config)
    }

    /// 使用动态分发的 Reader/Writer 执行同步
    pub async fn run_dynamic(
        &self,
        reader: Box<dyn ReaderJob>,
        writer: Box<dyn WriterJob<PipelineMessage>>,
    ) -> Result<RunResult> {
        let task_id = uuid::Uuid::new_v4().to_string();
        let start_time = Instant::now();
        let pipeline_config = self.config.to_pipeline_config();

        let pipeline_stats = run_dynamic_pipeline(pipeline_config, reader, writer).await?;

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
            task_id,
            stats,
            status,
            duration: elapsed,
            error: None,
        })
    }

    /// 使用静态分发的 Reader/Writer 执行同步
    pub async fn run_with_jobs<R, W>(&self, reader: Arc<R>, writer: Arc<W>) -> Result<RunResult>
    where
        R: ReaderJob + 'static,
        W: WriterJob<PipelineMessage> + 'static,
    {
        let task_id = uuid::Uuid::new_v4().to_string();
        let start_time = Instant::now();
        let pipeline_config = self.config.to_pipeline_config();

        let pipeline_stats = run_processor(pipeline_config, reader, writer).await?;

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
            task_id,
            stats,
            status,
            duration: elapsed,
            error: None,
        })
    }
}

/// 根据配置动态创建 Reader/Writer 并执行同步
///
/// 通过 GlobalRegistry 根据 source_type 动态创建对应的数据源实例，
/// 支持任意已注册的 Reader/Writer 组合。
pub async fn run_sync(config: Arc<JobConfig>) -> Result<RunResult> {
    let registry = GlobalRegistry::instance();

    // 动态创建 Reader 和 Writer
    let reader = registry.create_reader(&config.input.source_type, Arc::clone(&config))?;
    let writer = registry.create_writer(&config.output.source_type, Arc::clone(&config))?;

    // 执行同步
    let runner = Runner::from_config(&config);
    runner.run_dynamic(reader, writer).await
}
