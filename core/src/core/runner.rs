//! Pipeline Runner 模块
//!
//! 职责：调起数据同步任务
//! - 解析配置
//! - 通过 Registry 动态创建 Reader/Writer
//! - 委托给 Pipeline 执行
//! - 返回结果

use anyhow::Result;
use relus_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_NUMBER, DEFAULT_PER_GROUP_CHANNEL,
    DEFAULT_READER_THREADS,
};
use relus_common::job_config::JobConfig;
use relus_reader::DataReader;
use relus_writer::DataWriter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

    /// 执行同步任务
    pub async fn run(
        &self,
        reader: Box<dyn DataReader>,
        writer: Box<dyn DataWriter>,
        job_config: Arc<JobConfig>,
    ) -> Result<RunResult> {
        let start_time = Instant::now();
        let pipeline_config = self.config.to_pipeline_config();
        let pipeline_stats = run_pipeline(pipeline_config, reader, writer, job_config).await?;
        let is_shutdown = pipeline_stats.shutdown;
        let elapsed = start_time.elapsed();
        let mut stats = RunnerStats::from_pipeline(pipeline_stats);
        stats.elapsed_secs = elapsed.as_secs_f64();
        stats.calculate_throughput();

        let status = if is_shutdown {
            RunStatus::Shutdown
        } else if stats.records_failed > 0 {
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

/// 根据配置动态创建 Reader/Writer 并执行同步
///
/// 通过 ReaderRegistry / WriterRegistry 根据 source_type 动态创建对应的数据源实例，
/// 支持任意已注册的 Reader/Writer 组合。
pub async fn run_sync(config: Arc<JobConfig>) -> Result<RunResult> {
    super::registry::ensure_initialized();

    let reader_registry = ReaderRegistry::instance();
    let writer_registry = WriterRegistry::instance();

    let reader = reader_registry.prepare_reader(&config.source.source_type, Arc::clone(&config))?;
    let writer = writer_registry.prepare_writer(&config.target.source_type, Arc::clone(&config))?;

    // 执行同步
    let runner = Runner::from_config(&config);
    runner.run(reader, writer, config).await
}
