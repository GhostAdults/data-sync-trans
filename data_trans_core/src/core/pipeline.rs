//! 数据同步管道执行器
//!
//! Reader → Channel → Writer 1:1 配对架构
//!
//! - 一个 Job 被 Reader split 为 N 个 Task
//! - Writer 以相同数量 N split，形成 N 个 1:1 Pair
//! - 每个 Pair 拥有独立 mpsc channel，天然反压、无竞争
//! - Reader 任务先启动，Writer 任务后启动
//! - 任一任务失败时快速传播，其他任务尽快终止

use anyhow::Result;
use data_trans_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_READER_THREADS,
};
use data_trans_common::interface::{ReaderJob, WriterJob};
use data_trans_common::pipeline::PipelineMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::progress::create_progress_bars;

// ==========================================
// 配置
// ==========================================

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（决定 Task 数量）
    pub reader_threads: usize,
    /// Channel 缓冲区大小
    pub buffer_size: usize,
    /// 批处理大小
    pub batch_size: usize,
    /// 是否使用事务
    pub use_transaction: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: DEFAULT_READER_THREADS,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl PipelineConfig {
    /// 从系统配置读取 pipeline 参数
    ///
    /// 优先级：系统配置 (default.config.json) > 常量默认值
    pub fn from_system_config(job_config: &data_trans_common::JobConfig) -> Self {
        let (sys_reader, sys_buffer, sys_batch, sys_tx) = crate::get_config_manager()
            .map(|mgr| {
                let m = mgr.read();
                (
                    m.get("pipeline.reader_threads")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as usize),
                    m.get("pipeline.buffer_size")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as usize),
                    m.get("pipeline.batch_size")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as usize),
                    m.get("pipeline.use_transaction").and_then(|v| v.as_bool()),
                )
            })
            .unwrap_or((None, None, None, None));

        Self {
            reader_threads: sys_reader.unwrap_or(DEFAULT_READER_THREADS),
            buffer_size: sys_buffer.unwrap_or(DEFAULT_BUFFER_SIZE),
            // batch_size代表了Reader每次读取多少条记录后发送到Channel，过大可能导致内存压力，过小可能增加通信开销
            batch_size: sys_batch
                .or(job_config.batch_size)
                .unwrap_or(DEFAULT_BATCH_SIZE),
            use_transaction: sys_tx.unwrap_or(true),
        }
    }
}

/// 管道执行统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub elapsed_secs: f64,
    pub throughput: f64,
}

impl PipelineStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }

    pub fn records_failed(&self) -> usize {
        self.records_read.saturating_sub(self.records_written)
    }
}

/// 执行管道：Reader → Writer 1:1 配对
pub async fn run_pipeline(
    config: PipelineConfig,
    reader: Box<dyn ReaderJob>,
    writer: Box<dyn WriterJob<PipelineMessage>>,
) -> Result<PipelineStats> {
    let reader: Arc<dyn ReaderJob> = Arc::from(reader);
    let writer: Arc<dyn WriterJob<PipelineMessage>> = Arc::from(writer);
    run_paired_pipeline(&config, reader, writer).await
}

/// 1:1 Pair 架构核心实现
async fn run_paired_pipeline(
    config: &PipelineConfig,
    reader: Arc<dyn ReaderJob>,
    writer: Arc<dyn WriterJob<PipelineMessage>>,
) -> Result<PipelineStats> {
    let start_time = Instant::now();

    // 1. 先 split Reader，得到 Task 数量
    let reader_split = reader.split(config.reader_threads).await?;
    let task_count = reader_split.tasks.len();
    info!(
        "Reader: 总记录数 {}, 切分为 {} 个任务",
        reader_split.total_records, task_count
    );

    if task_count == 0 {
        info!("无读取任务，Pipeline 结束");
        return Ok(PipelineStats::default());
    }

    let progress_ctx = create_progress_bars(reader_split.total_records);

    // 2. 以 Reader Task 数量 split Writer，保证 1:1
    let writer_split = writer.split(task_count).await?;
    info!(
        "Writer: 切分为 {} 个任务（与 Reader 1:1 配对）",
        writer_split.tasks.len()
    );

    // 修复问题6：配对不足直接报错，避免静默丢数据
    if writer_split.tasks.len() < task_count {
        return Err(anyhow::anyhow!(
            "Writer 只产出 {} 个任务，但有 {} 个 Reader 任务，无法完全配对",
            writer_split.tasks.len(),
            task_count
        ));
    }

    // 3. 快速失败：通过 cancel 信号通知所有任务终止
    let cancel = Arc::new(tokio::sync::Notify::new());

    // 4. 为每对创建独立 channel + spawn 任务
    let mut pair_handles = Vec::with_capacity(task_count);

    for i in 0..task_count {
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // spawn Reader
        let r = Arc::clone(&reader);
        let read_task = reader_split.tasks[i].clone();
        let cancel_r = Arc::clone(&cancel);
        let r_handle = tokio::spawn(async move {
            // 检查是否已被取消
            tokio::select! {
                result = r.execute_task(read_task, tx.clone()) => {
                    match &result {
                        Ok(_) => {
                            let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        }
                        Err(e) => {
                            error!("Reader-{} 失败: {}", i, e);
                            let _ = tx.send(PipelineMessage::Error(e.to_string())).await;
                            // 通知其他 Pair 终止
                            cancel_r.notify_waiters();
                        }
                    }
                    result
                }
                () = cancel_r.notified() => {
                    warn!("Reader-{} 被取消（其他任务失败）", i);
                    Err(anyhow::anyhow!("Reader-{} 被取消", i))
                }
            }
        });

        // spawn Writer
        let w = Arc::clone(&writer);
        let write_task = writer_split.tasks[i].clone();
        let cancel_w = Arc::clone(&cancel);
        let w_handle = tokio::spawn(async move {
            tokio::select! {
                result = w.execute_task(write_task, rx) => {
                    if let Err(ref e) = result {
                        error!("Writer-{} 失败: {}", i, e);
                        // Writer 失败也要通知其他 Pair 终止
                        cancel_w.notify_waiters();
                    }
                    result
                }
                () = cancel_w.notified() => {
                    warn!("Writer-{} 被取消（其他任务失败）", i);
                    Err(anyhow::anyhow!("Writer-{} 被取消", i))
                }
            }
        });

        pair_handles.push((r_handle, w_handle));
    }

    // 5. 等待所有 Pair 完成（Reader 先完成，Writer 后完成）
    let mut total_read = 0;
    let mut total_written = 0;
    let mut any_failed = false;
    let mut first_error: Option<anyhow::Error> = None;

    for (i, (r_handle, w_handle)) in pair_handles.into_iter().enumerate() {
        match r_handle.await {
            Ok(Ok(count)) => {
                total_read += count;
                progress_ctx.reader_bar.inc(count as u64);
                info!("Reader-{} 完成，读取 {} 条", i, count);
            }
            Ok(Err(e)) => {
                error!("Reader-{} 失败: {}", i, e);
                any_failed = true;
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) => {
                error!("Reader-{} 任务崩溃: {}", i, e);
                any_failed = true;
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("Reader-{} 任务崩溃: {}", i, e));
                }
            }
        }

        match w_handle.await {
            Ok(Ok(count)) => {
                total_written += count;
                progress_ctx.writer_bar.inc(count as u64);
                info!("Writer-{} 完成，写入 {} 条", i, count);
            }
            Ok(Err(e)) => {
                error!("Writer-{} 失败: {}", i, e);
                any_failed = true;
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) => {
                error!("Writer-{} 任务崩溃: {}", i, e);
                any_failed = true;
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("Writer-{} 任务崩溃: {}", i, e));
                }
            }
        }
    }

    // 6. 汇总统计
    let elapsed = start_time.elapsed();
    let mut stats = PipelineStats {
        records_read: total_read,
        records_written: total_written,
        elapsed_secs: elapsed.as_secs_f64(),
        ..Default::default()
    };
    stats.records_failed = stats.records_failed();
    stats.calculate_throughput();

    progress_ctx.finish(stats.elapsed_secs);

    info!(
        "Pipeline 完成: 读取 {} 条, 写入 {} 条, 失败 {} 条, 耗时 {:.2}s, 吞吐 {:.2} 条/秒",
        stats.records_read,
        stats.records_written,
        stats.records_failed,
        stats.elapsed_secs,
        stats.throughput
    );

    if any_failed {
        return Err(first_error.unwrap_or_else(|| anyhow::anyhow!("Pipeline 执行存在失败任务")));
    }

    Ok(stats)
}
