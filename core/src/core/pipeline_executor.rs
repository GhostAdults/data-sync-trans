//! Pipeline Executor 模块
//!
//! Reader → Channel → Writer 1:1
//!
//! - 一个 Job 被 Reader split 为 N 个 ReadTask
//! - Writer 以相同数量 N split，形成 N 个 1:1 Pair
//! - 通过 TaskGroup + TaskExecutor 控制并发
//!
//! Core 层负责 stream 消费、buffer 切分、RecordBuilder mapping 和 channel 发送

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use relus_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_NUMBER, DEFAULT_PER_GROUP_CHANNEL,
    DEFAULT_READER_THREADS,
};
use relus_common::pipeline::PipelineMessage;
use relus_common::types::SourceType;
use relus_reader::{DataReader, ReadTask};
use relus_writer::{DataWriter, WriteTask};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::progress::create_progress_bars;
use crate::pipeline::RecordBuilder;

// ==========================================
// 配置
// ==========================================

struct PairResult {
    pair_id: usize,
    read_count: usize,
    write_count: usize,
    error: Option<anyhow::Error>,
    shutdown: bool,
}

struct GroupResult {
    group_id: usize,
    total_read: usize,
    total_written: usize,
    error: Option<anyhow::Error>,
    shutdown: bool,
}

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（决定 Task 数量）
    pub reader_threads: usize,
    /// Channel 缓冲区大小
    pub buffer_size: usize,
    /// 全局并发 channel 数
    pub channel_number: usize,
    /// 每个 TaskGroup 内并发 channel 数
    pub per_group_channel: usize,
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
            channel_number: DEFAULT_CHANNEL_NUMBER,
            per_group_channel: DEFAULT_PER_GROUP_CHANNEL,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl PipelineConfig {
    /// 从系统配置读取 pipeline 参数
    ///
    /// 优先级：系统配置 (default.config.json) > 常量默认值
    pub fn from_system_config(job_config: &relus_common::JobConfig) -> Self {
        let (sys_reader, sys_buffer, sys_channel, sys_per_group, sys_batch, sys_tx) =
            crate::get_config_manager()
                .map(|mgr| {
                    let m = mgr.read();
                    (
                        m.get("pipeline.reader_threads")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as usize),
                        m.get("pipeline.buffer_size")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as usize),
                        m.get("pipeline.channel_number")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as usize),
                        m.get("pipeline.per_group_channel")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as usize),
                        m.get("pipeline.batch_size")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as usize),
                        m.get("pipeline.use_transaction").and_then(|v| v.as_bool()),
                    )
                })
                .unwrap_or((None, None, None, None, None, None));

        Self {
            reader_threads: sys_reader.unwrap_or(DEFAULT_READER_THREADS),
            buffer_size: sys_buffer.unwrap_or(DEFAULT_BUFFER_SIZE),
            channel_number: sys_channel.unwrap_or(DEFAULT_CHANNEL_NUMBER),
            per_group_channel: sys_per_group.unwrap_or(DEFAULT_PER_GROUP_CHANNEL),
            batch_size: job_config
                .batch_size
                .or(sys_batch)
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
    pub shutdown: bool,
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

/// Pipeline 的执行逻辑：Reader → Writer 1:1
pub async fn start_run(
    config: PipelineConfig,
    reader: Arc<dyn DataReader>,
    writer: Arc<dyn DataWriter>,
    job_config: Arc<relus_common::JobConfig>,
    cancel_token: CancellationToken,
) -> Result<PipelineStats> {
    // 从 JobConfig 构建 RecordBuilder
    let source_type = SourceType::from_str(&job_config.source.source_type);
    let record_builder = Arc::new(
        RecordBuilder::new(
            job_config.column_mapping.clone(),
            job_config.column_types.clone(),
        )
        .with_source_type(source_type),
    );

    run_paired_pipeline(&config, reader, writer, record_builder, cancel_token).await
}

/// 从 Reader 获取 JsonStream，消费并通过 RecordBuilder mapping 后发送到 channel
/// consume_stream_and_send
async fn csas(
    pair_id: usize,
    reader: Arc<dyn DataReader>,
    task: &ReadTask,
    batch_size: usize,
    builder: &RecordBuilder,
    tx: &mpsc::Sender<PipelineMessage>,
    reader_bar: &indicatif::ProgressBar,
) -> Result<usize> {
    let stream = reader.read_data(task).await?;
    let mut sent = 0;
    let mut buffer = Vec::with_capacity(batch_size);

    futures::pin_mut!(stream);
    while let Some(result) = stream.next().await {
        let json_val = result?;
        buffer.push(json_val);

        if buffer.len() >= batch_size {
            let count = buffer.len();
            let message = builder.build_message(&buffer)?;
            tx.send(message)
                .await
                .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
            sent += count;
            reader_bar.inc(count as u64);
            buffer.clear();
        }
    }

    // 发送残余数据
    if !buffer.is_empty() {
        let count = buffer.len();
        let message = builder.build_message(&buffer)?;
        tx.send(message)
            .await
            .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
        sent += count;
        reader_bar.inc(count as u64);
    }

    info!("Reader-{} 已发送 {} 条（core mapping）", pair_id, sent);
    Ok(sent)
}

async fn run_task_pair(
    pair_id: usize,
    reader: Arc<dyn DataReader>,
    writer: Arc<dyn DataWriter>,
    read_task: ReadTask,
    write_task: WriteTask,
    buffer_size: usize,
    batch_size: usize,
    record_builder: Arc<RecordBuilder>,
    cancel_token: CancellationToken,
    reader_bar: indicatif::ProgressBar,
    writer_bar: indicatif::ProgressBar,
) -> PairResult {
    let (tx, rx) = mpsc::channel(buffer_size);
    let was_cancelled = cancel_token.is_cancelled();

    let r = Arc::clone(&reader);
    let builder = Arc::clone(&record_builder);
    let reader_cancel = cancel_token.clone();
    let r_bar = reader_bar.clone();
    let r_handle = tokio::spawn(async move {
        tokio::select! {
            result = csas(pair_id, r, &read_task, batch_size, &builder, &tx, &r_bar) => {
                match result {
                    Ok(count) => {
                        let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        Ok(count)
                    }
                    Err(e) => {
                        error!("Reader-{} 失败: {}", pair_id, e);
                        let _ = tx.send(PipelineMessage::Error(e.to_string())).await;
                        Err(e)
                    }
                }
            }
            () = reader_cancel.cancelled() => {
                warn!("Reader-{} being eliminated.【outside】", pair_id);
                reader.shutdown();
                Err(anyhow::anyhow!("Reader-{} process terminates unexpectedly.", pair_id))
            }
        }
    });

    // 中间转发 task: rx → writer_bar.inc → tx2，Writer 拿 rx2
    let (tx2, rx2) = mpsc::channel(buffer_size);
    let w_bar = writer_bar.clone();
    let relay_handle = tokio::spawn(async move {
        let mut rx = rx;
        while let Some(msg) = rx.recv().await {
            if let PipelineMessage::DataBatch(rows) = &msg {
                w_bar.inc(rows.len() as u64);
            }
            if tx2.send(msg).await.is_err() {
                break;
            }
        }
    });

    let w = Arc::clone(&writer);
    let writer_cancel = cancel_token.clone();
    let w_handle = tokio::spawn(async move {
        tokio::select! {
            result = w.write_data(write_task, rx2) => {
                if let Err(ref e) = result {
                    error!("Writer-{} 失败: {}", pair_id, e);
                }
                result
            }
            () = writer_cancel.cancelled() => {
                warn!("Writer-{} 正常关闭", pair_id);
                Ok(0)
            }
        }
    });

    // 确保 relay task 不泄漏
    let _ = relay_handle.await;

    let reader_result = r_handle
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Reader-{} 任务崩溃: {}", pair_id, e)));
    let writer_result = w_handle
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Writer-{} 任务崩溃: {}", pair_id, e)));

    let (read_count, read_err) = match reader_result {
        Ok(n) => (n, None),
        Err(e) => (0, Some(e)),
    };
    let (write_count, write_err) = match writer_result {
        Ok(n) => (n, None),
        Err(e) => (0, Some(e)),
    };

    let error = match (read_err, write_err) {
        (Some(r), Some(w)) => Some(anyhow::anyhow!("R/W FULL FAIL: {}; {}", r, w)),
        (Some(e), None) | (None, Some(e)) => Some(e),
        _ => None,
    };

    PairResult {
        pair_id,
        read_count,
        write_count,
        error,
        shutdown: was_cancelled,
    }
}

async fn run_task_group(
    group_id: usize,
    tasks: Vec<(usize, ReadTask, WriteTask)>,
    concurrency: usize,
    reader: Arc<dyn DataReader>,
    writer: Arc<dyn DataWriter>,
    buffer_size: usize,
    batch_size: usize,
    record_builder: Arc<RecordBuilder>,
    cancel_token: CancellationToken,
    reader_bar: indicatif::ProgressBar,
    writer_bar: indicatif::ProgressBar,
) -> GroupResult {
    let mut queue: VecDeque<(usize, ReadTask, WriteTask)> = VecDeque::from(tasks);
    let mut running = FuturesUnordered::new();

    let mut total_read = 0usize;
    let mut total_written = 0usize;
    let mut first_error: Option<anyhow::Error> = None;
    let mut group_shutdown = false;

    while running.len() < concurrency {
        if let Some((pair_id, read_task, write_task)) = queue.pop_front() {
            running.push(run_task_pair(
                pair_id,
                Arc::clone(&reader),
                Arc::clone(&writer),
                read_task,
                write_task,
                buffer_size,
                batch_size,
                Arc::clone(&record_builder),
                cancel_token.clone(),
                reader_bar.clone(),
                writer_bar.clone(),
            ));
        } else {
            break;
        }
    }

    while let Some(pair_result) = running.next().await {
        if pair_result.shutdown {
            group_shutdown = true;
        }

        if let Some(e) = pair_result.error {
            error!(
                "TaskGroup-{} 的 Pair-{} 失败: {}",
                group_id, pair_result.pair_id, e
            );
            if first_error.is_none() && !pair_result.shutdown {
                first_error = Some(e);
            }
            cancel_token.cancel();
        } else {
            total_read += pair_result.read_count;
            total_written += pair_result.write_count;
            info!(
                "TaskGroup-{} 的 Pair-{} 完成，读取 {} 条，写入 {} 条",
                group_id, pair_result.pair_id, pair_result.read_count, pair_result.write_count
            );
        }

        if first_error.is_none() {
            while running.len() < concurrency {
                if let Some((pair_id, read_task, write_task)) = queue.pop_front() {
                    running.push(run_task_pair(
                        pair_id,
                        Arc::clone(&reader),
                        Arc::clone(&writer),
                        read_task,
                        write_task,
                        buffer_size,
                        batch_size,
                        Arc::clone(&record_builder),
                        cancel_token.clone(),
                        reader_bar.clone(),
                        writer_bar.clone(),
                    ));
                } else {
                    break;
                }
            }
        }
    }

    GroupResult {
        group_id,
        total_read,
        total_written,
        error: first_error,
        shutdown: group_shutdown,
    }
}

/// 1:1 Pair
async fn run_paired_pipeline(
    config: &PipelineConfig,
    reader: Arc<dyn DataReader>,
    writer: Arc<dyn DataWriter>,
    record_builder: Arc<RecordBuilder>,
    cancel_token: CancellationToken,
) -> Result<PipelineStats> {
    let start_time = Instant::now();

    let reader_split = reader.split(config.reader_threads).await?;
    let task_count = reader_split.tasks.len();
    info!(
        "[{}] 总记录数 {}, 切分为 {} 个任务",
        reader.description(),
        reader_split.total_records,
        task_count
    );
    if task_count == 0 {
        info!("[{}] 无读取任务，Pipeline 关闭", reader.description());
        return Ok(PipelineStats::default());
    }

    let progress_ctx = create_progress_bars(reader_split.total_records);
    let reader_bar = progress_ctx.reader_bar.clone();
    let writer_bar = progress_ctx.writer_bar.clone();

    let writer_split = writer.split(task_count).await?;
    info!(
        "[{}] 切分为 {} 个任务（R1:W1）",
        writer.description(),
        writer_split.tasks.len()
    );

    if writer_split.tasks.len() < task_count {
        return Err(anyhow::anyhow!(
            "Writer 只产出 {} 个任务，但有 {} 个 Reader 任务.",
            writer_split.tasks.len(),
            task_count
        ));
    }

    let need_channel = config.channel_number.max(1).min(task_count);
    let per_group_channel = config.per_group_channel.max(1);
    let group_count = need_channel.div_ceil(per_group_channel);

    let mut grouped_tasks: Vec<Vec<(usize, ReadTask, WriteTask)>> = vec![Vec::new(); group_count];
    for i in 0..task_count {
        let group_id = i % group_count;
        grouped_tasks[group_id].push((
            i,
            reader_split.tasks[i].clone(),
            writer_split.tasks[i].clone(),
        ));
    }

    let base_group_concurrency = need_channel / group_count;
    let extra_group_concurrency = need_channel % group_count;

    info!(
        "Pipeline 准备配置: task_count={}, need_channel={}, group_count={}, per_group_channel={}",
        task_count, need_channel, group_count, per_group_channel
    );

    let mut group_handles = FuturesUnordered::new();

    for (group_id, tasks) in grouped_tasks.into_iter().enumerate() {
        if tasks.is_empty() {
            continue;
        }

        let group_concurrency =
            base_group_concurrency + usize::from(group_id < extra_group_concurrency);

        info!(
            "TaskGroup-{} 启动: tasks={}, concurrency={}",
            group_id,
            tasks.len(),
            group_concurrency
        );

        let group_future = run_task_group(
            group_id,
            tasks,
            group_concurrency,
            Arc::clone(&reader),
            Arc::clone(&writer),
            config.buffer_size,
            config.batch_size,
            Arc::clone(&record_builder),
            cancel_token.clone(),
            reader_bar.clone(),
            writer_bar.clone(),
        );

        group_handles.push(tokio::spawn(group_future));
    }

    let mut total_read = 0usize;
    let mut total_written = 0usize;
    let mut first_error: Option<anyhow::Error> = None;
    let mut pipeline_shutdown = false;

    while let Some(group_result) = group_handles.next().await {
        match group_result {
            Ok(result) => {
                total_read += result.total_read;
                total_written += result.total_written;
                if result.shutdown {
                    pipeline_shutdown = true;
                }
                info!(
                    "TaskGroup-{} 结束: read={}, write={}",
                    result.group_id, result.total_read, result.total_written
                );
                if let Some(e) = result.error {
                    if first_error.is_none() && !result.shutdown {
                        first_error = Some(e);
                    }
                }
            }
            Err(e) => {
                error!("TaskGroup 任务崩溃: {}", e);
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("TaskGroup 任务崩溃: {}", e));
                }
            }
        }
    }

    let elapsed = start_time.elapsed();
    let mut stats = PipelineStats {
        records_read: total_read,
        records_written: total_written,
        elapsed_secs: elapsed.as_secs_f64(),
        shutdown: pipeline_shutdown,
        ..Default::default()
    };
    stats.records_failed = stats.records_failed();
    stats.calculate_throughput();

    progress_ctx.finish();

    if let Some(e) = first_error {
        return Err(e);
    }

    Ok(stats)
}
