//! 数据同步管道执行器
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
use relus_reader::{ReadTask, Reader, StreamMode};
use relus_writer::{WriteTask, Writer};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Notify};
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
}

struct GroupResult {
    group_id: usize,
    total_read: usize,
    total_written: usize,
    error: Option<anyhow::Error>,
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

/// 执行管道：Reader → Writer 1:1
pub async fn run_pipeline(
    config: PipelineConfig,
    reader: Box<dyn Reader>,
    writer: Box<dyn Writer>,
    job_config: Arc<relus_common::JobConfig>,
) -> Result<PipelineStats> {
    let reader: Arc<dyn Reader> = Arc::from(reader);
    let writer: Arc<dyn Writer> = Arc::from(writer);

    // 从 JobConfig 构建 RecordBuilder
    let source_type = SourceType::from_str(&job_config.input.source_type);
    let record_builder = Arc::new(
        RecordBuilder::new(
            job_config.column_mapping.clone(),
            job_config.column_types.clone(),
        )
        .with_source_type(source_type),
    );

    run_paired_pipeline(&config, reader, writer, record_builder).await
}

/// 从 Reader 获取 JsonStream，消费并通过 RecordBuilder mapping 后发送到 channel
/// consume_stream_and_send
async fn csas(
    pair_id: usize,
    reader: Arc<dyn Reader>,
    task: &ReadTask,
    batch_size: usize,
    builder: &RecordBuilder,
    tx: &mpsc::Sender<PipelineMessage>,
) -> Result<usize> {
    let stream = reader.read_data(task).await?;
    let mut sent = 0;
    let mut buffer = Vec::with_capacity(batch_size);

    futures::pin_mut!(stream);
    while let Some(result) = stream.next().await {
        let json_val = result?;
        buffer.push(json_val);

        if buffer.len() >= batch_size {
            let message = builder.build_message(&buffer)?;
            tx.send(message)
                .await
                .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
            sent += buffer.len();
            buffer.clear();
        }
    }

    // 发送残余数据
    if !buffer.is_empty() {
        let message = builder.build_message(&buffer)?;
        tx.send(message)
            .await
            .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
        sent += buffer.len();
    }

    info!("Reader-{} 已发送 {} 条（core mapping）", pair_id, sent);
    Ok(sent)
}

async fn run_task_pair(
    pair_id: usize,
    reader: Arc<dyn Reader>,
    writer: Arc<dyn Writer>,
    read_task: ReadTask,
    write_task: WriteTask,
    buffer_size: usize,
    batch_size: usize,
    record_builder: Arc<RecordBuilder>,
    cancel: Arc<Notify>,
    stream_mode: StreamMode,
) -> PairResult {
    let (tx, rx) = mpsc::channel(buffer_size);

    let r = Arc::clone(&reader);
    let cancel_r = Arc::clone(&cancel);
    let builder = Arc::clone(&record_builder);
    let r_handle = tokio::spawn(async move {
        tokio::select! {
            result = csas(pair_id, r, &read_task, batch_size, &builder, &tx) => {
                match stream_mode {
                    StreamMode::Finite => {
                        if result.is_ok() {
                            let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        } else if let Err(ref e) = result {
                            error!("Reader-{} 失败: {}", pair_id, e);
                            let _ = tx.send(PipelineMessage::Error(e.to_string())).await;
                            cancel_r.notify_waiters();
                        }
                    }
                    StreamMode::Infinite => {
                        if let Err(ref e) = result {
                            error!("Reader-{} 流错误: {}", pair_id, e);
                            let _ = tx.send(PipelineMessage::Error(e.to_string())).await;
                            cancel_r.notify_waiters();
                        }
                    }
                }
                result.map(|count| count)
            }
            () = cancel_r.notified() => {
                warn!("Reader-{} 被终止（其他任务失败）", pair_id);
                Err(anyhow::anyhow!("Reader-{} 被终止", pair_id))
            }
            // os windows ctrl+c 信号
            () = async {
                if matches!(stream_mode, StreamMode::Infinite) {
                    let _ = tokio::signal::ctrl_c().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                println!("[Pipeline] Stoping...");
                info!("[Pipeline] Exit with: Ctrl+C signal，开始关闭");
                reader.shutdown(); // close reader stream
                cancel_r.notify_waiters();
                Err(anyhow::anyhow!("Reader-{} 进程被终止", pair_id))
            }
        }
    });

    let w = Arc::clone(&writer);
    let cancel_w = Arc::clone(&cancel);
    let w_handle = tokio::spawn(async move {
        tokio::select! {
            result = w.write_data(write_task, rx) => {
                if let Err(ref e) = result {
                    error!("Writer-{} 失败: {}", pair_id, e);
                    cancel_w.notify_waiters();
                }
                result
            }
            () = cancel_w.notified() => {
                warn!("Writer-{} 被终止（其他任务失败）", pair_id);
                Err(anyhow::anyhow!("Writer-{} 被终止", pair_id))
            }
        }
    });

    let reader_result = match r_handle.await {
        Ok(r) => r,
        Err(e) => Err(anyhow::anyhow!("Reader-{} 任务崩溃: {}", pair_id, e)),
    };

    let writer_result = match w_handle.await {
        Ok(r) => r,
        Err(e) => Err(anyhow::anyhow!("Writer-{} 任务崩溃: {}", pair_id, e)),
    };

    match (reader_result, writer_result) {
        (Ok(read_count), Ok(write_count)) => PairResult {
            pair_id,
            read_count,
            write_count,
            error: None,
        },
        (Err(e), Ok(_)) => PairResult {
            pair_id,
            read_count: 0,
            write_count: 0,
            error: Some(e),
        },
        (Ok(_), Err(e)) => PairResult {
            pair_id,
            read_count: 0,
            write_count: 0,
            error: Some(e),
        },
        (Err(reader_err), Err(writer_err)) => PairResult {
            pair_id,
            read_count: 0,
            write_count: 0,
            error: Some(anyhow::anyhow!(
                "Reader/Writer 同时失败: {}; {}",
                reader_err,
                writer_err
            )),
        },
    }
}

async fn run_task_group(
    group_id: usize,
    tasks: Vec<(usize, ReadTask, WriteTask)>,
    concurrency: usize,
    reader: Arc<dyn Reader>,
    writer: Arc<dyn Writer>,
    buffer_size: usize,
    batch_size: usize,
    record_builder: Arc<RecordBuilder>,
    cancel: Arc<Notify>,
    reader_bar: indicatif::ProgressBar,
    writer_bar: indicatif::ProgressBar,
    stream_mode: StreamMode,
) -> GroupResult {
    let mut queue: VecDeque<(usize, ReadTask, WriteTask)> = VecDeque::from(tasks);
    let mut running = FuturesUnordered::new();

    let mut total_read = 0usize;
    let mut total_written = 0usize;
    let mut first_error: Option<anyhow::Error> = None;

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
                Arc::clone(&cancel),
                stream_mode,
            ));
        } else {
            break;
        }
    }

    while let Some(pair_result) = running.next().await {
        if let Some(e) = pair_result.error {
            error!(
                "TaskGroup-{} 的 Pair-{} 失败: {}",
                group_id, pair_result.pair_id, e
            );
            if first_error.is_none() {
                first_error = Some(e);
            }
            cancel.notify_waiters();
        } else {
            total_read += pair_result.read_count;
            total_written += pair_result.write_count;
            reader_bar.inc(pair_result.read_count as u64);
            writer_bar.inc(pair_result.write_count as u64);
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
                        Arc::clone(&cancel),
                        stream_mode,
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
    }
}

/// 1:1 Pair
async fn run_paired_pipeline(
    config: &PipelineConfig,
    reader: Arc<dyn Reader>,
    writer: Arc<dyn Writer>,
    record_builder: Arc<RecordBuilder>,
) -> Result<PipelineStats> {
    let start_time = Instant::now();

    let reader_split = reader.split(config.reader_threads).await?;
    let task_count = reader_split.tasks.len();
    let stream_mode = reader_split.stream_mode;
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

    let cancel = Arc::new(Notify::new());
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
            Arc::clone(&cancel),
            reader_bar.clone(),
            writer_bar.clone(),
            stream_mode,
        );

        group_handles.push(tokio::spawn(group_future));
    }

    let mut total_read = 0usize;
    let mut total_written = 0usize;
    let mut first_error: Option<anyhow::Error> = None;

    while let Some(group_result) = group_handles.next().await {
        match group_result {
            Ok(result) => {
                total_read += result.total_read;
                total_written += result.total_written;
                info!(
                    "TaskGroup-{} 结束: read={}, write={}",
                    result.group_id, result.total_read, result.total_written
                );
                if let Some(e) = result.error {
                    if first_error.is_none() {
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
