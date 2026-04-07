//! 数据同步管道执行器
//!
//! Reader → Channel → Writer 架构
//!
//! - 创建和管理工作 Channel
//! - 启动 Reader/Writer 任务
//! - 实现背压控制和消息分发
//! - 收集执行统计

use anyhow::Result;
use data_trans_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_READER_THREADS, DEFAULT_WRITER_THREADS,
};
use data_trans_common::interface::{ReaderJob, WriterJob};
use data_trans_common::pipeline::PipelineMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// ==========================================
// 配置
// ==========================================

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数
    pub reader_threads: usize,
    /// Writer 线程数
    pub writer_threads: usize,
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
            writer_threads: DEFAULT_WRITER_THREADS,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
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

// ==========================================
// 管道处理器
// ==========================================

/// 管道处理器：协调 Reader → Writer
pub struct Pipeline<R, W>
where
    R: ReaderJob + 'static,
    W: WriterJob<PipelineMessage> + 'static,
{
    config: PipelineConfig,
    reader: Arc<R>,
    writer: Arc<W>,
}

impl<R, W> Pipeline<R, W>
where
    R: ReaderJob + 'static,
    W: WriterJob<PipelineMessage> + 'static,
{
    pub fn new(config: PipelineConfig, reader: Arc<R>, writer: Arc<W>) -> Self {
        Self {
            config,
            reader,
            writer,
        }
    }

    pub async fn run_processor(&self) -> Result<PipelineStats> {
        let start_time = Instant::now();
        let buffer_size = self.config.buffer_size;
        let reader_threads = self.config.reader_threads;
        let writer_threads = self.config.writer_threads;

        info!(
            "Pipeline 启动: {} Reader, {} Writer, 缓冲区 {}",
            reader_threads, writer_threads, buffer_size
        );

        // 创建 Channel: Reader -> Writer 分发器
        let (tx, rx) = mpsc::channel(buffer_size);

        // 启动 Writer 任务
        let writer_handle =
            spawn_writer_task(Arc::clone(&self.writer), rx, writer_threads, reader_threads);

        // 启动 Reader 任务
        let reader_handle = spawn_reader_task(Arc::clone(&self.reader), tx, reader_threads);

        // 等待所有任务完成
        let (reader_result, writer_result) = tokio::try_join!(reader_handle, writer_handle)?;

        // 汇总统计
        let elapsed = start_time.elapsed();
        let mut stats = PipelineStats {
            records_read: reader_result.unwrap_or(0),
            records_written: writer_result.unwrap_or(0),
            elapsed_secs: elapsed.as_secs_f64(),
            ..Default::default()
        };
        stats.records_failed = stats.records_failed();
        stats.calculate_throughput();

        info!(
            "Pipeline 完成: 读取 {} 条, 写入 {} 条, 失败 {} 条, 耗时 {:.2}s, 吞吐 {:.2} 条/秒",
            stats.records_read,
            stats.records_written,
            stats.records_failed,
            stats.elapsed_secs,
            stats.throughput
        );

        Ok(stats)
    }
}

/// 启动 Reader 任务
fn spawn_reader_task<R>(
    reader: Arc<R>,
    tx: mpsc::Sender<PipelineMessage>,
    reader_threads: usize,
) -> tokio::task::JoinHandle<Result<usize>>
where
    R: ReaderJob + 'static,
{
    tokio::spawn(async move {
        let split_result = reader.split(reader_threads).await?;
        let total = split_result.total_records;
        info!(
            "Reader: 总记录数 {}, 切分为 {} 个任务",
            total,
            split_result.tasks.len()
        );

        let mut handles = Vec::new();
        for task in split_result.tasks {
            let reader = Arc::clone(&reader);
            let tx = tx.clone();
            let handle = tokio::spawn(async move { reader.execute_task(task, tx).await });
            handles.push(handle);
        }

        let mut total_read = 0;
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_read += count;
                    info!("Reader-{} 完成，读取 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    error!("Reader-{} 失败: {}", i, e);
                }
                Err(e) => {
                    error!("Reader-{} 任务崩溃: {}", i, e);
                }
            }
        }

        // 发送完成信号
        if tx.send(PipelineMessage::ReaderFinished).await.is_err() {
            warn!("Reader: 发送完成信号失败");
        }

        Ok::<_, anyhow::Error>(total_read)
    })
}

/// 启动 Writer 任务（含内部分发器）
fn spawn_writer_task<W>(
    writer: Arc<W>,
    rx: mpsc::Receiver<PipelineMessage>,
    writer_threads: usize,
    reader_total: usize,
) -> tokio::task::JoinHandle<Result<usize>>
where
    W: WriterJob<PipelineMessage> + 'static,
{
    tokio::spawn(async move {
        let split_result = writer.split(writer_threads).await?;
        info!("Writer: 切分为 {} 个任务", split_result.tasks.len());

        let writer_count = split_result.tasks.len();
        if writer_count == 0 {
            return Ok(0);
        }

        // 为每个 Writer 创建独立 Channel
        let mut writer_txs = Vec::with_capacity(writer_count);
        let mut writer_handles = Vec::with_capacity(writer_count);

        for (i, task) in split_result.tasks.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel(100);
            writer_txs.push(tx);

            let writer = Arc::clone(&writer);
            let handle = tokio::spawn(async move { writer.execute_task(task, rx).await });
            writer_handles.push((i, handle));
        }

        // 分发器：轮询分发到各 Writer
        let mut rx = rx;
        let mut current_writer = 0;
        let mut reader_finished_count = 0;
        let mut writer_error: Option<anyhow::Error> = None;

        while let Some(msg) = rx.recv().await {
            match &msg {
                PipelineMessage::DataBatch(_) => {
                    if writer_txs[current_writer].send(msg).await.is_err() {
                        let err_msg = format!(
                            "Writer 分发器: Writer-{} Channel 已关闭，可能已失败",
                            current_writer
                        );
                        error!("{}", err_msg);
                        // 立即停止分发，检查 Writer 状态
                        drop(writer_txs);
                        writer_error = Some(anyhow::anyhow!("{}", err_msg));
                        break;
                    }
                    current_writer = (current_writer + 1) % writer_count;
                }
                PipelineMessage::ReaderFinished => {
                    reader_finished_count += 1;
                    info!(
                        "Writer 分发器: 收到 Reader 完成信号 ({}/{})",
                        reader_finished_count, reader_total
                    );

                    if reader_finished_count >= reader_total {
                        info!("Writer 分发器: 所有 Reader 完成，通知所有 Writer");
                        for tx in &writer_txs {
                            let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        }
                        drop(writer_txs);
                        break;
                    }
                }
                PipelineMessage::Error(err) => {
                    error!("Writer 分发器: 收到错误 - {}", err);
                    for tx in &writer_txs {
                        let _ = tx.send(PipelineMessage::Error(err.clone())).await;
                    }
                    drop(writer_txs);
                    writer_error = Some(anyhow::anyhow!("Reader 错误: {}", err));
                    break;
                }
            }
        }

        // 等待所有 Writer 完成
        let mut total_written = 0;
        for (i, handle) in writer_handles.into_iter() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_written += count;
                    info!("Writer-{} 完成，写入 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    error!("Writer-{} 失败: {}", i, e);
                    // 保存第一个错误
                    if writer_error.is_none() {
                        writer_error = Some(e);
                    }
                }
                Err(e) => {
                    error!("Writer-{} 任务崩溃: {}", i, e);
                    if writer_error.is_none() {
                        writer_error = Some(anyhow::anyhow!("Writer-{} 任务崩溃: {}", i, e));
                    }
                }
            }
        }

        // 如果有错误，返回错误而不是成功
        if let Some(e) = writer_error {
            return Err(e);
        }

        Ok::<_, anyhow::Error>(total_written)
    })
}

/// 使用 Reader/Writer 执行管道
pub async fn run_processor<R, W>(
    config: PipelineConfig,
    reader: Arc<R>,
    writer: Arc<W>,
) -> Result<PipelineStats>
where
    R: ReaderJob + 'static,
    W: WriterJob<PipelineMessage> + 'static,
{
    let processor = Pipeline::new(config, reader, writer);
    processor.run_processor().await
}

/// 动态分发版本的 Pipeline 执行
pub async fn run_dynamic_pipeline(
    config: PipelineConfig,
    reader: Box<dyn ReaderJob>,
    writer: Box<dyn WriterJob<PipelineMessage>>,
) -> Result<PipelineStats> {
    let reader: Arc<dyn ReaderJob> = Arc::from(reader);
    let writer: Arc<dyn WriterJob<PipelineMessage>> = Arc::from(writer);
    run_dynamic(config, reader, writer).await
}

/// 动态分发内部实现
async fn run_dynamic(
    config: PipelineConfig,
    reader: Arc<dyn ReaderJob>,
    writer: Arc<dyn WriterJob<PipelineMessage>>,
) -> Result<PipelineStats> {
    let start_time = Instant::now();
    let buffer_size = config.buffer_size;
    let reader_threads = config.reader_threads;
    let writer_threads = config.writer_threads;

    info!(
        "Pipeline 启动: {} Reader, {} Writer, 缓冲区 {}",
        reader_threads, writer_threads, buffer_size
    );

    // 创建 Channel: Reader -> Writer 分发器
    let (tx, rx) = mpsc::channel(buffer_size);

    // 启动 Writer 任务
    let writer_handle = spawn_writer_task_dynamic(writer, rx, writer_threads, reader_threads);

    // 启动 Reader 任务
    let reader_handle = spawn_reader_task_dynamic(reader, tx, reader_threads);

    // 等待所有任务完成
    let (reader_result, writer_result) = tokio::try_join!(reader_handle, writer_handle)?;

    // 汇总统计
    let elapsed = start_time.elapsed();
    let mut stats = PipelineStats {
        records_read: reader_result.unwrap_or(0),
        records_written: writer_result.unwrap_or(0),
        elapsed_secs: elapsed.as_secs_f64(),
        ..Default::default()
    };
    stats.records_failed = stats.records_failed();
    stats.calculate_throughput();

    info!(
        "Pipeline 完成: 读取 {} 条, 写入 {} 条, 失败 {} 条, 耗时 {:.2}s, 吞吐 {:.2} 条/秒",
        stats.records_read,
        stats.records_written,
        stats.records_failed,
        stats.elapsed_secs,
        stats.throughput
    );

    Ok(stats)
}

/// 启动 Reader 任务 (动态分发版本)
fn spawn_reader_task_dynamic(
    reader: Arc<dyn ReaderJob>,
    tx: mpsc::Sender<PipelineMessage>,
    reader_threads: usize,
) -> tokio::task::JoinHandle<Result<usize>> {
    tokio::spawn(async move {
        let split_result = reader.split(reader_threads).await?;
        let total = split_result.total_records;
        info!(
            "Reader: 总记录数 {}, 切分为 {} 个任务",
            total,
            split_result.tasks.len()
        );

        let mut handles = Vec::new();
        for task in split_result.tasks {
            let reader = Arc::clone(&reader);
            let tx = tx.clone();
            let handle = tokio::spawn(async move { reader.execute_task(task, tx).await });
            handles.push(handle);
        }

        let mut total_read = 0;
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_read += count;
                    info!("Reader-{} 完成，读取 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    error!("Reader-{} 失败: {}", i, e);
                }
                Err(e) => {
                    error!("Reader-{} 任务崩溃: {}", i, e);
                }
            }
        }

        // 发送完成信号
        if tx.send(PipelineMessage::ReaderFinished).await.is_err() {
            warn!("Reader: 发送完成信号失败");
        }

        Ok::<_, anyhow::Error>(total_read)
    })
}

/// 启动 Writer 任务 (动态分发)
fn spawn_writer_task_dynamic(
    writer: Arc<dyn WriterJob<PipelineMessage>>,
    rx: mpsc::Receiver<PipelineMessage>,
    writer_threads: usize,
    reader_total: usize,
) -> tokio::task::JoinHandle<Result<usize>> {
    tokio::spawn(async move {
        let split_result = writer.split(writer_threads).await?;
        info!("Writer: 切分为 {} 个任务", split_result.tasks.len());

        let writer_count = split_result.tasks.len();
        if writer_count == 0 {
            return Ok(0);
        }

        // 为每个 Writer 创建独立 Channel
        let mut writer_txs = Vec::with_capacity(writer_count);
        let mut writer_handles = Vec::with_capacity(writer_count);

        for (i, task) in split_result.tasks.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel(100);
            writer_txs.push(tx);

            let writer = Arc::clone(&writer);
            let handle = tokio::spawn(async move { writer.execute_task(task, rx).await });
            writer_handles.push((i, handle));
        }

        // 分发器：轮询分发到各 Writer
        let mut rx = rx;
        let mut current_writer = 0;
        let mut reader_finished_count = 0;
        let mut writer_error: Option<anyhow::Error> = None;

        while let Some(msg) = rx.recv().await {
            match &msg {
                PipelineMessage::DataBatch(_) => {
                    if writer_txs[current_writer].send(msg).await.is_err() {
                        let err_msg = format!(
                            "Writer 分发器: Writer-{} Channel 已关闭，可能已失败",
                            current_writer
                        );
                        error!("{}", err_msg);
                        // 立即停止分发，检查 Writer 状态
                        drop(writer_txs);
                        writer_error = Some(anyhow::anyhow!("{}", err_msg));
                        break;
                    }
                    current_writer = (current_writer + 1) % writer_count;
                }
                PipelineMessage::ReaderFinished => {
                    reader_finished_count += 1;
                    info!(
                        "Writer 分发器: 收到 Reader 完成信号 ({}/{})",
                        reader_finished_count, reader_total
                    );

                    if reader_finished_count >= reader_total {
                        info!("Writer 分发器: 所有 Reader 完成，通知所有 Writer");
                        for tx in &writer_txs {
                            let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        }
                        drop(writer_txs);
                        break;
                    }
                }
                PipelineMessage::Error(err) => {
                    error!("Writer 分发器: 收到错误 - {}", err);
                    for tx in &writer_txs {
                        let _ = tx.send(PipelineMessage::Error(err.clone())).await;
                    }
                    drop(writer_txs);
                    writer_error = Some(anyhow::anyhow!("Reader 错误: {}", err));
                    break;
                }
            }
        }

        // 等待所有 Writer 完成
        let mut total_written = 0;
        for (i, handle) in writer_handles.into_iter() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_written += count;
                    info!("Writer-{} 完成，写入 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    error!("Writer-{} 失败: {}", i, e);
                    // 保存第一个错误
                    if writer_error.is_none() {
                        writer_error = Some(e);
                    }
                }
                Err(e) => {
                    error!("Writer-{} 任务崩溃: {}", i, e);
                    if writer_error.is_none() {
                        writer_error = Some(anyhow::anyhow!("Writer-{} 任务崩溃: {}", i, e));
                    }
                }
            }
        }

        // 如果有错误，返回错误而不是成功
        if let Some(e) = writer_error {
            return Err(e);
        }

        Ok::<_, anyhow::Error>(total_written)
    })
}
