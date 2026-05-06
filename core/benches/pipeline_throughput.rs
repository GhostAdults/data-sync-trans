//! 管道性能基准测试
//!
//! 测试目标:
//! - 评估 Reader → Channel → Writer 架构的吞吐量
//! - 找出最优的线程配置组合
//! - 评估 channel_buffer_size 的影响

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// ==========================================
// 简化的数据结构
// ==========================================

#[derive(Debug, Clone)]
struct TestRow {
    id: i64,
    name: String,
    score: f64,
    active: bool,
}

#[derive(Debug, Clone)]
enum TestMessage {
    DataBatch(Vec<TestRow>),
    Finished,
}

// ==========================================
// 测试数据生成
// ==========================================

fn generate_test_row(id: usize) -> TestRow {
    TestRow {
        id: id as i64,
        name: format!("user_{}", id),
        score: id as f64 * 0.1,
        active: id % 2 == 0,
    }
}

fn generate_test_batch(start_id: usize, size: usize) -> Vec<TestRow> {
    (start_id..start_id + size).map(generate_test_row).collect()
}

// ==========================================
// 简化管道执行器
// ==========================================

struct BenchmarkPipeline {
    total_records: usize,
    batch_size: usize,
    reader_threads: usize,
    writer_threads: usize,
    channel_buffer_size: usize,
    write_delay: Duration,
}

impl BenchmarkPipeline {
    fn new(
        total_records: usize,
        batch_size: usize,
        reader_threads: usize,
        writer_threads: usize,
        channel_buffer_size: usize,
        write_delay: Duration,
    ) -> Self {
        Self {
            total_records,
            batch_size,
            reader_threads,
            writer_threads,
            channel_buffer_size,
            write_delay,
        }
    }

    async fn execute(&self) -> BenchmarkResult {
        let start = Instant::now();

        let (main_tx, main_rx) = mpsc::channel(self.channel_buffer_size);

        // 启动 Readers
        let records_per_reader = self.total_records.div_ceil(self.reader_threads);
        let mut reader_handles: Vec<JoinHandle<Result<usize, anyhow::Error>>> = Vec::new();

        for reader_id in 0..self.reader_threads {
            let start_id = reader_id * records_per_reader;
            let end_id = (start_id + records_per_reader).min(self.total_records);

            if start_id >= self.total_records {
                break;
            }

            let tx = main_tx.clone();
            let batch_size = self.batch_size;

            let handle = tokio::spawn(async move {
                let mut sent = 0;
                let mut current_id = start_id;

                while current_id < end_id {
                    let remaining = end_id - current_id;
                    let chunk_size = batch_size.min(remaining);

                    let batch = generate_test_batch(current_id, chunk_size);
                    tx.send(TestMessage::DataBatch(batch)).await?;
                    sent += chunk_size;
                    current_id += chunk_size;
                }

                Ok(sent)
            });

            reader_handles.push(handle);
        }

        drop(main_tx);

        // 启动 Writers 和分发器
        let writer_count = self.writer_threads;
        let mut writer_txs: Vec<mpsc::Sender<TestMessage>> = Vec::new();
        let mut writer_handles: Vec<JoinHandle<usize>> = Vec::new();

        for _ in 0..writer_count {
            let (writer_tx, mut writer_rx) = mpsc::channel(100);
            writer_txs.push(writer_tx);

            let delay = self.write_delay;
            let handle = tokio::spawn(async move {
                let mut received = 0;
                while let Some(msg) = writer_rx.recv().await {
                    match msg {
                        TestMessage::DataBatch(batch) => {
                            received += batch.len();
                            tokio::time::sleep(delay).await;
                        }
                        TestMessage::Finished => break,
                    }
                }
                received
            });
            writer_handles.push(handle);
        }

        // 分发器任务
        let reader_count = self.reader_threads;
        let writer_txs_clone = writer_txs.clone();
        tokio::spawn(async move {
            let mut current_writer = 0;
            let mut reader_finished = 0;
            let mut main_rx = main_rx;

            while let Some(msg) = main_rx.recv().await {
                match &msg {
                    TestMessage::DataBatch(_) => {
                        if writer_txs_clone[current_writer].send(msg).await.is_err() {
                            break;
                        }
                        current_writer = (current_writer + 1) % writer_count;
                    }
                    TestMessage::Finished => {
                        reader_finished += 1;
                        if reader_finished >= reader_count {
                            for tx in &writer_txs_clone {
                                let _ = tx.send(TestMessage::Finished).await;
                            }
                            break;
                        }
                    }
                }
            }
        });

        // 发送 Reader 完成信号
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for tx in writer_txs {
                let _ = tx.send(TestMessage::Finished).await;
            }
        });

        // 等待 Readers 完成
        let mut total_read = 0;
        for handle in reader_handles {
            match handle.await {
                Ok(Ok(count)) => total_read += count,
                Ok(Err(e)) => {
                    eprintln!("Reader error: {}", e);
                    break;
                }
                Err(e) => {
                    eprintln!("Reader panicked: {}", e);
                    break;
                }
            }
        }

        // 等待 Writers 完成
        let mut total_written = 0;
        for handle in writer_handles {
            match handle.await {
                Ok(count) => total_written += count,
                Err(e) => {
                    eprintln!("Writer panicked: {}", e);
                    break;
                }
            }
        }

        let elapsed = start.elapsed();

        BenchmarkResult {
            total_read,
            total_written,
            elapsed,
            throughput: total_written as f64 / elapsed.as_secs_f64(),
        }
    }
}

#[derive(Debug)]
struct BenchmarkResult {
    total_read: usize,
    total_written: usize,
    elapsed: Duration,
    throughput: f64,
}

// 运行管道的辅助函数
fn run_pipeline(
    total_records: usize,
    batch_size: usize,
    reader_threads: usize,
    writer_threads: usize,
    channel_buffer_size: usize,
) -> BenchmarkResult {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let pipeline = BenchmarkPipeline::new(
            total_records,
            batch_size,
            reader_threads,
            writer_threads,
            channel_buffer_size,
            Duration::from_micros(10),
        );
        pipeline.execute().await
    })
}

// ==========================================
// Criterion 基准测试
// ==========================================

fn bench_thread_configuration(c: &mut criterion::Criterion) {
    let total_records = 10_000;
    let mut group = c.benchmark_group("thread_config");

    for (reader_threads, writer_threads) in &[
        (1, 1),
        (1, 2),
        (1, 4),
        (2, 1),
        (2, 2),
        (2, 4),
        (4, 1),
        (4, 2),
        (4, 4),
    ] {
        group.bench_with_input(
            criterion::BenchmarkId::new(
                "config",
                format!("R{}W{}", reader_threads, writer_threads),
            ),
            &(*reader_threads, *writer_threads),
            |b, &(rt_count, wt_count)| {
                b.iter(|| run_pipeline(total_records, 100, rt_count, wt_count, 1000));
            },
        );
    }

    group.finish();
}

fn bench_channel_buffer(c: &mut criterion::Criterion) {
    let total_records = 10_000;
    let mut group = c.benchmark_group("channel_buffer");

    for buffer_size in [100, 500, 1000, 2000, 5000] {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(buffer_size),
            &buffer_size,
            |b, &size| {
                b.iter(|| run_pipeline(total_records, 100, 2, 2, size));
            },
        );
    }

    group.finish();
}

fn bench_batch_size(c: &mut criterion::Criterion) {
    let total_records = 10_000;
    let mut group = c.benchmark_group("batch_size");

    for batch_size in [10, 50, 100, 500, 1000] {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| run_pipeline(total_records, size, 2, 2, 1000));
            },
        );
    }

    group.finish();
}

fn bench_data_scale(c: &mut criterion::Criterion) {
    let mut group = c.benchmark_group("data_scale");

    for records in [1_000, 5_000, 10_000, 50_000] {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(records),
            &records,
            |b, &total| {
                b.iter(|| run_pipeline(total, 100, 2, 2, 1000));
            },
        );
    }

    group.finish();
}

criterion::criterion_group! {
    name = benches;
    config = criterion::Criterion::default().sample_size(10);
    targets = bench_thread_configuration, bench_channel_buffer, bench_batch_size, bench_data_scale
}

criterion::criterion_main!(benches);
