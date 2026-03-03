# 数据同步管道架构设计

## 概述

基于 DataX 的设计思想，将数据同步流程重构为 **Reader → Channel → Writer** 的多线程管道架构。

**重要更新**：
- ✅ 已删除 `sync_nodes.rs`，所有功能整合到 `sync_pipeline.rs`
- ✅ 根据 `input.type` 和 `output.type` 自动判断数据源和目标
- ✅ 支持 API→DB, DB→DB, API→API, DB→API 等任意组合

---

## 架构对比

### 旧架构（单线程）

```
┌────────────────────────────────────────┐
│          sync_workflow                 │
├────────────────────────────────────────┤
│  1. fetch_api_data()                   │  ← 串行执行
│  2. apply_mapping()                    │  ← 串行执行
│  3. prepare_db_batch()                 │  ← 串行执行
│  4. execute_db_write()                 │  ← 串行执行
└────────────────────────────────────────┘
```

**问题**:
- ✗ API 获取和数据库写入串行，无法充分利用 CPU
- ✗ 单线程写入，数据库连接池利用率低
- ✗ 无法处理大批量数据（内存占用高）

---

### 新架构（多线程管道）

```
┌─────────────────────────────────────────────────────────────┐
│                   Pipeline (管道执行器)                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌────────────┐      ┌──────────┐      ┌────────────┐     │
│  │  Reader 1  │──┐   │          │   ┌─→│  Writer 1  │     │
│  │ (API 获取) │  │   │          │   │  │ (数据库写)  │     │
│  └────────────┘  │   │ Channel  │   │  └────────────┘     │
│                  ├──→│ (mpsc)   │───┤                      │
│  ┌────────────┐  │   │ 缓冲队列  │   │  ┌────────────┐     │
│  │  Reader 2  │──┘   │          │   └─→│  Writer 2  │     │
│  │ (可选分片) │      └──────────┘      │ (并发写)    │     │
│  └────────────┘                        └────────────┘     │
│                                                             │
│  多个 Reader 并发 → 内存队列解耦 → 多个 Writer 并发写入     │
└─────────────────────────────────────────────────────────────┘
```

**优势**:
- ✓ Reader 和 Writer 解耦，并发执行
- ✓ Channel 缓冲，削峰填谷
- ✓ 多 Writer 并发写入，充分利用数据库连接池
- ✓ 流式处理，内存占用可控

---

## 核心组件

### 1. Pipeline (管道执行器)

```rust
pub struct Pipeline {
    config: Arc<Config>,
    pool: Arc<DbPool>,
    pipeline_config: Arc<PipelineConfig>,
}

impl Pipeline {
    pub async fn execute(&self) -> Result<PipelineStats>
}
```

**职责**:
- 协调 Reader 和 Writer 的生命周期
- 管理 Channel 的创建和关闭
- 收集统计信息

---

### 2. 数据源抽象

```rust
pub enum DataSourceType {
    Api,
    Database {
        query: String,
        limit: Option<usize>,
        offset: Option<usize>,
    },
}

pub struct DataShard {
    pub shard_id: usize,
    pub total_shards: usize,
    pub offset: usize,
    pub limit: usize,
}
```

**职责**:
- 定义数据源类型（API 或数据库）
- 支持数据库数据源的分片配置
- 为多 Reader 并发提供任务切分能力

---

### 3. Reader Task

```rust
async fn reader_task(
    config: Arc<Config>,
    pool: Option<Arc<DbPool>>,
    tx: mpsc::Sender<PipelineMessage>,
    reader_id: usize,
    data_source: DataSourceType,
    shard: Option<DataShard>,
) -> Result<usize>
```

**职责**:
1. 根据数据源类型获取数据（API 或数据库）
2. 应用字段映射（DSL 转换）
3. 按批次发送到 Channel

**数据源支持**:
- **API 数据源**: 调用 `fetch_from_api()` 从 REST API 获取数据
- **数据库数据源**: 调用 `fetch_from_database()` 从数据库查询数据，支持分片

**流程**:
```
fetch_from_api() 或 fetch_from_database()
    ↓
apply_mapping() (DSL 集成点)
    ↓
分批发送到 Channel
    ↓
发送完成信号
```

**数据库分片示例**:
```rust
// Reader 0: LIMIT 1000 OFFSET 0
// Reader 1: LIMIT 1000 OFFSET 1000
// Reader 2: LIMIT 1000 OFFSET 2000
// 多个 Reader 并发查询不同数据片段
```

---

### 3. Channel (消息队列)

使用 Tokio 的 `mpsc::channel`:

```rust
let (tx, rx) = mpsc::channel::<PipelineMessage>(buffer_size);
```

**消息类型**:
```rust
pub enum PipelineMessage {
    DataBatch(Vec<MappedRow>),  // 数据批次
    ReaderFinished,              // Reader 完成
    Error(String),               // 错误信号
}
```

**特性**:
- 异步非阻塞
- 背压控制（Channel 满时 Reader 自动等待）
- 多生产者单消费者（通过分发器扩展为多消费者）

---

### 4. Writer Task

```rust
async fn writer_task(
    config: Arc<Config>,
    pool: Arc<DbPool>,
    mut rx: mpsc::Receiver<PipelineMessage>,
    writer_id: usize,
    pipeline_config: Arc<PipelineConfig>,
) -> Result<usize>
```

**职责**:
1. 从 Channel 接收数据批次
2. 准备数据库批次（构建 SQL）
3. 并发写入数据库

**流程**:
```
从 Channel 接收数据
    ↓
prepare_db_batch() (构建 SQL)
    ↓
execute_db_write() (执行写入)
    ↓
统计并返回
```

---

### 5. Dispatcher (分发器)

```rust
// 轮询分发策略
current_writer = (current_writer + 1) % writer_count;
```

**职责**:
- 从主 Channel 读取消息
- 轮询分发到各个 Writer 的 Channel
- 处理完成和错误信号

**分发策略**:
- Round-Robin（轮询）
- 负载均衡
- 确保任务均匀分配

---

## 配置参数

```rust
pub struct PipelineConfig {
    /// Reader 线程数（用于分片读取）
    pub reader_threads: usize,          // 默认: 1

    /// Writer 线程数（用于并发写入）
    pub writer_threads: usize,          // 默认: 4

    /// Channel 缓冲区大小
    pub channel_buffer_size: usize,     // 默认: 1000

    /// 每个批次的大小
    pub batch_size: usize,              // 默认: 100

    /// 是否使用事务
    pub use_transaction: bool,          // 默认: true

    /// 数据源类型
    pub data_source: DataSourceType,    // 默认: Api
}
```

---

## 使用示例

### 示例 1: 使用默认配置

```rust
use data_trans_core::core::sync_pipeline::sync_with_pipeline;

#[tokio::main]
async fn main() -> Result<()> {
    let config = get_config("task_id")?;
    let pool = get_pool(&config).await?;

    // 使用默认配置执行管道
    let stats = sync_with_pipeline(config, pool).await?;

    println!("同步完成:");
    println!("  读取: {} 条", stats.records_read);
    println!("  写入: {} 条", stats.records_written);
    println!("  吞吐: {:.2} 条/秒", stats.throughput);

    Ok(())
}
```

### 示例 2: 自定义配置（API 数据源）

```rust
use data_trans_core::core::sync_pipeline::{Pipeline, PipelineConfig, DataSourceType};

#[tokio::main]
async fn main() -> Result<()> {
    let config = get_config("task_id")?;
    let pool = get_pool(&config).await?;

    // 自定义管道配置 - API 数据源
    let pipeline_config = PipelineConfig {
        reader_threads: 1,            // 1 个 Reader
        writer_threads: 8,            // 8 个 Writer（高并发）
        channel_buffer_size: 2000,    // 缓冲 2000 条
        batch_size: 200,              // 每批 200 条
        use_transaction: true,
        data_source: DataSourceType::Api,
    };

    let pipeline = Pipeline::new(config, pool, pipeline_config);
    let stats = pipeline.execute().await?;

    println!("同步完成: {:#?}", stats);
    Ok(())
}
```

### 示例 3: 数据库数据源（多 Reader 并发）

```rust
use data_trans_core::core::sync_pipeline::{Pipeline, PipelineConfig, DataSourceType};

#[tokio::main]
async fn main() -> Result<()> {
    let config = get_config("task_id")?;
    let pool = get_pool(&config).await?;

    // 数据库数据源配置 - 多 Reader 并发分片读取
    let pipeline_config = PipelineConfig {
        reader_threads: 4,            // 4 个 Reader 并发读取
        writer_threads: 8,            // 8 个 Writer
        channel_buffer_size: 2000,
        batch_size: 200,
        use_transaction: true,
        data_source: DataSourceType::Database {
            query: String::new(),     // 暂未使用（使用 config.db.table）
            limit: Some(10000),       // 总共读取 10000 条
            offset: Some(0),          // 从偏移 0 开始
        },
    };

    // Reader 0: LIMIT 2500 OFFSET 0
    // Reader 1: LIMIT 2500 OFFSET 2500
    // Reader 2: LIMIT 2500 OFFSET 5000
    // Reader 3: LIMIT 2500 OFFSET 7500

    let pipeline = Pipeline::new(config, pool, pipeline_config);
    let stats = pipeline.execute().await?;

    println!("同步完成: {:#?}", stats);
    Ok(())
}
```

### 示例 4: 集成到现有 sync 函数

```rust
// src/core/server.rs

pub async fn sync(cfg: &Config, pool: &DbPool) -> Result<()> {
    // 方式 1: 使用旧的单线程实现
    // crate::core::sync_nodes::sync_workflow(cfg, pool).await?;

    // 方式 2: 使用新的管道实现（推荐）
    let pipeline_config = PipelineConfig {
        reader_threads: 1,
        writer_threads: 4,
        channel_buffer_size: 1000,
        batch_size: 100,
        use_transaction: cfg.db.use_transaction.unwrap_or(true),
    };

    crate::core::sync_pipeline::sync_with_pipeline_custom(
        cfg.clone(),
        pool.clone(),
        pipeline_config
    ).await?;

    Ok(())
}
```

---

## 性能优化

### 1. Writer 线程数调优

```rust
// 根据数据库连接池大小调整
let max_conns = config.db.max_connections.unwrap_or(10);
let writer_threads = (max_conns / 2).max(1); // 使用一半连接

PipelineConfig {
    writer_threads,
    ..Default::default()
}
```

### 2. Channel 缓冲区调优

```rust
// 根据数据量和内存调整
let data_size = api_data.items.len();
let buffer_size = (data_size / writer_threads).min(2000);

PipelineConfig {
    channel_buffer_size: buffer_size,
    ..Default::default()
}
```

### 3. 批次大小调优

```rust
// 根据单条记录大小调整
let avg_record_size = 1000; // bytes
let target_batch_mem = 100 * 1024; // 100KB per batch
let batch_size = target_batch_mem / avg_record_size;

PipelineConfig {
    batch_size,
    ..Default::default()
}
```

---

## 监控和统计

### 统计信息

```rust
pub struct PipelineStats {
    pub records_read: usize,      // 读取的记录数
    pub records_written: usize,   // 写入的记录数
    pub records_failed: usize,    // 失败的记录数
    pub elapsed_secs: f64,        // 总耗时（秒）
    pub throughput: f64,          // 吞吐量（记录/秒）
}
```

### 实时日志

```
🚀 启动数据同步管道
📊 配置: 1 Reader, 4 Writer, Channel 缓冲 1000

📖 Reader-0 启动
📖 Reader-0 获取了 10000 条数据
📖 Reader-0 完成字段映射
📖 Reader-0 已发送 100/10000 条
...

✍️  Writer-0 启动
✍️  Writer-1 启动
✍️  Writer-2 启动
✍️  Writer-3 启动

✍️  Writer-0 写入了 100 条数据（累计：100）
✍️  Writer-1 写入了 100 条数据（累计：100）
...

✅ Reader-0 完成，共发送 10000 条数据
📭 分发器: 收到 Reader 完成信号 (1/1)
📭 分发器: 所有 Reader 完成，通知所有 Writer

✅ Writer-0 完成，共写入 2500 条数据
✅ Writer-1 完成，共写入 2500 条数据
✅ Writer-2 完成，共写入 2500 条数据
✅ Writer-3 完成，共写入 2500 条数据

📊 同步完成
   读取: 10000 条
   写入: 10000 条
   失败: 0 条
   耗时: 5.32 秒
   吞吐: 1879.70 条/秒
```

---

## 扩展点

### 1. 数据源扩展

当前支持两种数据源：
- **API 数据源**: 通过 HTTP REST API 获取数据
- **数据库数据源**: 直接从数据库查询数据，支持分片

**未来可扩展数据源**:
- 文件数据源（CSV, JSON, Parquet）
- 消息队列数据源（Kafka, RabbitMQ）
- 流式数据源（WebSocket, SSE）

**扩展方式**:
```rust
// 添加新的数据源类型
pub enum DataSourceType {
    Api,
    Database { ... },
    File { path: String, format: FileFormat },  // 新增
    Kafka { topic: String, partition: usize },  // 新增
}

// 在 reader_task 中添加对应的处理逻辑
match data_source {
    DataSourceType::Api => fetch_from_api(...).await?,
    DataSourceType::Database { ... } => fetch_from_database(...).await?,
    DataSourceType::File { path, format } => fetch_from_file(path, format).await?,
}
```

### 2. DSL 集成

在 `Reader Task` 中的 `apply_mapping()` 调用处集成 DSL 引擎：

```rust
// reader_task 中
let mapped_rows = super::sync_nodes::apply_mapping(
    &items,  // 可以来自 API 或数据库
    &config.column_mapping,
    &config.column_types,
)?;

// apply_mapping 内部可以调用 DSL 引擎
// 参考 sync_nodes.rs:115-125 的 TODO 注释
```

### 3. 自定义分发策略

```rust
// 可以实现不同的分发策略
enum DispatchStrategy {
    RoundRobin,          // 轮询
    LeastBusy,           // 选择最空闲的 Writer
    Hash(String),        // 根据某个字段 Hash 分配
    Weighted(Vec<f32>),  // 加权分配
}
```

### 4. 错误重试

```rust
// 在 Writer Task 中添加重试逻辑
let mut retry_count = 0;
loop {
    match execute_db_write(...).await {
        Ok(count) => break,
        Err(e) if retry_count < 3 => {
            retry_count += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(e) => return Err(e),
    }
}
```

---

## 对比总结

| 特性             | 旧架构(sync_workflow) | 新架构(Pipeline)      |
|------------------|-----------------------|-----------------------|
| 执行模式         | 单线程串行            | 多线程并发            |
| 吞吐量           | 低                    | 高（线性扩展）        |
| 内存占用         | 高（全量加载）        | 低（流式处理）        |
| 资源利用         | CPU/IO 利用率低       | 充分利用多核和连接池  |
| 扩展性           | 差                    | 好（可配置线程数）    |
| 复杂度           | 简单                  | 中等                  |
| 适用场景         | 小批量数据            | 大批量数据            |

---

## 何时使用

### 使用单线程 (sync_workflow)
- ✓ 数据量小（< 1000 条）
- ✓ 简单场景，不需要高性能
- ✓ 调试和测试

### 使用管道 (Pipeline)
- ✓ 数据量大（> 10000 条）
- ✓ 需要高吞吐量
- ✓ 数据库连接池较大（> 5）
- ✓ 生产环境

---

## 数据源详解

### API 数据源

**特点**:
- 通过 HTTP REST API 获取数据
- 通常不支持分片（除非 API 支持分页）
- 建议使用单 Reader（`reader_threads: 1`）

**配置**:
```rust
PipelineConfig {
    reader_threads: 1,
    data_source: DataSourceType::Api,
    ..Default::default()
}
```

**执行流程**:
```
Reader → fetch_json(api.url) → 提取 items → apply_mapping → 发送到 Channel
```

### 数据库数据源

**特点**:
- 直接从数据库查询数据
- 支持分片并发读取
- 适合大批量数据迁移

**配置**:
```rust
PipelineConfig {
    reader_threads: 4,  // 4 个 Reader 并发
    data_source: DataSourceType::Database {
        query: String::new(),  // 保留字段（暂未使用）
        limit: Some(100000),   // 总共读取 10 万条
        offset: Some(0),       // 从第 0 条开始
    },
    ..Default::default()
}
```

**分片策略**:
```
总数据量: 100000 条
Reader 数量: 4 个
每个分片: 100000 / 4 = 25000 条

Reader 0: SELECT * FROM table LIMIT 25000 OFFSET 0
Reader 1: SELECT * FROM table LIMIT 25000 OFFSET 25000
Reader 2: SELECT * FROM table LIMIT 25000 OFFSET 50000
Reader 3: SELECT * FROM table LIMIT 25000 OFFSET 75000
```

**执行流程**:
```
Reader 0 → SELECT ... LIMIT 25000 OFFSET 0     → apply_mapping → 发送到 Channel
Reader 1 → SELECT ... LIMIT 25000 OFFSET 25000 → apply_mapping → 发送到 Channel
Reader 2 → SELECT ... LIMIT 25000 OFFSET 50000 → apply_mapping → 发送到 Channel
Reader 3 → SELECT ... LIMIT 25000 OFFSET 75000 → apply_mapping → 发送到 Channel
```

**优势**:
- 多 Reader 并发读取，充分利用数据库连接池
- 数据库到数据库的迁移更高效（省去 API 层）
- 支持大批量数据的分片处理

**注意事项**:
- 需要确保数据顺序不影响业务逻辑
- 分片查询可能对数据库造成压力，建议根据数据库性能调整 `reader_threads`
- 如果数据有更新，建议按主键或时间戳排序后分片

---

## 后续优化方向

1. **动态调整线程数**: 根据系统负载自动调整 Writer 数量
2. **监控面板**: 提供实时的任务进度和吞吐量监控
3. **失败重试**: 对失败的批次自动重试
4. **更多数据源**: 文件（CSV/JSON/Parquet）、消息队列（Kafka）、流式数据（WebSocket）
5. **压测工具**: 提供性能测试和调优工具
6. **智能分片**: 根据数据分布自动优化分片策略
