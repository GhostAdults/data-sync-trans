# Job Trait 设计文档

## 概述

Job trait 是数据读取任务的通用接口，定义了任务切分和执行的标准方法。通过实现此 trait，可以支持不同类型的数据源（数据库、API、文件、Kafka 等）。

## 设计动机

### 问题
原先的实现中，任务切分逻辑（`JobSplitter`）和任务执行逻辑（`reader_task_db_streaming`, `reader_task_api`）是分离的，导致：
1. 代码耦合度高，难以扩展新数据源
2. 无法统一管理不同类型的 Reader
3. Pipeline 需要根据数据源类型硬编码不同的调用逻辑

### 解决方案
引入 Job trait，将任务切分和执行封装为统一接口：
- **抽象**：定义通用的任务处理接口
- **多态**：不同数据源实现各自的 Job
- **解耦**：Pipeline 只依赖 Job trait，不关心具体实现

## Trait 定义

```rust
#[async_trait::async_trait]
pub trait Job: Send + Sync {
    /// 切分任务为多个子任务
    /// - reader_threads: 期望的并发读取线程数
    /// - 返回: 任务切分结果（总记录数 + 子任务列表）
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult>;

    /// 执行单个任务，读取数据并发送到 Channel
    /// - task: 要执行的子任务（包含 offset/limit 等信息）
    /// - tx: 数据发送通道
    /// - 返回: 读取并发送的记录数
    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize>;

    /// 获取任务描述（用于日志）
    fn description(&self) -> String;
}
```

### 关键设计点

1. **async_trait**
   - Rust 目前不直接支持 trait 中的 async 方法
   - 使用 `async_trait` crate 提供 async trait 支持

2. **Send + Sync**
   - Job 需要在多线程环境中使用
   - `Send`: 可以跨线程传递所有权
   - `Sync`: 可以安全地在多线程间共享引用

3. **split() 方法**
   - 负责分析数据源并切分任务
   - 数据库：根据 COUNT(*) 均匀切分
   - API：通常返回单个任务（API 不支持分片）

4. **execute_task() 方法**
   - 执行具体的数据读取逻辑
   - 流式处理：边读边发送，内存恒定
   - 支持批量发送：减少 Channel 传输次数

## 实现类

### 1. DatabaseJob

数据库数据源的 Job 实现。

```rust
pub struct DatabaseJob {
    config: Arc<Config>,
    pool: Arc<DbPool>,
}

impl DatabaseJob {
    pub fn new(config: Arc<Config>, pool: Arc<DbPool>) -> Self {
        Self { config, pool }
    }
}
```

#### split() 实现

```rust
async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
    // 1. 查询总记录数
    let sql = format!("SELECT COUNT(*) as count FROM {}", table);
    let total = sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await? as usize;

    // 2. 计算每个任务的大小
    let task_size = (total + reader_threads - 1) / reader_threads;

    // 3. 生成任务列表
    let mut tasks = Vec::new();
    for i in 0..reader_threads {
        let offset = i * task_size;
        let limit = task_size.min(total - offset);

        tasks.push(Task {
            task_id: i,
            offset,
            limit,
        });
    }

    Ok(JobSplitResult {
        total_records: total,
        tasks,
    })
}
```

**特点**：
- 使用 `COUNT(*)` 查询总记录数
- 均匀切分为 N 个子任务
- 每个子任务独立的 OFFSET/LIMIT

#### execute_task() 实现

```rust
async fn execute_task(
    &self,
    task: Task,
    tx: mpsc::Sender<PipelineMessage>,
) -> Result<usize> {
    use futures::StreamExt;

    // 1. 构建查询 SQL
    let sql = format!(
        "SELECT * FROM {} LIMIT {} OFFSET {}",
        table, task.limit, task.offset
    );

    // 2. 流式查询
    let mut stream = sqlx::query(&sql).fetch(pg_pool);
    let mut buffer = Vec::with_capacity(100);

    while let Some(row_result) = stream.next().await {
        let row = row_result?;

        // 3. 转换为 JSON
        let json_obj = row_to_json(&row);
        buffer.push(json_obj);

        // 4. 批次满了就映射并发送
        if buffer.len() >= 100 {
            let mapped = apply_mapping(&buffer, ...)?;
            tx.send(PipelineMessage::DataBatch(mapped)).await?;
            buffer.clear();
        }
    }

    // 5. 发送剩余数据
    if !buffer.is_empty() {
        let mapped = apply_mapping(&buffer, ...)?;
        tx.send(PipelineMessage::DataBatch(mapped)).await?;
    }

    Ok(sent)
}
```

**特点**：
- 使用 `futures::StreamExt` 流式读取
- ��存占用恒定（~100KB/任务）
- 批量发送（100 条/批次）
- 自动类型转换和字段映射

### 2. ApiJob

API 数据源的 Job 实现。

```rust
pub struct ApiJob {
    config: Arc<Config>,
}

impl ApiJob {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}
```

#### split() 实现

```rust
async fn split(&self, _reader_threads: usize) -> Result<JobSplitResult> {
    // API 通常不支持分片，返回单个任务
    Ok(JobSplitResult {
        total_records: 0,  // 未知总数
        tasks: vec![Task {
            task_id: 0,
            offset: 0,
            limit: 0,
        }],
    })
}
```

**特点**：
- 忽略 `reader_threads` 参数
- 返回单个任务（不切分）

#### execute_task() 实现

```rust
async fn execute_task(
    &self,
    task: Task,
    tx: mpsc::Sender<PipelineMessage>,
) -> Result<usize> {
    // 1. 一次性获取所有数据
    let items = fetch_from_api(&self.config).await?;

    // 2. 分批映射并发送
    let mut sent = 0;
    for chunk in items.chunks(100) {
        let mapped = apply_mapping(chunk, ...)?;
        tx.send(PipelineMessage::DataBatch(mapped)).await?;
        sent += chunk.len();
    }

    Ok(sent)
}
```

**特点**：
- 一次性获取全部数据（大多数 API 不支持流式）
- 分批发送到 Channel（避免阻塞）

## Pipeline 集成

### 使用 Job trait

```rust
pub async fn execute(&self) -> Result<PipelineStats> {
    // 创建 Channel
    let (tx, rx) = mpsc::channel::<PipelineMessage>(buffer_size);

    // 根据数据源类型创建 Job
    let reader_handles = match &self.pipeline_config.data_source {
        DataSourceType::Database { .. } => {
            // 创建 DatabaseJob
            let job: Arc<dyn Job> = Arc::new(DatabaseJob::new(
                Arc::clone(&self.config),
                Arc::clone(&self.pool),
            ));

            // 切分任务
            let split_result = job.split(reader_threads).await?;

            // 为每个任务启动独立线程
            let mut handles = Vec::new();
            for task in split_result.tasks {
                let job = Arc::clone(&job);
                let tx = tx.clone();

                let handle = tokio::spawn(async move {
                    job.execute_task(task, tx).await
                });

                handles.push(handle);
            }

            handles
        }

        DataSourceType::Api => {
            // 创建 ApiJob
            let job: Arc<dyn Job> = Arc::new(ApiJob::new(
                Arc::clone(&self.config)
            ));

            // 切分任务（返回单个任务）
            let split_result = job.split(1).await?;
            let task = split_result.tasks[0].clone();

            // 启动单个线程
            let handle = tokio::spawn(async move {
                job.execute_task(task, tx).await
            });

            vec![handle]
        }
    };

    // 等待所有 Reader 完成...
}
```

### 优势

1. **统一接口**：Pipeline 只需要与 Job trait 交互
2. **多态调用**：`Arc<dyn Job>` 支持动态分发
3. **易于扩展**：添加新数据源只需实现 Job trait

## 扩展新数据源

### 示例：FileJob

```rust
pub struct FileJob {
    config: Arc<Config>,
}

impl FileJob {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Job for FileJob {
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
        // 1. 获取文件总行数
        let file_path = &self.config.input.config["path"];
        let total_lines = count_file_lines(file_path)?;

        // 2. 按行切分任务
        let lines_per_task = (total_lines + reader_threads - 1) / reader_threads;

        let mut tasks = Vec::new();
        for i in 0..reader_threads {
            let offset = i * lines_per_task;
            let limit = lines_per_task.min(total_lines - offset);

            tasks.push(Task {
                task_id: i,
                offset,
                limit,
            });
        }

        Ok(JobSplitResult {
            total_records: total_lines,
            tasks,
        })
    }

    async fn execute_task(
        &self,
        task: Task,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        // 1. 打开文件
        let file_path = &self.config.input.config["path"];
        let file = tokio::fs::File::open(file_path).await?;
        let reader = BufReader::new(file);

        // 2. 跳过 offset 行
        let mut lines = reader.lines();
        for _ in 0..task.offset {
            lines.next_line().await?;
        }

        // 3. 读取 limit 行
        let mut buffer = Vec::new();
        for _ in 0..task.limit {
            if let Some(line) = lines.next_line().await? {
                // 解析 JSON/CSV
                let json = parse_line(&line)?;
                buffer.push(json);

                if buffer.len() >= 100 {
                    let mapped = apply_mapping(&buffer, ...)?;
                    tx.send(PipelineMessage::DataBatch(mapped)).await?;
                    buffer.clear();
                }
            }
        }

        // 4. 发送剩余数据
        if !buffer.is_empty() {
            let mapped = apply_mapping(&buffer, ...)?;
            tx.send(PipelineMessage::DataBatch(mapped)).await?;
        }

        Ok(task.limit)
    }

    fn description(&self) -> String {
        format!("FileJob (source: {})", self.config.input.name)
    }
}
```

### 集成到 Pipeline

```rust
DataSourceType::File => {
    let job: Arc<dyn Job> = Arc::new(FileJob::new(
        Arc::clone(&self.config)
    ));

    let split_result = job.split(reader_threads).await?;

    let mut handles = Vec::new();
    for task in split_result.tasks {
        let job = Arc::clone(&job);
        let tx = tx.clone();

        let handle = tokio::spawn(async move {
            job.execute_task(task, tx).await
        });

        handles.push(handle);
    }

    handles
}
```

## 性能分析

### 对比：旧实现 vs Job Trait

| 特性 | 旧实现 | Job Trait |
|------|-------|-----------|
| 代码行数 | ~300 行 | ~400 行 |
| 扩展性 | ❌ 硬编码 | ✅ 接口化 |
| 可测试性 | ⚠️ 依赖具体实现 | ✅ 可 mock |
| 运行时开销 | 无 | ~1% (动态分发) |
| 内存开销 | 相同 | 相同 |

### 运行时开销

Job trait 使用 `Arc<dyn Job>` 实现动态分发，会有轻微的性能开销：
- **虚函数调用**：~1-2 纳秒/次
- **内存布局**：额外 16 字节（vtable 指针）

对于数据同步场景（每批次 100 条），这个开销可以忽略不计。

## 依赖

```toml
[dependencies]
async-trait = "0.1"   # 支持 async trait
futures = "0.3"       # StreamExt
```

## 总结

### 核心优势

1. ✅ **统一抽象**：所有数据源实现统一的 Job trait
2. ✅ **易于扩展**：添加新数据源只需实现 3 个方法
3. ✅ **解耦合**：Pipeline 不依赖具体数据源实现
4. ✅ **可测试**：可以轻松 mock Job 实现
5. ✅ **类型安全**：Rust 类型系统保证正确性

### 适用场景

- ✅ 需要支持多种数据源
- ✅ 需要统一的任务切分和执行逻辑
- ✅ 需要高度可扩展的架构

### 限制

- ⚠️ 增加了一定的代码复杂度
- ⚠️ 动态分发有微小的性能开销（可忽略）
- ⚠️ 需要理解 Rust trait 和 async_trait

## 未来扩展

1. [ ] 支持更多数据源（Kafka, S3, Redis, MongoDB）
2. [ ] 支持自定义分片策略（按主键、时间范围等）
3. [ ] 支持动态调整任务数（根据负载）
4. [ ] 支持任务优先级（不同任务不同优先级）
5. [ ] 支持断点续传（记录已完成的任务）
