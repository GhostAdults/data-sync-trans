# 流式传输设计

## 当前问题

Pipeline 目前是**半流式**，存在以下问题：

### 数据流动过程

```
Reader 阶段:
1. fetch_from_api() → 等待全部 HTTP 响应 ❌ 阻塞，全量加载
2. apply_mapping() → 处理全部数据       ❌ 全量在内存
3. chunks() → 分批发送                  ✅ 流式发送

Writer 阶段:
4. recv() → 接收批次                    ✅ 流式接收
5. execute_db_write() → 批量写入        ✅ 流式写入
```

**内存峰值**：在步骤 1-2 时，所有数据都在内存中

**示例**：
- 10 万条数据
- 每条 1KB
- 内存峰值：~100MB（在 Reader 阶段）

---

## 解决方案

### 方案 1：数据库流式查询（推荐）

使用 `sqlx::query().fetch()` 替代 `fetch_all()`：

```rust
async fn fetch_from_database_streaming(
    config: &Config,
    pool: &DbPool,
    tx: mpsc::Sender<PipelineMessage>,
    shard: Option<DataShard>,
) -> Result<usize> {
    use futures::StreamExt;

    let db_config = Config::parse_db_config(&config.input)?;
    let sql = build_query_sql(&db_config, shard);

    let mut sent = 0;
    let mut buffer = Vec::with_capacity(100);

    match pool {
        DbPool::Postgres(pg_pool) => {
            let mut stream = sqlx::query(&sql).fetch(pg_pool);

            // 流式读取，不等待全部数据
            while let Some(row) = stream.next().await {
                let row = row?;

                // 转换为 JSON
                let json = row_to_json(&row);
                buffer.push(json);

                // 达到批次大小就发送
                if buffer.len() >= 100 {
                    let mapped = apply_mapping(&buffer, ...)?;
                    tx.send(PipelineMessage::DataBatch(mapped)).await?;
                    sent += buffer.len();
                    buffer.clear();  // 释放内存
                }
            }

            // 发送剩余数据
            if !buffer.is_empty() {
                let mapped = apply_mapping(&buffer, ...)?;
                tx.send(PipelineMessage::DataBatch(mapped)).await?;
                sent += buffer.len();
            }
        }
        DbPool::Mysql(my_pool) => {
            // 类似实现
        }
    }

    Ok(sent)
}
```

**优势**：
- ✅ 内存占用恒定（只有当前批次）
- ✅ 适合大数据量（百万级）
- ✅ 边读边发，延迟低

**内存占用**：
```
100 条 × 1KB = 100KB（恒定）
```

---

### 方案 2：API 流式解析（复杂）

对于超大 JSON 响应，使用流式 JSON 解析器：

```rust
async fn fetch_from_api_streaming(
    config: &Config,
    tx: mpsc::Sender<PipelineMessage>,
) -> Result<usize> {
    use serde_json::Deserializer;
    use tokio::io::AsyncBufReadExt;

    let api_config = Config::parse_api_config(&config.input)?;

    // 1. 流式 HTTP 请求
    let response = reqwest::get(&api_config.url).await?;
    let mut stream = response.bytes_stream();

    // 2. 流式 JSON 解析
    let mut buffer = Vec::with_capacity(100);
    let mut deserializer = Deserializer::from_reader(stream);

    while let Some(item) = deserializer.next_item()? {
        buffer.push(item);

        if buffer.len() >= 100 {
            let mapped = apply_mapping(&buffer, ...)?;
            tx.send(PipelineMessage::DataBatch(mapped)).await?;
            buffer.clear();
        }
    }

    Ok(sent)
}
```

**问题**：
- ⚠️ 大多数 API 返回的是完整 JSON（非流式）
- ⚠️ 需要 API 支持流式输出（如 JSON Lines）
- ⚠️ 实现复杂度高

**适用场景**：
- API 返回超大 JSON（> 100MB）
- API 支持流式格式（JSON Lines, NDJSON）

---

### 方案 3：混合流式（推荐采用）

对不同数据源采用不同策略：

```rust
async fn reader_task_streaming(
    config: Arc<Config>,
    pool: Option<Arc<DbPool>>,
    tx: mpsc::Sender<PipelineMessage>,
    reader_id: usize,
    data_source: DataSourceType,
    shard: Option<DataShard>,
) -> Result<usize> {
    match data_source {
        DataSourceType::Api => {
            // API 通常返回完整数据，使用现有方式
            let items = fetch_from_api(&config).await?;
            stream_and_send(items, tx, &config).await
        }

        DataSourceType::Database { .. } => {
            // 数据库使用流式查询
            let pool = pool.ok_or_else(|| anyhow!("需要 pool"))?;
            fetch_from_database_streaming(&config, &pool, tx, shard).await
        }
    }
}

// 辅助函数：批量数据流式发送
async fn stream_and_send(
    items: Vec<JsonValue>,
    tx: mpsc::Sender<PipelineMessage>,
    config: &Config,
) -> Result<usize> {
    let mut sent = 0;

    // 分批处理和发送
    for chunk in items.chunks(100) {
        let mapped = apply_mapping(chunk, &config.column_mapping, &config.column_types)?;
        tx.send(PipelineMessage::DataBatch(mapped)).await?;
        sent += chunk.len();
    }

    Ok(sent)
}
```

---

## 实现步骤

### 步骤 1：添加流式数据库查询

```rust
// 在 sync_pipeline.rs 中添加
async fn fetch_from_database_streaming(
    config: &Config,
    pool: &DbPool,
    tx: mpsc::Sender<Vec<JsonValue>>,  // 发送批次
    shard: Option<DataShard>,
) -> Result<usize> {
    use futures::StreamExt;

    let db_config = Config::parse_db_config(&config.input)?;
    let table = &db_config.table;

    let (sql, _limit, _offset) = if let Some(s) = shard {
        (format!("SELECT * FROM {} LIMIT {} OFFSET {}", table, s.limit, s.offset), s.limit, s.offset)
    } else {
        (format!("SELECT * FROM {}", table), 0, 0)
    };

    let mut sent = 0;
    let mut buffer = Vec::with_capacity(100);

    match pool {
        DbPool::Postgres(pg_pool) => {
            let mut stream = sqlx::query(&sql).fetch(pg_pool);

            while let Some(row_result) = stream.next().await {
                let row = row_result?;

                // 转换为 JSON
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let col_name = col.name();
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(col_name.to_string(), val.map(JsonValue::String).unwrap_or(JsonValue::Null));
                }

                buffer.push(JsonValue::Object(obj));

                // 批次满了就发送
                if buffer.len() >= 100 {
                    tx.send(buffer.clone()).await?;
                    sent += buffer.len();
                    buffer.clear();

                    println!("📊 流式发送了 {} 条数据", sent);
                }
            }

            // 发送剩余数据
            if !buffer.is_empty() {
                tx.send(buffer).await?;
                sent += buffer.len();
            }
        }

        DbPool::Mysql(my_pool) => {
            // 类似实现
            let mut stream = sqlx::query(&sql).fetch(my_pool);
            // ... 相同逻辑
        }
    }

    Ok(sent)
}
```

### 步骤 2：重构 Reader 任务

```rust
async fn reader_task(
    config: Arc<Config>,
    pool: Option<Arc<DbPool>>,
    tx: mpsc::Sender<PipelineMessage>,
    reader_id: usize,
    data_source: DataSourceType,
    shard: Option<DataShard>,
) -> Result<usize> {
    println!("📖 Reader-{} 启动 (数据源: {:?})", reader_id, data_source);

    let mut sent = 0;

    match data_source {
        DataSourceType::Api => {
            // API: 先全量获取（大多数 API 不支持流式）
            let items = fetch_from_api(&config).await?;
            println!("📖 Reader-{} 获取了 {} 条数据", reader_id, items.len());

            // 分批映射和发送
            for chunk in items.chunks(100) {
                let mapped = apply_mapping(chunk, &config.column_mapping, &config.column_types)?;
                tx.send(PipelineMessage::DataBatch(mapped)).await?;
                sent += chunk.len();
                println!("📖 Reader-{} 已发送 {}/{} 条", reader_id, sent, items.len());
            }
        }

        DataSourceType::Database { .. } => {
            // 数据库: 流式查询
            let pool = pool.ok_or_else(|| anyhow::anyhow!("需要 pool"))?;

            // 创建内部 Channel 用于流式传输原始数据
            let (json_tx, mut json_rx) = mpsc::channel::<Vec<JsonValue>>(10);

            // 启动流式查询任务
            let config_clone = Arc::clone(&config);
            let pool_clone = Arc::clone(&pool);
            tokio::spawn(async move {
                let _ = fetch_from_database_streaming(&config_clone, &pool_clone, json_tx, shard).await;
            });

            // 接收并映射数据
            while let Some(batch) = json_rx.recv().await {
                let mapped = apply_mapping(&batch, &config.column_mapping, &config.column_types)?;
                tx.send(PipelineMessage::DataBatch(mapped)).await?;
                sent += batch.len();
                println!("📖 Reader-{} 已发送 {} 条", reader_id, sent);
            }
        }
    }

    tx.send(PipelineMessage::ReaderFinished).await?;
    println!("✅ Reader-{} 完成，共发送 {} 条数据", reader_id, sent);

    Ok(sent)
}
```

---

## 性能对比

### 旧方式（半流式）

| 数据量 | 内存峰值 | 首批数据延迟 |
|--------|---------|-------------|
| 1 万条 | ~10MB | 2 秒（等待全部） |
| 10 万条 | ~100MB | 20 秒 |
| 100 万条 | ~1GB | 200 秒 |

### 新方式（真流式）

| 数据量 | 内存峰值 | 首批数据延迟 |
|--------|---------|-------------|
| 1 万条 | ~100KB | 0.1 秒 |
| 10 万条 | ~100KB | 0.1 秒 |
| 100 万条 | ~100KB | 0.1 秒 |

---

## 依赖添加

需要在 `Cargo.toml` 中添加：

```toml
[dependencies]
futures = "0.3"  # 用于 StreamExt
```

---

## 总结

### 当前状态：半流式 ⚠️
- Reader: ❌ 全量加载 → ❌ 全量映射 → ✅ 分批发送
- Writer: ✅ 批量接收 → ✅ 批量写入

### 改造后：真流式 ✅
- Reader: ✅ 流式读取 → ✅ 逐批映射 → ✅ 逐批发送
- Writer: ✅ 批量接收 → ✅ 批量写入

### 建议
1. **优先改造数据库数据源**（方案 3）
2. **API 数据源保持现状**（大多数 API 不支持流式）
3. **未来可选：支持流式 API**（JSON Lines 等格式）
