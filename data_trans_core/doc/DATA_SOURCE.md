# 数据源灵活配置功能

## 概述

Pipeline 架构现已支持灵活的数据源配置，用户可以自由选择从 **API** 或 **数据库** 获取数据，Reader 专注于任务切分和并发处理。

---

## 架构设计

### 数据源抽象

```rust
pub enum DataSourceType {
    /// API 数据源
    Api,

    /// 数据库数据源（支持分片）
    Database {
        query: String,           // 保留字段（暂未使用）
        limit: Option<usize>,    // 总数据量限制
        offset: Option<usize>,   // 起始偏移量
    },
}
```

### 数据分片

```rust
pub struct DataShard {
    pub shard_id: usize,      // 分片 ID
    pub total_shards: usize,  // 总分片数
    pub offset: usize,        // 数据偏移量
    pub limit: usize,         // 数据限制数量
}
```

### Reader 职责分离

**原架构问题**:
- Reader 既负责数据获取，又负责数据源选择
- 数据源固定为 API，无法从数据库读取
- 无法利用多 Reader 并发读取数据库

**新架构优势**:
- Reader 只负责**任务切分**和**并发处理**
- 数据源通过 `DataSourceType` 枚举灵活配置
- 支持多 Reader 并发分片读取数据库

---

## 使用指南

### 场景 1: API 数据源（默认）

适用于从 REST API 获取数据的场景。

```rust
use data_trans_core::core::sync_pipeline::{Pipeline, PipelineConfig, DataSourceType};

let pipeline_config = PipelineConfig {
    reader_threads: 1,         // API 通常使用单 Reader
    writer_threads: 4,
    channel_buffer_size: 1000,
    batch_size: 100,
    use_transaction: true,
    data_source: DataSourceType::Api,  // API 数据源
};

let pipeline = Pipeline::new(config, pool, pipeline_config);
let stats = pipeline.execute().await?;
```

**执行流程**:
```
Reader → fetch_json(api.url)
       → 提取 items
       → apply_mapping (DSL 转换)
       → 发送到 Channel
       → Writer 批量写入
```

---

### 场景 2: 数据库数据源（单 Reader）

适用于小批量数据库到数据库的迁移。

```rust
let pipeline_config = PipelineConfig {
    reader_threads: 1,         // 单 Reader 读取
    writer_threads: 4,
    channel_buffer_size: 1000,
    batch_size: 100,
    use_transaction: true,
    data_source: DataSourceType::Database {
        query: String::new(),  // 保留字段
        limit: Some(10000),    // 读取 1 万条
        offset: Some(0),       // 从第 0 条开始
    },
};

let pipeline = Pipeline::new(config, pool, pipeline_config);
let stats = pipeline.execute().await?;
```

**执行 SQL**:
```sql
-- Reader 0
SELECT * FROM source_table LIMIT 10000 OFFSET 0
```

---

### 场景 3: 数据库数据源（多 Reader 并发分片）

适用于**大批量**数据库到数据库的迁移，充分利用并发优势。

```rust
let pipeline_config = PipelineConfig {
    reader_threads: 4,         // 4 个 Reader 并发
    writer_threads: 8,         // 8 个 Writer 并发
    channel_buffer_size: 2000,
    batch_size: 200,
    use_transaction: true,
    data_source: DataSourceType::Database {
        query: String::new(),
        limit: Some(100000),   // 读取 10 万条
        offset: Some(0),
    },
};

let pipeline = Pipeline::new(config, pool, pipeline_config);
let stats = pipeline.execute().await?;
```

**分片策略**:
```
总数据量: 100000 条
Reader 数量: 4 个
每个分片: 100000 / 4 = 25000 条

Reader 0: SELECT * FROM source_table LIMIT 25000 OFFSET 0
Reader 1: SELECT * FROM source_table LIMIT 25000 OFFSET 25000
Reader 2: SELECT * FROM source_table LIMIT 25000 OFFSET 50000
Reader 3: SELECT * FROM source_table LIMIT 25000 OFFSET 75000
```

**并发流程**:
```
Reader 0 ─┐
Reader 1 ─┼─→ Channel ─→ Dispatcher ─┬─→ Writer 0 ─┐
Reader 2 ─┤                           ├─→ Writer 1 ─┤
Reader 3 ─┘                           ├─→ Writer 2 ─┼─→ Database
                                      ├─→ Writer 3 ─┤
                                      ├─→ Writer 4 ─┤
                                      ├─→ Writer 5 ─┤
                                      ├─→ Writer 6 ─┤
                                      └─→ Writer 7 ─┘
```

**性能提升**:
- 4 个 Reader 并发读取源数据库
- 8 个 Writer 并发写入目标数据库
- 理论吞吐量提升 4x（Reader 侧）+ 8x（Writer 侧）

---

## 配置参数对比

| 参数 | API 数据源推荐 | 数据库数据源推荐 |
|------|----------------|------------------|
| `reader_threads` | 1（API 通常不支持分片） | 2-8（根据数据量和数据库性能） |
| `writer_threads` | 4-8 | 4-16（根据目标数据库连接池） |
| `channel_buffer_size` | 1000 | 2000-5000（大批量数据） |
| `batch_size` | 100 | 200-500（减少数据库往返） |
| `data_source` | `DataSourceType::Api` | `DataSourceType::Database { ... }` |

---

## 性能调优

### 1. Reader 线程数调优

```rust
// 数据库数据源：根据数据量和源数据库性能
let total_rows = 1_000_000; // 100 万条数据
let reader_threads = if total_rows > 100_000 {
    8  // 大批量数据使用 8 个 Reader
} else if total_rows > 10_000 {
    4  // 中等批量使用 4 个 Reader
} else {
    1  // 小批量使用 1 个 Reader
};
```

### 2. Writer 线程数调优

```rust
// 根据目标数据库连接池大小
let max_conns = config.db.max_connections.unwrap_or(10);
let writer_threads = (max_conns * 3 / 4).max(1); // 使用 75% 连接

PipelineConfig {
    writer_threads,
    ..Default::default()
}
```

### 3. 分片大小调优

```rust
// 根据单条记录大小和内存限制
let avg_record_size = 2_000; // 2KB per record
let target_shard_mem = 50 * 1024 * 1024; // 50MB per shard
let shard_limit = target_shard_mem / avg_record_size;

PipelineConfig {
    data_source: DataSourceType::Database {
        query: String::new(),
        limit: Some(shard_limit * reader_threads),
        offset: Some(0),
    },
    ..Default::default()
}
```

---

## 实现细节

### 数据库数据源实现

```rust
async fn fetch_from_database(
    config: &Config,
    pool: &DbPool,
    shard: Option<DataShard>,
) -> Result<Vec<JsonValue>> {
    let table = &config.db.table;

    // 构建带分片的 SQL
    let (sql, limit, offset) = if let Some(s) = shard {
        (
            format!("SELECT * FROM {} LIMIT {} OFFSET {}", table, s.limit, s.offset),
            s.limit,
            s.offset
        )
    } else {
        (format!("SELECT * FROM {}", table), 0, 0)
    };

    // 执行查询并转换为 JSON
    let rows = match pool {
        DbPool::Postgres(pg_pool) => {
            sqlx::query(&sql).fetch_all(pg_pool).await?
                .iter().map(|row| { /* 转换为 JSON */ }).collect()
        }
        DbPool::Mysql(my_pool) => {
            sqlx::query(&sql).fetch_all(my_pool).await?
                .iter().map(|row| { /* 转换为 JSON */ }).collect()
        }
    };

    Ok(rows)
}
```

### Reader 任务分片逻辑

```rust
// 在 Pipeline::execute() 中
for i in 0..reader_count {
    let shard = match &data_source {
        DataSourceType::Database { limit, offset, .. } => {
            let total_limit = limit.unwrap_or(1000000);
            let shard_size = total_limit / reader_count;
            let shard_offset = offset.unwrap_or(0) + i * shard_size;

            Some(DataShard {
                shard_id: i,
                total_shards: reader_count,
                offset: shard_offset,
                limit: shard_size,
            })
        }
        DataSourceType::Api => None,
    };

    // 启动 Reader 任务
    tokio::spawn(async move {
        reader_task(config, pool, tx, i, data_source, shard).await
    });
}
```

---

## 注意事项

### 数据库分片的限制

1. **数据一致性**: 分片读取期间如果数据有更新，可能导致数据不一致
2. **顺序依赖**: 如果业务逻辑依赖数据顺序，需要在查询中添加 `ORDER BY`
3. **数据库压力**: 多 Reader 并发可能对源数据库造成压力，建议:
   - 在低峰期执行
   - 根据数据库性能调整 `reader_threads`
   - 使用只读副本作为数据源

### 推荐实践

1. **小批量数据（< 10000 条）**: 使用 API 数据源或单 Reader 数据库数据源
2. **中等批量（10000-100000 条）**: 使用 2-4 个 Reader 的数据库数据源
3. **大批量数据（> 100000 条）**: 使用 4-8 个 Reader 的数据库数据源
4. **极大批量（> 1000000 条）**: 考虑分批次执行，每批次使用 8 个 Reader

---

## 未来扩展

### 计划支持的数据源

1. **文件数据源**:
   ```rust
   DataSourceType::File {
       path: String,
       format: FileFormat,  // CSV, JSON, Parquet
   }
   ```

2. **消息队列数据源**:
   ```rust
   DataSourceType::Kafka {
       topic: String,
       partition: usize,
   }
   ```

3. **流式数据源**:
   ```rust
   DataSourceType::Stream {
       url: String,
       protocol: StreamProtocol,  // WebSocket, SSE
   }
   ```

### 智能分片

根据数据分布自动优化分片策略：
- 统计数据总量
- 分析数据分布
- 动态调整分片大小
- 负载均衡分配

---

## 总结

通过引入 `DataSourceType` 抽象，Pipeline 架构现在具备：

- ✅ **灵活性**: 支持 API 和数据库两种数据源
- ✅ **并发性**: 多 Reader 并发分片读取数据库
- ✅ **可扩展性**: 易于添加新的数据源类型
- ✅ **高性能**: 充分利用多核 CPU 和数据库连接池
- ✅ **职责分离**: Reader 专注于任务切分，数据源独立配置

这为后续支持更多数据源类型（文件、消息队列、流式数据）奠定了坚实基础。
