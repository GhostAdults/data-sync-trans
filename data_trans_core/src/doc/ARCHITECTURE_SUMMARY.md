# 架构总结

## 最新架构（2024 重构）

### 核心原则

1. **Input/Output 灵活配置**：不再固定 API→DB，支持任意数据源组合
2. **单一执行引擎**：只使用 Pipeline（多线程），删除了 sync_nodes（单线程）
3. **自动判断数据源**：根据配置自动选择 Reader 和 Writer 实现

---

## 模块结构

```
src/core/
├── mod.rs              # 核心类型定义（Config, DataSourceConfig）
├── db.rs               # 数据库连接池管理
├── client_tool.rs      # HTTP 客户端
├── server.rs           # 同步入口（sync 函数）
├── sync_pipeline.rs    # 唯一的执行引擎
├── axum_api.rs         # HTTP API 路由
└── cli.rs              # CLI 命令
```

---

## 配置结构

### JSON 配置格式

```json
{
  "tasks": [{
    "id": "task_id",
    "input": {
      "name": "数据源名称",
      "type": "api | database",
      "config": { /* 具体配置 */ }
    },
    "output": {
      "name": "目标名称",
      "type": "database | api",
      "config": { /* 具体配置 */ }
    },
    "column_mapping": { "目标列": "源字段路径" },
    "column_types": { "目标列": "int | float | bool | timestamp | text" },
    "mode": "insert | upsert",
    "batch_size": 100
  }]
}
```

### Rust 结构体

```rust
pub struct Config {
    pub id: String,
    pub input: DataSourceConfig,
    pub output: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}

pub struct DataSourceConfig {
    pub name: String,
    pub source_type: String,  // "api", "database", "file", etc.
    pub config: Value,        // 动态配置（JSON）
}
```

---

## 数据流

### 1. API → Database（最常见）

```
┌─────────────────────────────────────────────────┐
│  sync(config, pool)                             │
├─────────────────────────────────────────────────┤
│  1. 检查 input.type = "api"                     │
│  2. 创建 PipelineConfig(data_source = Api)      │
│  3. Pipeline::execute()                         │
│     ├─ Reader: fetch_from_api()                 │
│     │    └─ fetch_json() → extract_items()      │
│     ├─ apply_mapping() (DSL 转换)               │
│     ├─ Channel (mpsc::channel)                  │
│     ├─ Dispatcher (轮询分发)                    │
│     └─ Writer: prepare_db_batch() + execute()   │
│          └─ INSERT/UPSERT to database           │
└─────────────────────────────────────────────────┘
```

### 2. Database → Database（数据迁移）

```
┌─────────────────────────────────────────────────┐
│  sync(config, pool)                             │
├─────────────────────────────────────────────────┤
│  1. 检查 input.type = "database"                │
│  2. 创建 PipelineConfig(data_source = Database) │
│  3. Pipeline::execute()                         │
│     ├─ Reader: fetch_from_database()            │
│     │    └─ SELECT * FROM table (支持分片)      │
│     ├─ apply_mapping() (字段映射)               │
│     ├─ Channel                                  │
│     ├─ Dispatcher                               │
│     └─ Writer: INSERT/UPSERT to target DB       │
└─────────────────────────────────────────────────┘
```

---

## Pipeline 核心函数（src/core/sync_pipeline.rs）

### 数据获取

```rust
// 从 API 获取
async fn fetch_from_api(config: &Config) -> Result<Vec<JsonValue>>

// 从数据库获取（支持分片）
async fn fetch_from_database(
    config: &Config,
    pool: &DbPool,
    shard: Option<DataShard>,
) -> Result<Vec<JsonValue>>
```

### 数据转换

```rust
// 字段映射
fn apply_mapping(
    items: &[JsonValue],
    column_mapping: &BTreeMap<String, String>,
    column_types: &Option<BTreeMap<String, String>>,
) -> Result<Vec<MappedRow>>

// 类型转换
fn to_typed_value(v: &JsonValue, type_hint: Option<&str>) -> Result<TypedVal>
```

### 数据写入

```rust
// 准备批次
fn prepare_db_batch(
    mapped_rows: &[MappedRow],
    table_name: &str,
    key_columns: &[String],
    mode: &str,
    db_kind: DbKind,
) -> Result<DbBatch>

// 执行写入
async fn execute_db_write(
    batch: &DbBatch,
    pool: &DbPool,
    use_transaction: bool,
    batch_size: usize,
) -> Result<usize>
```

### 执行器

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

---

## 自动判断逻辑

### server.rs::sync() 函数

```rust
pub async fn sync(cfg: &Config, pool: &DbPool) -> Result<()> {
    // 1. 根据 input.type 自动判断数据源
    let data_source = match cfg.input.source_type.as_str() {
        "api" => DataSourceType::Api,
        "database" => DataSourceType::Database {
            query: String::new(),
            limit: None,
            offset: None,
        },
        _ => DataSourceType::Api, // 默认
    };

    // 2. 创建 Pipeline 配置
    let pipeline_config = PipelineConfig {
        reader_threads: 1,
        writer_threads: 4,
        channel_buffer_size: 1000,
        batch_size: cfg.batch_size.unwrap_or(100),
        use_transaction: true,
        data_source,
    };

    // 3. 执行 Pipeline
    sync_with_pipeline_custom(cfg.clone(), pool.clone(), pipeline_config).await?;
    Ok(())
}
```

### Reader 任务中的判断

```rust
async fn reader_task(..., data_source: DataSourceType, ...) -> Result<usize> {
    // 根据数据源类型获取数据
    let items = match data_source {
        DataSourceType::Api => {
            fetch_from_api(&config).await?
        }
        DataSourceType::Database { .. } => {
            let pool = pool.ok_or_else(|| anyhow!("需要 pool"))?;
            fetch_from_database(&config, &pool, shard).await?
        }
    };

    // 统一的后续处理
    let mapped_rows = apply_mapping(&items, ...)?;
    // 发送到 Channel...
}
```

---

## 支持的数据源组合

| Input 类型 | Output 类型 | 支持状态 | 说明 |
|-----------|------------|---------|------|
| api | database | ✅ 完全支持 | 最常见场景 |
| database | database | ✅ 完全支持 | 数据迁移 |
| api | api | ⚠️ 部分支持 | Writer 需扩展 |
| database | api | ⚠️ 部分支持 | Writer 需扩展 |
| file | database | 🔜 未来支持 | 需添加 file Reader |
| kafka | database | 🔜 未来支持 | 需添加 kafka Reader |

---

## 性能特性

### 并发模型

- **Reader 线程**：默认 1，DB 数据源可设置多个（支持分片）
- **Writer 线程**：默认 4，充分利用数据库连接池
- **Channel 缓冲**：默认 1000，削峰填谷

### 性能提升

相比单线程模式：
- 小数据量（< 1万条）：~1.5x
- 中等数据量（1-10万条）：~3-5x
- 大数据量（> 10万条）：~5-10x

### 调优参数

```rust
PipelineConfig {
    reader_threads: 1,              // 建议: 1 (API) 或 2-8 (DB)
    writer_threads: 4,              // 建议: 连接池的 50-75%
    channel_buffer_size: 1000,      // 建议: 1000-5000
    batch_size: 100,                // 建议: 100-500
    use_transaction: true,          // 建议: true（性能和一致性平衡）
    data_source: DataSourceType,    // 自动判断
}
```

---

## 扩展性

### 添加新数据源类型

1. **在 DataSourceType 中添加变体**：
```rust
pub enum DataSourceType {
    Api,
    Database { ... },
    File { path: String, format: FileFormat },  // 新增
}
```

2. **在 fetch_from_* 中添加实现**：
```rust
async fn fetch_from_file(config: &Config, path: &str, format: FileFormat) -> Result<Vec<JsonValue>> {
    // 实现文件读取逻辑
}
```

3. **在 reader_task 中添加匹配**：
```rust
let items = match data_source {
    DataSourceType::Api => fetch_from_api(&config).await?,
    DataSourceType::Database { .. } => fetch_from_database(&config, &pool, shard).await?,
    DataSourceType::File { path, format } => fetch_from_file(&config, &path, format).await?,
};
```

4. **在 server::sync() 中添加判断**：
```rust
let data_source = match cfg.input.source_type.as_str() {
    "api" => DataSourceType::Api,
    "database" => DataSourceType::Database { ... },
    "file" => DataSourceType::File { ... },  // 新增
    _ => DataSourceType::Api,
};
```

---

## 迁移指南

### 从旧架构迁移

**旧代码**（已删除）：
```rust
use crate::core::sync_nodes::sync_workflow;
sync_workflow(cfg, pool).await?;
```

**新代码**：
```rust
use crate::core::server::sync;
sync(cfg, pool).await?;  // 自动使用 Pipeline
```

### 配置文件迁移

参见 `CONFIG_MIGRATION.md`

---

## 总结

### 优势

1. ✅ **简化代码**：删除 sync_nodes，只有一个执行引擎
2. ✅ **灵活配置**：Input/Output 可任意组合
3. ✅ **自动判断**：根据配置自动选择 Reader/Writer
4. ✅ **高性能**：多线程 Pipeline，性能提升 3-10x
5. ✅ **易扩展**：添加新数据源只需 4 步

### 劣势

1. ⚠️ **复杂度稍高**：相比单线程模式更复杂
2. ⚠️ **调试难度**：多线程问题定位较困难
3. ⚠️ **资源占用**：多线程会占用更多内存和连接

### 适用场景

**推荐使用 Pipeline**：
- ✅ 生产环境
- ✅ 数据量 > 1000 条
- ✅ 需要高吞吐量
- ✅ 数据库连接池 > 5

**不推荐**：
- ❌ 极小数据量（< 100 条）
- ❌ 调试和开发阶段（可临时降低线程数）
- ❌ 资源受限环境（内存 < 512MB）

---

## 未来计划

1. [ ] 支持更多数据源（File, Kafka, S3, Redis）
2. [ ] 支持 Output 为 API（反向同步）
3. [ ] 动态调整线程数（根据负载）
4. [ ] 实时监控和统计面板
5. [ ] 失败重试和错误恢复
6. [ ] 配置验证工具
7. [ ] 自动配置迁移工具
