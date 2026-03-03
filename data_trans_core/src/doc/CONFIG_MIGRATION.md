# 配置格式迁移指南

## 概述

Config 结构已从固定的 "API 读取 → DB 写入" 模式升级为灵活的 "Input → Output" 工作流模式。

## 变更对比

### 旧配置格式（已废弃）

```json
{
  "tasks": [
    {
      "id": "default",
      "db_type": "postgres",
      "db": {
        "url": "postgres://user:pass@localhost/db",
        "table": "tb_example",
        "key_columns": ["id"]
      },
      "api": {
        "url": "http://api.example.com/data",
        "method": "GET",
        "headers": {"Authorization": "Bearer token"},
        "items_json_path": "/data/items"
      },
      "column_mapping": {
        "db_column": "api.field"
      },
      "mode": "upsert",
      "batch_size": 100
    }
  ]
}
```

### 新配置格式（推荐）

```json
{
  "tasks": [
    {
      "id": "default",
      "input": {
        "name": "api_source",
        "type": "api",
        "config": {
          "url": "http://api.example.com/data",
          "method": "GET",
          "headers": {"Authorization": "Bearer token"},
          "items_json_path": "/data/items",
          "timeout_secs": 30
        }
      },
      "output": {
        "name": "db_target",
        "type": "database",
        "config": {
          "db_type": "postgres",
          "url": "postgres://user:pass@localhost/db",
          "table": "tb_example",
          "key_columns": ["id"],
          "max_connections": 10,
          "acquire_timeout_secs": 30,
          "use_transaction": true
        }
      },
      "column_mapping": {
        "db_column": "api.field"
      },
      "column_types": {
        "db_column": "int"
      },
      "mode": "upsert",
      "batch_size": 100
    }
  ]
}
```

## 核心变更

### 1. 数据源抽象

**旧格式**:
- `db`: 固定为输出目标
- `api`: 固定为输入来源
- `db_type`: 顶层字段

**新格式**:
- `input`: 灵活配置输入源
- `output`: 灵活配置输出源
- 每个数据源包含:
  - `name`: 数据源名称（便于识别）
  - `type`: 数据源类型（api, database, file 等）
  - `config`: 该类型数据源的具体配置

### 2. 配置层级

**旧格式（扁平）**:
```json
{
  "db_type": "postgres",
  "db": { "url": "...", "table": "..." },
  "api": { "url": "...", "method": "..." }
}
```

**新格式（嵌套）**:
```json
{
  "input": {
    "type": "api",
    "config": { "url": "...", "method": "..." }
  },
  "output": {
    "type": "database",
    "config": { "db_type": "postgres", "url": "...", "table": "..." }
  }
}
```

## 支持的数据源类型

### Input 数据源

#### 1. API 数据源
```json
{
  "input": {
    "name": "my_api",
    "type": "api",
    "config": {
      "url": "https://api.example.com/data",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer token",
        "Content-Type": "application/json"
      },
      "body": {"key": "value"},
      "items_json_path": "/response/data",
      "timeout_secs": 60
    }
  }
}
```

#### 2. Database 数据源
```json
{
  "input": {
    "name": "source_db",
    "type": "database",
    "config": {
      "db_type": "mysql",
      "url": "mysql://user:pass@localhost/source_db",
      "table": "source_table",
      "max_connections": 5,
      "acquire_timeout_secs": 30
    }
  }
}
```

### Output 数据源

#### 1. Database 数据源（最常用）
```json
{
  "output": {
    "name": "target_db",
    "type": "database",
    "config": {
      "db_type": "postgres",
      "url": "postgres://user:pass@localhost/target_db",
      "table": "target_table",
      "key_columns": ["id", "date"],
      "max_connections": 10,
      "acquire_timeout_secs": 30,
      "use_transaction": true
    }
  }
}
```

## 工作流示例

### 示例 1: API → Database（标准场景）

```json
{
  "id": "api_to_db",
  "input": {
    "name": "rest_api",
    "type": "api",
    "config": {
      "url": "https://api.example.com/users",
      "method": "GET",
      "headers": {"Authorization": "Bearer xxx"},
      "items_json_path": "/data"
    }
  },
  "output": {
    "name": "postgres_db",
    "type": "database",
    "config": {
      "db_type": "postgres",
      "url": "postgres://localhost/mydb",
      "table": "users",
      "key_columns": ["user_id"],
      "use_transaction": true
    }
  },
  "column_mapping": {
    "user_id": "id",
    "user_name": "name",
    "email": "email"
  },
  "mode": "upsert",
  "batch_size": 100
}
```

### 示例 2: Database → Database（数据迁移）

```json
{
  "id": "db_migration",
  "input": {
    "name": "mysql_source",
    "type": "database",
    "config": {
      "db_type": "mysql",
      "url": "mysql://localhost/old_db",
      "table": "old_users"
    }
  },
  "output": {
    "name": "postgres_target",
    "type": "database",
    "config": {
      "db_type": "postgres",
      "url": "postgres://localhost/new_db",
      "table": "users",
      "key_columns": ["id"]
    }
  },
  "column_mapping": {
    "id": "user_id",
    "name": "username",
    "created_at": "create_time"
  },
  "mode": "insert",
  "batch_size": 500
}
```

### 示例 3: Database → API（反向同步，未来支持）

```json
{
  "id": "db_to_api",
  "input": {
    "name": "local_db",
    "type": "database",
    "config": {
      "db_type": "postgres",
      "url": "postgres://localhost/mydb",
      "table": "events"
    }
  },
  "output": {
    "name": "webhook_api",
    "type": "api",
    "config": {
      "url": "https://webhook.example.com/events",
      "method": "POST",
      "headers": {"X-API-Key": "secret"}
    }
  },
  "column_mapping": {
    "event_id": "id",
    "event_type": "type",
    "payload": "data"
  },
  "batch_size": 10
}
```

## 代码迁移

### 访问配置的方式变更

**旧代码**:
```rust
let api_url = &config.api.url;
let db_table = &config.db.table;
let db_type = &config.db_type;
```

**新代码**:
```rust
// 解析 API 配置
let api_config = Config::parse_api_config(&config.input)?;
let api_url = &api_config.url;

// 解析数据库配置
let db_config = Config::parse_db_config(&config.output)?;
let db_table = &db_config.table;

// 获取数据库类型
let db_type = Config::get_source_db_type(&config.output);
```

### 辅助方法

**Config 新增方法**:
```rust
impl Config {
    // 从 ConfigManager 创建 Config
    pub fn from_manager(mgr: &ConfigManager, task_id: &str) -> Result<Self>;

    // 解析 API 配置（从任意 DataSourceConfig）
    pub fn parse_api_config(source: &DataSourceConfig) -> Result<ApiConfig>;

    // 解析数据库配置（从任意 DataSourceConfig）
    pub fn parse_db_config(source: &DataSourceConfig) -> Result<DbConfig>;

    // 获取数据源的数据库类型
    pub fn get_source_db_type(source: &DataSourceConfig) -> Option<String>;

    // 创建默认配置
    pub fn default_with_id(id: String) -> Self;
}
```

## 向后兼容

为了保持向后兼容，保留了 `LegacyConfig` 结构：

```rust
pub struct LegacyConfig {
    pub id: String,
    pub db_type: Option<String>,
    pub db: DbConfig,
    pub api: ApiConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}
```

如果需要支持旧配置文件，可以将其转换为新格式。

## 迁移步骤

### 自动迁移工具（未来支持）

```bash
# 迁移配置文件
cargo run --bin data_trans_cli -- migrate-config --input old_config.json --output new_config.json
```

### 手动迁移步骤

1. **复制旧配置**: 备份 `defaults.json` 或用户配置文件
2. **创建 input 节点**:
   - 将 `api` 字段移动到 `input.config`
   - 设置 `input.type = "api"`
   - 设置 `input.name = "api_source"`

3. **创建 output 节点**:
   - 将 `db` 字段移动到 `output.config`
   - 将 `db_type` 移动到 `output.config.db_type`
   - 设置 `output.type = "database"`
   - 设置 `output.name = "db_target"`

4. **保留其他字段**:
   - `column_mapping`, `column_types`, `mode`, `batch_size` 保持不变

5. **验证配置**: 使用 `cargo test` 或启动应用验证配置加载

## 优势

### 1. 灵活性
- ✅ Input 和 Output 可以是任意类型组合
- ✅ API → DB, DB → DB, DB → API, File → DB 等

### 2. 可扩展性
- ✅ 易于添加新的数据源类型（File, Kafka, S3 等）
- ✅ 每种数据源有独立的配置结构

### 3. 清晰性
- ✅ 配置结构更清晰，职责明确
- ✅ `input` 和 `output` 语义明确，易于理解

### 4. 一致性
- ✅ 所有数据源使用统一的 `DataSourceConfig` 结构
- ✅ 便于统一管理和验证

## 常见问题

### Q: 旧配置文件还能用吗？
A: 不能直接使用。需要手动迁移到新格式，或等待自动迁移工具。

### Q: 如何知道数据源支持哪些配置项？
A: 查看 `Config::parse_api_config()` 和 `Config::parse_db_config()` 的实现，或参考本文档的示例。

### Q: 可以 Input 和 Output 都是 API 吗？
A: 理论上可以，但目前代码主要支持 Input=API/DB, Output=DB。未来会扩展 Output=API 的支持。

### Q: Database 数据源的 `query` 字段有什么用？
A: 当前保留字段，未来可用于自定义 SQL 查询。现在默认使用 `SELECT * FROM table`。

## 下一步

- [ ] 实现自动配置迁移工具
- [ ] 支持 Output=API（反向同步）
- [ ] 支持 File 数据源（CSV, JSON, Parquet）
- [ ] 支持 Kafka 数据源
- [ ] 配置验证工具（检查配置正确性）
