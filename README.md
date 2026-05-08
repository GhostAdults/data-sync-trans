# Relus

Relus 是一个用 Rust 编写的数据同步工具，目标是把不同来源的数据按配置同步到目标端。当前主要支持 RDBMS 场景，适合做数据库表同步、字段映射、批量写入和常驻调度执行。

## 能做什么

- 从 MySQL/PostgreSQL 等 RDBMS 读取数据并写入目标数据库。
- 支持 `insert`、`upsert`、`update`、`delete` 写入模式。
- 支持 `fullsnapshot`、`incremental`、`mix` 同步模式。
- 通过 `column_mapping` 做源字段到目标字段映射。
- 通过 `column_types` 指定字段逻辑类型。
- 支持一次性执行任务，也支持启动常驻调度器后通过 HTTP 提交任务 JSON。
- 提供表列表、表结构查看、mapping 生成等辅助命令。

## Build

安装 Rust stable 后，在仓库根目录执行：

```bash
cargo build
```

构建 release CLI：

```bash
cargo build --release -p relus_cli --bin relus_cli
```

构建完成后的二进制位置：

```bash
# Linux/macOS
./target/release/relus_cli

# Windows
.\target\release\relus_cli.exe
```

常用验证命令：

```bash
cargo fmt --check --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

## CLI 使用

开发环境可以直接用 `cargo run`：

```bash
cargo run -p relus_cli -- sync -c cli/user_config/default_job.json
```

release 二进制：

```bash
./target/release/relus_cli sync -c cli/user_config/default_job.json
```

常用命令：

```bash
# 一次性执行一个同步任务
relus_cli sync -c cli/user_config/default_job.json

# 启动常驻调度器、HTTP 控制面和 REPL
relus_cli run -c cli/user_config/default_job.json

# 启动常驻调度器，但不预加载任务；等待 HTTP/REPL 提交
relus_cli run --no-repl

# 从目录加载所有 *.json 任务
relus_cli run --jobs-dir cli/user_config --no-repl

# 指定调度器 HTTP 地址
relus_cli run --host 127.0.0.1 --port 30001 --no-repl

# 查看数据库表
relus_cli list-tables --db-url mysql://root:123456@127.0.0.1:3306/my_db

# 查看表结构
relus_cli describe-table --db-url mysql://root:123456@127.0.0.1:3306/my_db --table users

# 生成字段 mapping 模板
relus_cli gen-mapping --db-url mysql://root:123456@127.0.0.1:3306/my_db --table users --output mapping.json
```

## 提交任务 JSON

### 方式一：一次性执行

直接把任务 JSON 路径传给 `sync`：

```bash
relus_cli sync -c cli/user_config/default_job.json
```

### 方式二：启动调度器后通过 HTTP 提交

先启动常驻调度器：

```bash
relus_cli run --host 127.0.0.1 --port 30001 --no-repl
```

提交任务 JSON 文件路径：

```bash
curl -X POST http://127.0.0.1:30001/scheduler/tasks \
  -H "Content-Type: application/json" \
  -d '{"path":"cli/user_config/default_job.json"}'
```

查询任务：

```bash
curl http://127.0.0.1:30001/scheduler/tasks
```

取消任务：

```bash
curl -X POST http://127.0.0.1:30001/scheduler/tasks/default/cancel
```

注意：`serve` 命令只启动普通 HTTP API；要使用 `/scheduler/tasks` 提交任务，请使用 `run` 或 `start`。

## Job JSON 示例

任务文件可以参考 `cli/user_config/default_job.json`。一个最小的数据库到数据库同步配置如下：

```json
{
  "id": "default",
  "source": {
    "name": "db_source",
    "type": "database",
    "config": {
      "split_pk": "id",
      "connections": [
        {
          "type": "mysql",
          "host": "127.0.0.1",
          "port": 3306,
          "database": "source_db",
          "username": "root",
          "password": "password",
          "table": "source_table",
          "key_columns": ["id"],
          "max_connections": 20,
          "acquire_timeout_secs": 30,
          "use_transaction": true
        }
      ]
    }
  },
  "target": {
    "name": "db_target",
    "type": "database",
    "writer_mode": "insert",
    "config": {
      "connection": {
        "type": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "target_db",
        "username": "root",
        "password": "password",
        "timezone": "+08:00",
        "table": "target_table",
        "key_columns": ["id"],
        "max_connections": 20,
        "acquire_timeout_secs": 30,
        "use_transaction": true
      }
    }
  },
  "column_mapping": {
    "id": "id",
    "name": "name",
    "created_at": "created_at"
  },
  "column_types": {
    "id": "string",
    "name": "string",
    "created_at": "timestamp"
  },
  "sync_mode": "fullsnapshot",
  "batch_size": 1000,
  "channel_buffer_size": 1000
}
```

字段说明：

- `source`：数据来源。数据库来源使用 `type: "database"`。
- `target`：数据目标。数据库目标使用 `type: "database"`。
- `target.writer_mode`：写入模式，可选 `insert`、`upsert`、`update`、`delete`。
- `config.connections` / `config.connection`：数据库连接信息。读取端通常使用 `connections`，写入端通常使用 `connection`。
- `table`：读取或写入的表名。
- `key_columns`：主键或唯一键字段，`upsert`、`update`、`delete` 通常需要配置。
- `timezone`：数据库会话时区，例如 `+08:00`。未配置时会尝试使用系统当前时区。
- `column_mapping`：目标字段到源字段的映射。
- `column_types`：字段逻辑类型，常用值包括 `string`、`int`、`float`、`bool`、`timestamp`、`text`。
- `sync_mode`：同步模式，可选 `fullsnapshot`、`incremental`、`mix`。
- `batch_size`：批量读取/写入大小。
- `channel_buffer_size`：pipeline channel 缓冲区大小。
- `schedule`：可选调度配置。缺省时任务立即执行。

调度配置示例：

```json
{
  "schedule": "*/5 * * * *"
}
```

或：

```json
{
  "schedule": {
    "type": "once",
    "value": "2026-05-08T12:00:00Z"
  }
}
```

## 系统配置

系统配置示例在 `cli/user_config/default.config.json`：

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 30001
  },
  "pipeline": {
    "reader_threads": 8,
    "buffer_size": 1000,
    "batch_size": 100,
    "use_transaction": true
  }
}
```

启动 `run` 时，如果没有显式传入 `--host` 或 `--port`，会优先读取系统配置中的 `server.host` 和 `server.port`。

## 项目结构

- `cli/`：命令行入口和示例配置。
- `core/`：同步引擎、pipeline、调度器、HTTP API。
- `common/`：共享配置、类型、日志、消息结构。
- `reader/`：数据读取实现。
- `writer/`：数据写入实现。
- `connector-rdbms/`：RDBMS 连接池、SQL builder、schema 元数据。
