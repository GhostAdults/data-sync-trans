# 数据同步工具

- 连接 MySQL / Postgres
- 选择目标表（通过 `list-tables` 查看）
- 通过配置的接口获取 JSON 数据
- 将字段按配置映射并同步到目标表，支持 upsert
- 支持测试接口数据获取

## 构建

```bash
cargo build
```

如遇到镜像源导致依赖版本问题，请将 Cargo 使用的 crates.io 源切换到官方或能提供兼容版本的镜像。

## 示例配置

参考项目根目录的 `config.example.json`。

关键字段：
- `db.url`：数据库连接串
- `db.table`：目标表名
- `db.key_columns`：用于 Postgres `ON CONFLICT` 的键列（MySQL 使用表唯一键）
- `api.url`：接口地址
- `api.items_json_path`：响应中数据数组路径（支持 JSON Pointer `/a/b` 或点路径 `a.b`）
- `column_mapping`：表列到 JSON 字段路径的映射
- `column_types`：列类型，支持 `int|float|bool|text|json|timestamp`
- `mode`：`upsert` 或 `insert`

## 用法

列出表：
```bash
cargo run -- list-tables --db-url "postgres://user:password@localhost:5432/mydb" --db-type postgres
```

测试接口：
```bash
cargo run -- test-api --config ./config.example.json
```

同步数据：
```bash
cargo run -- sync --config ./config.example.json
```

## 类型与绑定

- `column_types` 决定参数绑定的类型，避免类型不匹配
- `timestamp` 需为 RFC3339 格式，如 `2024-01-01T12:00:00Z`

