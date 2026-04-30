# 数据同步工具CLi

- 连接 MySQL / Postgres
- 选择目标表（通过 `list-tables` 查看）
- 通过配置的接口获取 JSON 数据
- 将字段按配置映射并同步到目标表，支持 upsert
- 支持测试接口数据获取

## 构建

```bash
cargo build -p relus_cli
```

## 启动模式

```bash
# 一次性执行单个同步任务，任务结束后退出
cargo run -p relus_cli -- sync -c cli/user_config/default_job.json

# 只启动 HTTP API，不启动调度器
cargo run -p relus_cli -- serve --host 127.0.0.1 --port 30001

# 启动常驻调度器，默认同时启动 HTTP 控制面和 REPL
cargo run -p relus_cli -- run -c cli/user_config/default_job.json

# start 是 run 的别名
cargo run -p relus_cli -- start --jobs-dir cli/user_config --no-repl
```
