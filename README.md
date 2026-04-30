"# data sync transport tools"

# Relus

## Build

cd Relus

```bash
cargo build --release --bin relus_cli
```
or
```bash
cargo packager --release
```

## Usage

```bash
# 一次性同步
cargo run -p relus_cli -- sync -c cli/user_config/default_job.json

# 仅启动 HTTP API
cargo run -p relus_cli -- serve --host 127.0.0.1 --port 30001

# 启动常驻调度器 + HTTP 控制面 + REPL
cargo run -p relus_cli -- run -c cli/user_config/default_job.json

# 启动常驻调度器，禁用 REPL，仅保留 HTTP 控制面
cargo run -p relus_cli -- run --jobs-dir cli/user_config --no-repl

# release
./target/release/relus_cli sync -c xxxYourJobLocation/xxx/default_job.json
```
