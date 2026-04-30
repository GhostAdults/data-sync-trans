// cli 命令行参数解析

use crate::core::runner::RunStatus;
use crate::core::serve::*;
use crate::init_and_watch_config;
use crate::run_scheduler;
use crate::run_serve;
use clap::{Parser, Subcommand};
use relus_common::{ApiConfig, JobConfig};
use relus_connector_rdbms::pool::detect_db_kind;
use relus_connector_rdbms::pool::DbKind;

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{MySqlPool, PgPool, Row};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[derive(Parser)]
#[command(name = "Relus CLI")]
#[command(version)]
#[command(about = "Relus 数据迁移工具 relus-cli@tingfengyu", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    TestApi {
        #[arg(short, long)]
        config: PathBuf,
    },
    Sync {
        #[arg(short, long)]
        config: PathBuf,
    },
    ListTables {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DbKind>,
    },
    DescribeTable {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DbKind>,
        #[arg(short = 'b', long)]
        table: String,
    },
    GenMapping {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DbKind>,
        #[arg(short = 'b', long)]
        table: String,
        #[arg(short = 'o', long)]
        output: PathBuf,
    },
    SyncWithMapping {
        #[arg(short, long)]
        config: PathBuf,
    },
    Serve {
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'p', long, default_value_t = 30001)]
        port: u16,
    },
    /// 启动 TaskScheduler 常驻运行，进入交互式 REPL
    #[command(visible_alias = "start")]
    Run {
        /// 指定一个或多个配置文件
        #[arg(short, long)]
        config: Option<Vec<PathBuf>>,
        /// 指定配置文件目录，加载目录下所有 *.json
        #[arg(short, long)]
        jobs_dir: Option<PathBuf>,
        /// Scheduler HTTP 控制面监听地址；未指定时读取系统配置
        #[arg(short = 'H', long)]
        host: Option<String>,
        /// Scheduler HTTP 控制面端口；未指定时读取系统配置
        #[arg(short = 'p', long)]
        port: Option<u16>,
        /// 禁用交互式 REPL，仅保留 API 控制面
        #[arg(long, default_value_t = false)]
        no_repl: bool,
    },
}

pub async fn run_cli(cmd: Commands) -> Result<()> {
    match cmd {
        Commands::TestApi { config } => {
            let cfg = read_job_config(&config)?;
            let api_config = cfg.source.parse_api_config()?;
            let v = fetch_json(&api_config)?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            let items = if let Some(p) = &api_config.items_json_path {
                extract_by_path(&v, p).unwrap_or(&Value::Null).clone()
            } else {
                v.clone()
            };
            println!("{}", serde_json::to_string_pretty(&items)?);
        }
        Commands::Sync { config } => {
            init_and_watch_config();
            let cfg = read_job_config(&config)?;
            validate_job_identifiers(&cfg)?;
            let result = sync_cli(cfg).await?;
            print_run_result(&result);
        }
        Commands::ListTables { db_url, db_type } => {
            let kind = detect_db_kind(&db_url, db_type)?;
            match kind {
                DbKind::Postgres => {
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    for table in list_tables_postgres(&pool).await? {
                        println!("{}", table);
                    }
                }
                DbKind::Mysql => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    for table in list_tables_mysql(&pool).await? {
                        println!("{}", table);
                    }
                }
            }
        }
        Commands::DescribeTable {
            db_url,
            db_type,
            table,
        } => {
            sanitize_identifier(&table)?;
            let kind = detect_db_kind(&db_url, db_type)?;
            let cols = match kind {
                DbKind::Postgres => {
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_postgres(&pool, &table).await?
                }
                DbKind::Mysql => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_mysql(&pool, &table).await?
                }
            };
            let v = cols
                .into_iter()
                .map(|c| {
                    serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})
                })
                .collect::<Vec<_>>();
            println!("{}", serde_json::to_string_pretty(&serde_json::json!(v))?);
        }
        Commands::GenMapping {
            db_url,
            db_type,
            table,
            output,
        } => {
            sanitize_identifier(&table)?;
            let kind = detect_db_kind(&db_url, db_type)?;
            let cols = match kind {
                DbKind::Postgres => {
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_postgres(&pool, &table).await?
                }
                DbKind::Mysql => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_mysql(&pool, &table).await?
                }
            };
            let mut mapping = BTreeMap::new();
            let mut types = BTreeMap::new();
            for c in cols {
                mapping.insert(c.name.clone(), "".to_string());
                types.insert(c.name.clone(), guess_type(&c.data_type).to_string());
            }
            let obj = serde_json::json!({
                "table": table,
                "key_columns": Vec::<String>::new(),
                "column_mapping": mapping,
                "column_types": types
            });
            fs::write(&output, serde_json::to_string_pretty(&obj)?)
                .with_context(|| format!("写入 mapping 文件失败: {}", output.display()))?;
            println!("written {}", output.display());
        }
        Commands::SyncWithMapping { config } => {
            init_and_watch_config();
            let cfg = read_job_config(&config)?;
            validate_job_identifiers(&cfg)?;
            let result = sync_cli(cfg).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Serve { host, port } => {
            init_and_watch_config();
            println!("Server listening on {}:{}", host, port);
            run_serve(host, port).await?;
        }
        Commands::Run {
            config,
            jobs_dir,
            host,
            port,
            no_repl,
        } => {
            init_and_watch_config();
            let configs = collect_job_configs(config, jobs_dir)?;
            run_scheduler(configs, !no_repl, host, port).await?;
        }
    }
    Ok(())
}

fn sanitize_identifier(s: &str) -> Result<()> {
    let re = Regex::new(r"^[A-Za-z0-9_]+$").unwrap();
    if !re.is_match(s) {
        bail!("标识符仅允许字母、数字和下划线: {}", s);
    }
    Ok(())
}

fn read_job_config(path: &PathBuf) -> Result<JobConfig> {
    let data = fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
    let cfg: JobConfig = serde_json::from_str(&data)
        .map_err(|e| anyhow::anyhow!("配置文件解析失败: {} - 错误: {}", path.display(), e))?;
    Ok(cfg)
}

fn validate_job_identifiers(cfg: &JobConfig) -> Result<()> {
    if cfg.source.source_type == "database" {
        let db_config = cfg.source.parse_database_config()?;
        validate_database_identifiers(&db_config)?;
    }
    if cfg.target.source_type == "database" {
        let db_config = cfg.target.parse_database_config()?;
        validate_database_identifiers(&db_config)?;
    }
    for k in cfg.column_mapping.keys() {
        sanitize_identifier(k)?;
    }
    Ok(())
}

fn validate_database_identifiers(db_config: &relus_common::DbConfig) -> Result<()> {
    if db_config.table.is_empty() {
        bail!("数据库配置必须包含 table 字段");
    }
    sanitize_identifier(&db_config.table)?;
    if let Some(keys) = &db_config.key_columns {
        for k in keys {
            sanitize_identifier(k)?;
        }
    }
    Ok(())
}

fn print_run_result(result: &crate::core::runner::RunResult) {
    match result.status {
        RunStatus::Shutdown => {
            println!(
                "Stream 任务已停止\n 已读出 {} records\n 已写入 {} records\n 耗时 {:.2}s",
                result.stats.records_read, result.stats.records_written, result.stats.elapsed_secs
            );
        }
        _ => {
            println!(
                "complete:\n 任务读出 {} records\n 任务写入 {} records\n 耗时 {:.2}s\n TP {:.0} rec/s",
                result.stats.records_read,
                result.stats.records_written,
                result.stats.elapsed_secs,
                result.stats.throughput
            );
        }
    }
}

fn try_curl(cfg: &ApiConfig) -> Result<Vec<u8>> {
    let mut args: Vec<String> = Vec::new();
    args.push("-sS".to_string());
    if let Some(t) = cfg.timeout_secs {
        args.push("--max-time".to_string());
        args.push(t.to_string());
    }
    let method = cfg.method.as_deref().unwrap_or("GET").to_uppercase();
    args.push("-X".to_string());
    args.push(method.clone());
    if let Some(hs) = &cfg.headers {
        for (k, v) in hs {
            args.push("-H".to_string());
            args.push(format!("{}: {}", k, v));
        }
    }
    if let Some(b) = &cfg.body {
        let s = serde_json::to_string(b)?;
        args.push("-d".to_string());
        args.push(s.clone());
    }
    args.push(cfg.url.clone());
    let output = Command::new("curl").args(&args).output();
    match output {
        Ok(out) => {
            if !out.status.success() {
                bail!("curl 请求失败，状态码: {:?}", out.status.code());
            }
            Ok(out.stdout)
        }
        Err(e) => bail!("无法执行 curl: {}", e),
    }
}

fn try_powershell(cfg: &ApiConfig) -> Result<Vec<u8>> {
    let method = cfg.method.as_deref().unwrap_or("GET").to_uppercase();
    let mut ps = String::new();
    ps.push_str("$hdr=@{};");
    if let Some(hs) = &cfg.headers {
        for (k, v) in hs {
            ps.push_str(&format!(
                "$hdr['{}']='{}';",
                k.replace("'", "''"),
                v.replace("'", "''")
            ));
        }
    }
    let body = if let Some(b) = &cfg.body {
        let s = serde_json::to_string(b)?;
        format!(
            " -ContentType 'application/json' -Body @'{0}'@ ",
            s.replace("'", "''")
        )
    } else {
        "".to_string()
    };
    let timeout = cfg.timeout_secs.unwrap_or(30);
    ps.push_str(&format!(
        "$r=Invoke-RestMethod -Uri '{}' -Method {} -Headers $hdr{} -TimeoutSec {}; $r | ConvertTo-Json -Depth 20",
        cfg.url.replace("'", "''"),
        method,
        body,
        timeout
    ));
    let output = Command::new("powershell")
        .args(&["-NoProfile", "-Command", &ps])
        .output();
    match output {
        Ok(out) => {
            if !out.status.success() {
                bail!(
                    "powershell Invoke-RestMethod 失败，状态码: {:?}",
                    out.status.code()
                );
            }
            Ok(out.stdout)
        }
        Err(e) => bail!("无法执行 powershell: {}", e),
    }
}

fn extract_by_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    if path == "/" || path.is_empty() {
        return Some(root);
    }
    if path.starts_with('/') {
        return root.pointer(path);
    }
    let mut cur = root;
    for seg in path.split('.') {
        match cur {
            Value::Object(map) => {
                cur = map.get(seg)?;
            }
            Value::Array(arr) => {
                if let Ok(idx) = seg.parse::<usize>() {
                    cur = arr.get(idx)?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn fetch_json(cfg: &ApiConfig) -> Result<Value> {
    let data = match try_curl(cfg) {
        Ok(d) => d,
        Err(_) => try_powershell(cfg)?,
    };
    let v: Value = serde_json::from_slice(&data)?;
    Ok(v)
}

async fn list_tables_postgres(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query("select tablename from pg_catalog.pg_tables where schemaname not in ('pg_catalog','information_schema')")
        .fetch_all(pool)
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("tablename")?;
        out.push(name);
    }
    Ok(out)
}

async fn list_tables_mysql(pool: &MySqlPool) -> Result<Vec<String>> {
    let rows = sqlx::query("show tables").fetch_all(pool).await?;
    let mut out = Vec::new();
    for r in rows {
        let v: String = r.try_get(0)?;
        out.push(v);
    }
    Ok(out)
}

#[derive(Clone)]
struct ColInfo {
    name: String,
    data_type: String,
    is_nullable: bool,
}

async fn fetch_columns_postgres(pool: &PgPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select column_name, data_type, is_nullable from information_schema.columns where table_schema='public' and table_name=$1 order by ordinal_position")
        .bind(table)
        .fetch_all(pool)
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("column_name")?;
        let ty: String = r.try_get("data_type")?;
        let is_nullable: String = r.try_get("is_nullable")?;
        out.push(ColInfo {
            name,
            data_type: ty,
            is_nullable: is_nullable == "YES",
        });
    }
    Ok(out)
}

async fn fetch_columns_mysql(pool: &MySqlPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select COLUMN_NAME, DATA_TYPE, IS_NULLABLE from information_schema.columns where table_schema = database() and table_name = ? order by ORDINAL_POSITION")
        .bind(table)
        .fetch_all(pool)
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let name: String = r.try_get("COLUMN_NAME")?;
        let ty: String = r.try_get("DATA_TYPE")?;
        let is_nullable: String = r.try_get("IS_NULLABLE")?;
        out.push(ColInfo {
            name,
            data_type: ty,
            is_nullable: is_nullable == "YES",
        });
    }
    Ok(out)
}

fn guess_type(sql_type: &str) -> &'static str {
    let t = sql_type.to_ascii_lowercase();
    if t.contains("int") {
        "int"
    } else if t.contains("numeric")
        || t.contains("decimal")
        || t.contains("double")
        || t.contains("real")
        || t.contains("float")
    {
        "float"
    } else if t.contains("bool") {
        "bool"
    } else if t.contains("timestamp") || t == "datetime" || t == "date" || t == "time" {
        "timestamp"
    } else {
        "text"
    }
}

/// 收集所有 JobConfig：从 --config 和 --jobs-dir
fn collect_job_configs(
    config_paths: Option<Vec<PathBuf>>,
    jobs_dir: Option<PathBuf>,
) -> Result<Vec<(String, JobConfig)>> {
    let mut configs = Vec::new();

    // --config 指定的文件
    if let Some(paths) = config_paths {
        for path in paths {
            match load_job_config(&path) {
                Ok((id, cfg)) => configs.push((id, cfg)),
                Err(e) => eprintln!("Failed to load {}: {}", path.display(), e),
            }
        }
    }

    // --jobs-dir 指定的目录
    if let Some(dir) = jobs_dir {
        let entries = std::fs::read_dir(&dir)
            .with_context(|| format!("读取任务目录失败: {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("读取任务目录项失败: {}", dir.display()))?;
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                match load_job_config(&path) {
                    Ok((id, cfg)) => configs.push((id, cfg)),
                    Err(e) => eprintln!("Failed to load {}: {}", path.display(), e),
                }
            }
        }
    }

    Ok(configs)
}

fn load_job_config(path: &PathBuf) -> Result<(String, JobConfig)> {
    let cfg = read_job_config(path)?;
    validate_job_identifiers(&cfg)?;

    let job_id = cfg
        .job_id
        .clone()
        .or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("job_id is required (in config or from filename)"))?;

    Ok((job_id, cfg))
}
