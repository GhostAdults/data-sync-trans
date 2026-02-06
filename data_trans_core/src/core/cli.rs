// cli 命令行参数解析 
// 包含所有子命令的枚举类型 命令行同步配置

use clap::{Parser, Subcommand};
use crate::core::{Config, ApiConfig};
use crate::{run_serve,detect_db_kind};
use crate::core::server::*;
use crate::core::db::DbKind;
use tokio::runtime::Runtime;
use std::path::PathBuf;
use std::fs;
use std::collections::BTreeMap;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{MySqlPool, PgPool, Row};
use regex::Regex;
use anyhow::{bail, Context, Result};
use std::process::Command;

#[derive(Parser)]
#[command(name = "data_trans")]
#[command(version)]
#[command(about = "从接口获取 JSON 数据并同步到 MySQL/Postgres")]
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
}

pub fn run_cli(cmd: Commands) {
     match cmd {
        Commands::TestApi { config } => {
            let cfg = read_config(config).unwrap();
            let v = fetch_json(&cfg.api).unwrap();
            println!("{}", serde_json::to_string_pretty(&v).unwrap());
            let items = if let Some(p) = &cfg.api.items_json_path {
                extract_by_path(&v, p).unwrap_or(&Value::Null).clone()
            } else {
                v.clone()
            };
            println!("{}", serde_json::to_string_pretty(&items).unwrap());
        }
        Commands::Sync { config } => {
            match read_config(config) {
                Ok(cfg) => {
                    let rt = Runtime::new().unwrap();
                    rt.block_on(async {
                        match get_pool_from_config(&cfg).await {
                            Ok(pool) => {
                                if let Err(e) = sync(&cfg, &pool).await {
                                    eprintln!("{}", e);
                                }
                            }
                            Err(e) => eprintln!("{}", e),
                        }
                    });
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }
        Commands::ListTables { db_url, db_type } => {
            let kind = detect_db_kind(&db_url, db_type).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                match kind {
                    DbKind::Postgres => {
                        let pool = PgPoolOptions::new()
                            .max_connections(5)
                            .connect(&db_url)
                            .await
                            .unwrap();
                        let tabs = list_tables_postgres(&pool).await.unwrap();
                        for t in tabs {
                            println!("{}", t);
                        }
                    }
                    DbKind::Mysql => {
                        let pool = MySqlPoolOptions::new()
                            .max_connections(5)
                            .connect(&db_url)
                            .await
                            .unwrap();
                        let tabs = list_tables_mysql(&pool).await.unwrap();
                        for t in tabs {
                            println!("{}", t);
                        }
                    }
                }
            });
        }
        Commands::DescribeTable {
            db_url,
            db_type,
            table,
        } => {
            let kind = detect_db_kind(&db_url, db_type).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                match kind {
                    DbKind::Postgres => {
                        let pool =
                            PgPoolOptions::new().max_connections(5).connect(&db_url).await.unwrap();
                        let cols = fetch_columns_postgres(&pool, &table).await.unwrap();
                        let v = cols
                            .into_iter()
                            .map(|c| {
                                serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})
                            })
                            .collect::<Vec<_>>();
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!(v)).unwrap()
                        );
                    }
                    DbKind::Mysql => {
                        let pool =
                            MySqlPoolOptions::new().max_connections(5).connect(&db_url).await.unwrap();
                        let cols = fetch_columns_mysql(&pool, &table).await.unwrap();
                        let v = cols
                            .into_iter()
                            .map(|c| {
                                serde_json::json!({"name": c.name, "data_type": c.data_type, "nullable": c.is_nullable})
                            })
                            .collect::<Vec<_>>();
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!(v)).unwrap()
                        );
                    }
                }
            });
        }
        Commands::GenMapping {
            db_url,
            db_type,
            table,
            output,
        } => {
            let kind = detect_db_kind(&db_url, db_type).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let (cols, key_cols) = match kind {
                    DbKind::Postgres => {
                        let pool = PgPoolOptions::new()
                            .max_connections(5)
                            .connect(&db_url)
                            .await
                            .unwrap();
                        let cols = fetch_columns_postgres(&pool, &table).await.unwrap();
                        (cols, Vec::<String>::new())
                    }
                    DbKind::Mysql => {
                        let pool = MySqlPoolOptions::new()
                            .max_connections(5)
                            .connect(&db_url)
                            .await
                            .unwrap();
                        let cols = fetch_columns_mysql(&pool, &table).await.unwrap();
                        (cols, Vec::<String>::new())
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
                    "key_columns": key_cols,
                    "column_mapping": mapping,
                    "column_types": types
                });
                fs::write(&output, serde_json::to_string_pretty(&obj).unwrap()).unwrap();
                println!("written {}", output.display());
            });
        }
        Commands::SyncWithMapping { config } => {
            match read_config(config) {
                Ok(cfg) => {
                    let rt = Runtime::new().unwrap();
                    rt.block_on(async {
                        match get_pool_from_config(&cfg).await {
                            Ok(pool) => {
                                if let Err(e) = sync(&cfg, &pool).await {
                                    eprintln!("{}", e);
                                }
                            }
                            Err(e) => eprintln!("{}", e),
                        }
                    });
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }
        Commands::Serve { host, port } => {
            if let Err(e) = run_serve(host, port) {
                eprintln!("{}", e);
            }
        }
    }
}

fn sanitize_identifier(s: &str) -> Result<()> {
    let re = Regex::new(r"^[A-Za-z0-9_]+$").unwrap();
    if !re.is_match(s) {
        bail!("标识符仅允许字母、数字和下划线: {}", s);
    }
    Ok(())
}

fn read_config(path: PathBuf) -> Result<Config> {
    let data =
        fs::read_to_string(&path).with_context(|| format!("读取配置文件失败: {}", path.display()))?;
    let cfg: Config = serde_json::from_str(&data)
        .or_else(|_| {
            let v: Value = serde_json::from_str(&data)?;
            serde_json::from_value(v)
        })
        .unwrap_or_else(|_| {
            Config {
                id: "default".to_string(),
                db_type: None,
                db: Default::default(),
                api: crate::core::ApiConfig {
                    url: "".to_string(),
                    method: None,
                    headers: None,
                    body: None,
                    items_json_path: None,
                    timeout_secs: None,
                },
                column_mapping: std::collections::BTreeMap::new(),
                column_types: None,
                mode: None,
                batch_size: None,
            }
        });
    sanitize_identifier(&cfg.db.table)?;
    for k in cfg.column_mapping.keys() {
        sanitize_identifier(k)?;
    }
    if let Some(keys) = &cfg.db.key_columns {
        for k in keys {
            sanitize_identifier(k)?;
        }
    }
    Ok(cfg)
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