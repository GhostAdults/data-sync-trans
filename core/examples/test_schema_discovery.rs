//! Schema 自动发现与演进检测测试
//!
//! 演示如何从真实数据库发现表结构并检测 Schema 演进
//! Schema 缓存持久化到磁盘

use anyhow::Result;
use relus_common::JobConfig;
use relus_connector_rdbms::pool::RdbmsPool;
use relus_connector_rdbms::schema::{
    CompatibilityResult, EvolutionChecker, MetadataDiscoverer, RdbmsDiscoverer, SchemaCache,
    SchemaChange, TableSchema,
};
use relus_connector_rdbms::util::get_pool_from_config;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Schema 缓存默认目录
const SCHEMA_CACHE_DIR: &str = "./schema_cache";

/// 从配置创建数据库连接池
async fn create_pool_from_config(config: &JobConfig) -> Result<Arc<RdbmsPool>> {
    let pool = get_pool_from_config(config).await?;
    Ok(pool)
}

/// 打印表结构信息
fn print_schema(schema: &TableSchema, show_columns: bool) {
    println!("\n表名: {} ({})", schema.table_name, schema.db_kind);
    println!("版本: {}, 列数: {}", schema.version, schema.columns.len());
    println!("主键: {:?}", schema.primary_keys);

    if show_columns {
        println!("\n列详情:");
        println!("{:-<70}", "");
        for col in &schema.columns {
            let pk = if col.is_primary_key { " [PK]" } else { "" };
            let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
            println!(
                "  {:<20} {:<12} {}{}",
                col.name, col.logical_type, nullable, pk
            );
        }
        println!("{:-<70}", "");
    }
}

/// 检测并打印 Schema 变更
fn detect_and_print_changes(cache: &SchemaCache, new_schema: &TableSchema) -> Result<bool> {
    let changes = cache.detect_changes(new_schema)?;

    if changes.is_empty() {
        println!("\n✅ 未检测到 Schema 变更");
        return Ok(false);
    }

    println!("\n检测到 {} 个变更:", changes.len());
    for change in &changes {
        match change {
            SchemaChange::ColumnAdded(col) => {
                let nullable = if col.nullable { "可空" } else { "非空" };
                println!(
                    "  📥 新增列: {} ({}, {})",
                    col.name, col.logical_type, nullable
                );
            }
            SchemaChange::ColumnRemoved(name) => {
                println!("  📤 删除列: {}", name);
            }
            SchemaChange::ColumnTypeChanged {
                column_name,
                old_type,
                new_type,
            } => {
                println!(
                    "  🔄 类型变更: {} ({} -> {})",
                    column_name, old_type, new_type
                );
            }
            SchemaChange::NullabilityChanged {
                column_name,
                old_nullable,
                new_nullable,
            } => {
                println!(
                    "  ⚠️  可空性变更: {} ({} -> {})",
                    column_name,
                    if *old_nullable { "可空" } else { "非空" },
                    if *new_nullable { "可空" } else { "非空" }
                );
            }
            SchemaChange::NoChange => {}
        }
    }

    // 使用演进检查器判断兼容性
    let old_schema = cache.get_latest(&new_schema.table_name)?;
    let checker = EvolutionChecker::new(true);
    let result = checker.check(old_schema.as_ref(), new_schema);

    println!("\n兼容性检查:");
    match result {
        CompatibilityResult::Compatible => {
            println!("  ✅ 完全兼容");
        }
        CompatibilityResult::ForwardCompatible { changes } => {
            println!("  ⚠️  向前兼容 ({} 个变更)", changes.len());
        }
        CompatibilityResult::BackwardCompatible { changes } => {
            println!("  ⚠️  向后兼容 ({} 个变更)", changes.len());
        }
        CompatibilityResult::Breaking { reason, .. } => {
            println!("  ❌ 破坏性变更: {}", reason);
        }
    }

    Ok(!changes.is_empty())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("========================================");
    println!("Schema 自动发现与演进检测测试");
    println!("========================================");

    // 读取配置文件
    let config_content = include_str!("E:\\github\\Relus\\cli\\user_config\\default_job.json");
    let configs: serde_json::Value = serde_json::from_str(config_content)?;

    // 获取数据库配置
    let input_config = &configs["tasks"][0]["input"]["config"]["connections"][0];
    let db_type = input_config
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("mysql");
    let host = input_config
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
    let port = input_config
        .get("port")
        .and_then(|v| v.as_u64())
        .unwrap_or(3306);
    let database = input_config
        .get("database")
        .and_then(|v| v.as_str())
        .unwrap_or("my_db");
    let username = input_config
        .get("username")
        .and_then(|v| v.as_str())
        .unwrap_or("root");
    let password = input_config
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let table_name = input_config["table"].as_str().unwrap();

    println!(
        "数据库: {}://{}@{}:{}/{}",
        db_type, username, host, port, database
    );
    println!("目标表: {}", table_name);
    println!("缓存目录: {}", SCHEMA_CACHE_DIR);

    // 创建 JobConfig
    let job_config = JobConfig {
        source: relus_common::data_source_config::DataSourceConfig {
            name: "mysql_source".to_string(),
            source_type: "database".to_string(),
            is_table_mode: true,
            query_sql: None,
            writer_mode: None,
            config: json!({
                "connection": {
                    "type": db_type,
                    "host": host,
                    "port": port,
                    "database": database,
                    "username": username,
                    "password": password,
                    "table": table_name,
                }
            }),
        },
        target: relus_common::data_source_config::DataSourceConfig {
            name: "dummy_target".to_string(),
            source_type: "database".to_string(),
            is_table_mode: true,
            query_sql: None,
            writer_mode: None,
            config: json!({}),
        },
        column_mapping: BTreeMap::new(),
        column_types: None,
        sync_mode: None,
        batch_size: Some(100),
        channel_buffer_size: None,
        job_id: None,
        schedule: None,
    };

    // 创建连接池
    let pool = match create_pool_from_config(&job_config).await {
        Ok(p) => p,
        Err(e) => {
            println!("\n❌ 无法连接数据库: {}", e);
            println!("请确保数据库服务正在运行。");
            return Err(e);
        }
    };

    // 创建持久化缓存
    println!("\n初始化 Schema 缓存...");
    let cache = SchemaCache::new(SCHEMA_CACHE_DIR)?;

    // 检查是否有缓存
    let has_cache = cache.has_cache(table_name)?;
    println!("已有缓存: {}", has_cache);

    // 创建 Schema 发现器
    let discoverer = RdbmsDiscoverer::new(pool.clone(), table_name.to_string());

    // 检查表是否存在
    let exists = discoverer.table_exists(table_name).await?;
    if !exists {
        println!("\n❌ 表 '{}' 不存在", table_name);
        return Ok(());
    }

    // 发现当前表结构
    println!("\n从数据库发现表结构...");
    let current_schema = discoverer.discover().await?;

    // 打印当前 Schema
    print_schema(&current_schema, true);

    // 检测变更
    println!("\n========================================");
    println!("Schema 演进检测");
    println!("========================================");

    let has_changes = detect_and_print_changes(&cache, &current_schema)?;

    // 注册新版本
    let version = cache.register(&current_schema)?;
    println!("\nSchema 已注册，版本: {}", version);

    // 打印生成的配置
    println!("\n========================================");
    println!("自动生成的配置");
    println!("========================================");

    let mapping = current_schema.to_column_mapping();
    println!("\ncolumn_mapping ({} 个字段):", mapping.len());
    println!("{}", serde_json::to_string_pretty(&mapping)?);

    let types = current_schema.to_column_types();
    println!("\ncolumn_types ({} 个字段):", types.len());
    println!("{}", serde_json::to_string_pretty(&types)?);

    // 列出所有表
    println!("\n========================================");
    println!("数据库所有表");
    println!("========================================");

    let tables = discoverer.list_tables().await?;
    println!("共 {} 个表", tables.len());
    for (i, table) in tables.iter().enumerate() {
        if i < 10 {
            println!("  - {}", table);
        }
    }
    if tables.len() > 10 {
        println!("  ... 还有 {} 个表", tables.len() - 10);
    }

    // 总结
    println!("\n========================================");
    println!("测试完成!");
    println!("========================================");
    println!("缓存位置: {}", SCHEMA_CACHE_DIR);
    println!("表结构版本: {}", version);

    if has_changes {
        println!("状态: ⚠️  检测到 Schema 变更，请检查兼容性");
    } else {
        println!("状态: ✅ Schema 无变更");
    }

    println!("\n提示:");
    println!("  - 修改数据库表结构后再次运行此测试，可检测演进变更");
    println!("  - 缓存已持久化到磁盘，重启后仍可对比历史版本");

    Ok(())
}
