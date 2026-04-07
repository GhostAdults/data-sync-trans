//! 同步示例 - 包含类型转换测试
//!
//! 测试多种类型的转换：
//! - int: 整数类型
//! - float: 浮点数类型
//! - bool: 布尔类型
//! - timestamp: 时间戳类型
//! - decimal: 高精度小数
//! - json: JSON 类型
//! - text: 文本类型

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::RecordBuilder;
use data_trans_common::types::{SourceType, TypeConverterRegistry};
use data_trans_core::core::serve::sync;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

/// 测试类型转换功能
fn test_type_conversion() {
    println!("\n========== 类型转换测试 ==========\n");

    let registry = Arc::new(TypeConverterRegistry::new());

    // 测试数据: (原始值, 类型提示, 描述)
    let test_cases = vec![
        (json!(42), Some("int"), "整数转换"),
        (json!("123"), Some("int"), "字符串转整数"),
        (json!(null), Some("int"), "null 转整数"),
        (json!(3.14159), Some("float"), "浮点数转换"),
        (json!("2.718"), Some("float"), "字符串转浮点数"),
        (json!(true), Some("bool"), "布尔值转换"),
        (json!("yes"), Some("bool"), "字符串 'yes' 转布尔"),
        (json!("false"), Some("bool"), "字符串 'false' 转布尔"),
        (
            json!("2024-01-15 10:30:00"),
            Some("timestamp"),
            "时间戳转换",
        ),
        (
            json!("2024-01-15T10:30:00Z"),
            Some("timestamp"),
            "ISO8601 时间戳",
        ),
        (json!(1234567.89), Some("decimal"), "高精度小数"),
        (
            json!({"key": "value", "num": 123}),
            Some("json"),
            "JSON 对象",
        ),
        (json!([1, 2, 3]), Some("json"), "JSON 数组"),
        (json!("hello world"), Some("text"), "文本类型"),
        (json!(123), Some("text"), "数字转文本"),
    ];

    for (value, type_hint, desc) in test_cases {
        let result = registry.convert(&value, type_hint);
        match result {
            Ok(converted) => {
                println!(
                    "✓ {}:\n  输入: {} (类型提示: {})\n  输出: {:?}\n  类型: {}\n",
                    desc,
                    value,
                    type_hint.unwrap_or("none"),
                    converted,
                    converted.type_name()
                );
            }
            Err(e) => {
                println!("✗ {}:\n  输入: {}\n  错误: {}\n", desc, value, e);
            }
        }
    }
}

/// 测试 RecordBuilder 构建记录
fn test_record_builder() {
    println!("\n========== RecordBuilder 测试 ==========\n");

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("name".to_string(), "user.name".to_string());
    column_mapping.insert("age".to_string(), "user.age".to_string());
    column_mapping.insert("score".to_string(), "user.score".to_string());
    column_mapping.insert("active".to_string(), "user.active".to_string());
    column_mapping.insert("created_at".to_string(), "user.created_at".to_string());
    column_mapping.insert("metadata".to_string(), "user.metadata".to_string());

    let mut column_types = BTreeMap::new();
    column_types.insert("id".to_string(), "int".to_string());
    column_types.insert("name".to_string(), "text".to_string());
    column_types.insert("age".to_string(), "int".to_string());
    column_types.insert("score".to_string(), "float".to_string());
    column_types.insert("active".to_string(), "bool".to_string());
    column_types.insert("created_at".to_string(), "timestamp".to_string());
    column_types.insert("metadata".to_string(), "json".to_string());

    let builder = RecordBuilder::new(column_mapping, Some(column_types))
        .with_source_type(SourceType::MySQL)
        .with_table_name("users");

    let test_data = vec![
        json!({
            "id": 1,
            "user": {
                "name": "Alice",
                "age": 30,
                "score": 95.5,
                "active": true,
                "created_at": "2024-01-15 10:30:00",
                "metadata": {"role": "admin", "level": 5}
            }
        }),
        json!({
            "id": 2,
            "user": {
                "name": "Bob",
                "age": 25,
                "score": 87.3,
                "active": false,
                "created_at": "2024-01-16 14:20:00",
                "metadata": {"role": "user", "level": 2}
            }
        }),
        json!({
            "id": 3,
            "user": {
                "name": "Charlie",
                "age": null,
                "score": "92.1",
                "active": "yes",
                "created_at": "2024-01-17T09:00:00Z",
                "metadata": null
            }
        }),
    ];

    println!("构建单条记录:");
    match builder.build(&test_data[0]) {
        Ok(record) => {
            println!("  表名: {:?}", record.table_name());
            println!("  字段数: {}", record.len());
            println!("  字段值:");
            for (name, field) in &record.fields {
                println!(
                    "    {}: {:?} (类型: {})",
                    name,
                    field.value,
                    field.value.type_name()
                );
            }
        }
        Err(e) => println!("  错误: {}", e),
    }

    println!("\n批量构建记录:");
    match builder.build_batch(&test_data) {
        Ok(records) => {
            println!("  成功构建 {} 条记录", records.len());
            for (i, record) in records.iter().enumerate() {
                println!(
                    "  记录 {}: id={:?}, name={:?}",
                    i + 1,
                    record.fields.get("id").map(|f| &f.value),
                    record.fields.get("name").map(|f| &f.value)
                );
            }
        }
        Err(e) => println!("  错误: {}", e),
    }
}

#[tokio::main]
async fn main() {
    // 先运行类型转换测试
    test_type_conversion();

    // 运行 RecordBuilder 测试
    test_record_builder();

    println!("\n========== 数据库同步测试 ==========\n");

    let input = DataSourceConfig {
        name: "mysql_source".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: Some(vec![
            "SELECT id, type, site_number FROM zone_data".to_string()
        ]),
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data",
            "key_columns": ["id"]
        }),
    };

    let output = DataSourceConfig {
        name: "mysql_target".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data_r",
            "key_columns": ["id"]
        }),
    };

    // 扩展的列映射配置
    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("type".to_string(), "type".to_string());
    column_mapping.insert("site_number".to_string(), "site_number".to_string());
    // column_mapping.insert("isActive".to_string(), "isActive".to_string());
    // column_mapping.insert("score".to_string(), "score".to_string());
    // column_mapping.insert("createdAt".to_string(), "createdAt".to_string());

    // 扩展的类型配置 - 测试多种类型转换
    let mut column_types = BTreeMap::new();
    column_types.insert("id".to_string(), "int".to_string());
    column_types.insert("type".to_string(), "text".to_string());
    column_types.insert("site_number".to_string(), "text".to_string());
    // column_types.insert("isActive".to_string(), "bool".to_string());
    // column_types.insert("score".to_string(), "float".to_string());
    // column_types.insert("createdAt".to_string(), "timestamp".to_string());

    let config = JobConfig {
        input,
        output,
        column_mapping,
        column_types: Some(column_types),
        mode: Some("insert".to_string()),
        batch_size: Some(100),
        reader_threads: 1,
        writer_threads: 1,
        channel_buffer_size: 1000,
        use_transaction: true,
    };

    println!("配置信息:");
    println!("  同步模式: {:?}", config.mode);
    println!("  批次大小: {:?}", config.batch_size);
    println!("  列类型配置:");
    if let Some(ref types) = config.column_types {
        for (col, t) in types {
            println!("    {}: {}", col, t);
        }
    }

    println!("\n开始同步...");
    match sync(config).await {
        Ok(result) => {
            println!("同步成功!");
            println!("  状态: {:?}", result.status);
            println!("  读取行数: {}", result.stats.records_read);
            println!("  写入行数: {}", result.stats.records_written);
            println!("  失败行数: {}", result.stats.records_failed);
            println!("  耗时: {:.2}s", result.stats.elapsed_secs);
            println!("  吞吐量: {:.2} rows/s", result.stats.throughput);
            if let Some(err) = result.error {
                println!("  错误: {}", err);
            }
        }
        Err(e) => println!("同步失败: {}", e),
    }

    println!("\n========== 测试完成 ==========\n");
}
