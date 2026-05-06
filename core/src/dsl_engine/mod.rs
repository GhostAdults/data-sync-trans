// 新的模块化架构
pub mod ast; // AST 节点定义
pub mod evaluator;
pub mod functions; // 函数实现
pub mod parser; // 解析器
pub mod registry; // 函数注册表 // 求值器

// 重新导出主要类型
pub use evaluator::SyncEngine;

#[cfg(test)]
mod tests {
    use super::evaluator::*;
    use serde_json::json;

    #[test]
    fn test_dsl_engine() {
        println!("=== DSL Engine Test ===\n");

        let user_config = vec![
            ("user_name".to_string(), "upper(source.name)".to_string()),
            (
                "full_info".to_string(),
                "concat(source.region, source.name)".to_string(),
            ),
            (
                "status".to_string(),
                "coalesce(source.state, 'UNKNOWN')".to_string(),
            ),
            ("constant_tag".to_string(), "'IMPORTED_DATA'".to_string()),
        ];

        println!("--- 1. 初始化引擎 ---");
        let engine = SyncEngine::new(user_config);

        let api_data_batch = vec![
            json!({ "name": "alice", "region": "CN_", "state": "active" }),
            json!({ "name": "bob", "region": "US_" }),
        ];

        println!("--- 2. 开始同步 ---");
        for (i, row) in api_data_batch.iter().enumerate() {
            let target_data = engine.process_row(row);
            println!("Row {}: {:?}", i, target_data);
        }

        println!("\n=== 测试完成 ===");
    }

    #[test]
    fn test_if_function() {
        println!("=== If Function Test ===\n");

        let user_config = vec![
            (
                "is_active".to_string(),
                "if(source.flag == '1', 1, 0)".to_string(),
            ),
            (
                "status_text".to_string(),
                "if(source.status == 'active', 'ACTIVE', 'INACTIVE')".to_string(),
            ),
            (
                "priority".to_string(),
                "if(source.level > 5, 'HIGH', 'LOW')".to_string(),
            ),
        ];

        println!("--- 1. 初始化引擎 ---");
        let engine = SyncEngine::new(user_config);

        let api_data_batch = vec![
            json!({ "flag": "1", "status": "active", "level": 8 }),
            json!({ "flag": "0", "status": "inactive", "level": 3 }),
            json!({ "flag": "1", "status": "pending", "level": 6 }),
        ];

        println!("--- 2. 测试 if 函数 ---");
        for (i, row) in api_data_batch.iter().enumerate() {
            let target_data = engine.process_row(row);
            println!("Row {}: {:?}", i, target_data);
        }

        println!("\n=== 测试完成 ===");
    }

    #[test]
    fn test_nested_if_else() {
        println!("=== Nested If-Else Test ===\n");

        let user_config = vec![
            // 嵌套 if 实现 if-else 链
            ("grade".to_string(), "if(source.score >= 90, 'A', if(source.score >= 80, 'B', if(source.score >= 60, 'C', 'F')))".to_string()),
        ];

        println!("--- 1. 初始化引擎 ---");
        let engine = SyncEngine::new(user_config);

        let api_data_batch = vec![
            json!({ "score": 95 }),
            json!({ "score": 85 }),
            json!({ "score": 70 }),
            json!({ "score": 50 }),
        ];

        println!("--- 2. 测试嵌套 if-else ---");
        for (i, row) in api_data_batch.iter().enumerate() {
            let target_data = engine.process_row(row);
            println!("Row {}: {:?}", i, target_data);
        }

        println!("\n=== 测试完成 ===");
    }

    #[test]
    fn test_list_with_transformation() {
        println!("=== List Data with Transformation Test ===\n");

        // 配置字段映射，并添加一些转换逻辑
        let user_config = vec![
            ("n".to_string(), "upper(source.name)".to_string()), // 名字转大写
            ("a".to_string(), "source.age".to_string()),         // 年龄保持不变
            ("s".to_string(), "source.sex".to_string()),         // 性别保持不变
            (
                "full_info".to_string(),
                "concat(source.name, source.sex)".to_string(),
            ), // 拼接信息
            (
                "is_adult".to_string(),
                "if(source.age >= 18, 1, 0)".to_string(),
            ), // 判断是否成年
        ];
        println!("--- 1. 初始化引擎 ---");
        let engine = SyncEngine::new(user_config);
        // 模拟从 API 获取的 3 条数据
        let api_data_list = vec![
            json!({ "name": "Alice", "age": 25, "sex": "F" }),
            json!({ "name": "Bob", "age": 17, "sex": "M" }),
            json!({ "name": "Charlie", "age": 28, "sex": "M" }),
        ];

        println!("--- 2. 处理并转换 list 数据 ---");
        let mut results = Vec::new();
        for (i, row) in api_data_list.iter().enumerate() {
            let processed_data = engine.process_row(row);
            println!("Row {}: {:?}", i, processed_data);
            results.push(processed_data);
        }

        println!("\n--- 3. 验证转换结果 ---");
        // 验证第一条数据的转换
        assert_eq!(results[0].get("n").and_then(|v| v.as_str()), Some("ALICE"));
        assert_eq!(results[0].get("a").and_then(|v| v.as_i64()), Some(25));
        assert_eq!(results[0].get("s").and_then(|v| v.as_str()), Some("F"));
        assert_eq!(
            results[0].get("full_info").and_then(|v| v.as_str()),
            Some("AliceF")
        );
        assert_eq!(results[0].get("is_adult").and_then(|v| v.as_i64()), Some(1));

        // 验证第二条数据的转换（未成年）
        assert_eq!(results[1].get("n").and_then(|v| v.as_str()), Some("BOB"));
        assert_eq!(results[1].get("is_adult").and_then(|v| v.as_i64()), Some(0));

        // 验证第三条数据的转换
        assert_eq!(
            results[2].get("n").and_then(|v| v.as_str()),
            Some("CHARLIE")
        );
        assert_eq!(
            results[2].get("full_info").and_then(|v| v.as_str()),
            Some("CharlieM")
        );

        println!("✅ 所有转换验证通过！");
        println!("\n=== 测试完成 ===");
    }
}
