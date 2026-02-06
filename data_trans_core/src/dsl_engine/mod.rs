// 新的模块化架构
pub mod ast;           // AST 节点定义
pub mod parser;        // 解析器
pub mod registry;      // 函数注册表
pub mod functions;     // 函数实现
pub mod evaluator;     // 求值器

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
            ("full_info".to_string(), "concat(source.region, source.name)".to_string()),
            ("status".to_string(), "coalesce(source.state, 'UNKNOWN')".to_string()),
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
            ("is_active".to_string(), "if(source.flag == '1', 1, 0)".to_string()),
            ("status_text".to_string(), "if(source.status == 'active', 'ACTIVE', 'INACTIVE')".to_string()),
            ("priority".to_string(), "if(source.level > 5, 'HIGH', 'LOW')".to_string()),
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
}