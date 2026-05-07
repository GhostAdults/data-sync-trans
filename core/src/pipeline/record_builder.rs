//! Record 构建器
//!
//! 统一数据转换入口，支持从 JSON 构建 Record (MappingRow)
//! 支持两种映射模式：
//! - 纯路径映射：`"user.name"` → 直接提取 JSON 字段
//! - DSL 表达式：`"upper(source.name)"` → 通过 SyncEngine 执行变换

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::dsl_engine::SyncEngine;
use relus_common::types::{
    MappingRow, MappingSchema, OriginalTypeInfo, SourceType, TypeConverterRegistry,
};

use super::PipelineMessage;

fn extract_by_path<'a>(item: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = item;

    for part in parts {
        if part.is_empty() {
            continue;
        }
        current = current.get(part)?;
    }

    Some(current)
}

/// 映射策略：纯路径 ：：DSL 表达式
enum MappingStrategy {
    Path,
    Dsl,
}

/// 判断 mapping 值的解析策略
///
/// DSL 表达式必须以显式前缀引导：
/// - `= ` 或 `expr:` → DSL 表达式，前缀后的内容交给 SyncEngine
/// - 其他 → 纯 JSON 路径，走 extract_by_path
///
/// 示例：
/// - `"name"` → 纯路径
/// - `"user.name"` → 纯路径（支持点号嵌套）
/// - `"= upper(source.name)"` → DSL
/// - `"expr: if(source.age >= 18, 1, 0)"` → DSL
fn classify_mapping(value: &str) -> MappingStrategy {
    let trimmed = value.trim_start();
    if trimmed.starts_with('=') || trimmed.starts_with("expr:") {
        MappingStrategy::Dsl
    } else {
        MappingStrategy::Path
    }
}

/// 从 mapping 值中提取 DSL 表达式（去掉前缀）
fn extract_dsl_rule(value: &str) -> &str {
    let trimmed = value.trim_start();
    if let Some(rest) = trimmed.strip_prefix("expr:") {
        rest.trim_start()
    } else if let Some(rest) = trimmed.strip_prefix('=') {
        rest.trim_start()
    } else {
        trimmed
    }
}

/// Record 构建器 - 统一数据转换入口
///
/// 直接构建 MappingRow (Record)，包含完整的类型元信息
pub struct RecordBuilder {
    column_mapping: BTreeMap<String, String>,
    column_types: Option<BTreeMap<String, String>>,
    registry: Arc<TypeConverterRegistry>,
    source_type: SourceType,
    table_name: Option<String>,
    transform_engine: Option<SyncEngine>,
}

impl RecordBuilder {
    pub fn new(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
    ) -> Result<Self> {
        let transform_engine = Self::build_engine_if_needed(&column_mapping)?;

        Ok(Self {
            column_mapping,
            column_types,
            registry: Arc::new(TypeConverterRegistry::new()),
            source_type: SourceType::Other("unknown".to_string()),
            table_name: None,
            transform_engine,
        })
    }

    pub fn with_registry(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
        registry: Arc<TypeConverterRegistry>,
    ) -> Result<Self> {
        let transform_engine = Self::build_engine_if_needed(&column_mapping)?;

        Ok(Self {
            column_mapping,
            column_types,
            registry,
            source_type: SourceType::Other("unknown".to_string()),
            table_name: None,
            transform_engine,
        })
    }

    fn build_engine_if_needed(
        column_mapping: &BTreeMap<String, String>,
    ) -> Result<Option<SyncEngine>> {
        let has_dsl = column_mapping
            .values()
            .any(|v| matches!(classify_mapping(v), MappingStrategy::Dsl));
        if !has_dsl {
            return Ok(None);
        }

        let rules: Vec<(String, String)> = column_mapping
            .iter()
            .map(|(target, source)| match classify_mapping(source) {
                MappingStrategy::Dsl => (target.clone(), extract_dsl_rule(source).to_string()),
                MappingStrategy::Path => (target.clone(), format!("source.{}", source)),
            })
            .collect();

        Ok(Some(SyncEngine::new(rules)?))
    }

    pub fn with_source_type(mut self, source_type: SourceType) -> Self {
        self.source_type = source_type;
        self
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    fn get_type_hint(&self, column: &str) -> Option<&str> {
        self.column_types
            .as_ref()
            .and_then(|m| m.get(column))
            .map(|s| s.as_str())
    }

    fn build_schema(&self) -> MappingSchema {
        let mut schema = MappingSchema::new();

        if let Some(ref table) = self.table_name {
            schema = schema.with_table_name(table);
        }

        for target_col in self.column_mapping.keys() {
            let type_hint = self.get_type_hint(target_col).unwrap_or("text");
            let type_info = OriginalTypeInfo::new(self.source_type.clone(), type_hint.to_string());
            schema = schema.add_field(target_col, type_info);
        }

        schema
    }

    /// 构建单个 Record (MappingRow)
    pub fn build(&self, item: &JsonValue) -> Result<MappingRow> {
        let schema = self.build_schema();
        let mut row = MappingRow::new(schema);

        if let Some(ref table) = self.table_name {
            row = row.with_source_table(table.clone());
        }

        row.source = item.clone();

        if let Some(ref engine) = self.transform_engine {
            self.build_with_transform(engine, item, &mut row)?;
        } else {
            self.build_with_path(item, &mut row)?;
        }

        Ok(row)
    }

    /// DSL transform 路径：通过 SyncEngine 处理
    fn build_with_transform(
        &self,
        engine: &SyncEngine,
        item: &JsonValue,
        row: &mut MappingRow,
    ) -> Result<()> {
        let transformed = engine.process_row(item)?;

        for target_col in self.column_mapping.keys() {
            let source_val = transformed
                .get(target_col)
                .cloned()
                .unwrap_or(JsonValue::Null);
            let type_hint = self.get_type_hint(target_col);
            let typed_val = self.registry.convert(&source_val, type_hint)?;

            let type_info = OriginalTypeInfo::new(
                self.source_type.clone(),
                type_hint.unwrap_or("text").to_string(),
            );

            row.insert_value(target_col, typed_val, type_info);
        }

        Ok(())
    }

    /// 纯路径映射：通过 extract_by_path 提取
    fn build_with_path(&self, item: &JsonValue, row: &mut MappingRow) -> Result<()> {
        for (target_col, source_path) in &self.column_mapping {
            let source_val = extract_by_path(item, source_path).unwrap_or(&JsonValue::Null);
            let type_hint = self.get_type_hint(target_col);
            let typed_val = self.registry.convert(source_val, type_hint)?;

            let type_info = OriginalTypeInfo::new(
                self.source_type.clone(),
                type_hint.unwrap_or("text").to_string(),
            );

            row.insert_value(target_col, typed_val, type_info);
        }

        Ok(())
    }

    /// 批量构建 Record
    pub fn build_batch(&self, items: &[JsonValue]) -> Result<Vec<MappingRow>> {
        items.iter().map(|item| self.build(item)).collect()
    }

    /// 构建 PipelineMessage
    pub fn build_message(&self, items: &[JsonValue]) -> Result<PipelineMessage> {
        let records = self.build_batch(items)?;
        Ok(PipelineMessage::DataBatch(records))
    }

    /// 获取列映射
    pub fn column_mapping(&self) -> &BTreeMap<String, String> {
        &self.column_mapping
    }

    /// 获取列类型
    pub fn column_types(&self) -> Option<&BTreeMap<String, String>> {
        self.column_types.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn test_build_simple_record() {
        let mut mapping = BTreeMap::new();
        mapping.insert("name".to_string(), "user.name".to_string());
        mapping.insert("age".to_string(), "user.age".to_string());

        let mut types = BTreeMap::new();
        types.insert("age".to_string(), "int".to_string());

        let builder = RecordBuilder::new(mapping, Some(types))
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e))
            .with_source_type(SourceType::MySQL)
            .with_table_name("users");

        let item = json!({
            "user": {
                "name": "Alice",
                "age": 30
            }
        });

        let record = builder.build(&item).unwrap();
        assert_eq!(record.len(), 2);
        assert_eq!(record.table_name(), Some("users"));
    }

    #[test]
    fn test_build_batch() {
        let mut mapping = BTreeMap::new();
        mapping.insert("id".to_string(), "id".to_string());

        let builder = RecordBuilder::new(mapping, None)
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));
        let items = vec![json!({"id": 1}), json!({"id": 2})];

        let records = builder.build_batch(&items).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_extract_by_path() {
        let item = json!({
            "user": {
                "profile": {
                    "name": "Bob"
                }
            }
        });

        let result = extract_by_path(&item, "user.profile.name");
        assert_eq!(result.unwrap().as_str().unwrap(), "Bob");
    }

    #[test]
    fn test_build_message() {
        let mut mapping = BTreeMap::new();
        mapping.insert("id".to_string(), "id".to_string());

        let builder = RecordBuilder::new(mapping, None)
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));
        let items = vec![json!({"id": 1}), json!({"id": 2})];

        let message = builder.build_message(&items).unwrap();
        match message {
            PipelineMessage::DataBatch(records) => {
                assert_eq!(records.len(), 2);
            }
            _ => panic!("Expected DataBatch"),
        }
    }

    #[test]
    fn test_dsl_transform_upper() {
        let mut mapping = BTreeMap::new();
        mapping.insert("name".to_string(), "= upper(source.name)".to_string());
        mapping.insert("age".to_string(), "age".to_string());

        let builder = RecordBuilder::new(mapping, None)
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));
        let item = json!({"name": "alice", "age": 25});

        let record = builder.build(&item).unwrap();
        match record.get_value("name").unwrap() {
            relus_common::types::UnifiedValue::String(s) => assert_eq!(s, "ALICE"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_dsl_transform_if() {
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "is_adult".to_string(),
            "= if(source.age >= 18, 1, 0)".to_string(),
        );

        let mut types = BTreeMap::new();
        types.insert("is_adult".to_string(), "int".to_string());

        let builder = RecordBuilder::new(mapping, Some(types))
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));

        let adult = builder.build(&json!({"age": 25})).unwrap();
        match adult.get_value("is_adult").unwrap() {
            relus_common::types::UnifiedValue::Int(n) => assert_eq!(*n, 1),
            other => panic!("expected Int(1), got {:?}", other),
        }

        let child = builder.build(&json!({"age": 12})).unwrap();
        match child.get_value("is_adult").unwrap() {
            relus_common::types::UnifiedValue::Int(n) => assert_eq!(*n, 0),
            other => panic!("expected Int(0), got {:?}", other),
        }
    }

    #[test]
    fn test_dsl_transform_constant() {
        let mut mapping = BTreeMap::new();
        mapping.insert("tag".to_string(), "expr: 'IMPORTED'".to_string());
        mapping.insert("id".to_string(), "id".to_string());

        let builder = RecordBuilder::new(mapping, None)
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));
        let record = builder.build(&json!({"id": 42})).unwrap();

        match record.get_value("tag").unwrap() {
            relus_common::types::UnifiedValue::String(s) => assert_eq!(s, "IMPORTED"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_no_dsl_pure_path_unchanged() {
        let mut mapping = BTreeMap::new();
        mapping.insert("name".to_string(), "name".to_string());

        let builder = RecordBuilder::new(mapping, None)
            .unwrap_or_else(|e| panic!("record builder should be valid: {}", e));
        assert!(builder.transform_engine.is_none());

        let record = builder.build(&json!({"name": "test"})).unwrap();
        match record.get_value("name").unwrap() {
            relus_common::types::UnifiedValue::String(s) => assert_eq!(s, "test"),
            other => panic!("expected String, got {:?}", other),
        }
    }

    #[test]
    fn invalid_dsl_mapping_returns_error() {
        let mut mapping = BTreeMap::new();
        mapping.insert(
            "userAccount".to_string(),
            "expr: if(source.userAccount == user_2500, 1, 0)".to_string(),
        );

        let err = RecordBuilder::new(mapping, None)
            .err()
            .unwrap_or_else(|| panic!("invalid DSL should return an error"));

        assert!(err.to_string().contains("DSL rule compile failed"));
    }
}
