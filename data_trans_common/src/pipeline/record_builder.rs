//! Record 构建器
//!
//! 统一数据转换入口，支持从 JSON 构建 Record (MappingRow)

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::types::{
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

/// Record 构建器 - 统一数据转换入口
///
/// 直接构建 MappingRow (Record)，包含完整的类型元信息
pub struct RecordBuilder {
    column_mapping: BTreeMap<String, String>,
    column_types: Option<BTreeMap<String, String>>,
    registry: Arc<TypeConverterRegistry>,
    source_type: SourceType,
    table_name: Option<String>,
}

impl RecordBuilder {
    pub fn new(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            column_mapping,
            column_types,
            registry: Arc::new(TypeConverterRegistry::new()),
            source_type: SourceType::Other("unknown".to_string()),
            table_name: None,
        }
    }

    pub fn with_registry(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
        registry: Arc<TypeConverterRegistry>,
    ) -> Self {
        Self {
            column_mapping,
            column_types,
            registry,
            source_type: SourceType::Other("unknown".to_string()),
            table_name: None,
        }
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

        Ok(row)
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

        let builder = RecordBuilder::new(mapping, None);
        let items = vec![json!({"id": 1}), json!({"id": 2})];

        let records = builder.build_batch(&items).unwrap();
        println!("{:#?}", records);
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

        let builder = RecordBuilder::new(mapping, None);
        let items = vec![json!({"id": 1}), json!({"id": 2})];

        let message = builder.build_message(&items).unwrap();
        match message {
            PipelineMessage::DataBatch(records) => {
                assert_eq!(records.len(), 2);
            }
            _ => panic!("Expected DataBatch"),
        }
    }
}
