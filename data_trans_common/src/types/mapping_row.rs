//! MappingRow - 核心数据载体
//!
//! Reader -> Channel -> Pipeline -> Writer 全程唯一传输对象
//! 保留原始类型信息 + 目标类型信息，确保跨库类型信息永不丢失

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::{MappingField, MappingSchema, OriginalTypeInfo, UnifiedValue};

/// 一行数据（全流程唯一传输对象）
///
/// Reader -> Channel -> Pipeline -> Writer 全程仅传输此结构体
/// Record 是 MappingRow 的类型别名
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRow {
    /// 字段名 -> 字段封装（值 + 元信息）
    pub fields: HashMap<String, MappingField>,
    /// 表结构定义（所有字段的元信息）
    pub schema: MappingSchema,
    /// 源表名
    pub source_table: Option<String>,
    /// 原始 JSON 数据
    #[serde(default)]
    pub source: JsonValue,
}

impl MappingRow {
    /// 创建新的 MappingRow
    pub fn new(schema: MappingSchema) -> Self {
        Self {
            fields: HashMap::new(),
            schema,
            source_table: None,
            source: JsonValue::Null,
        }
    }

    /// 创建简单的 MappingRow（空 schema）
    pub fn simple() -> Self {
        Self::new(MappingSchema::new())
    }

    /// 设置源表名
    pub fn with_source_table(mut self, table: impl Into<String>) -> Self {
        self.source_table = Some(table.into());
        self
    }

    /// 别名：设置源表名（兼容性）
    pub fn with_source_table_alias(mut self, table: impl Into<String>) -> Self {
        self.source_table = Some(table.into());
        self
    }

    /// 设置原始数据
    pub fn with_source(mut self, source: JsonValue) -> Self {
        self.source = source;
        self
    }

    /// 插入字段
    pub fn insert(&mut self, name: impl Into<String>, field: MappingField) {
        self.fields.insert(name.into(), field);
    }

    /// 插入值和类型信息
    pub fn insert_value(&mut self, name: impl Into<String>, value: UnifiedValue, info: OriginalTypeInfo) {
        self.fields.insert(name.into(), MappingField::new(value, info));
    }

    /// 插入简单字段（自动创建类型信息）
    pub fn insert_simple(&mut self, name: impl Into<String>, value: UnifiedValue, type_name: &str) {
        self.fields.insert(
            name.into(),
            MappingField::simple(value, type_name),
        );
    }

    /// 获取字段
    pub fn get(&self, name: &str) -> Option<&MappingField> {
        self.fields.get(name)
    }

    /// 获取字段值
    pub fn get_value(&self, name: &str) -> Option<&UnifiedValue> {
        self.fields.get(name).map(|f| &f.value)
    }

    /// 获取原始类型信息
    pub fn get_original_info(&self, name: &str) -> Option<&OriginalTypeInfo> {
        self.fields.get(name).map(|f| &f.original_info)
    }

    /// 检查字段是否存在
    pub fn contains(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// 获取所有字段名
    pub fn field_names(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    /// 字段数量
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// 列数量（len 的别名）
    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    /// 获取表名
    pub fn table_name(&self) -> Option<&str> {
        self.schema.table_name.as_deref().or(self.source_table.as_deref())
    }

    /// 获取所有字段值
    pub fn values(&self) -> HashMap<String, UnifiedValue> {
        self.fields
            .iter()
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }

    /// 从值 map 创建 MappingRow
    pub fn from_values(values: HashMap<String, UnifiedValue>, source: JsonValue) -> Self {
        let mut row = Self::simple();
        row.source = source;
        for (name, value) in values {
            let type_name = value.type_name().to_string();
            row.insert_simple(name, value, &type_name);
        }
        row
    }
}

impl Default for MappingRow {
    fn default() -> Self {
        Self::new(MappingSchema::new())
    }
}

/// 批量数据传输容器
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingBatch {
    /// 数据行集合
    pub rows: Vec<MappingRow>,
    /// 批次元数据
    pub metadata: BatchMetadata,
}

/// 批次元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMetadata {
    /// 批次序号
    pub sequence: u64,
    /// 是否为最后一批
    pub is_last: bool,
    /// 时间戳
    pub timestamp: u64,
}

impl BatchMetadata {
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            is_last: false,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    pub fn with_is_last(mut self, is_last: bool) -> Self {
        self.is_last = is_last;
        self
    }
}

impl MappingBatch {
    pub fn new(rows: Vec<MappingRow>, sequence: u64) -> Self {
        Self {
            rows,
            metadata: BatchMetadata::new(sequence),
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn is_last(&self) -> bool {
        self.metadata.is_last
    }

    pub fn into_rows(self) -> Vec<MappingRow> {
        self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SourceType;

    #[test]
    fn test_mapping_row_creation() {
        let schema = MappingSchema::new()
            .with_table_name("users")
            .add_field(
                "id",
                OriginalTypeInfo::new(SourceType::MySQL, "int".to_string()),
            )
            .add_field(
                "name",
                OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string()),
            );

        let mut row = MappingRow::new(schema).with_source_table("users");

        row.insert(
            "id",
            MappingField::new(
                UnifiedValue::Int(1),
                OriginalTypeInfo::new(SourceType::MySQL, "int".to_string()),
            ),
        );
        row.insert(
            "name",
            MappingField::new(
                UnifiedValue::String("Alice".to_string()),
                OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string()),
            ),
        );

        assert_eq!(row.len(), 2);
        assert!(row.contains("id"));
        assert!(row.contains("name"));
        assert_eq!(row.table_name(), Some("users"));
    }

    #[test]
    fn test_mapping_row_from_values() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), UnifiedValue::Int(1));
        values.insert("name".to_string(), UnifiedValue::String("Alice".to_string()));

        let row = MappingRow::from_values(values, serde_json::json!({"source": "test"}));

        assert_eq!(row.len(), 2);
        assert!(row.contains("id"));
        assert!(row.contains("name"));
        assert_eq!(row.source["source"], "test");
    }

    #[test]
    fn test_mapping_batch() {
        let row = MappingRow::simple();
        let batch = MappingBatch::new(vec![row], 1);

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_last());
    }
}
