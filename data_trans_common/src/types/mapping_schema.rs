//! MappingSchema - 表结构定义
//!
//! 存储表的所有字段类型元信息

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::OriginalTypeInfo;

/// 表结构定义
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MappingSchema {
    /// 字段名 -> 原始类型信息
    pub fields: HashMap<String, OriginalTypeInfo>,
    /// 表名（可选）
    pub table_name: Option<String>,
    /// Schema 版本（用于演进检测）
    pub version: Option<u64>,
}

impl MappingSchema {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            table_name: None,
            version: None,
        }
    }

    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }

    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }

    pub fn add_field(mut self, name: impl Into<String>, info: OriginalTypeInfo) -> Self {
        self.fields.insert(name.into(), info);
        self
    }

    pub fn insert(&mut self, name: impl Into<String>, info: OriginalTypeInfo) {
        self.fields.insert(name.into(), info);
    }

    pub fn get(&self, name: &str) -> Option<&OriginalTypeInfo> {
        self.fields.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    pub fn field_names(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    pub fn merge(&mut self, other: MappingSchema) {
        for (name, info) in other.fields {
            self.fields.insert(name, info);
        }
    }
}

impl Default for MappingSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<(String, OriginalTypeInfo)> for MappingSchema {
    fn from_iter<T: IntoIterator<Item = (String, OriginalTypeInfo)>>(iter: T) -> Self {
        Self {
            fields: iter.into_iter().collect(),
            table_name: None,
            version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SourceType;

    #[test]
    fn test_schema_builder() {
        let schema = MappingSchema::new()
            .with_table_name("users")
            .with_version(1)
            .add_field(
                "id",
                OriginalTypeInfo::new(SourceType::MySQL, "int".to_string()),
            )
            .add_field(
                "name",
                OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string()),
            );

        assert_eq!(schema.table_name, Some("users".to_string()));
        assert_eq!(schema.version, Some(1));
        assert_eq!(schema.len(), 2);
        assert!(schema.contains("id"));
        assert!(schema.contains("name"));
    }

    #[test]
    fn test_schema_from_iterator() {
        let fields = vec![
            (
                "id".to_string(),
                OriginalTypeInfo::new(SourceType::MySQL, "int".to_string()),
            ),
            (
                "name".to_string(),
                OriginalTypeInfo::new(SourceType::MySQL, "varchar".to_string()),
            ),
        ];

        let schema: MappingSchema = fields.into_iter().collect();
        assert_eq!(schema.len(), 2);
    }
}
