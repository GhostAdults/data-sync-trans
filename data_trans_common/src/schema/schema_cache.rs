//! Schema 缓存实现（基于 redb）

use anyhow::Result;
use redb::{Database, TableDefinition};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

use super::{ColumnInfo, SchemaChange, TableSchema};

const SCHEMA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("schemas");
const VERSION_TABLE: TableDefinition<&str, u64> = TableDefinition::new("versions");

/// Schema 缓存（基于 redb）
pub struct SchemaCache {
    db: Arc<Database>,
}

impl SchemaCache {
    /// 创建新的 Schema 缓存
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path.as_ref())?;

        // 初始化表
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(SCHEMA_TABLE)?;
            let _ = write_txn.open_table(VERSION_TABLE)?;
        }
        write_txn.commit()?;

        info!("Schema cache initialized at {:?}", path.as_ref());
        Ok(Self {
            db: Arc::new(db),
        })
    }

    /// 创建内存缓存（用于测试）
    pub fn in_memory() -> Result<Self> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;

        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(SCHEMA_TABLE)?;
            let _ = write_txn.open_table(VERSION_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self {
            db: Arc::new(db),
        })
    }

    /// 生成缓存键
    fn cache_key(&self, table_name: &str, version: u64) -> String {
        format!("{}:v{}", table_name, version)
    }

    /// 注册新版本 Schema
    pub fn register(&self, schema: &TableSchema) -> Result<u64> {
        let table_name = &schema.table_name;

        // 获取下一个版本号
        let next_version = self.get_next_version(table_name)?;

        let mut schema_with_version = schema.clone();
        schema_with_version.version = next_version;

        let key = self.cache_key(table_name, next_version);
        let serialized = serde_json::to_vec(&schema_with_version)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut schema_table = write_txn.open_table(SCHEMA_TABLE)?;
            schema_table.insert(key.as_str(), serialized.as_slice())?;

            let mut version_table = write_txn.open_table(VERSION_TABLE)?;
            version_table.insert(table_name.as_str(), next_version)?;
        }
        write_txn.commit()?;

        info!(
            "Registered schema version {} for table {}",
            next_version, table_name
        );
        Ok(next_version)
    }

    /// 获取最新版本号
    pub fn get_latest_version(&self, table_name: &str) -> Result<Option<u64>> {
        let read_txn = self.db.begin_read()?;
        let version_table = read_txn.open_table(VERSION_TABLE)?;

        Ok(version_table.get(table_name)?.map(|v| v.value()))
    }

    /// 获取下一个版本号
    fn get_next_version(&self, table_name: &str) -> Result<u64> {
        let current = self.get_latest_version(table_name)?;
        Ok(current.unwrap_or(0) + 1)
    }

    /// 获取最新版本 Schema
    pub fn get_latest(&self, table_name: &str) -> Result<Option<TableSchema>> {
        let version = self.get_latest_version(table_name)?;
        match version {
            Some(v) => self.get_version(table_name, v),
            None => Ok(None),
        }
    }

    /// 获取指定版本 Schema
    pub fn get_version(&self, table_name: &str, version: u64) -> Result<Option<TableSchema>> {
        let key = self.cache_key(table_name, version);

        let read_txn = self.db.begin_read()?;
        let schema_table = read_txn.open_table(SCHEMA_TABLE)?;

        match schema_table.get(key.as_str())? {
            Some(value) => {
                let bytes = value.value();
                let schema: TableSchema = serde_json::from_slice(bytes)?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// 检测 Schema 变更
    pub fn detect_changes(&self, new_schema: &TableSchema) -> Result<Vec<SchemaChange>> {
        let cached = self.get_latest(&new_schema.table_name)?;

        match cached {
            None => {
                debug!(
                    "No cached schema for {}, treating as new",
                    new_schema.table_name
                );
                Ok(vec![])
            }
            Some(old_schema) => {
                let changes = self.compare_schemas(&old_schema, new_schema);
                if !changes.is_empty() {
                    info!(
                        "Detected {} schema changes for {}",
                        changes.len(),
                        new_schema.table_name
                    );
                }
                Ok(changes)
            }
        }
    }

    /// 比较两个 Schema 的差异
    fn compare_schemas(&self, old: &TableSchema, new: &TableSchema) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        let old_columns: std::collections::HashMap<&str, &ColumnInfo> = old
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        let new_columns: std::collections::HashMap<&str, &ColumnInfo> = new
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        // 检查新增列
        for (name, col) in &new_columns {
            if !old_columns.contains_key(*name) {
                changes.push(SchemaChange::ColumnAdded((*col).clone()));
            }
        }

        // 检查删除列
        for name in old_columns.keys() {
            if !new_columns.contains_key(*name) {
                changes.push(SchemaChange::ColumnRemoved((*name).to_string()));
            }
        }

        // 检查类型变更
        for (name, new_col) in &new_columns {
            if let Some(old_col) = old_columns.get(*name) {
                if old_col.logical_type != new_col.logical_type {
                    changes.push(SchemaChange::ColumnTypeChanged {
                        column_name: (*name).to_string(),
                        old_type: old_col.logical_type.clone(),
                        new_type: new_col.logical_type.clone(),
                    });
                }

                if old_col.nullable != new_col.nullable {
                    changes.push(SchemaChange::NullabilityChanged {
                        column_name: (*name).to_string(),
                        old_nullable: old_col.nullable,
                        new_nullable: new_col.nullable,
                    });
                }
            }
        }

        changes
    }

    /// 检查表是否有缓存
    pub fn has_cache(&self, table_name: &str) -> Result<bool> {
        Ok(self.get_latest_version(table_name)?.is_some())
    }

    /// 清除表的缓存
    pub fn clear(&self, table_name: &str) -> Result<()> {
        let latest_version = self.get_latest_version(table_name)?;

        if let Some(max_version) = latest_version {
            let write_txn = self.db.begin_write()?;
            {
                let mut schema_table = write_txn.open_table(SCHEMA_TABLE)?;
                let mut version_table = write_txn.open_table(VERSION_TABLE)?;

                // 删除所有版本
                for v in 1..=max_version {
                    let key = self.cache_key(table_name, v);
                    schema_table.remove(key.as_str())?;
                }

                version_table.remove(table_name)?;
            }
            write_txn.commit()?;

            info!("Cleared schema cache for table {}", table_name);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_cache() {
        let cache = SchemaCache::in_memory().unwrap();

        let schema = TableSchema::new("users".to_string(), "postgres".to_string())
            .with_columns(vec![
                ColumnInfo::new("id".to_string(), "bigint".to_string(), "int".to_string()),
                ColumnInfo::new("name".to_string(), "varchar".to_string(), "text".to_string()),
            ]);

        // 注册 schema
        let version = cache.register(&schema).unwrap();
        assert_eq!(version, 1);

        // 获取最新版本
        let latest = cache.get_latest("users").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 1);
    }

    #[test]
    fn test_schema_changes() {
        let cache = SchemaCache::in_memory().unwrap();

        let old_schema = TableSchema::new("users".to_string(), "postgres".to_string())
            .with_columns(vec![
                ColumnInfo::new("id".to_string(), "bigint".to_string(), "int".to_string()),
                ColumnInfo::new("name".to_string(), "varchar".to_string(), "text".to_string()),
            ]);

        cache.register(&old_schema).unwrap();

        let new_schema = TableSchema::new("users".to_string(), "postgres".to_string())
            .with_columns(vec![
                ColumnInfo::new("id".to_string(), "bigint".to_string(), "int".to_string()),
                ColumnInfo::new("name".to_string(), "varchar".to_string(), "text".to_string()),
                ColumnInfo::new("email".to_string(), "varchar".to_string(), "text".to_string()),
            ]);

        let changes = cache.detect_changes(&new_schema).unwrap();
        assert_eq!(changes.len(), 1);

        match &changes[0] {
            SchemaChange::ColumnAdded(col) => assert_eq!(col.name, "email"),
            _ => panic!("Expected ColumnAdded"),
        }
    }
}
