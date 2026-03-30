//! Schema Registry 类型定义

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::constant::schema::DEFAULT_SCHEMA_CACHE_DIR;

/// 表结构信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// 表名
    pub table_name: String,
    /// 列信息
    pub columns: Vec<ColumnInfo>,
    /// 主键列
    pub primary_keys: Vec<String>,
    /// Schema 版本号
    pub version: u64,
    /// 发现时间
    pub discovered_at: DateTime<Utc>,
    /// 数据库类型 (postgres / mysql)
    pub db_kind: String,
}

impl TableSchema {
    pub fn new(table_name: String, db_kind: String) -> Self {
        Self {
            table_name,
            columns: Vec::new(),
            primary_keys: Vec::new(),
            version: 1,
            discovered_at: Utc::now(),
            db_kind,
        }
    }

    pub fn with_columns(mut self, columns: Vec<ColumnInfo>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_primary_keys(mut self, primary_keys: Vec<String>) -> Self {
        self.primary_keys = primary_keys;
        self
    }

    /// 转换为 column_mapping 配置
    pub fn to_column_mapping(&self) -> BTreeMap<String, String> {
        self.columns
            .iter()
            .map(|col| (col.name.clone(), col.name.clone()))
            .collect()
    }

    /// 转换为 column_types 配置
    pub fn to_column_types(&self) -> BTreeMap<String, String> {
        self.columns
            .iter()
            .map(|col| (col.name.clone(), col.logical_type.clone()))
            .collect()
    }

    /// 获取列名列表
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// 根据 名称获取列信息
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// 列信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// 列名
    pub name: String,
    /// 原生数据库类型
    pub native_type: String,
    /// 逻辑类型 (int / float / bool / decimal / timestamp / json / text)
    pub logical_type: String,
    /// 是否可空
    pub nullable: bool,
    /// 精度 (DECIMAL)
    pub precision: Option<u32>,
    /// 标度 (DECIMAL)
    pub scale: Option<u32>,
    /// 列位置
    pub ordinal: u32,
    /// 是否为主键
    pub is_primary_key: bool,
    /// 默认值
    pub default_value: Option<String>,
}

impl ColumnInfo {
    pub fn new(name: String, native_type: String, logical_type: String) -> Self {
        Self {
            name,
            native_type,
            logical_type,
            nullable: true,
            precision: None,
            scale: None,
            ordinal: 0,
            is_primary_key: false,
            default_value: None,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_precision(mut self, precision: u32) -> Self {
        self.precision = Some(precision);
        self
    }

    pub fn with_scale(mut self, scale: u32) -> Self {
        self.scale = Some(scale);
        self
    }

    pub fn with_primary_key(mut self, is_pk: bool) -> Self {
        self.is_primary_key = is_pk;
        self
    }
}

/// Schema 变更类型
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// 无变更
    NoChange,
    /// 新增列
    ColumnAdded(ColumnInfo),
    /// 删除列
    ColumnRemoved(String),
    /// 类型变更
    ColumnTypeChanged {
        column_name: String,
        old_type: String,
        new_type: String,
    },
    /// 可空性变更
    NullabilityChanged {
        column_name: String,
        old_nullable: bool,
        new_nullable: bool,
    },
}

/// 兼容性检查结果
#[derive(Debug, Clone)]
pub enum CompatibilityResult {
    /// 完全兼容
    Compatible,
    /// 向前兼容（新版本可读旧数据）
    ForwardCompatible { changes: Vec<SchemaChange> },
    /// 向后兼容（旧版本可读新数据）
    BackwardCompatible { changes: Vec<SchemaChange> },
    /// 破坏性变更
    Breaking { reason: String, changes: Vec<SchemaChange> },
}

impl CompatibilityResult {
    pub fn is_breaking(&self) -> bool {
        matches!(self, CompatibilityResult::Breaking { .. })
    }

    pub fn changes(&self) -> Option<&[SchemaChange]> {
        match self {
            CompatibilityResult::ForwardCompatible { changes }
            | CompatibilityResult::BackwardCompatible { changes }
            | CompatibilityResult::Breaking { changes, .. } => Some(changes),
            _ => None,
        }
    }
}

/// Schema 发现配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiscoveryConfig {
    /// 是否启用自动发现
    pub enabled: bool,
    /// 缓存目录
    pub cache_dir: Option<String>,
    /// 是否检测 Schema 演进
    pub check_evolution: bool,
    /// 破坏性变更时是否中断任务
    pub break_on_incompatible: bool,
}

impl Default for SchemaDiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cache_dir: Some(DEFAULT_SCHEMA_CACHE_DIR.to_string()),
            check_evolution: true,
            break_on_incompatible: true,
        }
    }
}

/// 类型映射规则
#[derive(Debug, Clone)]
pub struct TypeMappingRule {
    /// 原生类型模式（正则）
    pub native_pattern: String,
    /// 逻辑类型
    pub logical_type: String,
}

impl TypeMappingRule {
    pub fn new(native_pattern: &str, logical_type: &str) -> Self {
        Self {
            native_pattern: native_pattern.to_string(),
            logical_type: logical_type.to_string(),
        }
    }
}
