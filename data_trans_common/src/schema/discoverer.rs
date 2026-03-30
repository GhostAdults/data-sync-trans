//! 元数据发现 trait 定义

use anyhow::Result;
use async_trait::async_trait;

use super::TableSchema;

/// 元数据发现器 trait
#[async_trait]
pub trait MetadataDiscoverer: Send + Sync {
    /// 发现默认表结构
    async fn discover(&self) -> Result<TableSchema>;

    /// 发现指定表的结构
    async fn discover_table(&self, table_name: &str) -> Result<TableSchema>;

    /// 获取支持的数据库类型
    fn db_kind(&self) -> &str;

    /// 检查表是否存在
    async fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// 获取所有表名
    async fn list_tables(&self) -> Result<Vec<String>>;
}

/// 类型映射器 trait
pub trait TypeMapper: Send + Sync {
    /// 将原生类型映射为逻辑类型
    fn map_to_logical(&self, native_type: &str) -> String;

    /// 获取类型精度和标度
    fn extract_precision_scale(&self, native_type: &str) -> (Option<u32>, Option<u32>);
}
