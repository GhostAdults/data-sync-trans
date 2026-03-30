//! RDBMS 元数据发现实现

use anyhow::{bail, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::db::{DbKind, DbPool};

use super::{ColumnInfo, MetadataDiscoverer, TableSchema, TypeMapper};

/// RDBMS 元数据发现器
pub struct RdbmsDiscoverer {
    pool: Arc<DbPool>,
    table_name: String,
    db_kind: DbKind,
    type_mapper: Box<dyn TypeMapper>,
}

impl RdbmsDiscoverer {
    pub fn new(pool: Arc<DbPool>, table_name: String) -> Self {
        let db_kind = match pool.as_ref() {
            DbPool::Postgres(_) => DbKind::Postgres,
            DbPool::Mysql(_) => DbKind::Mysql,
        };

        let type_mapper: Box<dyn TypeMapper> = match db_kind {
            DbKind::Postgres => Box::new(PostgresTypeMapper),
            DbKind::Mysql => Box::new(MysqlTypeMapper),
        };

        Self {
            pool,
            table_name,
            db_kind,
            type_mapper,
        }
    }

    async fn query_columns_postgres(&self) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_name = '{}'
            ORDER BY c.ordinal_position
            "#,
            self.table_name
        );

        // 使用 sqlx 直接查询
        let columns = match self.pool.as_ref() {
            DbPool::Postgres(p) => self.query_columns_sqlx_pg(p, &sql).await?,
            DbPool::Mysql(_) => {
                bail!("PostgreSQL pool expected")
            }
        };

        Ok(columns)
    }

    async fn query_columns_sqlx_pg(
        &self,
        pool: &sqlx::PgPool,
        sql: &str,
    ) -> Result<Vec<ColumnInfo>> {
        use sqlx::Row;

        let rows = sqlx::query(sql).fetch_all(pool).await?;

        let mut columns = Vec::new();
        for row in rows {
            let name: String = row.try_get("column_name")?;
            let native_type: String = row.try_get("data_type")?;
            let nullable: String = row.try_get("is_nullable")?;
            let ordinal: i32 = row.try_get("ordinal_position")?;
            let default_value: Option<String> = row.try_get("column_default")?;

            let logical_type = self.type_mapper.map_to_logical(&native_type);
            let (precision, scale) = self.type_mapper.extract_precision_scale(&native_type);

            columns.push(ColumnInfo {
                name,
                native_type,
                logical_type,
                nullable: nullable == "YES",
                precision,
                scale,
                ordinal: ordinal as u32,
                is_primary_key: false, // 将在后续步骤设置
                default_value,
            });
        }

        Ok(columns)
    }

    async fn query_columns_mysql(&self) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            r#"
            SELECT
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.IS_NULLABLE as is_nullable,
                c.ORDINAL_POSITION as ordinal_position,
                c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                c.NUMERIC_PRECISION as numeric_precision,
                c.NUMERIC_SCALE as numeric_scale,
                c.COLUMN_DEFAULT as column_default
            FROM information_schema.COLUMNS c
            WHERE c.TABLE_NAME = '{}'
            AND c.TABLE_SCHEMA = DATABASE()
            ORDER BY c.ORDINAL_POSITION
            "#,
            self.table_name
        );

        let columns = match self.pool.as_ref() {
            DbPool::Postgres(_) => {
                bail!("MySQL pool expected")
            }
            DbPool::Mysql(p) => self.query_columns_sqlx_my(p, &sql).await?,
        };

        Ok(columns)
    }

    async fn query_columns_sqlx_my(
        &self,
        pool: &sqlx::MySqlPool,
        sql: &str,
    ) -> Result<Vec<ColumnInfo>> {
        use sqlx::Row;

        let rows = sqlx::query(sql).fetch_all(pool).await?;

        let mut columns = Vec::new();
        for row in rows {
            let name: String = row.try_get("column_name")?;
            let native_type: String = row.try_get("data_type")?;
            let nullable: String = row.try_get("is_nullable")?;
            let ordinal: u32 = row.try_get("ordinal_position")?;
            let default_value: Option<String> = row.try_get("column_default")?;

            let logical_type = self.type_mapper.map_to_logical(&native_type);
            let (precision, scale) = self.type_mapper.extract_precision_scale(&native_type);

            columns.push(ColumnInfo {
                name,
                native_type,
                logical_type,
                nullable: nullable == "YES",
                precision,
                scale,
                ordinal,
                is_primary_key: false,
                default_value,
            });
        }

        Ok(columns)
    }

    async fn query_primary_keys(&self) -> Result<Vec<String>> {
        let sql = match self.db_kind {
            DbKind::Postgres => format!(
                r#"
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = '{}'
                AND tc.table_schema = 'public'
                ORDER BY kcu.ordinal_position
                "#,
                self.table_name
            ),
            DbKind::Mysql => format!(
                r#"
                SELECT COLUMN_NAME as column_name
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE CONSTRAINT_NAME = 'PRIMARY'
                    AND TABLE_NAME = '{}'
                ORDER BY ORDINAL_POSITION
                "#,
                self.table_name
            ),
        };

        let pks = match self.pool.as_ref() {
            DbPool::Postgres(p) => self.query_pks_sqlx_pg(p, &sql).await?,
            DbPool::Mysql(p) => self.query_pks_sqlx_my(p, &sql).await?,
        };

        Ok(pks)
    }

    async fn query_pks_sqlx_pg(&self, pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>> {
        use sqlx::Row;

        let rows = sqlx::query(sql).fetch_all(pool).await?;
        let pks: Vec<String> = rows
            .iter()
            .filter_map(|r| r.try_get("column_name").ok())
            .collect();
        Ok(pks)
    }

    async fn query_pks_sqlx_my(&self, pool: &sqlx::MySqlPool, sql: &str) -> Result<Vec<String>> {
        use sqlx::Row;

        let rows = sqlx::query(sql).fetch_all(pool).await?;
        let pks: Vec<String> = rows
            .iter()
            .filter_map(|r| r.try_get("column_name").ok())
            .collect();
        Ok(pks)
    }
}

#[async_trait]
impl MetadataDiscoverer for RdbmsDiscoverer {
    async fn discover(&self) -> Result<TableSchema> {
        self.discover_table(&self.table_name).await
    }

    async fn discover_table(&self, table_name: &str) -> Result<TableSchema> {
        info!("Discovering schema for table: {}", table_name);

        // 查询列信息
        let mut columns = match self.db_kind {
            DbKind::Postgres => self.query_columns_postgres().await?,
            DbKind::Mysql => self.query_columns_mysql().await?,
        };

        if columns.is_empty() {
            bail!("Table '{}' not found or has no columns", table_name);
        }

        // 查询主键
        let primary_keys = self.query_primary_keys().await?;
        let pk_set: std::collections::HashSet<_> = primary_keys.iter().cloned().collect();

        // 设置主键标记
        for col in &mut columns {
            col.is_primary_key = pk_set.contains(&col.name);
        }

        let db_kind_str = match self.db_kind {
            DbKind::Postgres => "postgres",
            DbKind::Mysql => "mysql",
        };

        let schema = TableSchema::new(table_name.to_string(), db_kind_str.to_string())
            .with_columns(columns)
            .with_primary_keys(primary_keys);

        debug!(
            "Discovered schema for {}: {} columns, {} primary keys",
            table_name,
            schema.columns.len(),
            schema.primary_keys.len()
        );

        Ok(schema)
    }

    fn db_kind(&self) -> &str {
        match self.db_kind {
            DbKind::Postgres => "postgres",
            DbKind::Mysql => "mysql",
        }
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = match self.db_kind {
            DbKind::Postgres => {
                "SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1"
            }
            DbKind::Mysql => "SELECT 1 FROM information_schema.TABLES WHERE TABLE_NAME = ? LIMIT 1",
        };

        let result = match self.pool.as_ref() {
            DbPool::Postgres(p) => {
                let row: Option<(i32,)> = sqlx::query_as(sql)
                    .bind(table_name)
                    .fetch_optional(p)
                    .await?;
                row.is_some()
            }
            DbPool::Mysql(p) => {
                let row: Option<(i32,)> = sqlx::query_as(sql)
                    .bind(table_name)
                    .fetch_optional(p)
                    .await?;
                row.is_some()
            }
        };

        Ok(result)
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let sql = match self.db_kind {
            DbKind::Postgres => {
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            }
            DbKind::Mysql => {
                "SELECT TABLE_NAME as table_name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"
            }
        };

        let tables = match self.pool.as_ref() {
            DbPool::Postgres(p) => {
                let rows: Vec<(String,)> = sqlx::query_as(sql).fetch_all(p).await?;
                rows.into_iter().map(|r| r.0).collect()
            }
            DbPool::Mysql(p) => {
                let rows: Vec<(String,)> = sqlx::query_as(sql).fetch_all(p).await?;
                rows.into_iter().map(|r| r.0).collect()
            }
        };

        Ok(tables)
    }
}

/// PostgreSQL 类型映射器
struct PostgresTypeMapper;

impl TypeMapper for PostgresTypeMapper {
    fn map_to_logical(&self, native_type: &str) -> String {
        let upper = native_type.to_uppercase();

        // Integer types
        if matches!(
            upper.as_str(),
            "SMALLINT"
                | "INTEGER"
                | "INT"
                | "INT4"
                | "BIGINT"
                | "INT8"
                | "SMALLSERIAL"
                | "SERIAL"
                | "BIGSERIAL"
        ) {
            return "int".to_string();
        }

        // Float types
        if matches!(
            upper.as_str(),
            "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOAT8"
        ) {
            return "float".to_string();
        }

        // Boolean
        if upper == "BOOLEAN" || upper == "BOOL" {
            return "bool".to_string();
        }

        // Decimal/Numeric
        if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
            return "decimal".to_string();
        }

        // Timestamp
        if upper.starts_with("TIMESTAMP") || upper.starts_with("DATE") || upper.starts_with("TIME")
        {
            return "timestamp".to_string();
        }

        // JSON
        if matches!(upper.as_str(), "JSON" | "JSONB") {
            return "json".to_string();
        }

        // Default to text
        "text".to_string()
    }

    fn extract_precision_scale(&self, _native_type: &str) -> (Option<u32>, Option<u32>) {
        (None, None)
    }
}

/// MySQL 类型映射器
struct MysqlTypeMapper;

impl TypeMapper for MysqlTypeMapper {
    fn map_to_logical(&self, native_type: &str) -> String {
        let lower = native_type.to_lowercase();

        // Integer types
        if matches!(
            lower.as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
        ) {
            return "int".to_string();
        }

        // Float types
        if matches!(lower.as_str(), "float" | "double") {
            return "float".to_string();
        }

        // Boolean (MySQL uses TINYINT(1))
        if lower == "bool" {
            return "bool".to_string();
        }

        // Decimal
        if lower.starts_with("decimal") || lower.starts_with("numeric") {
            return "decimal".to_string();
        }

        // Timestamp
        if matches!(
            lower.as_str(),
            "date" | "time" | "datetime" | "timestamp" | "year"
        ) {
            return "timestamp".to_string();
        }

        // JSON
        if lower == "json" {
            return "json".to_string();
        }

        // Default to text
        "text".to_string()
    }

    fn extract_precision_scale(&self, _native_type: &str) -> (Option<u32>, Option<u32>) {
        (None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_type_mapping() {
        let mapper = PostgresTypeMapper;

        assert_eq!(mapper.map_to_logical("integer"), "int");
        assert_eq!(mapper.map_to_logical("bigint"), "int");
        assert_eq!(mapper.map_to_logical("double precision"), "float");
        assert_eq!(mapper.map_to_logical("boolean"), "bool");
        assert_eq!(mapper.map_to_logical("numeric(10,2)"), "decimal");
        assert_eq!(mapper.map_to_logical("timestamp"), "timestamp");
        assert_eq!(mapper.map_to_logical("jsonb"), "json");
        assert_eq!(mapper.map_to_logical("varchar(255)"), "text");
    }

    #[test]
    fn test_mysql_type_mapping() {
        let mapper = MysqlTypeMapper;

        assert_eq!(mapper.map_to_logical("int"), "int");
        assert_eq!(mapper.map_to_logical("bigint"), "int");
        assert_eq!(mapper.map_to_logical("double"), "float");
        assert_eq!(mapper.map_to_logical("decimal(10,2)"), "decimal");
        assert_eq!(mapper.map_to_logical("datetime"), "timestamp");
        assert_eq!(mapper.map_to_logical("json"), "json");
        assert_eq!(mapper.map_to_logical("varchar"), "text");
    }
}
