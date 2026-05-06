//! 数据源类型定义
//!
//! 整合 DataSourceType 和 SourceType，提供统一的数据源表示
//! 支持数据源类型分类和连接配置

use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

/// 数据源类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceType {
    MySQL,
    PostgreSQL,
    SQLServer,
    MongoDB,
    Kafka,
    Redis,
    Oracle,
    SQLite,
    ClickHouse,
    Elasticsearch,
    S3,
    File,
    API,
    Database,
    Other(String),
}

impl SourceType {
    pub fn as_str(&self) -> &str {
        match self {
            SourceType::MySQL => "mysql",
            SourceType::PostgreSQL => "postgresql",
            SourceType::SQLServer => "sqlserver",
            SourceType::MongoDB => "mongodb",
            SourceType::Kafka => "kafka",
            SourceType::Redis => "redis",
            SourceType::Oracle => "oracle",
            SourceType::SQLite => "sqlite",
            SourceType::ClickHouse => "clickhouse",
            SourceType::Elasticsearch => "elasticsearch",
            SourceType::S3 => "s3",
            SourceType::File => "file",
            SourceType::API => "api",
            SourceType::Database => "database",
            SourceType::Other(s) => s,
        }
    }

    pub fn is_rdbms(&self) -> bool {
        matches!(
            self,
            SourceType::MySQL
                | SourceType::PostgreSQL
                | SourceType::SQLServer
                | SourceType::Oracle
                | SourceType::SQLite
                | SourceType::ClickHouse
                | SourceType::Database
        )
    }

    pub fn is_nosql(&self) -> bool {
        matches!(
            self,
            SourceType::MongoDB | SourceType::Redis | SourceType::Elasticsearch
        )
    }

    pub fn is_streaming(&self) -> bool {
        matches!(self, SourceType::Kafka)
    }

    pub fn is_file_based(&self) -> bool {
        matches!(self, SourceType::File | SourceType::S3)
    }

    pub fn is_api(&self) -> bool {
        matches!(self, SourceType::API)
    }
}

impl FromStr for SourceType {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let source_type = match s.to_lowercase().as_str() {
            "mysql" => SourceType::MySQL,
            "postgresql" | "postgres" | "pg" => SourceType::PostgreSQL,
            "sqlserver" | "mssql" => SourceType::SQLServer,
            "mongodb" | "mongo" => SourceType::MongoDB,
            "kafka" => SourceType::Kafka,
            "redis" => SourceType::Redis,
            "oracle" => SourceType::Oracle,
            "sqlite" => SourceType::SQLite,
            "clickhouse" => SourceType::ClickHouse,
            "elasticsearch" | "es" => SourceType::Elasticsearch,
            "s3" => SourceType::S3,
            "file" => SourceType::File,
            "api" => SourceType::API,
            "database" => SourceType::Database,
            other => SourceType::Other(other.to_string()),
        };

        Ok(source_type)
    }
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for SourceType {
    fn default() -> Self {
        SourceType::Other("unknown".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_type_conversion() {
        let parse = |value: &str| {
            value
                .parse::<SourceType>()
                .unwrap_or_else(|err| match err {})
        };

        assert_eq!(parse("mysql"), SourceType::MySQL);
        assert_eq!(parse("PostgreSQL"), SourceType::PostgreSQL);
        assert_eq!(parse("pg"), SourceType::PostgreSQL);
        assert_eq!(parse("mongodb"), SourceType::MongoDB);
    }

    #[test]
    fn test_source_type_categories() {
        assert!(SourceType::MySQL.is_rdbms());
        assert!(SourceType::PostgreSQL.is_rdbms());
        assert!(!SourceType::MongoDB.is_rdbms());
        assert!(SourceType::MongoDB.is_nosql());
        assert!(!SourceType::MySQL.is_nosql());
        assert!(SourceType::Kafka.is_streaming());
        assert!(SourceType::S3.is_file_based());
        assert!(SourceType::API.is_api());
    }
}
