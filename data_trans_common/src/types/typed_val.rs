//! 统一中间类型定义

use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use std::fmt;

/// 数据源类型
#[derive(Debug, Clone)]
pub enum DataSourceType {
    Api,
    Database {
        query: String,
        limit: Option<usize>,
        offset: Option<usize>,
    },
}

/// 类型转换结果 - 内部统一的中间类型表示
#[derive(Debug, Clone)]
pub enum TypedVal {
    I64(i64),
    F64(f64),
    Bool(bool),
    Decimal(Decimal),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptDecimal(Option<Decimal>),
    OptNaiveTs(Option<NaiveDateTime>),
    Text(String),
}

/// 类型标识 - 用于类型提示和映射配置
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeKind {
    Int,
    Float,
    Bool,
    Decimal,
    Timestamp,
    Json,
    Text,
}

impl TypeKind {
    pub fn from_str(s: &str) -> Self {
        match s {
            "int" => TypeKind::Int,
            "float" => TypeKind::Float,
            "bool" => TypeKind::Bool,
            "decimal" => TypeKind::Decimal,
            "timestamp" => TypeKind::Timestamp,
            "json" => TypeKind::Json,
            _ => TypeKind::Text,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TypeKind::Int => "int",
            TypeKind::Float => "float",
            TypeKind::Bool => "bool",
            TypeKind::Decimal => "decimal",
            TypeKind::Timestamp => "timestamp",
            TypeKind::Json => "json",
            TypeKind::Text => "text",
        }
    }
}

impl fmt::Display for TypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TypedVal {
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            TypedVal::OptI64(None)
                | TypedVal::OptF64(None)
                | TypedVal::OptBool(None)
                | TypedVal::OptDecimal(None)
                | TypedVal::OptNaiveTs(None)
        )
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            TypedVal::I64(_) => "i64",
            TypedVal::F64(_) => "f64",
            TypedVal::Bool(_) => "bool",
            TypedVal::Decimal(_) => "decimal",
            TypedVal::OptI64(_) => "option<i64>",
            TypedVal::OptF64(_) => "option<f64>",
            TypedVal::OptBool(_) => "option<bool>",
            TypedVal::OptDecimal(_) => "option<decimal>",
            TypedVal::OptNaiveTs(_) => "option<timestamp>",
            TypedVal::Text(_) => "text",
        }
    }
}
