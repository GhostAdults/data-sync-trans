/// 这个模块定义了API的响应结构和一些通用的数据库查询参数结构。
use serde::{Deserialize,Serialize};
#[derive(Serialize,Debug)]
pub struct ApiResp<T> {
    pub ok: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}


// 通用的数据库查询参数
#[derive(Deserialize)]
pub struct BaseDbQuery {
    pub db_url: Option<String>,
    pub db_type: Option<String>,
}

#[derive(Deserialize)]
pub struct TablesQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
}

#[derive(Deserialize)]
pub struct DescribeQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
    pub table: String,
}
#[derive(Deserialize)]
pub struct GenMapQuery {
    #[serde(flatten)]
    pub base: BaseDbQuery,
    pub table: String,
}
#[derive(Clone)]
pub struct ColInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}
