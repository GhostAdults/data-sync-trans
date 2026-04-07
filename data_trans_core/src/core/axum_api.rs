use axum::{extract::Query, Json};
use axum::http::StatusCode;
use serde_json::Value;
use data_trans_common::resp::{ApiResp};
use data_trans_common::{TablesQuery, DescribeQuery, GenMapQuery};
use crate::core::SyncReq;
use crate::core::serve::{list_tables, sync_command, describe, gen_mapping};

// get tables list
pub async fn h_list_tables(Query(q): Query<TablesQuery>) -> (StatusCode, Json<ApiResp<Vec<String>>>) {
    let (status, resp) = list_tables(q).await;
    (status, resp)
}
// sync data with mapping
pub async fn h_sync(Json(body): Json<SyncReq>) -> (StatusCode, Json<ApiResp<Value>>) {
    let (status, resp) = sync_command(body.config, body.mapping).await;
    (status, resp)
}
// describe table schema
pub async fn h_describe(Query(q): Query<DescribeQuery>) -> (StatusCode, Json<ApiResp<Vec<Value>>>) {
    let (status, resp) = describe(q).await;
    (status, resp)
}
// generate mapping config
pub async fn h_gen_mapping(Query(q): Query<GenMapQuery>) -> (StatusCode, Json<ApiResp<Value>>) {
    let (status, resp) = gen_mapping(q).await;
    (status, resp)
}