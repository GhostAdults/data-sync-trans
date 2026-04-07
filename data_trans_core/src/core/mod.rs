pub mod axum_api;
pub mod cli;
pub mod pipeline;
pub mod registry;
pub mod runner;
pub mod serve;
use axum::{extract::Query, Json};
use axum::{
    routing::{get, post},
    Router,
};
use axum_api::*;
use data_trans_common::job_config::{ApiConfig, DbConfig, JobConfig, MappingConfig};
use data_trans_common::resp::{DescribeQuery, GenMapQuery, TablesQuery};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LegacyConfig {
    pub id: String,
    pub db_type: Option<String>,
    pub db: DbConfig,
    pub api: ApiConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub mode: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Deserialize)]
pub struct SyncReq {
    pub config: JobConfig,
    pub mapping: Option<MappingConfig>,
}

pub fn run_serve(host: String, port: u16) -> anyhow::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async { serve_http(host, port).await })?;
    Ok(())
}
pub async fn serve_http(host: String, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/tables",
            get(|q: Query<TablesQuery>| async move { h_list_tables(q).await }),
        )
        .route(
            "/describe",
            get(|q: Query<DescribeQuery>| async move { h_describe(q).await }),
        )
        .route(
            "/gen-mapping",
            get(|q: Query<GenMapQuery>| async move { h_gen_mapping(q).await }),
        )
        .route(
            "/sync",
            post(|body: Json<SyncReq>| async move { h_sync(body).await }),
        );
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
