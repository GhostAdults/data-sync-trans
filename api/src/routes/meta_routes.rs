use axum::extract::Query;
use axum::routing::{get, post};
use axum::Router;
use relus_common::resp::{DescribeQuery, GenMapQuery, TablesQuery};

use crate::handlers::meta_handlers::{
    get_scheduler_tasks, h_describe, h_gen_mapping, h_list_tables, h_sync, post_scheduler_cancel,
    post_scheduler_task,
};
use crate::server::SharedState;

pub fn routes() -> Router<SharedState> {
    Router::<SharedState>::new()
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
        .route("/sync", post(h_sync))
        .route("/scheduler/tasks", get(get_scheduler_tasks))
        .route("/scheduler/tasks/post", post(post_scheduler_task))
        .route(
            "/scheduler/tasks/:job_id/cancel",
            post(post_scheduler_cancel),
        )
}
