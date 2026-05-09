use crate::routes::meta_routes;
pub use axum::http::StatusCode;
use axum::Router;
use axum::{extract::Request, response::IntoResponse};
use relus_common::app_config::manager::SharedConfig;
use relus_common::job_config::JobConfig;
use relus_common::resp::ApiResp;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;

pub type ApiFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ApiHandlerResult = (StatusCode, ApiResp<Value>);

pub trait SyncExecutor: Send + Sync {
    fn execute_sync(&self, config: JobConfig) -> ApiFuture<ApiHandlerResult>;
}

pub trait SchedulerControl: Send + Sync {
    fn query_tasks(&self, job_id: Option<String>) -> ApiFuture<ApiHandlerResult>;
    fn submit_task(&self, path: String) -> ApiFuture<ApiHandlerResult>;
    fn cancel_task(&self, job_id: String) -> ApiFuture<ApiHandlerResult>;
}

pub type SharedState = Arc<AppState>;

#[derive(Clone, Default)]
pub struct AppState {
    pub config_manager: Option<Arc<SharedConfig>>,
    pub sync_executor: Option<Arc<dyn SyncExecutor>>,
    pub scheduler: Option<Arc<dyn SchedulerControl>>,
}

impl AppState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_sync_executor(mut self, sync_executor: Arc<dyn SyncExecutor>) -> Self {
        self.sync_executor = Some(sync_executor);
        self
    }

    pub fn with_config_manager(mut self, config_manager: Arc<SharedConfig>) -> Self {
        self.config_manager = Some(config_manager);
        self
    }

    pub fn with_scheduler(mut self, scheduler: Arc<dyn SchedulerControl>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }
}

pub async fn start(host: String, port: u16, app_state: SharedState) -> anyhow::Result<()> {
    let router = Router::new()
        // Nesting the API routes under /api/meta
        .nest("/api/meta", meta_routes::routes())
        .fallback(error_404_handler)
        .with_state(Arc::clone(&app_state));
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;
    // Start the API service.
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        unix::signal(SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("received termination signal, shutting down...");
}

// error_404_handler
pub async fn error_404_handler(request: Request) -> impl IntoResponse {
    tracing::error!("route not found: {:?}", request);
    StatusCode::NOT_FOUND
}
