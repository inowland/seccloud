use std::sync::Arc;

use seccloud_lake::accepted_batch::{
    AcceptedBatchStore, InMemoryAcceptedBatchStore, LocalAcceptedBatchStore,
};
use seccloud_lake::intake_queue::{InMemoryIntakeQueue, LocalIntakeQueue};
use seccloud_lake::object_store::{InMemoryObjectStore, LocalObjectStore};
use tracing_subscriber::EnvFilter;

pub mod auth;
pub mod config;
pub mod detector;
pub mod event_index;
pub mod feature_runtime;
pub mod features;
pub mod ingest;
pub mod local_runtime;
pub mod model_runtime;
pub mod model_scoring;
pub mod normalize;
pub mod projector;
pub mod routes;
pub mod runtime;
pub mod service;
pub mod service_runtime;
#[cfg(test)]
mod tests;
pub mod validate;
pub mod worker;

pub fn init_runtime_logging(service: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .try_init();
    tracing::info!(
        service,
        version = env!("CARGO_PKG_VERSION"),
        "logging initialized"
    );
}

pub async fn run_gateway_from_env() -> anyhow::Result<()> {
    init_runtime_logging("seccloud-gateway");

    let config = config::GatewayConfig::from_env()?;
    let listen_addr = config.listen_addr;

    let (store, queue, accepted_batches): (
        Arc<dyn seccloud_lake::object_store::ObjectStore>,
        Arc<dyn seccloud_lake::intake_queue::IntakeQueue>,
        Arc<dyn AcceptedBatchStore>,
    ) = match std::env::var("SECCLOUD_WORKSPACE").ok() {
        Some(root) => {
            tracing::info!(workspace = %root, "using local filesystem storage");
            (
                Arc::new(LocalObjectStore::new(&root)),
                Arc::new(LocalIntakeQueue::new(&root)),
                Arc::new(LocalAcceptedBatchStore::new(&root)),
            )
        }
        None => {
            tracing::info!("using in-memory storage (set SECCLOUD_WORKSPACE for filesystem)");
            (
                Arc::new(InMemoryObjectStore::new()),
                Arc::new(InMemoryIntakeQueue::new()),
                Arc::new(InMemoryAcceptedBatchStore::new()),
            )
        }
    };

    let state = service::AppState {
        config: Arc::new(config),
        store,
        queue,
        accepted_batches,
        load: Arc::new(service::LoadTracker::default()),
    };

    let app = service::build_router(state);

    tracing::info!(%listen_addr, "starting gateway");

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("gateway shut down");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    tracing::info!("received shutdown signal");
}
