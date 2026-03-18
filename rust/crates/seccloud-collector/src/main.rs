use std::sync::Arc;
use std::time::Duration;

use seccloud_collector::checkpoint::CheckpointStore;
use seccloud_lake::accepted_batch::{
    AcceptedBatchStore, InMemoryAcceptedBatchStore, LocalAcceptedBatchStore,
};
use seccloud_lake::intake_queue::{InMemoryIntakeQueue, LocalIntakeQueue};
use seccloud_lake::object_store::{InMemoryObjectStore, LocalObjectStore};
use tracing_subscriber::EnvFilter;

type SharedObjectStore = Arc<dyn seccloud_lake::object_store::ObjectStore>;
type SharedIntakeQueue = Arc<dyn seccloud_lake::intake_queue::IntakeQueue>;
type SharedAcceptedBatchStore = Arc<dyn AcceptedBatchStore>;
type BoxedCheckpointStore = Box<dyn CheckpointStore>;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let source = std::env::var("SECCLOUD_COLLECTOR_SOURCE").unwrap_or_else(|_| "okta".into());
    let tenant_id = std::env::var("SECCLOUD_TENANT_ID").unwrap_or_else(|_| "local".into());
    let mode = std::env::var("SECCLOUD_COLLECTOR_MODE").unwrap_or_else(|_| "once".into());
    let fixture_dir = std::env::var("SECCLOUD_FIXTURE_DIR")
        .unwrap_or_else(|_| "examples/poc/vendor-fixtures/fixed-source-pack".into());
    let poll_interval_secs: u64 = std::env::var("SECCLOUD_COLLECTOR_POLL_INTERVAL")
        .unwrap_or_else(|_| "10".into())
        .parse()?;

    let (store, queue, accepted_batches, checkpoint_store): (
        SharedObjectStore,
        SharedIntakeQueue,
        SharedAcceptedBatchStore,
        BoxedCheckpointStore,
    ) = match std::env::var("SECCLOUD_WORKSPACE").ok() {
        Some(root) => {
            tracing::info!(workspace = %root, "using local filesystem storage");
            (
                Arc::new(LocalObjectStore::new(&root)),
                Arc::new(LocalIntakeQueue::new(&root)),
                Arc::new(LocalAcceptedBatchStore::new(&root)),
                Box::new(seccloud_collector::checkpoint::FileCheckpointStore::new(
                    &root,
                )),
            )
        }
        None => {
            tracing::info!("using in-memory storage (set SECCLOUD_WORKSPACE for filesystem)");
            (
                Arc::new(InMemoryObjectStore::new()),
                Arc::new(InMemoryIntakeQueue::new()),
                Arc::new(InMemoryAcceptedBatchStore::new()),
                Box::new(seccloud_collector::checkpoint::InMemoryCheckpointStore::new()),
            )
        }
    };
    let producer_run_id = format!("collector_{}", uuid::Uuid::now_v7());

    let fixture_path = |filename: &str| -> std::path::PathBuf {
        std::path::PathBuf::from(&fixture_dir).join(filename)
    };

    let collector: Box<dyn seccloud_collector::collector::Collector> = match source.as_str() {
        "okta" => Box::new(
            seccloud_collector::collectors::okta::OktaFixtureCollector::new(fixture_path(
                "okta_system_log.jsonl",
            )),
        ),
        "github" => Box::new(
            seccloud_collector::collectors::github::GitHubFixtureCollector::new(fixture_path(
                "github_audit_log.jsonl",
            )),
        ),
        "gworkspace" => Box::new(
            seccloud_collector::collectors::gworkspace::GWorkspaceFixtureCollector::new(
                fixture_path("gworkspace_drive_audit.jsonl"),
            ),
        ),
        "snowflake" => Box::new(
            seccloud_collector::collectors::snowflake::SnowflakeFixtureCollector::new(
                fixture_path("snowflake_query_history.jsonl"),
            ),
        ),
        other => anyhow::bail!("unknown collector source: {other}"),
    };

    tracing::info!(
        source = source.as_str(),
        mode = mode.as_str(),
        "starting collector"
    );

    match mode.as_str() {
        "once" => {
            let result = seccloud_collector::runner::run_collector_job(
                collector.as_ref(),
                &tenant_id,
                checkpoint_store.as_ref(),
                &store,
                &queue,
                &accepted_batches,
                &producer_run_id,
                None,
            )
            .await?;

            match result {
                seccloud_collector::runner::JobResult::Submitted {
                    batch_id,
                    record_count,
                    ..
                } => {
                    tracing::info!(%batch_id, record_count, "batch submitted");
                }
                seccloud_collector::runner::JobResult::Idle => {
                    tracing::info!("no records to collect");
                }
            }
        }
        "loop" => {
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutdown signal received");
                shutdown_tx.send(true).ok();
            });

            seccloud_collector::runner::run_collector_loop(
                collector.as_ref(),
                &tenant_id,
                checkpoint_store.as_ref(),
                &store,
                &queue,
                &accepted_batches,
                &producer_run_id,
                Duration::from_secs(poll_interval_secs),
                shutdown_rx,
            )
            .await?;
        }
        other => anyhow::bail!("unknown mode: {other} (use 'once' or 'loop')"),
    }

    Ok(())
}
