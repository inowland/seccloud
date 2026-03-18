use std::sync::Arc;
use std::time::Duration;

use seccloud_lake::accepted_batch::AcceptedBatchStore;
use seccloud_lake::intake_queue::IntakeQueue;
use seccloud_lake::object_store::ObjectStore;

use crate::checkpoint::CheckpointStore;
use crate::collector::Collector;
use crate::submit;

/// Result of a single collector job run.
#[derive(Debug)]
pub enum JobResult {
    Submitted {
        batch_id: String,
        record_count: usize,
        idempotency_key: String,
        has_more: bool,
    },
    Idle,
}

/// Run a single collector poll: load checkpoint → poll → submit → save checkpoint.
#[allow(clippy::too_many_arguments)]
pub async fn run_collector_job(
    collector: &dyn Collector,
    tenant_id: &str,
    checkpoint_store: &dyn CheckpointStore,
    object_store: &Arc<dyn ObjectStore>,
    intake_queue: &Arc<dyn IntakeQueue>,
    accepted_batches: &Arc<dyn AcceptedBatchStore>,
    producer_run_id: &str,
    limit: Option<usize>,
) -> anyhow::Result<JobResult> {
    let mut state = checkpoint_store
        .load(
            tenant_id,
            collector.kind(),
            collector.source(),
            collector.integration_id(),
        )
        .await?;

    state.start_attempt();

    let page = match collector.poll(&state.checkpoint, limit).await {
        Ok(p) => p,
        Err(e) => {
            state.record_failure(&e.to_string());
            checkpoint_store.save(&state).await?;
            return Err(e);
        }
    };

    if page.records.is_empty() {
        state.record_idle();
        checkpoint_store.save(&state).await?;
        return Ok(JobResult::Idle);
    }

    // Build idempotency key matching PoC format
    let cursor_start = serde_json::to_string(&state.checkpoint.cursor).unwrap_or_default();
    let cursor_end = serde_json::to_string(&page.next_checkpoint.cursor).unwrap_or_default();
    let idempotency_key = format!(
        "collector:{}:{}:{}:{cursor_start}:{cursor_end}",
        collector.kind(),
        collector.source(),
        collector.integration_id(),
    );

    // Build checkpoint payload for the manifest
    let mut checkpoint_payload = serde_json::Map::new();
    checkpoint_payload.insert(
        "collector_kind".into(),
        serde_json::Value::String(collector.kind().into()),
    );
    checkpoint_payload.insert(
        "checkpoint_start".into(),
        serde_json::to_value(&state.checkpoint)?,
    );
    checkpoint_payload.insert(
        "checkpoint_end".into(),
        serde_json::to_value(&page.next_checkpoint)?,
    );

    match submit::submit_collected_batch(
        object_store,
        intake_queue,
        accepted_batches,
        tenant_id,
        collector.source(),
        collector.integration_id(),
        &page.records,
        &idempotency_key,
        collector.kind(),
        producer_run_id,
        &checkpoint_payload,
    )
    .await
    {
        Ok(result) => {
            state.record_success(
                page.next_checkpoint,
                &result.batch_id,
                &result.idempotency_key,
            );
            checkpoint_store.save(&state).await?;

            tracing::info!(
                collector_kind = collector.kind(),
                source = collector.source(),
                batch_id = %result.batch_id,
                record_count = result.record_count,
                has_more = page.has_more,
                "collector batch submitted"
            );

            Ok(JobResult::Submitted {
                batch_id: result.batch_id,
                record_count: result.record_count,
                idempotency_key: result.idempotency_key,
                has_more: page.has_more,
            })
        }
        Err(e) => {
            state.record_failure(&e.to_string());
            checkpoint_store.save(&state).await?;
            Err(e)
        }
    }
}

/// Run a collector in a loop: poll repeatedly until no more pages, then wait.
#[allow(clippy::too_many_arguments)]
pub async fn run_collector_loop(
    collector: &dyn Collector,
    tenant_id: &str,
    checkpoint_store: &dyn CheckpointStore,
    object_store: &Arc<dyn ObjectStore>,
    intake_queue: &Arc<dyn IntakeQueue>,
    accepted_batches: &Arc<dyn AcceptedBatchStore>,
    producer_run_id: &str,
    poll_interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        if *shutdown.borrow() {
            tracing::info!(collector = collector.kind(), "shutdown received");
            break;
        }

        match run_collector_job(
            collector,
            tenant_id,
            checkpoint_store,
            object_store,
            intake_queue,
            accepted_batches,
            producer_run_id,
            None,
        )
        .await
        {
            Ok(JobResult::Submitted { has_more: true, .. }) => {
                // More pages available — poll immediately
                backoff = Duration::from_secs(1);
                continue;
            }
            Ok(JobResult::Submitted {
                has_more: false, ..
            })
            | Ok(JobResult::Idle) => {
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                tracing::warn!(
                    collector = collector.kind(),
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "collector error, backing off"
                );
                backoff = (backoff * 2).min(max_backoff);
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(poll_interval.max(backoff)) => {}
            _ = shutdown.changed() => {
                tracing::info!(collector = collector.kind(), "shutdown received during sleep");
                break;
            }
        }
    }

    Ok(())
}
