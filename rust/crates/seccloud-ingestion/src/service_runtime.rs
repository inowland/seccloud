use anyhow::Context;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::detector;
use crate::feature_runtime;
use crate::local_runtime;
use crate::projector;
use crate::runtime::LocalRuntimeContext;
use crate::worker;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ServiceOnceResult {
    pub status: String,
    pub workspace: String,
    pub tenant_id: String,
    pub pending_batch_count_before: usize,
    pub pending_batch_count: usize,
    pub processed_batch_count: usize,
    pub source_stats_refresh_requested: bool,
    pub projection_refresh_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ServiceLoopResult {
    pub workspace: String,
    pub tenant_id: String,
    pub iterations: usize,
    pub processed_iterations: usize,
    pub idle_iterations: usize,
    pub pending_batch_count: usize,
    pub processed_batch_count: usize,
}

fn detection_context_summary(runtime: &LocalRuntimeContext) -> anyhow::Result<Value> {
    let context = detector::ensure_detection_context(runtime.workspace())?;
    Ok(serde_json::json!({
        "event_count": context.event_count,
        "context_version": context.context_version,
        "input_signature": context.input_signature,
    }))
}

fn source_stats_summary(runtime: &LocalRuntimeContext) -> anyhow::Result<Value> {
    let stats = local_runtime::load_source_stats(runtime.workspace())?;
    Ok(local_runtime::source_stats_summary(&stats))
}

fn normalization_summary(
    normalization: &worker::NormalizationWorkerResult,
    pending_batch_count: usize,
) -> Value {
    serde_json::json!({
        "processed_batch_count": normalization.processed_batch_count,
        "processed_batch_ids": normalization.processed_batch_ids,
        "landed_record_count": normalization.raw_event_count,
        "pending_batch_count": pending_batch_count,
        "ingest": {
            "raw_events_seen": normalization.raw_events_seen,
            "normalized_events_seen": normalization.normalized_events_seen,
            "added_raw_events": normalization.added_raw_events,
            "added_normalized_events": normalization.normalized_event_count,
            "duplicate_semantic_events": normalization.duplicate_semantic_events,
            "late_arrival_count": normalization.late_arrival_count,
            "dead_letter_count": normalization.dead_letter_count,
            "dead_letter_reasons": normalization.dead_letter_reasons,
        },
    })
}

fn latest_timestamp<'a>(timestamps: impl IntoIterator<Item = Option<&'a str>>) -> Option<&'a str> {
    timestamps.into_iter().flatten().max()
}

fn refresh_requests_from_state(
    control: &local_runtime::WorkerControlFile,
    worker_state: &local_runtime::WorkerStateFile,
) -> (bool, bool) {
    let latest_materialized = latest_timestamp([
        worker_state.last_normalization_at.as_deref(),
        worker_state.last_feature_at.as_deref(),
        worker_state.last_detection_at.as_deref(),
    ]);
    let source_stats_requested = control.source_stats_refresh_requested
        || latest_materialized.is_some_and(|latest| {
            worker_state
                .last_source_stats_at
                .as_deref()
                .is_none_or(|current| current < latest)
        });
    let projection_requested = control.projection_refresh_requested
        || latest_timestamp([
            latest_materialized,
            worker_state.last_source_stats_at.as_deref(),
        ])
        .is_some_and(|latest| {
            worker_state
                .last_projection_at
                .as_deref()
                .is_none_or(|current| current < latest)
        });
    (source_stats_requested, projection_requested)
}

pub async fn run_service_once(
    runtime: &LocalRuntimeContext,
    max_batches: Option<usize>,
) -> anyhow::Result<ServiceOnceResult> {
    let queue = runtime.intake_queue();
    let store = runtime.object_store();
    let pending_batch_count = queue.list_pending(runtime.tenant_id()).await?.len();
    let control = local_runtime::load_worker_control(runtime.workspace())?;
    let worker_state = local_runtime::load_worker_state(runtime.workspace())?;
    let (source_stats_requested, projection_requested) =
        refresh_requests_from_state(&control, &worker_state);

    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        pending_batch_count_before = pending_batch_count,
        source_stats_refresh_requested = source_stats_requested,
        projection_refresh_requested = projection_requested,
        max_batches = ?max_batches,
        "starting service iteration"
    );

    local_runtime::mark_service_started(runtime.workspace())?;

    if pending_batch_count == 0 && !source_stats_requested && !projection_requested {
        tracing::info!(
            workspace = %runtime.workspace().display(),
            tenant_id = runtime.tenant_id(),
            "service iteration idle"
        );
        local_runtime::finalize_service_status(runtime.workspace(), "idle")?;
        return Ok(ServiceOnceResult {
            status: "idle".to_string(),
            workspace: runtime.workspace().display().to_string(),
            tenant_id: runtime.tenant_id().to_string(),
            pending_batch_count_before: pending_batch_count,
            pending_batch_count: 0,
            processed_batch_count: runtime.count_processed_batches()?,
            source_stats_refresh_requested: source_stats_requested,
            projection_refresh_requested: projection_requested,
            result: None,
        });
    }

    if pending_batch_count == 0 {
        let mut result = Map::new();
        if source_stats_requested {
            tracing::info!(
                workspace = %runtime.workspace().display(),
                tenant_id = runtime.tenant_id(),
                "refreshing source stats"
            );
            let stats = local_runtime::rebuild_source_stats(runtime.workspace())
                .context("source stats rebuild failed")?;
            local_runtime::save_source_stats(runtime.workspace(), &stats)?;
            local_runtime::clear_source_stats_refresh_request(runtime.workspace())?;
            local_runtime::record_source_stats_run(runtime.workspace())?;
            result.insert(
                "source_stats".to_string(),
                local_runtime::source_stats_summary(&stats),
            );
        }
        if projection_requested {
            tracing::info!(
                workspace = %runtime.workspace().display(),
                tenant_id = runtime.tenant_id(),
                "syncing projection"
            );
            let projection = projector::sync_workspace_projection(
                runtime.workspace(),
                runtime.tenant_id(),
                runtime.require_dsn()?,
            )
            .context("projection sync failed")?;
            local_runtime::clear_projection_refresh_request(runtime.workspace())?;
            local_runtime::record_projection_run(runtime.workspace())?;
            result.insert("projection".to_string(), serde_json::to_value(projection)?);
        }
        let status = if projection_requested {
            "projected"
        } else {
            "materialized"
        };
        local_runtime::finalize_service_status(runtime.workspace(), status)?;
        return Ok(ServiceOnceResult {
            status: status.to_string(),
            workspace: runtime.workspace().display().to_string(),
            tenant_id: runtime.tenant_id().to_string(),
            pending_batch_count_before: pending_batch_count,
            pending_batch_count: queue.list_pending(runtime.tenant_id()).await?.len(),
            processed_batch_count: runtime.count_processed_batches()?,
            source_stats_refresh_requested: source_stats_requested,
            projection_refresh_requested: projection_requested,
            result: Some(Value::Object(result)),
        });
    }

    let manifest = local_runtime::load_ingest_manifest(runtime.workspace())?;
    let state = local_runtime::state_from_manifest(manifest);
    let (normalization, next_state) = worker::process_pending_batches_with_state(
        runtime.tenant_id(),
        &store,
        &queue,
        state,
        max_batches,
    )
    .await?;
    local_runtime::save_ingest_manifest(
        runtime.workspace(),
        &local_runtime::manifest_from_state(next_state),
    )?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        processed_batch_count = normalization.processed_batch_count,
        "completed normalization stage"
    );
    let stats = local_runtime::rebuild_source_stats(runtime.workspace())
        .context("source stats rebuild failed")?;
    local_runtime::save_source_stats(runtime.workspace(), &stats)?;
    local_runtime::clear_source_stats_refresh_request(runtime.workspace())?;
    local_runtime::record_source_stats_run(runtime.workspace())?;
    local_runtime::record_normalization_run(
        runtime.workspace(),
        normalization.processed_batch_ids.last().cloned(),
    )?;

    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        "building feature tables"
    );
    let features =
        feature_runtime::build_workspace_feature_tables(runtime.workspace(), runtime.tenant_id())
            .await
            .context("feature build failed")?;
    local_runtime::record_feature_run(runtime.workspace())?;

    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        "building detection context"
    );
    let detection_context = detection_context_summary(runtime)?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        "running detection worker"
    );
    let detection =
        detector::run_detection_worker(runtime.workspace()).context("detection worker failed")?;
    local_runtime::record_detection_run(runtime.workspace())?;

    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        "syncing projection"
    );
    let projection = projector::sync_workspace_projection(
        runtime.workspace(),
        runtime.tenant_id(),
        runtime.require_dsn()?,
    )
    .context("projection sync failed")?;
    local_runtime::clear_projection_refresh_request(runtime.workspace())?;
    local_runtime::record_projection_run(runtime.workspace())?;

    local_runtime::finalize_service_status(runtime.workspace(), "processed")?;
    let pending_after = queue.list_pending(runtime.tenant_id()).await?.len();
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        pending_batch_count_after = pending_after,
        processed_batch_count = runtime.count_processed_batches()?,
        detection_count = detection.detect.total_detection_count,
        "service iteration processed successfully"
    );

    let mut result = Map::new();
    result.insert(
        "normalization".to_string(),
        normalization_summary(&normalization, pending_after),
    );
    result.insert("features".to_string(), serde_json::to_value(features)?);
    result.insert("detection_context".to_string(), detection_context);
    result.insert(
        "detect".to_string(),
        serde_json::to_value(detection.detect)?,
    );
    result.insert(
        "ops_metadata".to_string(),
        serde_json::to_value(detection.ops_metadata)?,
    );
    result.insert("source_stats".to_string(), source_stats_summary(runtime)?);
    result.insert("projection".to_string(), serde_json::to_value(projection)?);

    Ok(ServiceOnceResult {
        status: "processed".to_string(),
        workspace: runtime.workspace().display().to_string(),
        tenant_id: runtime.tenant_id().to_string(),
        pending_batch_count_before: pending_batch_count,
        pending_batch_count: pending_after,
        processed_batch_count: runtime.count_processed_batches()?,
        source_stats_refresh_requested: source_stats_requested,
        projection_refresh_requested: projection_requested,
        result: Some(Value::Object(result)),
    })
}

#[cfg(test)]
mod tests {
    use super::refresh_requests_from_state;
    use crate::local_runtime::{WorkerControlFile, WorkerStateFile};

    #[test]
    fn refresh_requests_trigger_when_projection_and_stats_are_stale() {
        let control = WorkerControlFile::default();
        let worker_state = WorkerStateFile {
            last_normalization_at: Some("2026-03-17T23:45:33Z".into()),
            last_feature_at: Some("2026-03-17T23:45:38Z".into()),
            last_detection_at: Some("2026-03-17T23:45:40Z".into()),
            last_source_stats_at: Some("2026-03-17T23:40:00Z".into()),
            last_projection_at: Some("2026-03-17T23:40:23Z".into()),
            ..WorkerStateFile::default()
        };

        let (source_stats_requested, projection_requested) =
            refresh_requests_from_state(&control, &worker_state);

        assert!(source_stats_requested);
        assert!(projection_requested);
    }

    #[test]
    fn explicit_control_requests_are_preserved() {
        let control = WorkerControlFile {
            projection_refresh_requested: true,
            projection_refresh_requested_at: Some("2026-03-17T23:41:00Z".into()),
            source_stats_refresh_requested: true,
            source_stats_refresh_requested_at: Some("2026-03-17T23:41:00Z".into()),
        };

        let (source_stats_requested, projection_requested) =
            refresh_requests_from_state(&control, &WorkerStateFile::default());

        assert!(source_stats_requested);
        assert!(projection_requested);
    }
}

pub async fn run_service_loop(
    runtime: &LocalRuntimeContext,
    poll_interval_seconds: f64,
    max_batches: Option<usize>,
    iterations: Option<usize>,
    exit_when_idle: bool,
) -> anyhow::Result<ServiceLoopResult> {
    let mut completed_iterations = 0usize;
    let mut processed_iterations = 0usize;
    let mut idle_iterations = 0usize;

    while iterations.is_none_or(|limit| completed_iterations < limit) {
        let result = run_service_once(runtime, max_batches).await?;
        completed_iterations += 1;
        if result.status == "processed" {
            processed_iterations += 1;
        } else if result.status == "idle" {
            idle_iterations += 1;
        }

        if exit_when_idle && result.status == "idle" {
            tracing::info!(
                workspace = %runtime.workspace().display(),
                tenant_id = runtime.tenant_id(),
                completed_iterations,
                "service loop exiting after idle iteration"
            );
            break;
        }
        if iterations.is_some_and(|limit| completed_iterations >= limit) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs_f64(
            poll_interval_seconds.max(0.0),
        ));
    }

    Ok(ServiceLoopResult {
        workspace: runtime.workspace().display().to_string(),
        tenant_id: runtime.tenant_id().to_string(),
        iterations: completed_iterations,
        processed_iterations,
        idle_iterations,
        pending_batch_count: runtime
            .intake_queue()
            .list_pending(runtime.tenant_id())
            .await?
            .len(),
        processed_batch_count: runtime.count_processed_batches()?,
    })
}
