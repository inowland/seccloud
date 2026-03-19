from __future__ import annotations

from pathlib import Path
from typing import Any

from seccloud.detection_context import ensure_detection_context
from seccloud.event_query import active_event_query_backend, canonical_event_store_status
from seccloud.feature_contract import feature_scoring_contract
from seccloud.feature_lake import load_feature_lake_snapshot
from seccloud.investigation import active_detection_count
from seccloud.local_postgres import postgres_paths
from seccloud.local_quickwit import local_quickwit_index_id, local_quickwit_url, quickwit_runtime_status
from seccloud.model_artifact import build_model_runtime_status
from seccloud.pipeline import collect_ops_metadata, sanitize_ops_metadata
from seccloud.quickwit_client import QuickwitError, get_index_stats
from seccloud.runtime_stream import get_runtime_stream_state
from seccloud.storage import Workspace
from seccloud.workers import get_worker_state


def build_workspace_overview(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    ops_metadata = workspace.load_ops_metadata()
    if not ops_metadata:
        ops_metadata = collect_ops_metadata(workspace)
    stream_state = get_runtime_stream_state(workspace)
    stream_state["detection_count"] = active_detection_count(workspace, dsn=dsn)
    return {
        "stream_state": stream_state,
        "ops_metadata": sanitize_ops_metadata(ops_metadata),
    }


def build_runtime_status(
    workspace: Workspace,
    *,
    dsn: str | None,
    runtime_root: str | Path = ".",
) -> dict[str, Any]:
    feature_snapshot = load_feature_lake_snapshot(workspace)
    feature_state = feature_scoring_contract(feature_snapshot.state)
    feature_vocab = workspace.load_feature_vocab()
    identity_profiles = workspace.load_identity_profiles()
    intake_manifest = workspace.load_intake_manifest()
    event_index = workspace.ensure_event_index()
    detection_context = ensure_detection_context(workspace)
    quickwit_state = workspace.load_quickwit_index_state()
    quickwit_runtime = quickwit_runtime_status(runtime_root)
    overview = build_workspace_overview(workspace, dsn=dsn)
    live_quickwit_state: dict[str, Any] = {}
    if quickwit_runtime.get("ready"):
        try:
            live_quickwit_state = get_index_stats(
                config={
                    "base_url": local_quickwit_url(runtime_root),
                    "index_id": local_quickwit_index_id(),
                }
            )
        except (QuickwitError, OSError, ValueError):
            live_quickwit_state = {}
    paths = postgres_paths(runtime_root)
    return {
        "workspace": str(workspace.root),
        "tenant_id": workspace.tenant_id,
        "stream_state": overview["stream_state"],
        "worker_state": get_worker_state(workspace),
        "feature_state": feature_state,
        "feature_tables": {
            "action_row_count": len(feature_snapshot.action_rows),
            "history_row_count": len(feature_snapshot.history_rows),
            "collaboration_row_count": len(feature_snapshot.collaboration_rows),
            "static_row_count": len(feature_snapshot.static_rows),
            "peer_group_row_count": len(feature_snapshot.peer_group_rows),
        },
        "feature_vocab": {
            "principal_count": len(feature_vocab.get("principal_entity_keys", [])),
            "resource_count": len(feature_vocab.get("resource_entity_keys", [])),
        },
        "scoring_input": {
            "ready": bool(feature_state.get("scoring_input_ready", False)),
            "mode": str(feature_state.get("scoring_input_mode", "python_feature_pipeline_fallback")),
            "reason": str(feature_state.get("scoring_input_reason", "unknown")),
            "materialized_table_count": int(feature_state.get("materialized_table_count", 0)),
            "materialized_tables": list(feature_state.get("materialized_tables", [])),
        },
        "model_runtime": build_model_runtime_status(workspace),
        "event_index": {
            "available": bool(event_index.get("events_by_id")),
            "event_count": int(event_index.get("event_count", 0)),
            "principal_key_count": len(event_index.get("principal_event_ids", {})),
            "resource_key_count": len(event_index.get("resource_event_ids", {})),
            "department_count": len(event_index.get("department_event_ids", {})),
            "input_signature": event_index.get("input_signature"),
        },
        "canonical_event_store": canonical_event_store_status(query_backend=active_event_query_backend()),
        "quickwit": {
            **quickwit_runtime,
            "watermark_at": live_quickwit_state.get("watermark_at", quickwit_state.get("watermark_at")),
            "indexed_event_count": int(
                live_quickwit_state.get(
                    "indexed_event_count",
                    quickwit_state.get("indexed_event_count", 0),
                )
            ),
            "last_sync_completed_at": quickwit_state.get("last_sync_completed_at"),
            "last_sync_duration_ms": quickwit_state.get("last_sync_duration_ms"),
            "last_sync_status": quickwit_state.get("last_sync_status"),
            "last_sync_error": quickwit_state.get("last_sync_error"),
        },
        "detection_context": {
            "available": bool(detection_context.get("ordered_event_ids")),
            "event_count": int(detection_context.get("event_count", 0)),
            "input_signature": detection_context.get("input_signature"),
            "context_version": int(detection_context.get("context_version", 1)),
        },
        "identity_profiles": {
            "available": bool(identity_profiles.get("principals") or identity_profiles.get("teams")),
            "source": identity_profiles.get("source"),
            "principal_count": len(identity_profiles.get("principals", [])),
            "team_count": len(identity_profiles.get("teams", [])),
        },
        "worker_control": workspace.load_worker_control(),
        "intake": {
            "pending_batch_count": len(workspace.list_pending_intake_batches()),
            "processed_batch_count": len(workspace.list_processed_intake_batches()),
            "submitted_batch_count": len(intake_manifest.get("submitted_batch_ids", [])),
            "accepted_idempotency_key_count": len(intake_manifest.get("accepted_batches_by_idempotency_key", {})),
        },
        "postgres": {
            "root": str(Path(runtime_root).resolve()),
            "dsn": dsn,
            "paths": {key: str(value) for key, value in paths.items()},
            "initialized": paths["data"].exists(),
        },
    }
