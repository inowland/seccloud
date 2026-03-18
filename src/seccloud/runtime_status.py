from __future__ import annotations

from pathlib import Path
from typing import Any

from seccloud.detection_context import ensure_detection_context
from seccloud.feature_contract import feature_scoring_contract
from seccloud.feature_lake import load_feature_lake_snapshot
from seccloud.local_postgres import postgres_paths
from seccloud.model_artifact import build_model_runtime_status
from seccloud.runtime_stream import get_runtime_stream_state
from seccloud.storage import Workspace
from seccloud.workers import get_worker_state


def build_runtime_status(
    workspace: Workspace,
    *,
    dsn: str | None,
    runtime_root: str | Path = ".",
) -> dict[str, Any]:
    from seccloud.projection_store import fetch_projection_overview

    feature_snapshot = load_feature_lake_snapshot(workspace)
    feature_state = feature_scoring_contract(feature_snapshot.state)
    feature_vocab = workspace.load_feature_vocab()
    identity_profiles = workspace.load_identity_profiles()
    event_index = workspace.ensure_event_index()
    detection_context = ensure_detection_context(workspace)
    projection: dict[str, Any]
    try:
        projection = {
            "available": True,
            "overview": fetch_projection_overview(dsn),
        }
    except Exception as exc:  # pragma: no cover - depends on local postgres availability
        projection = {
            "available": False,
            "error": str(exc),
        }
    paths = postgres_paths(runtime_root)
    return {
        "workspace": str(workspace.root),
        "tenant_id": workspace.tenant_id,
        "stream_state": get_runtime_stream_state(workspace),
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
        },
        "postgres": {
            "root": str(Path(runtime_root).resolve()),
            "dsn": dsn,
            "paths": {key: str(value) for key, value in paths.items()},
            "initialized": paths["data"].exists(),
        },
        "projection": projection,
    }
