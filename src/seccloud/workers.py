from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from seccloud.ids import new_prefixed_id
from seccloud.pipeline import (
    SOURCE_MAPPING,
    build_derived_state_and_detections,
    collect_ops_metadata,
    ingest_raw_events,
)
from seccloud.stats_projector import rebuild_source_stats, record_raw_event
from seccloud.storage import Workspace, canonical_json_bytes, sha256_digest


def _now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class IntakeValidationError(ValueError):
    pass


def validate_intake_records(*, source: str, records: list[dict[str, Any]]) -> None:
    if source not in SOURCE_MAPPING:
        raise IntakeValidationError(f"unsupported_source:{source}")
    if not records:
        raise IntakeValidationError("empty_records")
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise IntakeValidationError(f"invalid_record_type:{index}")
        nested_source = record.get("source")
        if nested_source is not None and nested_source != source:
            raise IntakeValidationError(f"record_source_mismatch:{index}")
        for required_field in ("source_event_id", "observed_at"):
            value = record.get(required_field)
            if not isinstance(value, str) or not value:
                raise IntakeValidationError(f"missing_required_field:{required_field}:{index}")


def build_raw_intake_batch(
    *,
    workspace: Workspace,
    source: str,
    records: list[dict[str, Any]],
    intake_kind: str,
    integration_id: str | None = None,
    received_at: str | None = None,
    metadata: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    payload_sha256: str | None = None,
    producer_run_id: str | None = None,
    checkpoint_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validate_intake_records(source=source, records=records)
    normalized_records = [{key: value for key, value in record.items() if key != "source"} for record in records]
    effective_received_at = received_at or _now_timestamp()
    effective_metadata = metadata or {}
    effective_payload_sha256 = payload_sha256 or sha256_digest(
        canonical_json_bytes(
            {
                "tenant_id": workspace.tenant_id,
                "source": source,
                "integration_id": integration_id or "default",
                "received_at": effective_received_at,
                "records": normalized_records,
                "metadata": effective_metadata,
                "intake_kind": intake_kind,
            }
        )
    )
    effective_idempotency_key = idempotency_key or f"push:{effective_payload_sha256}"
    return workspace.land_raw_intake_batch(
        source=source,
        records=normalized_records,
        intake_kind=intake_kind,
        integration_id=integration_id,
        received_at=effective_received_at,
        metadata=effective_metadata,
        idempotency_key=effective_idempotency_key,
        payload_sha256=effective_payload_sha256,
        producer_run_id=producer_run_id or new_prefixed_id("run"),
        checkpoint_payload=checkpoint_payload,
    )


def submit_raw_intake_batch(workspace: Workspace, batch: dict[str, Any]) -> dict[str, Any]:
    worker_state = workspace.load_worker_state()
    worker_state["last_submitted_batch_id"] = batch["batch_id"]
    workspace.save_worker_state(worker_state)
    return {
        "batch_id": batch["batch_id"],
        "tenant_id": batch["tenant_id"],
        "source": batch["source"],
        "integration_id": batch["integration_id"],
        "record_count": batch["record_count"],
        "manifest_key": batch["manifest_key"],
        "object_key": batch["object_key"],
        "idempotency_key": batch["idempotency_key"],
        "duplicate": batch["duplicate"],
        "queue_path": batch["queue_path"],
    }


def submit_raw_events(
    workspace: Workspace,
    *,
    source: str,
    records: list[dict[str, Any]],
    intake_kind: str,
    integration_id: str | None = None,
    received_at: str | None = None,
    metadata: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    payload_sha256: str | None = None,
    producer_run_id: str | None = None,
    checkpoint_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    batch = build_raw_intake_batch(
        workspace=workspace,
        source=source,
        records=records,
        intake_kind=intake_kind,
        integration_id=integration_id,
        received_at=received_at,
        metadata=metadata,
        idempotency_key=idempotency_key,
        payload_sha256=payload_sha256,
        producer_run_id=producer_run_id,
        checkpoint_payload=checkpoint_payload,
    )
    return submit_raw_intake_batch(workspace, batch)


def submit_grouped_raw_events(
    workspace: Workspace,
    *,
    records: list[dict[str, Any]],
    intake_kind: str,
    integration_id: str | None = None,
    received_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    grouped_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped_records[record["source"]].append(record)

    submitted_batches = [
        submit_raw_events(
            workspace,
            source=source,
            records=source_records,
            intake_kind=intake_kind,
            integration_id=integration_id,
            received_at=received_at,
            metadata=metadata,
        )
        for source, source_records in sorted(grouped_records.items())
    ]
    return {
        "batch_count": len(submitted_batches),
        "record_count": len(records),
        "batches": submitted_batches,
    }


def run_okta_fixture_collector(
    workspace: Workspace,
    *,
    fixture_path: str,
    integration_id: str = "okta-primary",
    limit: int | None = None,
) -> dict[str, Any]:
    from seccloud.collectors import OktaSystemLogFixtureCollector, run_collector_job

    adapter = OktaSystemLogFixtureCollector(
        fixture_path=fixture_path,
        integration_id=integration_id,
    )
    return run_collector_job(workspace, adapter=adapter, limit=limit)


def _empty_ingest_result(workspace: Workspace) -> dict[str, Any]:
    manifest = workspace.load_ingest_manifest()
    return {
        "raw_events_seen": len(manifest.get("raw_event_ids", [])),
        "normalized_events_seen": len(manifest.get("normalized_event_ids", [])),
        "added_raw_events": 0,
        "added_normalized_events": 0,
        "duplicate_semantic_events": 0,
        "late_arrival_count": 0,
        "dead_letter_count": 0,
        "dead_letter_reasons": {},
    }


def run_normalization_worker(workspace: Workspace, max_batches: int | None = None) -> dict[str, Any]:
    workspace.bootstrap()
    pending_batches = workspace.list_pending_intake_batches()
    if max_batches is not None:
        pending_batches = pending_batches[:max_batches]

    landed_record_count = 0
    processed_batch_ids: list[str] = []
    for batch in pending_batches:
        raw_manifest = workspace.object_store.get_json(batch["manifest_key"]) if batch.get("manifest_key") else None
        raw_object_descriptor = None
        if raw_manifest is not None:
            raw_object_descriptor = next(
                (item for item in raw_manifest.get("objects", []) if item.get("object_key") == batch.get("object_key")),
                None,
            )
        for envelope in workspace.read_raw_batch_records(batch):
            record = envelope.get("record", {})
            landed_record = {
                "integration_id": envelope.get("integration_id", batch.get("integration_id")),
                "intake_batch_id": batch["batch_id"],
                "intake_kind": envelope.get("intake_kind", batch.get("producer", {}).get("kind")),
                "received_at": envelope.get("received_at", batch.get("received_at")),
                "raw_manifest_key": batch.get("manifest_key"),
                "raw_batch_object_key": batch.get("object_key"),
                "raw_record_ordinal": envelope.get("record_ordinal"),
                "raw_object_sha256": raw_object_descriptor.get("sha256") if raw_object_descriptor else None,
                "raw_object_format": raw_object_descriptor.get("object_format") if raw_object_descriptor else None,
                **record,
            }
            record_source = record.get("source", batch["source"])
            landed_record["source"] = record_source
            _, created = workspace.write_raw_event(record_source, landed_record)
            record_raw_event(workspace, record_source, landed_record, created=created)
            landed_record_count += 1
        workspace.mark_intake_batch_processed(batch["batch_id"])
        processed_batch_ids.append(batch["batch_id"])

    ingest = ingest_raw_events(workspace) if processed_batch_ids else _empty_ingest_result(workspace)
    worker_state = workspace.load_worker_state()
    worker_state["normalization_runs"] = worker_state.get("normalization_runs", 0) + 1
    worker_state["last_processed_batch_id"] = processed_batch_ids[-1] if processed_batch_ids else None
    worker_state["last_normalization_at"] = _now_timestamp()
    workspace.save_worker_state(worker_state)
    return {
        "processed_batch_count": len(processed_batch_ids),
        "processed_batch_ids": processed_batch_ids,
        "landed_record_count": landed_record_count,
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
        "ingest": ingest,
    }


def run_detection_worker(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    detect = build_derived_state_and_detections(workspace)
    ops = collect_ops_metadata(workspace)
    worker_state = workspace.load_worker_state()
    worker_state["detection_runs"] = worker_state.get("detection_runs", 0) + 1
    worker_state["last_detection_at"] = _now_timestamp()
    workspace.save_worker_state(worker_state)
    return {"detect": detect, "ops_metadata": ops}


def run_local_processing_workers(workspace: Workspace, max_batches: int | None = None) -> dict[str, Any]:
    normalization = run_normalization_worker(workspace, max_batches=max_batches)
    detection = run_detection_worker(workspace)
    return {
        "normalization": normalization,
        "detect": detection["detect"],
        "ops_metadata": detection["ops_metadata"],
    }


def run_projector_worker(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    from seccloud.projection_store import sync_workspace_projection

    workspace.bootstrap()
    projection = sync_workspace_projection(workspace, dsn)
    workspace.clear_projection_refresh_request()
    worker_state = workspace.load_worker_state()
    worker_state["projection_runs"] = worker_state.get("projection_runs", 0) + 1
    worker_state["last_projection_at"] = _now_timestamp()
    workspace.save_worker_state(worker_state)
    return projection


def run_source_stats_projector(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    stats = rebuild_source_stats(workspace)
    workspace.clear_source_stats_refresh_request()
    worker_state = workspace.load_worker_state()
    worker_state["source_stats_runs"] = worker_state.get("source_stats_runs", 0) + 1
    worker_state["last_source_stats_at"] = _now_timestamp()
    workspace.save_worker_state(worker_state)
    return stats


def run_all_local_workers(
    workspace: Workspace,
    dsn: str | None = None,
    max_batches: int | None = None,
) -> dict[str, Any]:
    processing = run_local_processing_workers(workspace, max_batches=max_batches)
    source_stats = run_source_stats_projector(workspace)
    projection = run_projector_worker(workspace, dsn)
    return {
        "normalization": processing["normalization"],
        "detect": processing["detect"],
        "ops_metadata": processing["ops_metadata"],
        "source_stats": source_stats,
        "projection": projection,
    }


def run_worker_service_once(
    workspace: Workspace,
    dsn: str | None = None,
    max_batches: int | None = None,
) -> dict[str, Any]:
    workspace.bootstrap()
    pending_batch_count = len(workspace.list_pending_intake_batches())
    source_stats_requested = workspace.source_stats_refresh_requested()
    projection_requested = workspace.projection_refresh_requested()
    worker_state = workspace.load_worker_state()
    worker_state["service_runs"] = worker_state.get("service_runs", 0) + 1
    worker_state["last_service_at"] = _now_timestamp()
    workspace.save_worker_state(worker_state)

    if pending_batch_count == 0 and not source_stats_requested and not projection_requested:
        worker_state["last_service_status"] = "idle"
        workspace.save_worker_state(worker_state)
        return {
            "status": "idle",
            "pending_batch_count": 0,
            "processed_batch_count": len(workspace.list_processed_intake_batches()),
        }

    if pending_batch_count == 0:
        result: dict[str, Any] = {}
        if source_stats_requested:
            result["source_stats"] = run_source_stats_projector(workspace)
        if projection_requested:
            result["projection"] = run_projector_worker(workspace, dsn)
        status = "projected" if projection_requested else "materialized"
    else:
        result = run_all_local_workers(workspace, dsn=dsn, max_batches=max_batches)
        status = "processed"
    worker_state = workspace.load_worker_state()
    worker_state["last_service_at"] = _now_timestamp()
    worker_state["last_service_status"] = status
    workspace.save_worker_state(worker_state)
    return {
        "status": status,
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
        "processed_batch_count": len(workspace.list_processed_intake_batches()),
        "result": result,
    }


def run_worker_service_loop(
    workspace: Workspace,
    dsn: str | None = None,
    poll_interval_seconds: float = 1.0,
    max_batches: int | None = None,
    max_iterations: int | None = None,
    exit_when_idle: bool = False,
) -> dict[str, Any]:
    iterations = 0
    processed_iterations = 0
    idle_iterations = 0

    while max_iterations is None or iterations < max_iterations:
        result = run_worker_service_once(workspace, dsn=dsn, max_batches=max_batches)
        iterations += 1
        if result["status"] == "processed":
            processed_iterations += 1
        else:
            idle_iterations += 1
        if exit_when_idle and result["status"] == "idle":
            break
        if max_iterations is not None and iterations >= max_iterations:
            break
        time.sleep(max(poll_interval_seconds, 0))

    return {
        "iterations": iterations,
        "processed_iterations": processed_iterations,
        "idle_iterations": idle_iterations,
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
        "processed_batch_count": len(workspace.list_processed_intake_batches()),
    }


def get_worker_state(workspace: Workspace) -> dict[str, Any]:
    state = workspace.load_worker_state()
    state["pending_batch_count"] = len(workspace.list_pending_intake_batches())
    state["processed_batch_count"] = len(workspace.list_processed_intake_batches())
    return state
