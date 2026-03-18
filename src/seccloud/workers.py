from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from seccloud.detection_context import ensure_detection_context
from seccloud.feature_contract import feature_scoring_contract
from seccloud.ids import new_prefixed_id
from seccloud.pipeline import SOURCE_MAPPING
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
    _update_worker_state(workspace, last_submitted_batch_id=batch["batch_id"])
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


def _update_worker_state(workspace: Workspace, **updates: Any) -> dict[str, Any]:
    worker_state = workspace.load_worker_state()
    worker_state.update(updates)
    workspace.save_worker_state(worker_state)
    return worker_state


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _require_rust_local_runtime() -> None:
    if shutil.which("cargo") is None:
        raise RuntimeError("Rust local runtime requires `cargo` on PATH.")


def _run_rust_normalization_worker(
    workspace: Workspace,
    max_batches: int | None = None,
) -> dict[str, Any]:
    extra_args = ["--tenant-id", workspace.tenant_id]
    if max_batches is not None:
        extra_args.extend(["--max-batches", str(max_batches)])
    result = _run_rust_runtime_command(
        workspace,
        bin_name="seccloud-normalizer",
        extra_args=extra_args,
    )
    return {
        "processed_batch_count": result["processed_batch_count"],
        "processed_batch_ids": result.get("processed_batch_ids", []),
        "landed_record_count": result["raw_event_count"],
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
        "ingest": {
            "raw_events_seen": result["raw_events_seen"],
            "normalized_events_seen": result["normalized_events_seen"],
            "added_raw_events": result["added_raw_events"],
            "added_normalized_events": result["normalized_event_count"],
            "duplicate_semantic_events": result["duplicate_semantic_events"],
            "late_arrival_count": result["late_arrival_count"],
            "dead_letter_count": result["dead_letter_count"],
            "dead_letter_reasons": result["dead_letter_reasons"],
        },
    }


def _print_service_once_summary(result: dict[str, Any]) -> None:
    if result["status"] == "processed":
        norm = result.get("result", {}).get("normalization", {})
        detect = result.get("result", {}).get("detect", {})
        landed = norm.get("landed_record_count", 0)
        ingested = norm.get("ingest", {}).get("added_normalized_events", 0)
        new_det = detect.get("new_detection_count", 0)
        total_det = detect.get("total_detection_count", 0)
        print(f"  normalized {ingested} events ({landed} landed), detections: {new_det} new ({total_det} total)")
        return
    if result["status"] == "projected":
        projection = result.get("result", {}).get("projection", {})
        print(
            f"  projection: {projection.get('event_count', 0)} events, "
            f"{projection.get('detection_count', 0)} detections"
        )
        return
    if result["status"] == "materialized":
        print("  source stats: refreshing...")


def _detection_context_summary(workspace: Workspace) -> dict[str, Any]:
    context = ensure_detection_context(workspace)
    return {
        "event_count": int(context.get("event_count", 0)),
        "context_version": int(context.get("context_version", 1)),
        "input_signature": context.get("input_signature"),
    }


def _run_rust_source_stats_projector(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(workspace, bin_name="seccloud-source-stats")


def _run_rust_feature_builder(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(
        workspace,
        bin_name="seccloud-features",
        extra_args=["--tenant-id", workspace.tenant_id],
    )


def _run_rust_detection_worker(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(workspace, bin_name="seccloud-detections")


def _run_rust_detection_mode_comparison(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(workspace, bin_name="seccloud-detection-compare")


def _run_rust_detection_evaluation(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(workspace, bin_name="seccloud-detection-eval")


def _run_rust_detection_threshold_sweep(workspace: Workspace) -> dict[str, Any]:
    return _run_rust_runtime_command(workspace, bin_name="seccloud-detection-threshold-sweep")


def _resolved_projection_dsn(dsn: str | None) -> str:
    if dsn:
        return dsn
    from seccloud.projection_store import default_projection_dsn

    return default_projection_dsn()


def _run_rust_runtime_command(
    workspace: Workspace,
    *,
    bin_name: str,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "-p",
        "seccloud-ingestion",
        "--bin",
        bin_name,
        "--",
        "--workspace",
        str(workspace.root.resolve()),
    ]
    if extra_args:
        cmd.extend(extra_args)
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info")
    completed = subprocess.run(
        cmd,
        cwd=_repo_root() / "rust",
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details: list[str] = [
            f"Rust runtime command failed for {bin_name} with exit code {completed.returncode}.",
            f"Command: {' '.join(cmd)}",
        ]
        if stderr:
            details.append(f"stderr:\n{stderr}")
        if stdout:
            details.append(f"stdout:\n{stdout}")
        raise RuntimeError("\n".join(details))
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = [
            f"Rust runtime command returned invalid JSON for {bin_name}.",
            f"Command: {' '.join(cmd)}",
        ]
        if stderr:
            details.append(f"stderr:\n{stderr}")
        if stdout:
            details.append(f"stdout:\n{stdout}")
        raise RuntimeError("\n".join(details)) from exc


def _run_rust_projector_worker(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    return _run_rust_runtime_command(
        workspace,
        bin_name="seccloud-projector",
        extra_args=[
            "--tenant-id",
            workspace.tenant_id,
            "--dsn",
            _resolved_projection_dsn(dsn),
        ],
    )


def _run_rust_service_once(
    workspace: Workspace,
    dsn: str | None = None,
    max_batches: int | None = None,
) -> dict[str, Any]:
    extra_args = [
        "--tenant-id",
        workspace.tenant_id,
        "--dsn",
        _resolved_projection_dsn(dsn),
    ]
    if max_batches is not None:
        extra_args.extend(["--max-batches", str(max_batches)])
    return _run_rust_runtime_command(
        workspace,
        bin_name="seccloud-service-once",
        extra_args=extra_args,
    )


def _run_rust_service_loop(
    workspace: Workspace,
    dsn: str | None = None,
    poll_interval_seconds: float = 1.0,
    max_batches: int | None = None,
    max_iterations: int | None = None,
    exit_when_idle: bool = False,
) -> dict[str, Any]:
    extra_args = [
        "--tenant-id",
        workspace.tenant_id,
        "--dsn",
        _resolved_projection_dsn(dsn),
        "--poll-interval-seconds",
        str(poll_interval_seconds),
    ]
    if max_batches is not None:
        extra_args.extend(["--max-batches", str(max_batches)])
    if max_iterations is not None:
        extra_args.extend(["--iterations", str(max_iterations)])
    if exit_when_idle:
        extra_args.append("--exit-when-idle")
    return _run_rust_runtime_command(
        workspace,
        bin_name="seccloud-service-loop",
        extra_args=extra_args,
    )


def run_normalization_worker(
    workspace: Workspace,
    max_batches: int | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    result = _run_rust_normalization_worker(workspace, max_batches=max_batches)
    if result["processed_batch_count"] > 0:
        workspace.clear_source_stats_refresh_request()
    return result


def run_feature_builder(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    result = _run_rust_feature_builder(workspace)
    feature_state = feature_scoring_contract(workspace.load_feature_state() | result)
    workspace.feature_state_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.feature_state_path.write_text(
        json.dumps(feature_state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def run_detection_context_builder(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    return _detection_context_summary(workspace)


def run_detection_worker(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_detection_worker(workspace)


def run_detection_mode_comparison(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_detection_mode_comparison(workspace)


def run_detection_mode_evaluation(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_detection_evaluation(workspace)


def run_detection_threshold_sweep(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_detection_threshold_sweep(workspace)


def run_local_processing_workers(
    workspace: Workspace,
    max_batches: int | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    normalization = run_normalization_worker(workspace, max_batches=max_batches, dsn=dsn)
    features = run_feature_builder(workspace)
    detection = run_detection_worker(workspace)
    detection_context = _detection_context_summary(workspace)
    return {
        "normalization": normalization,
        "features": features,
        "detection_context": detection_context,
        "detect": detection["detect"],
        "ops_metadata": detection["ops_metadata"],
        "scoring_runtime": detection.get("scoring_runtime"),
    }


def run_projector_worker(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_projector_worker(workspace, dsn)


def run_source_stats_projector(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    return _run_rust_source_stats_projector(workspace)


def _source_stats_summary(workspace: Workspace) -> dict[str, Any]:
    stats = workspace.load_source_stats()
    return {
        "source_count": len(stats.get("sources", {})),
        "sources": sorted(stats.get("sources", {}).keys()),
        "window_days": {"raw": 1, "normalized": 1, "dead_letters": 7},
    }


def run_all_local_workers(
    workspace: Workspace,
    dsn: str | None = None,
    max_batches: int | None = None,
) -> dict[str, Any]:
    processing = run_local_processing_workers(workspace, max_batches=max_batches, dsn=dsn)
    source_stats = _source_stats_summary(workspace)
    projection = run_projector_worker(workspace, dsn)
    return {
        "normalization": processing["normalization"],
        "features": processing["features"],
        "detection_context": processing["detection_context"],
        "detect": processing["detect"],
        "ops_metadata": processing["ops_metadata"],
        "source_stats": source_stats,
        "projection": projection,
    }


def run_worker_service_once(
    workspace: Workspace,
    dsn: str | None = None,
    max_batches: int | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    workspace.bootstrap()
    _require_rust_local_runtime()
    response = _run_rust_service_once(workspace, dsn=dsn, max_batches=max_batches)
    if verbose:
        _print_service_once_summary(response)
    return response


def run_worker_service_loop(
    workspace: Workspace,
    dsn: str | None = None,
    poll_interval_seconds: float = 1.0,
    max_batches: int | None = None,
    max_iterations: int | None = None,
    exit_when_idle: bool = False,
) -> dict[str, Any]:
    _require_rust_local_runtime()
    return _run_rust_service_loop(
        workspace,
        dsn=dsn,
        poll_interval_seconds=poll_interval_seconds,
        max_batches=max_batches,
        max_iterations=max_iterations,
        exit_when_idle=exit_when_idle,
    )


def get_worker_state(workspace: Workspace) -> dict[str, Any]:
    state = workspace.load_worker_state()
    state["pending_batch_count"] = len(workspace.list_pending_intake_batches())
    state["processed_batch_count"] = len(workspace.list_processed_intake_batches())
    return state
