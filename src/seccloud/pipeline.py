from __future__ import annotations

from collections import Counter
from datetime import timedelta
from typing import Any

from seccloud.contracts import Action, Event, EvidencePointer, Principal, Resource
from seccloud.detection_context import ensure_detection_context
from seccloud.ids import entity_key, event_key
from seccloud.scoring import DetectionBaseline, score_event
from seccloud.stats_projector import record_dead_letter, record_normalized_event, record_raw_event
from seccloud.storage import Workspace, parse_timestamp, write_json
from seccloud.synthetic import generate_synthetic_dataset

SOURCE_MAPPING = {
    "okta": {"provider": "okta", "kind": "identity"},
    "gworkspace": {"provider": "google-workspace", "kind": "document"},
    "github": {"provider": "github", "kind": "repo"},
    "snowflake": {"provider": "snowflake", "kind": "dataset"},
}

ACTION_MAPPING = {
    ("okta", "login"): ("login", "authentication"),
    ("okta", "admin_change"): ("admin_change", "administration"),
    ("gworkspace", "view"): ("read", "access"),
    ("gworkspace", "share_external"): ("share_external", "sharing"),
    ("github", "view"): ("read", "access"),
    ("github", "clone"): ("clone", "code_access"),
    ("github", "archive_download"): ("archive_download", "code_access"),
    ("snowflake", "query"): ("query", "data_access"),
    ("snowflake", "export"): ("export", "data_access"),
}

REQUIRED_RAW_FIELDS = {
    "source",
    "source_event_id",
    "observed_at",
    "actor_email",
    "actor_name",
    "department",
    "role",
    "event_type",
    "resource_id",
    "resource_name",
    "resource_kind",
    "sensitivity",
}

LATE_ARRIVAL_THRESHOLD = timedelta(hours=12)


class RawEventValidationError(ValueError):
    pass


def semantic_event_key(raw_event: dict[str, Any]) -> str:
    parts = (
        raw_event["source"],
        raw_event["actor_email"],
        raw_event["event_type"],
        raw_event["resource_id"],
        raw_event["observed_at"],
    )
    return "|".join(parts)


def validate_raw_event(raw_event: dict[str, Any]) -> None:
    missing = sorted(REQUIRED_RAW_FIELDS - raw_event.keys())
    if missing:
        raise RawEventValidationError(f"missing_required_fields:{','.join(missing)}")
    if raw_event["source"] not in SOURCE_MAPPING:
        raise RawEventValidationError("unsupported_source")
    if (raw_event["source"], raw_event["event_type"]) not in ACTION_MAPPING:
        raise RawEventValidationError("unsupported_event_type")


def seed_workspace(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    dataset = generate_synthetic_dataset()
    for raw_event in dataset.raw_events:
        _, created = workspace.write_raw_event(raw_event["source"], raw_event)
        record_raw_event(workspace, raw_event["source"], raw_event, created=created)
    write_json(workspace.synthetic_manifest_path, {"expectations": dataset.expectations})
    return {"raw_event_count": len(dataset.raw_events), "expectations": dataset.expectations}


def normalize_raw_event(workspace: Workspace, raw_event: dict[str, Any], object_key: str) -> Event:
    principal_entity_key = entity_key(
        entity_kind="principal",
        source=raw_event["source"],
        native_id=raw_event["actor_email"],
        provider=raw_event["source"],
    )
    resource_entity_key = entity_key(
        entity_kind="resource",
        source=raw_event["source"],
        native_id=raw_event["resource_id"],
        provider=SOURCE_MAPPING[raw_event["source"]]["provider"],
    )
    principal = Principal(
        id=raw_event["actor_email"],
        entity_id=workspace.allocate_entity_id(principal_entity_key),
        entity_key=principal_entity_key,
        kind="human",
        provider=raw_event["source"],
        email=raw_event["actor_email"],
        display_name=raw_event["actor_name"],
        department=raw_event["department"],
        attributes={"role": raw_event["role"]},
    )
    source_meta = SOURCE_MAPPING[raw_event["source"]]
    resource = Resource(
        id=raw_event["resource_id"],
        entity_id=workspace.allocate_entity_id(resource_entity_key),
        entity_key=resource_entity_key,
        kind=raw_event["resource_kind"],
        provider=source_meta["provider"],
        name=raw_event["resource_name"],
        sensitivity=raw_event["sensitivity"],
        attributes={},
    )
    verb, category = ACTION_MAPPING[(raw_event["source"], raw_event["event_type"])]
    action = Action(source=raw_event["source"], verb=verb, category=category)
    stable_key = event_key(
        raw_event["source"],
        raw_event["source_event_id"],
        integration_id=raw_event.get("integration_id"),
    )
    event_id = workspace.allocate_event_id(stable_key)
    attributes = {
        key: value
        for key, value in raw_event.items()
        if key
        not in {
            "source",
            "source_event_id",
            "observed_at",
            "actor_email",
            "actor_name",
            "department",
            "role",
            "event_type",
            "resource_id",
            "resource_name",
            "resource_kind",
            "sensitivity",
        }
    }
    evidence = EvidencePointer(
        source=raw_event["source"],
        object_key=object_key,
        raw_event_id=raw_event["source_event_id"],
        observed_at=raw_event["observed_at"],
    )
    return Event(
        event_id=event_id,
        event_key=stable_key,
        integration_id=raw_event.get("integration_id"),
        source=raw_event["source"],
        source_event_id=raw_event["source_event_id"],
        principal=principal,
        resource=resource,
        action=action,
        observed_at=raw_event["observed_at"],
        environment={"source_kind": source_meta["kind"]},
        attributes=attributes,
        evidence=evidence,
    )


def ingest_raw_events(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    workspace.bootstrap()
    del dsn
    manifest = workspace.load_ingest_manifest()
    ingested_raw_ids = set(manifest["raw_event_ids"])
    ingested_raw_keys = set(manifest.get("raw_event_keys", []))
    normalized_ids = set(manifest["normalized_event_ids"])
    normalized_keys = set(manifest.get("normalized_event_keys", []))
    semantic_keys = set(manifest.get("semantic_event_keys", []))
    dead_letter_ids = set(manifest.get("dead_letter_ids", []))

    added_raw = 0
    added_normalized = 0
    duplicate_semantic_events = 0
    dead_letter_count = 0
    late_arrival_count = 0
    dead_letter_reasons: dict[str, int] = {}
    batch_events: list[dict[str, Any]] = []

    for raw_event in workspace.list_raw_events():
        raw_event_id = raw_event.get("source_event_id", "unknown")
        source = raw_event.get("source", "unknown")
        raw_key = event_key(source, raw_event_id, integration_id=raw_event.get("integration_id"))
        if raw_event_id not in ingested_raw_ids:
            ingested_raw_ids.add(raw_event_id)
            added_raw += 1
        ingested_raw_keys.add(raw_key)
        try:
            validate_raw_event(raw_event)
        except RawEventValidationError as exc:
            reason = str(exc)
            dead_letter_id = f"{source}:{raw_key}"
            if dead_letter_id not in dead_letter_ids:
                _, created = workspace.save_dead_letter(source, raw_event, reason)
                record_dead_letter(workspace, source, raw_event, reason, created=created)
                dead_letter_ids.add(dead_letter_id)
                dead_letter_count += 1
            dead_letter_reasons[reason] = dead_letter_reasons.get(reason, 0) + 1
            continue
        semantic_key = semantic_event_key(raw_event)
        if semantic_key in semantic_keys:
            duplicate_semantic_events += 1
            continue
        semantic_keys.add(semantic_key)
        observed_at = parse_timestamp(raw_event["observed_at"])
        received_at = parse_timestamp(raw_event.get("received_at", raw_event["observed_at"]))
        if received_at - observed_at > LATE_ARRIVAL_THRESHOLD:
            late_arrival_count += 1
        object_key = workspace.raw_object_key(source, raw_event)
        event = normalize_raw_event(workspace, raw_event, object_key)
        if event.event_key in normalized_keys:
            continue
        normalized_keys.add(event.event_key)
        normalized_ids.add(event.event_id)
        event_payload = event.to_dict()
        workspace.write_normalized_event(event_payload)
        record_normalized_event(workspace, event_payload, created=True)
        batch_events.append(event_payload)
        added_normalized += 1

    quickwit_result = None
    if batch_events:
        from seccloud.quickwit_index import index_canonical_event_batch

        quickwit_result = index_canonical_event_batch(workspace, batch_events)

    workspace.save_ingest_manifest(
        {
            "raw_event_ids": sorted(ingested_raw_ids),
            "raw_event_keys": sorted(ingested_raw_keys),
            "normalized_event_ids": sorted(normalized_ids),
            "normalized_event_keys": sorted(normalized_keys),
            "semantic_event_keys": sorted(semantic_keys),
            "dead_letter_ids": sorted(dead_letter_ids),
        }
    )
    return {
        "raw_events_seen": len(ingested_raw_ids),
        "normalized_events_seen": len(normalized_ids),
        "added_raw_events": added_raw,
        "added_normalized_events": added_normalized,
        "duplicate_semantic_events": duplicate_semantic_events,
        "late_arrival_count": late_arrival_count,
        "dead_letter_count": dead_letter_count,
        "dead_letter_reasons": dead_letter_reasons,
        "quickwit": quickwit_result,
    }


def _compact_derived_state(
    *,
    aggregates: dict[str, Any],
    case_artifacts: dict[str, Any],
    feedback_labels: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "principal_profiles": {},
        "peer_groups": {},
        "access_histories": {},
        "aggregates": dict(aggregates),
        "embeddings": {},
        "case_artifacts": dict(case_artifacts),
        "feedback_labels": dict(feedback_labels),
        "metadata": dict(metadata),
    }


def build_derived_state_and_detections(workspace: Workspace) -> dict[str, Any]:
    # Heuristic scoring path backed by persisted event context.
    event_index = workspace.ensure_event_index()
    detection_context = ensure_detection_context(workspace)
    prior_state = workspace.load_derived_state()
    existing_detection_ids = {item["detection_id"] for item in workspace.list_detections()}
    new_detections = 0

    for event_id in detection_context.get("ordered_event_ids", []):
        event = event_index.get("events_by_id", {}).get(event_id)
        if not isinstance(event, dict):
            continue
        context = detection_context.get("contexts_by_event_id", {}).get(event_id, {})
        baseline = DetectionBaseline(
            prior_event_count=int(context.get("principal_total_events", 0)),
            prior_action_count=int(context.get("action_count", 0)),
            prior_resource_count=int(context.get("resource_count", 0)),
            peer_resource_count=int(context.get("peer_resource_count", 0)),
            geo_history_count=int(context.get("geo_history_count", 0)),
            geo_seen_before=bool(context.get("geo_seen_before", False)),
        )
        detection = score_event(event, baseline)
        if detection and detection.detection_id not in existing_detection_ids:
            workspace.save_detection(detection.to_dict())
            existing_detection_ids.add(detection.detection_id)
            new_detections += 1

    metadata = dict(prior_state.get("metadata", {}))
    metadata["rebuild_source"] = "event_index_detection_context"
    metadata["normalized_event_count"] = int(detection_context.get("event_count", 0))
    metadata["detection_context_version"] = int(detection_context.get("context_version", 1))
    workspace.save_derived_state(
        _compact_derived_state(
            aggregates=dict(detection_context.get("aggregates", {})),
            case_artifacts=dict(prior_state.get("case_artifacts", {})),
            feedback_labels=dict(prior_state.get("feedback_labels", {})),
            metadata=metadata,
        )
    )
    return {
        "normalized_event_count": int(detection_context.get("event_count", 0)),
        "new_detection_count": new_detections,
        "total_detection_count": len(existing_detection_ids),
    }


def rebuild_derived_state(workspace: Workspace) -> dict[str, Any]:
    return build_derived_state_and_detections(workspace)


def sanitize_ops_metadata(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    return {
        "workspace": payload.get("workspace"),
        "event_counts_by_source": dict(payload.get("event_counts_by_source", {})),
        "dead_letter_count": payload.get("dead_letter_count", 0),
        "dead_letter_counts_by_source": dict(payload.get("dead_letter_counts_by_source", {})),
        "contains_raw_payloads": payload.get("contains_raw_payloads", False),
    }


def collect_ops_metadata(workspace: Workspace) -> dict[str, Any]:
    source_stats = workspace.load_source_stats().get("sources", {})
    sources = {s: stats.get("normalized_event_count", 0) for s, stats in source_stats.items()}
    dead_letters = workspace.list_dead_letters()
    metadata = sanitize_ops_metadata(
        {
            "workspace": str(workspace.root),
            "event_counts_by_source": dict(sources),
            "dead_letter_count": len(dead_letters),
            "dead_letter_counts_by_source": dict(Counter(item["source"] for item in dead_letters)),
            "contains_raw_payloads": False,
        }
    )
    workspace.save_ops_metadata(metadata)
    return metadata


def run_runtime(workspace: Workspace) -> dict[str, Any]:
    from seccloud.workers import run_local_processing_workers, submit_grouped_raw_events

    workspace.reset_runtime()
    dataset = generate_synthetic_dataset()
    seed_result = submit_grouped_raw_events(
        workspace,
        records=dataset.raw_events,
        intake_kind="synthetic_seed",
        integration_id="synthetic-dataset",
    )
    seed_result["raw_event_count"] = len(dataset.raw_events)
    seed_result["expectations"] = dataset.expectations
    write_json(workspace.synthetic_manifest_path, {"expectations": dataset.expectations})
    worker_result = run_local_processing_workers(workspace)
    return {
        "seed": seed_result,
        "ingest": worker_result["normalization"]["ingest"],
        "detect": worker_result["detect"],
        "ops_metadata": worker_result["ops_metadata"],
    }
