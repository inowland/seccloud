from __future__ import annotations

import hashlib
from collections import Counter
from datetime import timedelta
from typing import Any

from seccloud.contracts import Action, DerivedState, Event, EvidencePointer, Principal, Resource
from seccloud.scoring import build_embedding, score_event
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


def stable_event_id(source: str, source_event_id: str) -> str:
    digest = hashlib.sha256(f"{source}:{source_event_id}".encode()).hexdigest()[:16]
    return f"evt-{digest}"


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
        workspace.write_raw_event(raw_event["source"], raw_event)
    write_json(workspace.synthetic_manifest_path, {"expectations": dataset.expectations})
    return {"raw_event_count": len(dataset.raw_events), "expectations": dataset.expectations}


def normalize_raw_event(raw_event: dict[str, Any], object_key: str) -> Event:
    principal = Principal(
        id=raw_event["actor_email"],
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
        kind=raw_event["resource_kind"],
        provider=source_meta["provider"],
        name=raw_event["resource_name"],
        sensitivity=raw_event["sensitivity"],
        attributes={},
    )
    verb, category = ACTION_MAPPING[(raw_event["source"], raw_event["event_type"])]
    action = Action(source=raw_event["source"], verb=verb, category=category)
    event_id = stable_event_id(raw_event["source"], raw_event["source_event_id"])
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


def ingest_raw_events(workspace: Workspace) -> dict[str, Any]:
    workspace.bootstrap()
    manifest = workspace.load_ingest_manifest()
    ingested_raw_ids = set(manifest["raw_event_ids"])
    normalized_ids = set(manifest["normalized_event_ids"])
    semantic_keys = set(manifest.get("semantic_event_keys", []))
    dead_letter_ids = set(manifest.get("dead_letter_ids", []))

    added_raw = 0
    added_normalized = 0
    duplicate_semantic_events = 0
    dead_letter_count = 0
    late_arrival_count = 0
    dead_letter_reasons: dict[str, int] = {}
    for raw_event in workspace.list_raw_events():
        raw_event_id = raw_event.get("source_event_id", "unknown")
        source = raw_event.get("source", "unknown")
        if raw_event_id not in ingested_raw_ids:
            ingested_raw_ids.add(raw_event_id)
            added_raw += 1
        try:
            validate_raw_event(raw_event)
        except RawEventValidationError as exc:
            reason = str(exc)
            dead_letter_id = f"{source}:{raw_event_id}"
            if dead_letter_id not in dead_letter_ids:
                workspace.save_dead_letter(source, raw_event, reason)
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
        object_key = f"{source}/{received_at:%Y/%m/%d}/{raw_event_id}.json"
        event = normalize_raw_event(raw_event, object_key)
        if event.event_id in normalized_ids:
            continue
        workspace.write_normalized_event(event.to_dict())
        normalized_ids.add(event.event_id)
        added_normalized += 1

    workspace.save_ingest_manifest(
        {
            "raw_event_ids": sorted(ingested_raw_ids),
            "normalized_event_ids": sorted(normalized_ids),
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
    }


def _update_profile(state: dict[str, Any], event: dict[str, Any]) -> None:
    profiles = state.setdefault("principal_profiles", {})
    access_histories = state.setdefault("access_histories", {})
    aggregates = state.setdefault("aggregates", {})
    peer_groups = state.setdefault("peer_groups", {})
    embeddings = state.setdefault("embeddings", {})
    principal_id = event["principal"]["id"]
    peer_group = event["principal"]["department"]
    profile = profiles.setdefault(
        principal_id,
        {
            "total_events": 0,
            "action_counts": {},
            "resource_counts": {},
            "seen_geos": [],
            "peer_group": peer_group,
            "last_seen": None,
        },
    )
    action_counts = Counter(profile["action_counts"])
    resource_counts = Counter(profile["resource_counts"])
    action_counts[event["action"]["verb"]] += 1
    resource_counts[event["resource"]["id"]] += 1
    profile["total_events"] += 1
    profile["action_counts"] = dict(action_counts)
    profile["resource_counts"] = dict(resource_counts)
    profile["last_seen"] = event["observed_at"]
    geo = event["attributes"].get("geo")
    if geo and geo not in profile["seen_geos"]:
        profile["seen_geos"].append(geo)

    peer = peer_groups.setdefault(peer_group, {"resource_counts": {}, "principal_ids": []})
    if principal_id not in peer["principal_ids"]:
        peer["principal_ids"].append(principal_id)
    peer_resource_counts = Counter(peer["resource_counts"])
    peer_resource_counts[event["resource"]["id"]] += 1
    peer["resource_counts"] = dict(peer_resource_counts)

    history = access_histories.setdefault(
        event["resource"]["id"],
        {"resource_name": event["resource"]["name"], "principal_ids": [], "event_ids": []},
    )
    if principal_id not in history["principal_ids"]:
        history["principal_ids"].append(principal_id)
    history["event_ids"].append(event["event_id"])

    aggregates.setdefault("events_by_source", {})
    aggregate_counts = Counter(aggregates["events_by_source"])
    aggregate_counts[event["source"]] += 1
    aggregates["events_by_source"] = dict(aggregate_counts)
    aggregates["total_events"] = sum(aggregate_counts.values())

    peer_resource_count = peer["resource_counts"].get(event["resource"]["id"], 0)
    embeddings[principal_id] = build_embedding(event, profile, peer_resource_count)


def build_derived_state_and_detections(workspace: Workspace) -> dict[str, Any]:
    normalized_events = workspace.list_normalized_events()
    state = DerivedState().to_dict()
    existing_detection_ids = {item["detection_id"] for item in workspace.list_detections()}
    new_detections = 0

    for event in normalized_events:
        detection = score_event(event, state)
        if detection and detection.detection_id not in existing_detection_ids:
            workspace.save_detection(detection.to_dict())
            existing_detection_ids.add(detection.detection_id)
            new_detections += 1
        _update_profile(state, event)

    state.setdefault("case_artifacts", {})
    state.setdefault("feedback_labels", {})
    state.setdefault("metadata", {})
    state["metadata"]["rebuild_source"] = "normalized_segments"
    state["metadata"]["normalized_event_count"] = len(normalized_events)
    workspace.save_derived_state(state)
    return {
        "normalized_event_count": len(normalized_events),
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
    normalized_events = workspace.list_normalized_events()
    sources = Counter(item["source"] for item in normalized_events)
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
    workspace.reset_runtime()
    seed_result = seed_workspace(workspace)
    ingest_result = ingest_raw_events(workspace)
    detection_result = build_derived_state_and_detections(workspace)
    ops_metadata = collect_ops_metadata(workspace)
    return {
        "seed": seed_result,
        "ingest": ingest_result,
        "detect": detection_result,
        "ops_metadata": ops_metadata,
    }
