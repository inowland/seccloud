from __future__ import annotations

import hashlib
import json
from typing import Any

from seccloud.storage import Workspace, parse_timestamp

CANONICAL_EVENT_STORE = "iceberg_planned_normalized_lake"
CANONICAL_EVENT_FORMAT = "parquet_manifest_bridge"
DETAIL_HYDRATION_STRATEGY = "lake_pointer"


def canonical_event_pointer(workspace: Workspace, event: dict[str, Any]) -> dict[str, Any]:
    manifest = workspace.ensure_normalized_lake_artifacts(event)
    obj = manifest["objects"][0]
    return {
        "pointer_version": 1,
        "object_key": obj["object_key"],
        "object_format": obj["object_format"],
        "sha256": obj["sha256"],
        "manifest_key": workspace._normalized_lake_manifest_key(event),  # noqa: SLF001
        "record_locator": {
            "ordinal": 0,
            "row_group": 0,
        },
        "retention_class": "normalized_retained",
    }


def get_event_by_id(workspace: Workspace, event_id: str) -> dict[str, Any] | None:
    for event in workspace.list_normalized_events():
        if event.get("event_id") == event_id:
            return event
    return None


def hydrate_event_by_pointer(
    workspace: Workspace,
    pointer: dict[str, Any] | None,
    *,
    event_id: str | None = None,
) -> dict[str, Any] | None:
    return workspace.read_normalized_event_by_pointer(pointer, event_id=event_id)


def searchable_event_text(event: dict[str, Any]) -> str:
    values = [
        event.get("event_id"),
        event.get("event_key"),
        event.get("source"),
        event.get("source_event_id"),
        event.get("integration_id"),
        event.get("principal", {}).get("id"),
        event.get("principal", {}).get("entity_id"),
        event.get("principal", {}).get("entity_key"),
        event.get("principal", {}).get("display_name"),
        event.get("principal", {}).get("email"),
        event.get("principal", {}).get("department"),
        event.get("resource", {}).get("id"),
        event.get("resource", {}).get("entity_id"),
        event.get("resource", {}).get("entity_key"),
        event.get("resource", {}).get("name"),
        event.get("resource", {}).get("kind"),
        event.get("resource", {}).get("sensitivity"),
        event.get("action", {}).get("verb"),
        event.get("action", {}).get("category"),
        event.get("attributes", {}).get("geo"),
    ]
    return " ".join(str(value) for value in values if value is not None)


def event_sort_key(event_id: str) -> int:
    digest = hashlib.blake2b(event_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


def build_event_index_document(workspace: Workspace, event: dict[str, Any]) -> dict[str, Any]:
    pointer = canonical_event_pointer(workspace, event)
    return {
        "tenant_id": workspace.tenant_id,
        "event_id": event["event_id"],
        "event_sort_key": event_sort_key(event["event_id"]),
        "event_key": event["event_key"],
        "observed_at": event["observed_at"],
        "source": event["source"],
        "integration_id": event.get("integration_id"),
        "source_event_id": event["source_event_id"],
        "principal_id": event["principal"]["id"],
        "principal_entity_id": event["principal"]["entity_id"],
        "principal_entity_key": event["principal"]["entity_key"],
        "principal_display_name": event["principal"]["display_name"],
        "principal_email": event["principal"]["email"],
        "principal_department": event["principal"]["department"],
        "resource_id": event["resource"]["id"],
        "resource_entity_id": event["resource"]["entity_id"],
        "resource_entity_key": event["resource"]["entity_key"],
        "resource_name": event["resource"]["name"],
        "resource_kind": event["resource"]["kind"],
        "resource_sensitivity": event["resource"]["sensitivity"],
        "action_verb": event["action"]["verb"],
        "action_category": event["action"]["category"],
        "environment_json": json.dumps(event.get("environment", {}), sort_keys=True),
        "search_text": searchable_event_text(event),
        "normalized_pointer": pointer,
        "raw_pointer": event.get("evidence"),
        "raw_retention_state": "unknown",
    }


def scan_events(
    workspace: Workspace,
    *,
    limit: int,
    cursor: tuple[str, str] | None,
    start_time: str | None,
    end_time: str | None,
    query_text: str | None,
    sources: list[str] | None,
    action_categories: list[str] | None,
    sensitivities: list[str] | None,
    principal_reference: str | None,
    resource_reference: str | None,
) -> list[dict[str, Any]]:
    start_at = parse_timestamp(start_time) if start_time is not None else None
    end_at = parse_timestamp(end_time) if end_time is not None else None
    events = sorted(
        workspace.list_normalized_events(),
        key=lambda item: (item.get("observed_at", ""), item.get("event_id", "")),
        reverse=True,
    )
    filtered: list[dict[str, Any]] = []
    for event in events:
        observed_at = event.get("observed_at")
        event_id = event.get("event_id")
        if not isinstance(observed_at, str) or not isinstance(event_id, str):
            continue
        event_ts = parse_timestamp(observed_at)
        if cursor is not None and (observed_at, event_id) >= cursor:
            continue
        if start_at is not None and event_ts < start_at:
            continue
        if end_at is not None and event_ts > end_at:
            continue
        if sources and event.get("source") not in sources:
            continue
        if action_categories and event.get("action", {}).get("category") not in action_categories:
            continue
        if sensitivities and event.get("resource", {}).get("sensitivity") not in sensitivities:
            continue
        if principal_reference is not None and principal_reference not in {
            event.get("principal", {}).get("id"),
            event.get("principal", {}).get("entity_id"),
        }:
            continue
        if resource_reference is not None and resource_reference not in {
            event.get("resource", {}).get("id"),
            event.get("resource", {}).get("entity_id"),
        }:
            continue
        if query_text and query_text.lower() not in searchable_event_text(event).lower():
            continue
        filtered.append(event)
    return filtered[: limit + 1]
