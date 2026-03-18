from __future__ import annotations

from collections import Counter
from typing import Any

from seccloud.storage import Workspace


def build_detection_context(workspace: Workspace) -> dict[str, Any]:
    event_index = workspace.ensure_event_index()
    ordered_events = sorted(
        [event for event in event_index.get("events_by_id", {}).values() if isinstance(event, dict)],
        key=lambda item: (item.get("observed_at", ""), item.get("event_id", "")),
    )
    peer_groups: dict[str, dict[str, Any]] = {}
    aggregate_counts: Counter[str] = Counter()
    contexts_by_event_id: dict[str, dict[str, Any]] = {}
    ordered_event_ids: list[str] = []
    profiles: dict[str, dict[str, Any]] = {}

    for event in ordered_events:
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            continue
        principal = event.get("principal", {})
        resource = event.get("resource", {})
        action = event.get("action", {})
        attributes = event.get("attributes", {})
        principal_id = principal.get("id")
        resource_id = resource.get("id")
        action_verb = action.get("verb")
        peer_group = principal.get("department")
        if not isinstance(principal_id, str) or not isinstance(resource_id, str) or not isinstance(action_verb, str):
            continue
        profile = profiles.setdefault(
            principal_id,
            {
                "prior_event_count": 0,
                "action_counts": {},
                "resource_counts": {},
                "seen_geos": set(),
            },
        )
        peer_group_key = peer_group if isinstance(peer_group, str) and peer_group else "__unknown__"
        peer_state = peer_groups.setdefault(peer_group_key, {"resource_counts": Counter()})
        peer_resource_counts = Counter(peer_state.get("resource_counts", {}))
        geo = attributes.get("geo")
        seen_geos = {item for item in profile.get("seen_geos", set()) if isinstance(item, str) and item}
        contexts_by_event_id[event_id] = {
            "principal_total_events": int(profile.get("prior_event_count", 0)),
            "action_count": int(profile.get("action_counts", {}).get(action_verb, 0)),
            "resource_count": int(profile.get("resource_counts", {}).get(resource_id, 0)),
            "peer_resource_count": int(peer_resource_counts.get(resource_id, 0)),
            "geo_history_count": len(seen_geos),
            "geo_seen_before": isinstance(geo, str) and geo in seen_geos,
            "peer_group": peer_group_key,
            "source": event.get("source"),
        }
        ordered_event_ids.append(event_id)

        profile["prior_event_count"] = int(profile.get("prior_event_count", 0)) + 1
        action_counts = dict(profile.get("action_counts", {}))
        action_counts[action_verb] = int(action_counts.get(action_verb, 0)) + 1
        profile["action_counts"] = action_counts
        resource_counts = dict(profile.get("resource_counts", {}))
        resource_counts[resource_id] = int(resource_counts.get(resource_id, 0)) + 1
        profile["resource_counts"] = resource_counts
        if isinstance(geo, str) and geo:
            seen_geos.add(geo)
        profile["seen_geos"] = seen_geos
        peer_resource_counts[resource_id] += 1
        peer_state["resource_counts"] = dict(peer_resource_counts)
        source = event.get("source")
        if isinstance(source, str) and source:
            aggregate_counts[source] += 1

    payload = {
        "context_version": 1,
        "input_signature": event_index.get("input_signature", ""),
        "event_count": len(ordered_event_ids),
        "ordered_event_ids": ordered_event_ids,
        "contexts_by_event_id": contexts_by_event_id,
        "aggregates": {
            "events_by_source": dict(aggregate_counts),
            "total_events": sum(aggregate_counts.values()),
        },
    }
    workspace.save_detection_context(payload)
    return payload


def ensure_detection_context(workspace: Workspace) -> dict[str, Any]:
    event_index = workspace.ensure_event_index()
    current = workspace.load_detection_context()
    if current.get("input_signature") == event_index.get("input_signature"):
        return current
    return build_detection_context(workspace)
