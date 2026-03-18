from __future__ import annotations

from typing import Any

from seccloud.storage import Workspace, parse_timestamp


def _default_source_stats() -> dict[str, Any]:
    return {
        "seen_event_types": [],
        "seen_raw_fields": [],
        "raw_event_count": 0,
        "normalized_event_count": 0,
        "dead_letter_count": 0,
        "raw_last_seen_at": None,
        "normalized_last_seen_at": None,
        "recent_window_anchor_at": None,
        "raw_daily_counts": {},
        "normalized_daily_counts": {},
        "dead_letter_daily_counts": {},
        "dead_letter_reason_counts": {},
    }


def _merge_latest_timestamp(current: str | None, candidate: str | None) -> str | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def _increment_bucket(buckets: dict[str, int], timestamp: str | None) -> dict[str, int]:
    if timestamp is None:
        return buckets
    bucket = parse_timestamp(timestamp).strftime("%Y-%m-%d")
    buckets[bucket] = buckets.get(bucket, 0) + 1
    return buckets


def _source_stats(workspace: Workspace, source: str) -> tuple[dict[str, Any], dict[str, Any]]:
    stats = workspace.load_source_stats()
    source_stats = stats.setdefault("sources", {}).setdefault(source, _default_source_stats())
    return stats, source_stats


def record_raw_event(workspace: Workspace, source: str, raw_event: dict[str, Any], *, created: bool) -> None:
    if not created:
        return
    stats, source_stats = _source_stats(workspace, source)
    observed_at = raw_event.get("received_at", raw_event.get("observed_at"))
    source_stats["raw_event_count"] = source_stats.get("raw_event_count", 0) + 1
    source_stats["raw_last_seen_at"] = _merge_latest_timestamp(source_stats.get("raw_last_seen_at"), observed_at)
    source_stats["recent_window_anchor_at"] = _merge_latest_timestamp(
        source_stats.get("recent_window_anchor_at"),
        observed_at,
    )
    source_stats["raw_daily_counts"] = _increment_bucket(
        dict(source_stats.get("raw_daily_counts", {})),
        observed_at,
    )
    seen_fields = set(source_stats.get("seen_raw_fields", []))
    seen_fields.update(raw_event.keys())
    source_stats["seen_raw_fields"] = sorted(seen_fields)
    event_type = raw_event.get("event_type")
    if event_type:
        seen_event_types = set(source_stats.get("seen_event_types", []))
        seen_event_types.add(event_type)
        source_stats["seen_event_types"] = sorted(seen_event_types)
    workspace.save_source_stats(stats)


def record_normalized_event(workspace: Workspace, event: dict[str, Any], *, created: bool) -> None:
    if not created:
        return
    stats, source_stats = _source_stats(workspace, event["source"])
    observed_at = event.get("observed_at")
    source_stats["normalized_event_count"] = source_stats.get("normalized_event_count", 0) + 1
    source_stats["normalized_last_seen_at"] = _merge_latest_timestamp(
        source_stats.get("normalized_last_seen_at"),
        observed_at,
    )
    source_stats["recent_window_anchor_at"] = _merge_latest_timestamp(
        source_stats.get("recent_window_anchor_at"),
        observed_at,
    )
    source_stats["normalized_daily_counts"] = _increment_bucket(
        dict(source_stats.get("normalized_daily_counts", {})),
        observed_at,
    )
    workspace.save_source_stats(stats)


def record_dead_letter(
    workspace: Workspace,
    source: str,
    raw_event: dict[str, Any],
    reason: str,
    *,
    created: bool,
) -> None:
    if not created:
        return
    stats, source_stats = _source_stats(workspace, source)
    observed_at = raw_event.get("received_at", raw_event.get("observed_at"))
    source_stats["dead_letter_count"] = source_stats.get("dead_letter_count", 0) + 1
    source_stats["recent_window_anchor_at"] = _merge_latest_timestamp(
        source_stats.get("recent_window_anchor_at"),
        observed_at,
    )
    source_stats["dead_letter_daily_counts"] = _increment_bucket(
        dict(source_stats.get("dead_letter_daily_counts", {})),
        observed_at,
    )
    reason_counts = dict(source_stats.get("dead_letter_reason_counts", {}))
    reason_counts[reason] = reason_counts.get(reason, 0) + 1
    source_stats["dead_letter_reason_counts"] = reason_counts
    workspace.save_source_stats(stats)


def rebuild_source_stats(workspace: Workspace) -> dict[str, Any]:
    rebuilt = {"sources": {}}
    workspace.save_source_stats(rebuilt)

    for raw_event in workspace.list_raw_events():
        record_raw_event(workspace, raw_event["source"], raw_event, created=True)

    event_index = workspace.ensure_event_index()
    for event in event_index.get("events_by_id", {}).values():
        if not isinstance(event, dict):
            continue
        record_normalized_event(workspace, event, created=True)

    for dead_letter in workspace.list_dead_letters():
        record_dead_letter(
            workspace,
            dead_letter["source"],
            dead_letter["raw_event"],
            dead_letter["reason"],
            created=True,
        )

    stats = workspace.load_source_stats()
    return {
        "source_count": len(stats.get("sources", {})),
        "sources": sorted(stats.get("sources", {}).keys()),
        "window_days": {"raw": 1, "normalized": 1, "dead_letters": 7},
    }
