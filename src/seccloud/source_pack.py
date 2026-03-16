from __future__ import annotations

from datetime import timedelta
from typing import Any

from seccloud.stats_projector import rebuild_source_stats
from seccloud.storage import Workspace, parse_timestamp

SOURCE_CAPABILITY_CONTRACT: dict[str, dict[str, Any]] = {
    "okta": {
        "display_name": "Okta",
        "required_event_types": ["login"],
        "required_fields": ["geo", "ip", "privileged"],
    },
    "gworkspace": {
        "display_name": "Google Workspace",
        "required_event_types": ["view", "share_external"],
        "required_fields": ["external", "resource_kind", "resource_name"],
    },
    "github": {
        "display_name": "GitHub",
        "required_event_types": ["view", "archive_download"],
        "required_fields": ["bytes_transferred_mb", "resource_kind", "resource_name"],
    },
    "snowflake": {
        "display_name": "Snowflake",
        "required_event_types": ["query", "export"],
        "required_fields": ["rows_read", "warehouse", "resource_name"],
    },
}


def _count_recent_buckets(buckets: dict[str, int], anchor: str | None, *, days: int) -> int:
    if anchor is None:
        return 0
    anchor_date = parse_timestamp(anchor).date()
    window_dates = {(anchor_date - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days)}
    return sum(count for bucket, count in buckets.items() if bucket in window_dates)


def build_source_capability_matrix(workspace: Workspace) -> dict[str, Any]:
    source_stats = workspace.load_source_stats().get("sources", {})
    has_data = (
        workspace.load_ingest_manifest().get("normalized_event_ids")
        or workspace.list_raw_events()
        or workspace.list_dead_letters()
    )
    if not source_stats and has_data:
        rebuild_source_stats(workspace)
        source_stats = workspace.load_source_stats().get("sources", {})

    sources: dict[str, Any] = {}
    for source, contract in SOURCE_CAPABILITY_CONTRACT.items():
        stats = source_stats.get(source, {})
        seen_event_types = sorted(stats.get("seen_event_types", []))
        required_event_types = contract["required_event_types"]
        required_fields = contract["required_fields"]
        seen_raw_fields = set(stats.get("seen_raw_fields", []))
        field_coverage = {field: field in seen_raw_fields for field in required_fields}
        dead_letter_reason_counts = stats.get("dead_letter_reason_counts", {})
        raw_last_seen_at = stats.get("raw_last_seen_at")
        normalized_last_seen_at = stats.get("normalized_last_seen_at")
        recent_window_anchor_at = stats.get("recent_window_anchor_at")
        raw_daily_counts = stats.get("raw_daily_counts", {})
        normalized_daily_counts = stats.get("normalized_daily_counts", {})
        dead_letter_daily_counts = stats.get("dead_letter_daily_counts", {})
        sources[source] = {
            "display_name": contract["display_name"],
            "required_event_types": required_event_types,
            "required_fields": required_fields,
            "seen_event_types": seen_event_types,
            "missing_required_event_types": sorted(set(required_event_types) - set(seen_event_types)),
            "required_field_coverage": field_coverage,
            "missing_required_fields": sorted(field for field, covered in field_coverage.items() if not covered),
            "recent_window_anchor_at": recent_window_anchor_at,
            "raw_last_seen_at": raw_last_seen_at,
            "normalized_last_seen_at": normalized_last_seen_at,
            "raw_24h_count": _count_recent_buckets(raw_daily_counts, recent_window_anchor_at, days=1),
            "normalized_24h_count": _count_recent_buckets(
                normalized_daily_counts,
                recent_window_anchor_at,
                days=1,
            ),
            "dead_letter_7d_count": _count_recent_buckets(
                dead_letter_daily_counts,
                recent_window_anchor_at,
                days=7,
            ),
            "raw_event_count": stats.get("raw_event_count", 0),
            "normalized_event_count": stats.get("normalized_event_count", 0),
            "dead_letter_count": stats.get("dead_letter_count", 0),
            "dead_letter_reason_counts": dead_letter_reason_counts,
        }

    return {
        "source_pack": list(SOURCE_CAPABILITY_CONTRACT.keys()),
        "sources": sources,
    }


def build_source_capability_markdown(workspace: Workspace) -> str:
    matrix = build_source_capability_matrix(workspace)
    lines = [
        "# Source Capability Matrix",
        "",
        "## Fixed PoC Source Pack",
        "- Okta",
        "- Google Workspace",
        "- GitHub",
        "- Snowflake",
        "",
        "## Capability Status",
    ]
    for source, details in matrix["sources"].items():
        lines.extend(
            [
                f"### {details['display_name']} (`{source}`)",
                f"- Recent window anchor: `{details['recent_window_anchor_at']}`",
                f"- Last raw seen: `{details['raw_last_seen_at']}`",
                f"- Last normalized seen: `{details['normalized_last_seen_at']}`",
                f"- Raw events in latest 24h slice: `{details['raw_24h_count']}`",
                f"- Normalized events in latest 24h slice: `{details['normalized_24h_count']}`",
                f"- Dead letters in latest 7d slice: `{details['dead_letter_7d_count']}`",
                f"- Dead letters: `{details['dead_letter_count']}`",
                f"- Required event types: `{details['required_event_types']}`",
                f"- Seen event types: `{details['seen_event_types']}`",
                f"- Missing required event types: `{details['missing_required_event_types']}`",
                f"- Missing required fields: `{details['missing_required_fields']}`",
                f"- Dead-letter reasons: `{details['dead_letter_reason_counts']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            (
                "- This artifact shows what the current PoC expects from each source and whether"
                " the generated runtime satisfied those contracts."
            ),
            (
                "- Dead letters indicate source events that were observed but deliberately excluded"
                " from normalized analytics because the current product contract could not safely"
                " consume them."
            ),
        ]
    )
    return "\n".join(lines) + "\n"
