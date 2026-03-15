from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from seccloud.storage import Workspace

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


def build_source_capability_matrix(workspace: Workspace) -> dict[str, Any]:
    raw_events = workspace.list_raw_events()
    normalized_events = workspace.list_normalized_events()
    dead_letters = workspace.list_dead_letters()

    raw_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    normalized_by_source = Counter(item["source"] for item in normalized_events)
    dead_letters_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for raw_event in raw_events:
        raw_by_source[raw_event["source"]].append(raw_event)
    for dead_letter in dead_letters:
        dead_letters_by_source[dead_letter["source"]].append(dead_letter)

    sources: dict[str, Any] = {}
    for source, contract in SOURCE_CAPABILITY_CONTRACT.items():
        source_raw_events = raw_by_source.get(source, [])
        seen_event_types = sorted({item.get("event_type", "unknown") for item in source_raw_events})
        required_event_types = contract["required_event_types"]
        required_fields = contract["required_fields"]
        field_coverage = {field: any(field in item for item in source_raw_events) for field in required_fields}
        dead_letter_reason_counts = dict(Counter(item["reason"] for item in dead_letters_by_source.get(source, [])))
        sources[source] = {
            "display_name": contract["display_name"],
            "required_event_types": required_event_types,
            "required_fields": required_fields,
            "seen_event_types": seen_event_types,
            "missing_required_event_types": sorted(set(required_event_types) - set(seen_event_types)),
            "required_field_coverage": field_coverage,
            "missing_required_fields": sorted(field for field, covered in field_coverage.items() if not covered),
            "raw_event_count": len(source_raw_events),
            "normalized_event_count": normalized_by_source.get(source, 0),
            "dead_letter_count": len(dead_letters_by_source.get(source, [])),
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
                f"- Raw events: `{details['raw_event_count']}`",
                f"- Normalized events: `{details['normalized_event_count']}`",
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
            "- This artifact shows what the current PoC expects from each source and whether the generated runtime satisfied those contracts.",
            "- Dead letters indicate source events that were observed but deliberately excluded from normalized analytics because the current product contract could not safely consume them.",
        ]
    )
    return "\n".join(lines) + "\n"
