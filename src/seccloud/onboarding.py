from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from seccloud.pipeline import validate_raw_event
from seccloud.source_pack import SOURCE_CAPABILITY_CONTRACT, build_source_capability_matrix
from seccloud.storage import Workspace


def build_source_manifest() -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    for source, contract in SOURCE_CAPABILITY_CONTRACT.items():
        sources.append(
            {
                "source": source,
                "display_name": contract["display_name"],
                "fixture_file": f"{source}.jsonl",
                "required_event_types": contract["required_event_types"],
                "required_fields": contract["required_fields"],
                "mapping_version": "raw-event-v1",
            }
        )
    return {
        "source_pack": [item["source"] for item in sources],
        "mapping_version": "raw-event-v1",
        "sources": sources,
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def validate_fixture_bundle(fixtures_dir: str | Path) -> dict[str, Any]:
    root = Path(fixtures_dir)
    manifest = build_source_manifest()
    validation: dict[str, Any] = {
        "fixtures_dir": str(root),
        "mapping_version": manifest["mapping_version"],
        "sources": {},
        "summary": {
            "passes": True,
            "missing_sources": [],
            "invalid_event_count": 0,
            "valid_event_count": 0,
        },
    }

    for source_manifest in manifest["sources"]:
        source = source_manifest["source"]
        path = root / source_manifest["fixture_file"]
        events = _read_jsonl(path)
        seen_event_types = sorted({event.get("event_type", "unknown") for event in events})
        invalid_events: list[dict[str, Any]] = []
        for event in events:
            try:
                validate_raw_event(event)
            except ValueError as exc:
                invalid_events.append(
                    {
                        "source_event_id": event.get("source_event_id", "unknown"),
                        "reason": str(exc),
                    }
                )
        missing_required_event_types = sorted(set(source_manifest["required_event_types"]) - set(seen_event_types))
        source_result = {
            "fixture_file": str(path),
            "present": path.exists(),
            "event_count": len(events),
            "valid_event_count": len(events) - len(invalid_events),
            "invalid_event_count": len(invalid_events),
            "seen_event_types": seen_event_types,
            "missing_required_event_types": missing_required_event_types,
            "invalid_events": invalid_events,
        }
        validation["sources"][source] = source_result
        validation["summary"]["valid_event_count"] += source_result["valid_event_count"]
        validation["summary"]["invalid_event_count"] += source_result["invalid_event_count"]
        if not path.exists():
            validation["summary"]["missing_sources"].append(source)
            validation["summary"]["passes"] = False
        if missing_required_event_types or invalid_events:
            validation["summary"]["passes"] = False

    return validation


def import_fixture_bundle(workspace: Workspace, fixtures_dir: str | Path) -> dict[str, Any]:
    validation = validate_fixture_bundle(fixtures_dir)
    imported_count = 0
    skipped_invalid_count = 0
    for source, details in validation["sources"].items():
        if not details["present"]:
            continue
        invalid_event_ids = {item["source_event_id"] for item in details["invalid_events"]}
        for event in _read_jsonl(Path(details["fixture_file"])):
            if event.get("source_event_id", "unknown") in invalid_event_ids:
                skipped_invalid_count += 1
                continue
            workspace.write_raw_event(source, event)
            imported_count += 1
    return {
        "validation": validation,
        "imported_event_count": imported_count,
        "skipped_invalid_event_count": skipped_invalid_count,
    }


def build_onboarding_report_markdown(fixtures_dir: str | Path) -> str:
    validation = validate_fixture_bundle(fixtures_dir)
    lines = [
        "# Source Onboarding Report",
        "",
        f"- Fixture bundle: `{validation['fixtures_dir']}`",
        f"- Validation status: `{'pass' if validation['summary']['passes'] else 'fail'}`",
        f"- Valid events: `{validation['summary']['valid_event_count']}`",
        f"- Invalid events: `{validation['summary']['invalid_event_count']}`",
        f"- Missing sources: `{validation['summary']['missing_sources']}`",
        "",
        "## Source Results",
    ]
    for source, details in validation["sources"].items():
        lines.extend(
            [
                f"### `{source}`",
                f"- Fixture present: `{details['present']}`",
                f"- Event count: `{details['event_count']}`",
                f"- Valid events: `{details['valid_event_count']}`",
                f"- Invalid events: `{details['invalid_event_count']}`",
                f"- Seen event types: `{details['seen_event_types']}`",
                f"- Missing required event types: `{details['missing_required_event_types']}`",
                f"- Invalid events: `{details['invalid_events']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            "- This report is meant to answer whether a candidate customer source pack satisfies the current product contract before ingestion and scoring begin.",
            "- A passing report does not prove product value. It only proves the source bundle can enter the current substrate without bespoke parser work.",
        ]
    )
    return "\n".join(lines) + "\n"


def export_source_manifest(workspace: Workspace) -> str:
    content = json.dumps(build_source_manifest(), indent=2, sort_keys=True) + "\n"
    workspace.save_founder_artifact("source-manifest.json", content)
    return str(workspace.founder_dir / "source-manifest.json")


def build_workspace_onboarding_snapshot(workspace: Workspace) -> dict[str, Any]:
    return {
        "manifest": build_source_manifest(),
        "capability_matrix": build_source_capability_matrix(workspace),
    }
