from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from seccloud.onboarding import build_source_manifest
from seccloud.pipeline import validate_raw_event
from seccloud.storage import Workspace
from seccloud.workers import submit_grouped_raw_events


class VendorMappingError(ValueError):
    pass


VENDOR_SOURCE_MANIFEST = {
    "okta": {
        "display_name": "Okta System Log",
        "fixture_file": "okta_system_log.jsonl",
        "supported_event_types": ["user.session.start"],
    },
    "gworkspace": {
        "display_name": "Google Workspace Drive Audit",
        "fixture_file": "gworkspace_drive_audit.jsonl",
        "supported_event_types": ["view", "share"],
    },
    "github": {
        "display_name": "GitHub Audit Log",
        "fixture_file": "github_audit_log.jsonl",
        "supported_event_types": ["repo.view", "repo.archive_download"],
    },
    "snowflake": {
        "display_name": "Snowflake Query History",
        "fixture_file": "snowflake_query_history.jsonl",
        "supported_event_types": ["SELECT", "COPY"],
    },
}


def build_vendor_source_manifest() -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    for source, manifest in VENDOR_SOURCE_MANIFEST.items():
        sources.append(
            {
                "source": source,
                "display_name": manifest["display_name"],
                "fixture_file": manifest["fixture_file"],
                "supported_event_types": manifest["supported_event_types"],
                "mapping_target": "raw-event-v1",
            }
        )
    return {
        "source_pack": [item["source"] for item in sources],
        "mapping_version": "vendor-export-v1",
        "mapping_target": "raw-event-v1",
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


def _vendor_event_id(event: dict[str, Any]) -> str:
    if "uuid" in event:
        return str(event["uuid"])
    if isinstance(event.get("id"), dict):
        return str(event["id"].get("uniqueQualifier", "unknown"))
    if "id" in event:
        return str(event["id"])
    if "QUERY_ID" in event:
        return str(event["QUERY_ID"])
    return str(event.get("source_event_id", "unknown"))


def _geo_from_okta(event: dict[str, Any]) -> str:
    geo = event.get("client", {}).get("geographicalContext", {})
    country = geo.get("country") or "unknown"
    state = geo.get("state") or "unknown"
    return f"{country}-{state}"


def _map_okta_event(event: dict[str, Any]) -> dict[str, Any]:
    if event.get("eventType") != "user.session.start":
        raise VendorMappingError("unsupported_event_type")
    target = (event.get("target") or [{}])[0]
    return {
        "source": "okta",
        "source_event_id": event["uuid"],
        "observed_at": event["published"],
        "received_at": event.get("ingested_at", event["published"]),
        "actor_email": event["actor"]["alternateId"],
        "actor_name": event["actor"]["displayName"],
        "department": event["actor"].get("department", "unknown"),
        "role": event["actor"].get("role", "unknown"),
        "event_type": "login",
        "resource_id": target.get("id", "okta:unknown"),
        "resource_name": target.get("displayName", "Unknown Resource"),
        "resource_kind": target.get("type", "app"),
        "sensitivity": event.get("debugContext", {}).get("sensitivity", "high"),
        "geo": _geo_from_okta(event),
        "ip": event.get("client", {}).get("ipAddress", "unknown"),
        "privileged": event.get("securityContext", {}).get("isPrivileged", False),
        "vendor_event_type": event["eventType"],
    }


def _map_gworkspace_event(event: dict[str, Any]) -> dict[str, Any]:
    activity = event["activity"]
    if activity not in {"view", "share"}:
        raise VendorMappingError("unsupported_event_type")
    return {
        "source": "gworkspace",
        "source_event_id": event["id"]["uniqueQualifier"],
        "observed_at": event["id"]["time"],
        "received_at": event.get("ingested_at", event["id"]["time"]),
        "actor_email": event["actor"]["email"],
        "actor_name": event["actor"]["profileName"],
        "department": event["actor"].get("department", "unknown"),
        "role": event["actor"].get("role", "unknown"),
        "event_type": "share_external" if activity == "share" and event.get("visibility") == "external" else "view",
        "resource_id": f"gworkspace:doc/{event['item']['id']}",
        "resource_name": event["item"]["title"],
        "resource_kind": "document",
        "sensitivity": event.get("item", {}).get("sensitivity", "internal"),
        "external": event.get("visibility") == "external",
        "target_domain": event.get("target_domain"),
        "vendor_event_type": activity,
    }


def _map_github_event(event: dict[str, Any]) -> dict[str, Any]:
    action = event.get("action")
    if action not in {"repo.view", "repo.archive_download"}:
        raise VendorMappingError("unsupported_event_type")
    return {
        "source": "github",
        "source_event_id": event["id"],
        "observed_at": event["created_at"],
        "received_at": event.get("ingested_at", event["created_at"]),
        "actor_email": event["actor"]["email"],
        "actor_name": event["actor"]["display_name"],
        "department": event["actor"].get("department", "unknown"),
        "role": event["actor"].get("role", "unknown"),
        "event_type": "archive_download" if action == "repo.archive_download" else "view",
        "resource_id": f"github:repo/{event['repo']['name']}",
        "resource_name": event["repo"]["name"],
        "resource_kind": "repo",
        "sensitivity": event["repo"].get("sensitivity", "internal"),
        "bytes_transferred_mb": event.get("transfer", {}).get("mb", 0),
        "vendor_event_type": action,
    }


def _map_snowflake_event(event: dict[str, Any]) -> dict[str, Any]:
    query_type = event.get("QUERY_TYPE")
    if query_type not in {"SELECT", "COPY"}:
        raise VendorMappingError("unsupported_event_type")
    return {
        "source": "snowflake",
        "source_event_id": event["QUERY_ID"],
        "observed_at": event["START_TIME"],
        "received_at": event.get("INGESTED_AT", event["START_TIME"]),
        "actor_email": event["USER_NAME"],
        "actor_name": event.get("DISPLAY_NAME", event["USER_NAME"]),
        "department": event.get("DEPARTMENT", "unknown"),
        "role": event.get("ROLE_NAME", "unknown"),
        "event_type": "export" if query_type == "COPY" else "query",
        "resource_id": f"snowflake:dataset/{event['OBJECT_NAME']}",
        "resource_name": event["OBJECT_NAME"],
        "resource_kind": "dataset",
        "sensitivity": event.get("SENSITIVITY", "internal"),
        "rows_read": event.get("ROWS_PRODUCED", 0),
        "warehouse": event.get("WAREHOUSE_NAME", "unknown"),
        "vendor_event_type": query_type,
    }


VENDOR_MAPPERS = {
    "okta": _map_okta_event,
    "gworkspace": _map_gworkspace_event,
    "github": _map_github_event,
    "snowflake": _map_snowflake_event,
}


def map_vendor_event(source: str, event: dict[str, Any]) -> dict[str, Any]:
    if source not in VENDOR_MAPPERS:
        raise VendorMappingError("unsupported_source")
    mapped = VENDOR_MAPPERS[source](event)
    validate_raw_event(mapped)
    return mapped


def validate_vendor_fixture_bundle(fixtures_dir: str | Path) -> dict[str, Any]:
    root = Path(fixtures_dir)
    manifest = build_vendor_source_manifest()
    validation: dict[str, Any] = {
        "fixtures_dir": str(root),
        "mapping_version": manifest["mapping_version"],
        "mapping_target": manifest["mapping_target"],
        "sources": {},
        "summary": {
            "passes": True,
            "missing_sources": [],
            "mapped_event_count": 0,
            "invalid_event_count": 0,
        },
    }
    for source_manifest in manifest["sources"]:
        source = source_manifest["source"]
        path = root / source_manifest["fixture_file"]
        events = _read_jsonl(path)
        seen_vendor_event_types = sorted(
            {
                event.get("eventType")
                or event.get("activity")
                or event.get("action")
                or event.get("QUERY_TYPE")
                or "unknown"
                for event in events
            }
        )
        invalid_events: list[dict[str, Any]] = []
        mapped_events = 0
        for event in events:
            try:
                map_vendor_event(source, event)
                mapped_events += 1
            except (VendorMappingError, KeyError, ValueError) as exc:
                invalid_events.append(
                    {
                        "source_event_id": _vendor_event_id(event),
                        "reason": str(exc),
                    }
                )
        missing_event_types = sorted(set(source_manifest["supported_event_types"]) - set(seen_vendor_event_types))
        validation["sources"][source] = {
            "fixture_file": str(path),
            "present": path.exists(),
            "event_count": len(events),
            "mapped_event_count": mapped_events,
            "invalid_event_count": len(invalid_events),
            "seen_vendor_event_types": seen_vendor_event_types,
            "missing_supported_event_types": missing_event_types,
            "invalid_events": invalid_events,
        }
        validation["summary"]["mapped_event_count"] += mapped_events
        validation["summary"]["invalid_event_count"] += len(invalid_events)
        if not path.exists():
            validation["summary"]["missing_sources"].append(source)
            validation["summary"]["passes"] = False
        if invalid_events or missing_event_types:
            validation["summary"]["passes"] = False
    return validation


def import_vendor_fixture_bundle(workspace: Workspace, fixtures_dir: str | Path) -> dict[str, Any]:
    validation = validate_vendor_fixture_bundle(fixtures_dir)
    accepted_events: list[dict[str, Any]] = []
    skipped_invalid_count = 0
    manifest = build_vendor_source_manifest()
    for source_manifest in manifest["sources"]:
        source = source_manifest["source"]
        source_validation = validation["sources"][source]
        if not source_validation["present"]:
            continue
        invalid_ids = {item["source_event_id"] for item in source_validation["invalid_events"]}
        for event in _read_jsonl(Path(source_validation["fixture_file"])):
            source_event_id = _vendor_event_id(event)
            if source_event_id in invalid_ids:
                skipped_invalid_count += 1
                continue
            mapped = map_vendor_event(source, event)
            accepted_events.append(mapped)
    submission = submit_grouped_raw_events(
        workspace,
        records=accepted_events,
        intake_kind="vendor_fixture_import",
        integration_id="vendor-fixtures",
        metadata={"fixtures_dir": str(fixtures_dir)},
    )
    return {
        "validation": validation,
        "imported_event_count": len(accepted_events),
        "skipped_invalid_event_count": skipped_invalid_count,
        "batch_count": submission["batch_count"],
        "batches": submission["batches"],
        "mapping_target": build_source_manifest()["mapping_version"],
    }


def build_vendor_mapping_report_markdown(fixtures_dir: str | Path) -> str:
    validation = validate_vendor_fixture_bundle(fixtures_dir)
    lines = [
        "# Vendor Mapping Report",
        "",
        f"- Fixture bundle: `{validation['fixtures_dir']}`",
        f"- Validation status: `{'pass' if validation['summary']['passes'] else 'fail'}`",
        f"- Mapped events: `{validation['summary']['mapped_event_count']}`",
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
                f"- Mapped events: `{details['mapped_event_count']}`",
                f"- Invalid events: `{details['invalid_event_count']}`",
                f"- Seen vendor event types: `{details['seen_vendor_event_types']}`",
                f"- Missing supported event types: `{details['missing_supported_event_types']}`",
                f"- Invalid events: `{details['invalid_events']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            (
                "- This report tests whether realistic vendor-shaped exports can be translated into"
                " the current raw-event contract without bespoke parser work."
            ),
            (
                "- A passing report means the export shape is compatible with the current ingestion"
                " contract. It does not yet prove customer-specific semantic correctness."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def export_vendor_source_manifest(workspace: Workspace) -> str:
    content = json.dumps(build_vendor_source_manifest(), indent=2, sort_keys=True) + "\n"
    workspace.save_founder_artifact("vendor-source-manifest.json", content)
    return str(workspace.founder_dir / "vendor-source-manifest.json")
