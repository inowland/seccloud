from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from seccloud.contracts import Case
from seccloud.detection_context import ensure_detection_context
from seccloud.feature_lake import load_feature_lake_snapshot
from seccloud.ids import new_prefixed_id
from seccloud.projection_store import (
    fetch_detection_linked_events,
    fetch_hot_event_detail,
    fetch_timeline_events,
)
from seccloud.storage import Workspace, parse_timestamp


def _empty_peer_context() -> dict[str, Any]:
    return {
        "principal_role": "unknown",
        "principal_location": "unknown",
        "principal_privilege_level": "regular",
        "peer_group_resource_principal_count": 0,
        "department_peer_count": 0,
        "manager_peer_count": 0,
        "group_peer_count": 0,
    }


def _behavior_context_from_detection_context(
    workspace: Workspace,
    anchor_event: dict[str, Any],
) -> dict[str, Any]:
    event_id = anchor_event.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        return {
            "principal_prior_event_count": 0,
            "principal_prior_action_count": 0,
            "principal_prior_resource_access_count": 0,
            "peer_group_resource_access_count": 0,
            "geo_seen_before": False,
        }
    context = ensure_detection_context(workspace).get("contexts_by_event_id", {}).get(event_id, {})
    geo = anchor_event.get("attributes", {}).get("geo")
    seen_geos = {item for item in context.get("seen_geos", []) if isinstance(item, str) and item}
    return {
        "principal_prior_event_count": int(context.get("principal_total_events", 0)),
        "principal_prior_action_count": int(context.get("action_count", 0)),
        "principal_prior_resource_access_count": int(context.get("resource_count", 0)),
        "peer_group_resource_access_count": int(context.get("peer_resource_count", 0)),
        "geo_seen_before": bool(context.get("geo_seen_before", isinstance(geo, str) and geo in seen_geos)),
    }


def list_detections(workspace: Workspace) -> list[dict[str, Any]]:
    return workspace.list_detections()


def active_detection_count(workspace: Workspace) -> int:
    return sum(1 for detection in workspace.list_detections() if detection.get("status", "open") == "open")


def _find_case_for_detection(workspace: Workspace, detection_id: str) -> dict[str, Any] | None:
    for case in workspace.list_cases():
        if detection_id in case.get("detection_ids", []):
            return case
    return None


def _detection_anchor_event(
    workspace: Workspace,
    detection: dict[str, Any],
    dsn: str | None = None,
) -> dict[str, Any] | None:
    if not detection.get("event_ids"):
        return None
    return get_event_detail(workspace, detection["event_ids"][0], dsn=dsn)


def _case_anchor_event(workspace: Workspace, case: dict[str, Any], dsn: str | None = None) -> dict[str, Any] | None:
    detection_ids = case.get("detection_ids", [])
    if not detection_ids:
        return None
    return _detection_anchor_event(workspace, workspace.get_detection(detection_ids[0]), dsn=dsn)


def _find_groupable_case(
    workspace: Workspace,
    detection: dict[str, Any],
    dsn: str | None = None,
) -> dict[str, Any] | None:
    anchor_event = _detection_anchor_event(workspace, detection, dsn=dsn)
    if anchor_event is None:
        return None
    principal_id = anchor_event["principal"]["id"]
    observed_at = parse_timestamp(anchor_event["observed_at"])
    for case in workspace.list_cases():
        if case.get("status") != "open":
            continue
        case_anchor = _case_anchor_event(workspace, case, dsn=dsn)
        if case_anchor is None:
            continue
        if case_anchor["principal"]["id"] != principal_id:
            continue
        if abs(observed_at - parse_timestamp(case_anchor["observed_at"])) <= timedelta(hours=6):
            return case
    return None


def _persist_case_artifact(workspace: Workspace, case: dict[str, Any]) -> None:
    derived_state = workspace.load_derived_state()
    derived_state.setdefault("case_artifacts", {})
    derived_state["case_artifacts"][case["case_id"]] = {
        "detection_ids": case["detection_ids"],
        "timeline_event_ids": case["timeline_event_ids"],
        "created_at": case["created_at"],
        "updated_at": case["updated_at"],
    }
    workspace.save_derived_state(derived_state)


def get_entity_timeline(
    workspace: Workspace,
    principal_id: str | None = None,
    resource_id: str | None = None,
    limit: int = 25,
    dsn: str | None = None,
) -> list[dict[str, Any]]:
    if dsn is not None and (principal_id is not None or resource_id is not None):
        return fetch_timeline_events(
            tenant_id=workspace.tenant_id,
            principal_reference=principal_id,
            resource_reference=resource_id,
            limit=limit,
            dsn=dsn,
        )
    return workspace.query_indexed_events(
        principal_reference=principal_id,
        resource_reference=resource_id,
        limit=limit,
    )


def build_evidence_bundle(workspace: Workspace, detection_id: str) -> dict[str, Any]:
    detection = workspace.get_detection(detection_id)
    bundle_items = []
    for pointer in detection["evidence"]:
        raw_payload = workspace.read_raw_by_object_key(pointer["object_key"])
        bundle_items.append(
            {
                "pointer": pointer,
                "raw_payload": raw_payload,
                "retention_expired": raw_payload is None,
            }
        )
    return {
        "evidence_items": bundle_items,
    }


def get_event_detail(workspace: Workspace, event_id: str, dsn: str | None = None) -> dict[str, Any] | None:
    if dsn is not None:
        indexed = fetch_hot_event_detail(tenant_id=workspace.tenant_id, event_id=event_id, dsn=dsn)
        if indexed is not None:
            return indexed["event_payload"]
    if dsn is None:
        return workspace.get_indexed_event(event_id)
    return None


def build_peer_comparison(workspace: Workspace, detection_id: str, dsn: str | None = None) -> dict[str, Any]:
    detection = workspace.get_detection(detection_id)
    anchor_event = (
        get_event_detail(
            workspace,
            detection["event_ids"][0],
            dsn=dsn,
        )
        if detection.get("event_ids")
        else None
    )
    if anchor_event is None:
        return {
            "principal_id": detection.get("related_entity_ids", ["unknown"])[0],
            "peer_group": "unknown",
            "resource_id": "unknown",
            **_empty_peer_context(),
            "principal_total_events": 0,
            "principal_prior_event_count": 0,
            "principal_prior_action_count": 0,
            "principal_prior_resource_access_count": 0,
            "peer_group_resource_access_count": 0,
            "peer_group_principal_count": 0,
            "detection_reasons": detection["reasons"],
            "geo_seen_before": False,
        }

    principal_id = anchor_event["principal"]["id"]
    peer_group = anchor_event["principal"]["department"]
    resource_id = anchor_event["resource"]["id"]

    # Try to get live counts from postgres
    if dsn is not None:
        counts = _peer_counts_from_postgres(dsn, workspace.tenant_id, principal_id, peer_group, resource_id)
        if counts is not None:
            return {
                **counts,
                **_peer_context_from_feature_lake(workspace, anchor_event),
                **_behavior_context_from_detection_context(workspace, anchor_event),
                "detection_reasons": detection["reasons"],
            }

    feature_counts = _peer_counts_from_feature_lake(workspace, anchor_event)
    if feature_counts is not None:
        return {
            **feature_counts,
            **_behavior_context_from_detection_context(workspace, anchor_event),
            "detection_reasons": detection["reasons"],
        }

    event_scan_counts = _peer_counts_from_event_scan(workspace, anchor_event)
    if event_scan_counts is not None:
        return {
            **event_scan_counts,
            **_behavior_context_from_detection_context(workspace, anchor_event),
            "detection_reasons": detection["reasons"],
        }

    return {
        "principal_id": principal_id,
        "peer_group": peer_group,
        "resource_id": resource_id,
        **_empty_peer_context(),
        "principal_total_events": 0,
        "principal_prior_event_count": 0,
        "principal_prior_action_count": 0,
        "principal_prior_resource_access_count": 0,
        "peer_group_resource_access_count": 0,
        "peer_group_principal_count": 0,
        "detection_reasons": detection["reasons"],
        "geo_seen_before": False,
    }


def _peer_context_from_feature_lake(
    workspace: Workspace,
    anchor_event: dict[str, Any],
) -> dict[str, Any]:
    snapshot = load_feature_lake_snapshot(workspace)
    principal_entity_key = anchor_event.get("principal", {}).get("entity_key")
    resource_entity_key = anchor_event.get("resource", {}).get("entity_key")
    if not isinstance(principal_entity_key, str) or not isinstance(resource_entity_key, str):
        return _empty_peer_context()

    static_by_key = {row.principal_entity_key: row for row in snapshot.static_rows}
    static_row = static_by_key.get(principal_entity_key)
    if static_row is None:
        return _empty_peer_context()

    peer_rows = [row for row in snapshot.peer_group_rows if row.principal_entity_key == principal_entity_key]
    department_peer_keys = {row.peer_entity_key for row in peer_rows if row.peer_type == "department"}
    manager_peer_keys = {row.peer_entity_key for row in peer_rows if row.peer_type == "manager"}
    group_peer_keys = {row.peer_entity_key for row in peer_rows if row.peer_type == "group"}
    if not department_peer_keys:
        department_peer_keys = {
            row.principal_entity_key
            for row in snapshot.static_rows
            if row.department == static_row.department and row.principal_entity_key != principal_entity_key
        }

    peer_group_resource_principal_count = len(
        {
            row.principal_entity_key
            for row in snapshot.action_rows
            if row.resource_entity_key == resource_entity_key
            and (row.principal_entity_key == principal_entity_key or row.principal_entity_key in department_peer_keys)
        }
    )

    return {
        "principal_role": static_row.role,
        "principal_location": static_row.location,
        "principal_privilege_level": static_row.privilege_level,
        "peer_group_resource_principal_count": peer_group_resource_principal_count,
        "department_peer_count": len(department_peer_keys),
        "manager_peer_count": len(manager_peer_keys),
        "group_peer_count": len(group_peer_keys),
    }


def _peer_counts_from_feature_lake(
    workspace: Workspace,
    anchor_event: dict[str, Any],
) -> dict[str, Any] | None:
    snapshot = load_feature_lake_snapshot(workspace)
    principal_entity_key = anchor_event.get("principal", {}).get("entity_key")
    principal_id = anchor_event.get("principal", {}).get("id")
    resource_id = anchor_event.get("resource", {}).get("id")
    resource_entity_key = anchor_event.get("resource", {}).get("entity_key")
    if (
        not isinstance(principal_entity_key, str)
        or not isinstance(principal_id, str)
        or not isinstance(resource_id, str)
        or not isinstance(resource_entity_key, str)
    ):
        return None

    static_by_key = {row.principal_entity_key: row for row in snapshot.static_rows}
    static_row = static_by_key.get(principal_entity_key)
    if static_row is None:
        return None

    department_peer_keys = {
        row.peer_entity_key
        for row in snapshot.peer_group_rows
        if row.principal_entity_key == principal_entity_key and row.peer_type == "department"
    }
    department_member_keys = {principal_entity_key, *department_peer_keys}
    if len(department_member_keys) == 1:
        department_member_keys = {
            row.principal_entity_key for row in snapshot.static_rows if row.department == static_row.department
        }
    if not department_member_keys:
        return None

    principal_total_events = sum(
        row.access_count for row in snapshot.action_rows if row.principal_entity_key == principal_entity_key
    )

    return {
        "principal_id": principal_id,
        "peer_group": static_row.department,
        "resource_id": resource_id,
        **_peer_context_from_feature_lake(workspace, anchor_event),
        "principal_total_events": principal_total_events,
        "peer_group_principal_count": len(department_member_keys),
    }


def _peer_counts_from_event_scan(
    workspace: Workspace,
    anchor_event: dict[str, Any],
) -> dict[str, Any] | None:
    principal_id = anchor_event.get("principal", {}).get("id")
    peer_group = anchor_event.get("principal", {}).get("department")
    resource_id = anchor_event.get("resource", {}).get("id")
    if not isinstance(principal_id, str) or not isinstance(peer_group, str) or not isinstance(resource_id, str):
        return None

    principal_events = workspace.query_indexed_events(principal_reference=principal_id)
    peer_group_events = workspace.query_indexed_events(department=peer_group)
    peer_group_principal_ids = {
        event.get("principal", {}).get("id")
        for event in peer_group_events
        if isinstance(event.get("principal", {}).get("id"), str)
    }

    return {
        "principal_id": principal_id,
        "peer_group": peer_group,
        "resource_id": resource_id,
        **_peer_context_from_feature_lake(workspace, anchor_event),
        "principal_total_events": len(principal_events),
        "peer_group_principal_count": len(peer_group_principal_ids),
    }


def _peer_counts_from_postgres(
    dsn: str,
    tenant_id: str,
    principal_id: str,
    peer_group: str,
    resource_id: str,
) -> dict[str, Any] | None:
    """Query hot_event_index for live peer comparison counts."""
    try:
        import psycopg
        from psycopg.rows import dict_row

        from seccloud.projection_store import HOT_EVENT_INDEX_TABLE, _tbl

        hei = _tbl(HOT_EVENT_INDEX_TABLE)
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    psycopg.sql.SQL(
                        "select count(*) as n from {hei} where tenant_id = %s and principal_id = %s"
                    ).format(hei=hei),
                    (tenant_id, principal_id),
                )
                total_events = cur.fetchone()["n"]

                cur.execute(
                    psycopg.sql.SQL(
                        "select count(distinct principal_id) as n from {hei}"
                        " where tenant_id = %s and principal_department = %s"
                    ).format(hei=hei),
                    (tenant_id, peer_group),
                )
                peer_count = cur.fetchone()["n"]

        return {
            "principal_id": principal_id,
            "peer_group": peer_group,
            "resource_id": resource_id,
            "principal_total_events": total_events,
            "peer_group_principal_count": peer_count,
        }
    except Exception:
        return None


def get_detection_detail(workspace: Workspace, detection_id: str, dsn: str | None = None) -> dict[str, Any]:
    detection = workspace.get_detection(detection_id)
    if dsn is not None:
        event_details = fetch_detection_linked_events(
            tenant_id=workspace.tenant_id,
            detection_id=detection_id,
            dsn=dsn,
        )
        if not event_details and detection.get("event_ids"):
            event_details = [get_event_detail(workspace, event_id) for event_id in detection["event_ids"]]
    else:
        event_details = [get_event_detail(workspace, event_id) for event_id in detection["event_ids"]]
    return {
        "detection": detection,
        "peer_comparison": build_peer_comparison(workspace, detection_id, dsn=dsn),
        "evidence_bundle": build_evidence_bundle(workspace, detection_id),
        "events": [event for event in event_details if event is not None],
    }


def acknowledge_detection(workspace: Workspace, detection_id: str) -> dict[str, Any]:
    detection = workspace.get_detection(detection_id)
    if detection is None:
        raise KeyError(f"Detection not found: {detection_id}")
    detection["status"] = "acknowledged"
    workspace.save_detection(detection)
    return detection


def create_case_from_detection(workspace: Workspace, detection_id: str) -> dict[str, Any]:
    for existing_case in workspace.list_cases():
        if detection_id in existing_case.get("detection_ids", []):
            return existing_case
    detection = workspace.get_detection(detection_id)
    timeline = set(detection.get("event_ids", []))
    evidence_snapshots = []
    if detection.get("related_entity_ids"):
        for event in workspace.query_indexed_events(principal_reference=detection["related_entity_ids"][0]):
            event_id = event.get("event_id")
            if isinstance(event_id, str):
                timeline.add(event_id)
    timeline_ids = sorted(timeline)
    for item in build_evidence_bundle(workspace, detection_id)["evidence_items"]:
        evidence_snapshots.append(
            {
                "pointer": item["pointer"],
                "raw_payload": item["raw_payload"],
                "retention_expired": item["retention_expired"],
            }
        )
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    groupable_case = _find_groupable_case(workspace, detection)
    if groupable_case is not None:
        groupable_case["detection_ids"] = sorted(set(groupable_case.get("detection_ids", []) + [detection_id]))
        groupable_case["timeline_event_ids"] = sorted(set(groupable_case.get("timeline_event_ids", []) + timeline_ids))
        existing_pointers = {
            item["pointer"]["object_key"]
            for item in groupable_case.get("evidence_snapshots", [])
            if item.get("pointer")
        }
        for item in evidence_snapshots:
            if item["pointer"]["object_key"] not in existing_pointers:
                groupable_case.setdefault("evidence_snapshots", []).append(item)
        groupable_case["updated_at"] = now
        workspace.save_case(groupable_case)
        _persist_case_artifact(workspace, groupable_case)
        return groupable_case
    case = Case(
        case_id=new_prefixed_id("cas"),
        detection_ids=[detection_id],
        timeline_event_ids=timeline_ids,
        evidence_snapshots=evidence_snapshots,
        status="open",
        disposition=None,
        analyst_notes=[],
        feedback_labels=[],
        created_at=now,
        updated_at=now,
    )
    workspace.save_case(case.to_dict())
    _persist_case_artifact(workspace, case.to_dict())
    return case.to_dict()


def summarize_case(workspace: Workspace, case_id: str) -> dict[str, Any]:
    case = workspace.get_case(case_id)
    detections = [workspace.get_detection(detection_id) for detection_id in case.get("detection_ids", [])]
    primary_detection = detections[0] if detections else None
    peer_comparison = build_peer_comparison(workspace, primary_detection["detection_id"]) if primary_detection else {}
    anchor_event = _detection_anchor_event(workspace, primary_detection) if primary_detection else None
    return {
        "case_id": case_id,
        "status": case["status"],
        "disposition": case["disposition"],
        "detection_count": len(detections),
        "timeline_event_count": len(case.get("timeline_event_ids", [])),
        "evidence_item_count": len(case.get("evidence_snapshots", [])),
        "feedback_labels": case.get("feedback_labels", []),
        "case_title": (f"{anchor_event['principal']['display_name']} investigation" if anchor_event else case_id),
        "principal_id": anchor_event["principal"]["id"] if anchor_event else None,
        "principal_display_name": anchor_event["principal"]["display_name"] if anchor_event else None,
        "primary_detection": {
            "detection_id": primary_detection["detection_id"],
            "scenario": primary_detection["scenario"],
            "title": primary_detection["title"],
            "score": primary_detection["score"],
            "severity": primary_detection["severity"],
        }
        if primary_detection
        else None,
        "peer_comparison": peer_comparison,
    }


def get_case_detail(workspace: Workspace, case_id: str) -> dict[str, Any]:
    case = workspace.get_case(case_id)
    linked_detections = [workspace.get_detection(detection_id) for detection_id in case.get("detection_ids", [])]
    timeline_events = [
        event
        for event in (get_event_detail(workspace, event_id) for event_id in case.get("timeline_event_ids", []))
        if event is not None
    ]
    return {
        "summary": summarize_case(workspace, case_id),
        "case": case,
        "detections": linked_detections,
        "timeline_events": timeline_events,
    }


def update_case(
    workspace: Workspace,
    case_id: str,
    disposition: str | None = None,
    analyst_note: str | None = None,
    feedback_label: str | None = None,
) -> dict[str, Any]:
    case = workspace.get_case(case_id)
    if disposition is not None:
        case["disposition"] = disposition
    if analyst_note:
        case.setdefault("analyst_notes", []).append(analyst_note)
    if feedback_label:
        case.setdefault("feedback_labels", []).append(feedback_label)
        derived_state = workspace.load_derived_state()
        derived_state.setdefault("feedback_labels", {})
        for detection_id in case["detection_ids"]:
            derived_state["feedback_labels"].setdefault(detection_id, []).append(feedback_label)
        workspace.save_derived_state(derived_state)
    case["updated_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    workspace.save_case(case)
    return case
