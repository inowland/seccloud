from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from seccloud.contracts import Case
from seccloud.ids import new_prefixed_id
from seccloud.projection_store import (
    fetch_detection_linked_events,
    fetch_hot_event_detail,
    fetch_timeline_events,
)
from seccloud.storage import Workspace, parse_timestamp


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


def _matches_entity_reference(entity: dict[str, Any], reference: str | None) -> bool:
    if reference is None:
        return True
    return entity.get("id") == reference or entity.get("entity_id") == reference


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
    events = workspace.list_normalized_events()
    selected = []
    for event in events:
        if not _matches_entity_reference(event["principal"], principal_id):
            continue
        if not _matches_entity_reference(event["resource"], resource_id):
            continue
        selected.append(event)
    selected.sort(key=lambda item: (item["observed_at"], item["event_id"]), reverse=True)
    return selected[:limit]


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
    for event in workspace.list_normalized_events():
        if event["event_id"] == event_id:
            return event
    return None


def build_peer_comparison(workspace: Workspace, detection_id: str, dsn: str | None = None) -> dict[str, Any]:
    detection = workspace.get_detection(detection_id)
    anchor_event = get_event_detail(workspace, detection["event_ids"][0], dsn=dsn)
    if anchor_event is None:
        raise KeyError(f"Anchor event not found for detection {detection_id}")
    derived_state = workspace.load_derived_state()
    peer_group = anchor_event["principal"]["department"]
    peer_state = derived_state.get("peer_groups", {}).get(peer_group, {})
    profile = derived_state.get("principal_profiles", {}).get(anchor_event["principal"]["id"], {})
    resource_id = anchor_event["resource"]["id"]
    peer_resource_count = peer_state.get("resource_counts", {}).get(resource_id, 0)
    return {
        "principal_id": anchor_event["principal"]["id"],
        "peer_group": peer_group,
        "resource_id": resource_id,
        "principal_total_events": profile.get("total_events", 0),
        "principal_prior_resource_access_count": profile.get("resource_counts", {}).get(resource_id, 0),
        "peer_group_resource_access_count": peer_resource_count,
        "peer_group_principal_count": len(peer_state.get("principal_ids", [])),
        "detection_reasons": detection["reasons"],
    }


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
    timeline = []
    evidence_snapshots = []
    for event in workspace.list_normalized_events():
        if event["event_id"] in detection["event_ids"] or event["principal"]["id"] in detection["related_entity_ids"]:
            timeline.append(event["event_id"])
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
        groupable_case["timeline_event_ids"] = sorted(set(groupable_case.get("timeline_event_ids", []) + timeline))
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
        timeline_event_ids=timeline,
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
