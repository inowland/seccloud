from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from seccloud.contracts import Detection, EvidencePointer


@dataclass(slots=True)
class DetectorContribution:
    score_delta: float = 0.0
    reasons: list[str] = field(default_factory=list)
    attributions: dict[str, float] = field(default_factory=dict)


def build_embedding(event: dict[str, Any], profile: dict[str, Any], peer_resource_count: int) -> list[float]:
    return [
        float(profile.get("total_events", 0)),
        float(profile.get("action_counts", {}).get(event["action"]["verb"], 0)),
        float(profile.get("resource_counts", {}).get(event["resource"]["id"], 0)),
        float(peer_resource_count),
        1.0 if event["attributes"].get("external") else 0.0,
        1.0 if event["resource"]["sensitivity"] in {"high", "critical"} else 0.0,
    ]


def _principal_novelty_detector(
    event: dict[str, Any],
    profile: dict[str, Any],
) -> DetectorContribution:
    total_events = profile.get("total_events", 0)
    seen_action_count = profile.get("action_counts", {}).get(event["action"]["verb"], 0)
    seen_resource_count = profile.get("resource_counts", {}).get(event["resource"]["id"], 0)
    contribution = DetectorContribution()
    if total_events >= 3 and seen_action_count == 0:
        contribution.reasons.append("principal_has_not_performed_this_action_before")
        contribution.attributions["new_action_for_principal"] = 0.25
        contribution.score_delta += 0.25
    if total_events >= 3 and seen_resource_count == 0:
        contribution.reasons.append("principal_has_not_accessed_this_resource_before")
        contribution.attributions["new_resource_for_principal"] = 0.25
        contribution.score_delta += 0.25
    return contribution


def _peer_rarity_detector(
    event: dict[str, Any],
    total_events: int,
    peer_resource_count: int,
) -> DetectorContribution:
    contribution = DetectorContribution()
    if peer_resource_count == 0 and total_events >= 3:
        contribution.reasons.append("resource_is_rare_for_peer_group")
        contribution.attributions["peer_group_rarity"] = 0.15
        contribution.score_delta += 0.15
    return contribution


def _sensitivity_and_exfil_detector(event: dict[str, Any]) -> DetectorContribution:
    contribution = DetectorContribution()
    if event["resource"]["sensitivity"] in {"high", "critical"}:
        contribution.reasons.append("resource_is_sensitive")
        contribution.attributions["sensitive_resource"] = 0.15
        contribution.score_delta += 0.15
    if event["attributes"].get("privileged"):
        contribution.reasons.append("privileged_identity_activity")
        contribution.attributions["privileged_activity"] = 0.2
        contribution.score_delta += 0.2
    if event["action"]["verb"] in {"clone", "archive_download", "share_external", "export"}:
        contribution.reasons.append("action_has_exfiltration_characteristics")
        contribution.attributions["high_risk_action"] = 0.2
        contribution.score_delta += 0.2
    if event["attributes"].get("external"):
        contribution.reasons.append("resource_shared_externally")
        contribution.attributions["external_share"] = 0.2
        contribution.score_delta += 0.2
    return contribution


def _geo_anomaly_detector(event: dict[str, Any], profile: dict[str, Any]) -> DetectorContribution:
    contribution = DetectorContribution()
    total_events = profile.get("total_events", 0)
    has_geo_history = bool(profile.get("seen_geos"))
    geo = event["attributes"].get("geo")
    if event["source"] == "okta" and total_events >= 3 and has_geo_history and geo not in profile.get("seen_geos", []):
        contribution.reasons.append("login_from_unfamiliar_geography")
        contribution.attributions["new_geo"] = 0.2
        contribution.score_delta += 0.2
    if (
        event["source"] == "okta"
        and total_events >= 3
        and has_geo_history
        and geo not in profile.get("seen_geos", [])
        and event["attributes"].get("privileged")
    ):
        contribution.reasons.append("privileged_login_combines_unfamiliar_geo_and_sensitive_access")
        contribution.attributions["privileged_geo_combo"] = 0.15
        contribution.score_delta += 0.15
    return contribution


def _volume_detector(event: dict[str, Any]) -> DetectorContribution:
    contribution = DetectorContribution()
    if event["source"] == "snowflake" and event["attributes"].get("rows_read", 0) >= 100000:
        contribution.reasons.append("large_sensitive_query")
        contribution.attributions["large_query"] = 0.15
        contribution.score_delta += 0.15
    return contribution


def _context_suppression_detector(event: dict[str, Any]) -> DetectorContribution:
    contribution = DetectorContribution()
    if event["attributes"].get("expected_transition") or event["attributes"].get("role_change_window"):
        contribution.reasons.append("activity_occurs_during_expected_transition_window")
        contribution.attributions["expected_transition"] = -0.55
        contribution.score_delta -= 0.55
    if event["attributes"].get("approved_travel"):
        contribution.reasons.append("activity_occurs_during_approved_travel")
        contribution.attributions["approved_travel"] = -0.55
        contribution.score_delta -= 0.55
    if event["attributes"].get("incident_response_window") and event["attributes"].get("incident_ticket"):
        contribution.reasons.append("activity_occurs_during_documented_incident_response")
        contribution.attributions["incident_response"] = -0.65
        contribution.score_delta -= 0.65
    if event["attributes"].get("quarter_close_window") and event["attributes"].get("approved_workflow"):
        contribution.reasons.append("activity_occurs_during_approved_finance_close_workflow")
        contribution.attributions["finance_close_workflow"] = -0.60
        contribution.score_delta -= 0.60
    return contribution


def _fuse_contributions(contributions: list[DetectorContribution]) -> tuple[float, list[str], dict[str, float]]:
    score = 0.0
    reasons: list[str] = []
    attributions: dict[str, float] = {}
    for contribution in contributions:
        score += contribution.score_delta
        reasons.extend(contribution.reasons)
        attributions.update(contribution.attributions)
    return max(0.0, min(score, 0.99)), reasons, attributions


def score_event(event: dict[str, Any], state: dict[str, Any]) -> Detection | None:
    profiles = state.setdefault("principal_profiles", {})
    peer_groups = state.setdefault("peer_groups", {})
    profile = profiles.get(event["principal"]["id"], {})
    peer_group = event["principal"]["department"]
    peer_resources = peer_groups.get(peer_group, {}).get("resource_counts", {})
    peer_resource_count = peer_resources.get(event["resource"]["id"], 0)

    contributions = [
        _principal_novelty_detector(event, profile),
        _peer_rarity_detector(event, profile.get("total_events", 0), peer_resource_count),
        _sensitivity_and_exfil_detector(event),
        _geo_anomaly_detector(event, profile),
        _volume_detector(event),
        _context_suppression_detector(event),
    ]
    score, reasons, attributions = _fuse_contributions(contributions)
    if score < 0.60:
        return None

    if event["source"] == "okta" and event["attributes"].get("geo") and "login_from_unfamiliar_geography" in reasons:
        scenario = "compromised_privileged_identity"
        title = "Privileged identity used from an unfamiliar geography"
    elif event["source"] == "github" and event["action"]["verb"] in {"clone", "archive_download"}:
        scenario = "unusual_repo_export"
        title = "Unusual repository clone or export"
    elif event["source"] == "gworkspace" and event["attributes"].get("external"):
        scenario = "unusual_external_sharing"
        title = "Unusual external sharing of a sensitive document"
    elif event["source"] == "snowflake":
        scenario = "unusual_data_access"
        title = "Unusual sensitive dataset access"
    else:
        scenario = "general_behavioral_anomaly"
        title = "Behavioral anomaly"

    digest = hashlib.sha256(f"{event['event_id']}:{scenario}".encode()).hexdigest()[:12]
    return Detection(
        detection_id=f"det-{digest}",
        scenario=scenario,
        title=title,
        score=round(score, 2),
        confidence=round(min(0.99, 0.55 + score / 2), 2),
        severity="high" if score >= 0.8 else "medium",
        reasons=reasons,
        feature_attributions=attributions,
        event_ids=[event["event_id"]],
        related_entity_ids=[event["principal"]["id"], event["resource"]["id"]],
        evidence=[EvidencePointer(**event["evidence"])],
        model_version="rules-heuristics-fusion-v1",
    )
