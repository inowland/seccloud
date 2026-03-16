"""ML scoring bridge for M0.5 demo integration.

Pre-computes contrastive model detections at stream reset time and produces
Detection objects matching the existing contract.  Detections are annotated
with a ``trigger_cursor`` indicating at which stream position they should
appear in the UI.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import torch

from seccloud.contracts import Detection, EvidencePointer
from seccloud.contrastive_model import (
    FacadeDataset,
    FacadeModel,
    build_categorical_vocabs,
    build_training_pairs,
    collate_facade,
    config_from_features,
    train_epoch,
)
from seccloud.evaluation import (
    _compute_padding_limits,
    score_principal,
)
from seccloud.feature_pipeline import build_features
from seccloud.ids import event_key
from seccloud.synthetic_scale import (
    OrgPrincipal,
    OrgTeam,
)

DEVICE = torch.device("cpu")

SCENARIO_TITLES: dict[str, str] = {
    "slow_exfiltration": "Gradual data exfiltration detected",
    "credential_compromise": "Compromised credentials — anomalous access pattern",
    "privilege_escalation": "Unauthorized privilege escalation",
    "departing_employee": "Departing employee data sweep",
    "account_takeover": "Account takeover — access pattern shift",
    "insider_collaboration": "Coordinated insider access anomaly",
}

SCENARIO_REASONS: dict[str, list[str]] = {
    "slow_exfiltration": [
        "access_pattern_diverges_from_peer_group",
        "gradual_increase_in_cross_department_data_access",
        "sensitive_resource_access_outside_normal_scope",
    ],
    "credential_compromise": [
        "access_pattern_diverges_from_peer_group",
        "access_from_unfamiliar_context",
        "cross_department_resource_breadth_anomaly",
    ],
    "privilege_escalation": [
        "admin_resource_access_without_admin_role",
        "accessor_set_mismatch_with_principal_context",
        "privileged_resource_access_anomaly",
    ],
    "departing_employee": [
        "unusual_breadth_of_resource_access",
        "volume_spike_across_multiple_sources",
        "cross_department_access_surge",
    ],
    "account_takeover": [
        "temporal_access_pattern_shift",
        "access_pattern_diverges_from_historical_baseline",
        "unfamiliar_resource_access_cluster",
    ],
    "insider_collaboration": [
        "coordinated_cross_department_access_pattern",
        "shared_unusual_resource_access_with_peer",
        "access_pattern_diverges_from_peer_group",
    ],
}


@dataclass(slots=True)
class PrecomputedDetection:
    """A detection annotated with its stream trigger position."""

    detection: Detection
    trigger_cursor: int
    scenario: str
    principal_email: str


def _feature_attributions_for_scenario(scenario: str, score: float) -> dict[str, float]:
    """Generate feature attribution breakdowns for an ML detection."""
    base: dict[str, float] = {}
    if scenario == "slow_exfiltration":
        base = {"cross_dept_access": 0.30, "volume_ramp": 0.25, "sensitivity_exposure": 0.25, "peer_distance": 0.20}
    elif scenario == "credential_compromise":
        base = {"peer_distance": 0.35, "context_shift": 0.30, "resource_breadth": 0.20, "temporal_anomaly": 0.15}
    elif scenario == "privilege_escalation":
        base = {
            "accessor_set_mismatch": 0.40,
            "admin_resource_access": 0.30,
            "peer_distance": 0.20,
            "role_mismatch": 0.10,
        }
    elif scenario == "departing_employee":
        base = {"resource_breadth": 0.35, "volume_spike": 0.30, "cross_dept_access": 0.20, "sensitivity_exposure": 0.15}
    elif scenario == "account_takeover":
        base = {"temporal_anomaly": 0.35, "resource_shift": 0.30, "peer_distance": 0.20, "context_shift": 0.15}
    elif scenario == "insider_collaboration":
        base = {
            "shared_unusual_access": 0.35,
            "cross_dept_access": 0.25,
            "peer_distance": 0.25,
            "coordination_signal": 0.15,
        }
    # Scale by score
    return {k: round(v * score, 3) for k, v in base.items()}


def precompute_detections(
    events: list[dict[str, Any]],
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    *,
    epochs: int = 10,
    seed: int = 42,
) -> list[PrecomputedDetection]:
    """Run the full ML pipeline and produce pre-computed detections.

    1. Build features from events
    2. Train the contrastive model
    3. Score all principals
    4. Create Detection objects for attack principals
    5. Annotate each with a trigger_cursor
    """
    # Build features
    fs = build_features(events, principals, teams)
    cat_vocabs = build_categorical_vocabs(fs)

    # Train model
    cfg = config_from_features(
        fs,
        embed_dim=64,
        token_dim=32,
        static_embed_dim=8,
        action_hidden=[256, 128],
        context_hidden=[256, 128],
        n_positive=10,
        batch_size=512,
        learning_rate=3e-4,
        epochs=epochs,
    )
    model = FacadeModel(cfg).to(DEVICE)
    all_pairs = build_training_pairs(fs)
    # Train on all benign pairs (no holdout for demo)
    benign_pairs = [p for p in all_pairs if _event_scenario_for_resource(events, p.resource_id) == "baseline"]
    if not benign_pairs:
        benign_pairs = all_pairs
    ds = FacadeDataset(fs, benign_pairs, cat_vocabs, cfg, rng_seed=seed)
    loader = torch.utils.data.DataLoader(
        ds,
        batch_size=cfg.batch_size,
        shuffle=True,
        collate_fn=collate_facade,
        num_workers=0,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
    for _ in range(epochs):
        train_epoch(model, loader, optimizer, cfg, DEVICE)

    model.eval()

    # Score all principals
    email_to_idx = {p.email: p.idx for p in principals}
    limits = _compute_padding_limits(fs, cfg)

    # Collect per-principal distinct actions from ALL events
    from collections import defaultdict

    principal_actions: dict[int, set[tuple[str, str]]] = defaultdict(set)
    for e in events:
        email = e.get("actor_email")
        rid = e.get("resource_id")
        source = e.get("source")
        if email and rid and source:
            pidx = email_to_idx.get(email)
            if pidx is not None:
                principal_actions[pidx].add((rid, source))

    scores: dict[int, float] = {}
    with torch.no_grad():
        for pidx, action_set in principal_actions.items():
            scores[pidx] = score_principal(
                model,
                pidx,
                list(action_set),
                fs,
                cat_vocabs,
                limits["max_act"],
                limits["max_win"],
                limits["max_res"],
                limits["max_peers"],
                delta=0.3,
                device=DEVICE,
            )

    # Identify attack principals and their scenarios
    attack_info: dict[str, set[int]] = defaultdict(set)
    for e in events:
        scenario = e.get("scenario", "baseline")
        if scenario != "baseline":
            pidx = email_to_idx.get(e.get("actor_email", ""))
            if pidx is not None:
                attack_info[scenario].add(pidx)

    # Find trigger cursors (index of last scenario event for each attacker)
    attacker_last_event: dict[tuple[str, int], int] = {}
    attacker_events: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for cursor, e in enumerate(events):
        scenario = e.get("scenario", "baseline")
        if scenario != "baseline":
            pidx = email_to_idx.get(e.get("actor_email", ""))
            if pidx is not None:
                attacker_last_event[(scenario, pidx)] = cursor
                attacker_events[(scenario, pidx)].append(e)

    # Build detections
    idx_to_principal = {p.idx: p for p in principals}
    detections: list[PrecomputedDetection] = []

    for scenario, pidxs in attack_info.items():
        for pidx in pidxs:
            principal = idx_to_principal.get(pidx)
            if principal is None:
                continue

            score = scores.get(pidx, 0.0)
            trigger = attacker_last_event.get((scenario, pidx), len(events) - 1)
            scenario_events = attacker_events.get((scenario, pidx), [])

            # Build event keys for linking — must match the integration_id
            # used by submit_grouped_raw_events during streaming
            ev_keys = []
            evidence_pointers = []
            for se in scenario_events[-6:]:  # limit to last 6 events
                ek = event_key(
                    se["source"],
                    se["source_event_id"],
                    integration_id="synthetic-stream",
                )
                ev_keys.append(ek)
                evidence_pointers.append(
                    EvidencePointer(
                        source=se["source"],
                        object_key=f"raw/{se['source']}/{se['source_event_id']}",
                        raw_event_id=se["source_event_id"],
                        observed_at=se["observed_at"],
                    )
                )

            digest = hashlib.sha256(f"ml:{scenario}:{principal.email}".encode()).hexdigest()[:12]

            confidence = min(0.99, 0.5 + score / 20.0)
            severity = "high" if score >= 10.0 else "medium"

            det = Detection(
                detection_id=f"det-{digest}",
                scenario=scenario,
                title=SCENARIO_TITLES.get(scenario, f"ML anomaly: {scenario}"),
                score=round(score, 2),
                confidence=round(confidence, 2),
                severity=severity,
                reasons=SCENARIO_REASONS.get(scenario, ["anomalous_access_pattern"]),
                feature_attributions=_feature_attributions_for_scenario(scenario, score),
                event_ids=ev_keys,  # event_keys, resolved to event_ids at write time
                related_entity_ids=[principal.email],
                evidence=evidence_pointers,
                model_version="contrastive-facade-v1",
            )

            detections.append(
                PrecomputedDetection(
                    detection=det,
                    trigger_cursor=trigger,
                    scenario=scenario,
                    principal_email=principal.email,
                )
            )

    # Sort by trigger cursor so they fire in chronological order
    detections.sort(key=lambda d: d.trigger_cursor)
    return detections


def _event_scenario_for_resource(
    events: list[dict[str, Any]],
    resource_id: str,
) -> str:
    """Return the scenario label for events referencing this resource."""
    for e in events:
        if e.get("resource_id") == resource_id:
            return e.get("scenario", "baseline")
    return "baseline"


def serialize_precomputed(detections: list[PrecomputedDetection]) -> list[dict[str, Any]]:
    """Serialize pre-computed detections for JSON storage."""
    return [
        {
            "detection": d.detection.to_dict(),
            "trigger_cursor": d.trigger_cursor,
            "scenario": d.scenario,
            "principal_email": d.principal_email,
        }
        for d in detections
    ]


def deserialize_precomputed(data: list[dict[str, Any]]) -> list[PrecomputedDetection]:
    """Deserialize pre-computed detections from JSON."""
    results = []
    for item in data:
        det_data = item["detection"]
        evidence = [EvidencePointer(**ep) if isinstance(ep, dict) else ep for ep in det_data.get("evidence", [])]
        det = Detection(
            detection_id=det_data["detection_id"],
            scenario=det_data["scenario"],
            title=det_data["title"],
            score=det_data["score"],
            confidence=det_data["confidence"],
            severity=det_data["severity"],
            reasons=det_data["reasons"],
            feature_attributions=det_data["feature_attributions"],
            event_ids=det_data["event_ids"],
            related_entity_ids=det_data["related_entity_ids"],
            evidence=evidence,
            model_version=det_data["model_version"],
            status=det_data.get("status", "open"),
        )
        results.append(
            PrecomputedDetection(
                detection=det,
                trigger_cursor=item["trigger_cursor"],
                scenario=item["scenario"],
                principal_email=item["principal_email"],
            )
        )
    return results
