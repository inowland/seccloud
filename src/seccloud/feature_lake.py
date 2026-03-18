from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow.parquet as pq

from seccloud.feature_pipeline import (
    ActionFeatures,
    CollaborationFeatures,
    ContextFeatures,
    FeatureSet,
    HistoryWindow,
    PeerFeatures,
    StaticFeatures,
)
from seccloud.storage import Workspace


@dataclass(slots=True)
class ActionAccessorSetRow:
    feature_schema_version: str
    tenant_id: str
    source: str
    resource_entity_key: str
    principal_entity_key: str
    access_count: int
    accessor_weight: float


@dataclass(slots=True)
class HistoryWindowRow:
    feature_schema_version: str
    tenant_id: str
    principal_entity_key: str
    window_start: str
    resource_entity_key: str


@dataclass(slots=True)
class CollaborationRow:
    feature_schema_version: str
    tenant_id: str
    principal_entity_key: str
    collaborator_entity_key: str
    co_access_weight: float


@dataclass(slots=True)
class StaticPrincipalRow:
    feature_schema_version: str
    tenant_id: str
    principal_entity_key: str
    principal_id: str
    department: str
    role: str
    location: str
    employment_duration_bucket: str
    privilege_level: str


@dataclass(slots=True)
class PeerGroupRow:
    feature_schema_version: str
    tenant_id: str
    principal_entity_key: str
    peer_entity_key: str
    peer_type: str
    peer_weight: float


@dataclass(slots=True)
class FeatureLakeSnapshot:
    state: dict[str, Any]
    action_rows: list[ActionAccessorSetRow]
    history_rows: list[HistoryWindowRow]
    collaboration_rows: list[CollaborationRow]
    static_rows: list[StaticPrincipalRow]
    peer_group_rows: list[PeerGroupRow]


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _employment_duration_bucket_from_idx(principal_idx: int) -> str:
    tenure_years = (principal_idx * 7.3) % 6.0
    if tenure_years < 0.25:
        return "<3mo"
    if tenure_years < 1.0:
        return "3-12mo"
    if tenure_years < 3.0:
        return "1-3yr"
    if tenure_years < 5.0:
        return "3-5yr"
    return "5yr+"


def _load_manifest(workspace: Workspace, manifest_key: str | None) -> dict[str, Any] | None:
    if not manifest_key:
        return None
    manifest = workspace.object_store.get_json(manifest_key)
    if not isinstance(manifest, dict):
        return None
    return manifest


def _load_manifest_rows(workspace: Workspace, manifest_key: str | None) -> list[dict[str, Any]]:
    manifest = _load_manifest(workspace, manifest_key)
    if not manifest:
        return []
    objects = manifest.get("objects", [])
    if not isinstance(objects, list) or not objects:
        return []
    object_key = objects[0].get("object_key")
    if not isinstance(object_key, str) or not object_key:
        return []
    payload = workspace.object_store.get_bytes(object_key)
    if payload is None:
        return []
    return pq.read_table(io.BytesIO(payload)).to_pylist()


def _load_identity_profiles(workspace: Workspace) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = workspace.load_identity_profiles()
    principals = payload.get("principals", [])
    teams = payload.get("teams", [])
    if not isinstance(principals, list) or not isinstance(teams, list):
        return [], []
    return principals, teams


def load_feature_lake_snapshot(workspace: Workspace) -> FeatureLakeSnapshot:
    state = workspace.load_feature_state()
    action_rows = [
        ActionAccessorSetRow(**row) for row in _load_manifest_rows(workspace, state.get("action_manifest_key"))
    ]
    history_rows = [
        HistoryWindowRow(**row) for row in _load_manifest_rows(workspace, state.get("history_manifest_key"))
    ]
    collaboration_rows = [
        CollaborationRow(**row) for row in _load_manifest_rows(workspace, state.get("collaboration_manifest_key"))
    ]
    static_rows = [
        StaticPrincipalRow(**row) for row in _load_manifest_rows(workspace, state.get("static_manifest_key"))
    ]
    peer_group_rows = [
        PeerGroupRow(**row) for row in _load_manifest_rows(workspace, state.get("peer_group_manifest_key"))
    ]
    return FeatureLakeSnapshot(
        state=state,
        action_rows=action_rows,
        history_rows=history_rows,
        collaboration_rows=collaboration_rows,
        static_rows=static_rows,
        peer_group_rows=peer_group_rows,
    )


def build_feature_set_from_snapshot(
    snapshot: FeatureLakeSnapshot,
    *,
    principal_entity_keys: list[str] | None = None,
    resource_entity_keys: list[str] | None = None,
    runtime_principals: list[dict[str, Any]] | None = None,
    runtime_teams: list[dict[str, Any]] | None = None,
) -> FeatureSet:
    principal_source_rows = (
        snapshot.action_rows + snapshot.history_rows + snapshot.collaboration_rows + snapshot.static_rows
    )
    principal_keys = principal_entity_keys or sorted(
        {row.principal_entity_key for row in principal_source_rows}
        | {row.collaborator_entity_key for row in snapshot.collaboration_rows}
        | {row.principal_entity_key for row in snapshot.peer_group_rows}
        | {row.peer_entity_key for row in snapshot.peer_group_rows}
    )
    resource_keys = resource_entity_keys or sorted(
        {row.resource_entity_key for row in snapshot.action_rows + snapshot.history_rows}
    )
    principal_key_to_idx = {key: index for index, key in enumerate(principal_keys)}
    resource_vocab = {key: index for index, key in enumerate(resource_keys)}

    actions: dict[str, ActionFeatures] = {}
    for row in snapshot.action_rows:
        action = actions.setdefault(
            row.resource_entity_key,
            ActionFeatures(
                resource_id=row.resource_entity_key,
                source=row.source,
                accessor_weights={},
            ),
        )
        action.accessor_weights[principal_key_to_idx[row.principal_entity_key]] = row.accessor_weight

    histories: dict[int, dict[str, set[str]]] = {}
    for row in snapshot.history_rows:
        principal_idx = principal_key_to_idx[row.principal_entity_key]
        by_window = histories.setdefault(principal_idx, {})
        by_window.setdefault(row.window_start, set()).add(row.resource_entity_key)

    collaborations: dict[int, dict[int, float]] = {}
    for row in snapshot.collaboration_rows:
        principal_idx = principal_key_to_idx[row.principal_entity_key]
        collaborator_idx = principal_key_to_idx[row.collaborator_entity_key]
        collaborations.setdefault(principal_idx, {})[collaborator_idx] = row.co_access_weight

    static_by_idx: dict[int, StaticPrincipalRow] = {}
    department_members: dict[str, set[int]] = {}
    for row in snapshot.static_rows:
        principal_idx = principal_key_to_idx[row.principal_entity_key]
        static_by_idx[principal_idx] = row
        department_members.setdefault(row.department, set()).add(principal_idx)

    manager_peers_by_idx: dict[int, dict[int, float]] = {}
    group_peers_by_idx: dict[int, dict[int, float]] = {}
    department_peers_by_idx: dict[int, dict[int, float]] = {}

    if snapshot.peer_group_rows:
        for row in snapshot.peer_group_rows:
            principal_idx = principal_key_to_idx.get(row.principal_entity_key)
            peer_idx = principal_key_to_idx.get(row.peer_entity_key)
            if principal_idx is None or peer_idx is None or principal_idx == peer_idx:
                continue
            if row.peer_type == "department":
                department_peers_by_idx.setdefault(principal_idx, {})[peer_idx] = row.peer_weight
            elif row.peer_type == "manager":
                manager_peers_by_idx.setdefault(principal_idx, {})[peer_idx] = row.peer_weight
            elif row.peer_type == "group":
                group_peers_by_idx.setdefault(principal_idx, {})[peer_idx] = row.peer_weight

        department_peers_by_idx = {
            principal_idx: dict(sorted(peers.items())) for principal_idx, peers in department_peers_by_idx.items()
        }
        manager_peers_by_idx = {
            principal_idx: dict(sorted(peers.items())) for principal_idx, peers in manager_peers_by_idx.items()
        }
        group_peers_by_idx = {
            principal_idx: dict(sorted(peers.items())) for principal_idx, peers in group_peers_by_idx.items()
        }

    runtime_static_overrides: dict[int, dict[str, str]] = {}
    if runtime_principals:
        principal_id_to_idx = {
            row.principal_id: principal_key_to_idx[row.principal_entity_key]
            for row in snapshot.static_rows
            if row.principal_id in {principal.get("email") for principal in runtime_principals}
        }
        runtime_by_org_idx = {
            principal["idx"]: principal
            for principal in runtime_principals
            if isinstance(principal, dict) and isinstance(principal.get("idx"), int)
        }
        manager_reports: dict[int, set[int]] = {}
        for principal in runtime_principals:
            if not isinstance(principal, dict):
                continue
            org_idx = principal.get("idx")
            manager_idx = principal.get("manager_idx")
            email = principal.get("email")
            if not isinstance(org_idx, int) or not isinstance(email, str):
                continue
            feature_idx = principal_id_to_idx.get(email)
            if feature_idx is None:
                continue
            if not snapshot.peer_group_rows:
                runtime_static_overrides[feature_idx] = {
                    "role": principal.get("role", "unknown"),
                    "location": principal.get("location", "unknown"),
                    "employment_duration_bucket": _employment_duration_bucket_from_idx(org_idx),
                }
            if isinstance(manager_idx, int):
                manager_reports.setdefault(manager_idx, set()).add(org_idx)

        if not snapshot.peer_group_rows:
            for principal in runtime_principals:
                if not isinstance(principal, dict):
                    continue
                org_idx = principal.get("idx")
                manager_idx = principal.get("manager_idx")
                email = principal.get("email")
                if not isinstance(org_idx, int) or not isinstance(email, str):
                    continue
                feature_idx = principal_id_to_idx.get(email)
                if feature_idx is None:
                    continue
                manager_peer_weights: dict[int, float] = {}
                if isinstance(manager_idx, int):
                    for sibling_idx in manager_reports.get(manager_idx, set()):
                        if sibling_idx == org_idx:
                            continue
                        sibling = runtime_by_org_idx.get(sibling_idx)
                        sibling_email = sibling.get("email") if isinstance(sibling, dict) else None
                        sibling_feature_idx = principal_id_to_idx.get(sibling_email)
                        if sibling_feature_idx is not None:
                            manager_peer_weights[sibling_feature_idx] = 1.0
                    manager = runtime_by_org_idx.get(manager_idx)
                    grand_manager_idx = manager.get("manager_idx") if isinstance(manager, dict) else None
                    if isinstance(grand_manager_idx, int):
                        for uncle_idx in manager_reports.get(grand_manager_idx, set()):
                            if uncle_idx == manager_idx:
                                continue
                            for cousin_idx in manager_reports.get(uncle_idx, set()):
                                if cousin_idx == org_idx:
                                    continue
                                cousin = runtime_by_org_idx.get(cousin_idx)
                                cousin_email = cousin.get("email") if isinstance(cousin, dict) else None
                                cousin_feature_idx = principal_id_to_idx.get(cousin_email)
                                if cousin_feature_idx is not None and cousin_feature_idx not in manager_peer_weights:
                                    manager_peer_weights[cousin_feature_idx] = 0.5
                manager_peers_by_idx[feature_idx] = dict(sorted(manager_peer_weights.items()))

            principal_teams: dict[int, list[dict[str, Any]]] = {}
            for team in runtime_teams or []:
                if not isinstance(team, dict):
                    continue
                members = team.get("member_indices", [])
                if not isinstance(members, list):
                    continue
                for org_idx in members:
                    if isinstance(org_idx, int):
                        principal_teams.setdefault(org_idx, []).append(team)

            for principal in runtime_principals:
                if not isinstance(principal, dict):
                    continue
                org_idx = principal.get("idx")
                email = principal.get("email")
                if not isinstance(org_idx, int) or not isinstance(email, str):
                    continue
                feature_idx = principal_id_to_idx.get(email)
                if feature_idx is None:
                    continue
                group_peer_weights: dict[int, float] = {}
                for team in principal_teams.get(org_idx, []):
                    members = team.get("member_indices", [])
                    if not isinstance(members, list) or len(members) <= 1:
                        continue
                    weight = 1.0 / len(members)
                    for member_idx in members:
                        if not isinstance(member_idx, int) or member_idx == org_idx:
                            continue
                        member = runtime_by_org_idx.get(member_idx)
                        member_email = member.get("email") if isinstance(member, dict) else None
                        member_feature_idx = principal_id_to_idx.get(member_email)
                        if member_feature_idx is not None:
                            group_peer_weights[member_feature_idx] = (
                                group_peer_weights.get(member_feature_idx, 0.0) + weight
                            )
                group_peers_by_idx[feature_idx] = dict(sorted(group_peer_weights.items()))

    contexts: dict[int, ContextFeatures] = {}
    for principal_idx in principal_key_to_idx.values():
        windows = histories.get(principal_idx, {})
        static_row = static_by_idx.get(principal_idx)
        department_peers = department_peers_by_idx.get(principal_idx, {})
        if not snapshot.peer_group_rows and static_row is not None:
            department_peers = {
                peer_idx: 1.0
                for peer_idx in sorted(department_members.get(static_row.department, set()))
                if peer_idx != principal_idx
            }
        contexts[principal_idx] = ContextFeatures(
            principal_idx=principal_idx,
            history=[
                HistoryWindow(
                    window_start=_parse_timestamp(window_start),
                    resource_ids=frozenset(sorted(resource_ids)),
                )
                for window_start, resource_ids in sorted(windows.items())
            ],
            peers=PeerFeatures(
                department_peers=department_peers,
                manager_peers=manager_peers_by_idx.get(principal_idx, {}),
                group_peers=group_peers_by_idx.get(principal_idx, {}),
            ),
            collaboration=CollaborationFeatures(co_access=dict(sorted(collaborations.get(principal_idx, {}).items()))),
            static=StaticFeatures(
                role=runtime_static_overrides.get(principal_idx, {}).get(
                    "role",
                    static_row.role if static_row is not None else "unknown",
                ),
                employment_duration_bucket=(
                    runtime_static_overrides.get(principal_idx, {}).get(
                        "employment_duration_bucket",
                        static_row.employment_duration_bucket if static_row is not None else "1-3yr",
                    )
                ),
                location=runtime_static_overrides.get(principal_idx, {}).get(
                    "location",
                    static_row.location if static_row is not None else "unknown",
                ),
                privilege_level=static_row.privilege_level if static_row is not None else "regular",
            ),
        )

    return FeatureSet(
        actions=actions,
        contexts=contexts,
        principal_vocab_size=len(principal_key_to_idx),
        resource_vocab=resource_vocab,
    )


def load_feature_lake_feature_set(workspace: Workspace) -> FeatureSet:
    snapshot = load_feature_lake_snapshot(workspace)
    vocab = workspace.load_feature_vocab()
    principal_keys = vocab.get("principal_entity_keys", [])
    resource_keys = vocab.get("resource_entity_keys", [])
    runtime_principals, runtime_teams = _load_identity_profiles(workspace)
    return build_feature_set_from_snapshot(
        snapshot,
        principal_entity_keys=principal_keys if isinstance(principal_keys, list) and principal_keys else None,
        resource_entity_keys=resource_keys if isinstance(resource_keys, list) and resource_keys else None,
        runtime_principals=runtime_principals,
        runtime_teams=runtime_teams,
    )
