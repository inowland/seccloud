"""Helpers for loading scoring inputs from the durable feature lake."""

from __future__ import annotations

from typing import Any

from seccloud.feature_contract import feature_scoring_contract
from seccloud.feature_lake import load_feature_lake_feature_set
from seccloud.feature_pipeline import build_features
from seccloud.storage import Workspace
from seccloud.synthetic_scale import OrgPrincipal, OrgTeam


def _workspace_scoring_input_mode(workspace: Workspace) -> str:
    return str(
        feature_scoring_contract(workspace.load_feature_state()).get(
            "scoring_input_mode",
            "python_feature_pipeline_fallback",
        )
    )


def _load_feature_set_for_scoring(
    events: list[dict[str, Any]],
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    *,
    workspace: Workspace | None = None,
):
    """Prefer the durable feature lake when the scoring contract marks it ready."""
    if workspace is not None and _workspace_scoring_input_mode(workspace) == "feature_lake":
        try:
            return load_feature_lake_feature_set(workspace)
        except Exception:
            pass
    return build_features(events, principals, teams)
