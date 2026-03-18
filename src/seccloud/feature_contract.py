from __future__ import annotations

from typing import Any

REQUIRED_SCORING_TABLES = ("action", "history", "static", "peer_group")


def feature_scoring_contract(state: dict[str, Any] | None) -> dict[str, Any]:
    state = dict(state or {})

    def row_count(prefix: str) -> int:
        return int(state.get(f"{prefix}_feature_row_count", 0) or 0)

    def manifest_key(prefix: str) -> str | None:
        value = state.get(f"{prefix}_manifest_key")
        return value if isinstance(value, str) and value else None

    missing_tables = [
        prefix for prefix in REQUIRED_SCORING_TABLES if row_count(prefix) <= 0 or manifest_key(prefix) is None
    ]
    materialized_tables = [
        prefix
        for prefix in ("action", "history", "collaboration", "static", "peer_group")
        if row_count(prefix) > 0 and manifest_key(prefix) is not None
    ]

    ready = not missing_tables
    reason = "durable_feature_lake_ready" if ready else f"missing_tables:{','.join(missing_tables)}"
    mode = "feature_lake" if ready else "python_feature_pipeline_fallback"

    return {
        **state,
        "scoring_input_ready": ready,
        "scoring_input_mode": mode,
        "scoring_input_reason": reason,
        "materialized_table_count": len(materialized_tables),
        "materialized_tables": materialized_tables,
    }
