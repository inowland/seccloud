from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

WORKFLOW_CASES_TABLE = "workflow_cases"
WORKFLOW_DETECTION_STATE_TABLE = "workflow_detection_state"
WORKFLOW_DSN_ENV_VAR = "SECCLOUD_WORKFLOW_DSN"


def get_workflow_dsn(environ: dict[str, str] | None = None) -> str | None:
    env = environ if environ is not None else os.environ
    dsn = env.get(WORKFLOW_DSN_ENV_VAR)
    if isinstance(dsn, str) and dsn:
        return dsn
    return None


def _connect(dsn: str) -> psycopg.Connection[Any]:
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def _ensure_schema(conn: psycopg.Connection[Any]) -> None:
    conn.execute(
        f"""
        create table if not exists {WORKFLOW_DETECTION_STATE_TABLE} (
            detection_id text primary key,
            status text not null,
            updated_at text not null
        )
        """
    )
    conn.execute(
        f"""
        create table if not exists {WORKFLOW_CASES_TABLE} (
            case_id text primary key,
            detection_ids jsonb not null,
            timeline_event_ids jsonb not null,
            evidence_snapshots jsonb not null,
            status text not null,
            disposition text,
            analyst_notes jsonb not null,
            feedback_labels jsonb not null,
            created_at text not null,
            updated_at text not null
        )
        """
    )


def list_detection_states(dsn: str | None, detection_ids: Iterable[str] | None = None) -> dict[str, dict[str, Any]]:
    if not dsn:
        return {}
    ids = [item for item in detection_ids or [] if isinstance(item, str) and item]
    try:
        with _connect(dsn) as conn:
            _ensure_schema(conn)
            if ids:
                rows = conn.execute(
                    f"""
                    select detection_id, status, updated_at
                    from {WORKFLOW_DETECTION_STATE_TABLE}
                    where detection_id = any(%s)
                    """,
                    (ids,),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    select detection_id, status, updated_at
                    from {WORKFLOW_DETECTION_STATE_TABLE}
                    """
                ).fetchall()
    except psycopg.Error:
        return {}
    return {
        str(row["detection_id"]): {
            "detection_id": str(row["detection_id"]),
            "status": str(row["status"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in rows
    }


def get_detection_state(dsn: str | None, detection_id: str) -> dict[str, Any] | None:
    if not dsn or not detection_id:
        return None
    return list_detection_states(dsn, [detection_id]).get(detection_id)


def upsert_detection_state(
    dsn: str,
    *,
    detection_id: str,
    status: str,
    updated_at: str,
) -> dict[str, Any]:
    with _connect(dsn) as conn:
        _ensure_schema(conn)
        conn.execute(
            f"""
            insert into {WORKFLOW_DETECTION_STATE_TABLE} (
                detection_id,
                status,
                updated_at
            ) values (%s, %s, %s)
            on conflict (detection_id) do update
            set
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (detection_id, status, updated_at),
        )
    return {
        "detection_id": detection_id,
        "status": status,
        "updated_at": updated_at,
    }


def list_cases(dsn: str | None) -> list[dict[str, Any]]:
    if not dsn:
        return []
    try:
        with _connect(dsn) as conn:
            _ensure_schema(conn)
            rows = conn.execute(
                f"""
                select
                    case_id,
                    detection_ids,
                    timeline_event_ids,
                    evidence_snapshots,
                    status,
                    disposition,
                    analyst_notes,
                    feedback_labels,
                    created_at,
                    updated_at
                from {WORKFLOW_CASES_TABLE}
                order by case_id
                """
            ).fetchall()
    except psycopg.Error:
        return []
    return [_deserialize_case_row(row) for row in rows]


def get_case(dsn: str | None, case_id: str) -> dict[str, Any] | None:
    if not dsn or not case_id:
        return None
    try:
        with _connect(dsn) as conn:
            _ensure_schema(conn)
            row = conn.execute(
                f"""
                select
                    case_id,
                    detection_ids,
                    timeline_event_ids,
                    evidence_snapshots,
                    status,
                    disposition,
                    analyst_notes,
                    feedback_labels,
                    created_at,
                    updated_at
                from {WORKFLOW_CASES_TABLE}
                where case_id = %s
                """,
                (case_id,),
            ).fetchone()
    except psycopg.Error:
        return None
    return _deserialize_case_row(row) if row is not None else None


def upsert_case(dsn: str, case_payload: dict[str, Any]) -> dict[str, Any]:
    with _connect(dsn) as conn:
        _ensure_schema(conn)
        conn.execute(
            f"""
            insert into {WORKFLOW_CASES_TABLE} (
                case_id,
                detection_ids,
                timeline_event_ids,
                evidence_snapshots,
                status,
                disposition,
                analyst_notes,
                feedback_labels,
                created_at,
                updated_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (case_id) do update
            set
                detection_ids = excluded.detection_ids,
                timeline_event_ids = excluded.timeline_event_ids,
                evidence_snapshots = excluded.evidence_snapshots,
                status = excluded.status,
                disposition = excluded.disposition,
                analyst_notes = excluded.analyst_notes,
                feedback_labels = excluded.feedback_labels,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                case_payload["case_id"],
                Jsonb(list(case_payload.get("detection_ids", []))),
                Jsonb(list(case_payload.get("timeline_event_ids", []))),
                Jsonb(list(case_payload.get("evidence_snapshots", []))),
                case_payload["status"],
                case_payload.get("disposition"),
                Jsonb(list(case_payload.get("analyst_notes", []))),
                Jsonb(list(case_payload.get("feedback_labels", []))),
                case_payload["created_at"],
                case_payload["updated_at"],
            ),
        )
    return dict(case_payload)


def _deserialize_case_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": str(row["case_id"]),
        "detection_ids": list(row.get("detection_ids") or []),
        "timeline_event_ids": list(row.get("timeline_event_ids") or []),
        "evidence_snapshots": list(row.get("evidence_snapshots") or []),
        "status": str(row["status"]),
        "disposition": row.get("disposition"),
        "analyst_notes": list(row.get("analyst_notes") or []),
        "feedback_labels": list(row.get("feedback_labels") or []),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }
