from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from seccloud.demo_stream import get_demo_stream_state
from seccloud.investigation import summarize_case
from seccloud.reports import build_evaluation_summary_markdown
from seccloud.source_pack import build_source_capability_matrix
from seccloud.storage import Workspace

DEFAULT_DEMO_PGPORT = 55432
DEFAULT_DEMO_PGDATABASE = "seccloud_demo"
DEFAULT_DEMO_PGUSER = "inowland"


def default_demo_dsn() -> str:
    return f"dbname={DEFAULT_DEMO_PGDATABASE} user={DEFAULT_DEMO_PGUSER} host=127.0.0.1 port={DEFAULT_DEMO_PGPORT}"


def ensure_demo_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists demo_events (
              event_id text primary key,
              observed_at timestamptz not null,
              source text not null,
              principal_id text not null,
              resource_id text not null,
              action_verb text not null,
              scenario text,
              payload jsonb not null
            );
            create table if not exists demo_detections (
              detection_id text primary key,
              observed_at timestamptz not null,
              scenario text not null,
              severity text not null,
              score double precision not null,
              title text not null,
              payload jsonb not null
            );
            create table if not exists demo_cases (
              case_id text primary key,
              updated_at timestamptz not null,
              status text not null,
              disposition text,
              payload jsonb not null
            );
            create table if not exists demo_state (
              key text primary key,
              payload jsonb not null
            );
            """
        )
    conn.commit()


def reset_demo_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("truncate table demo_events, demo_detections, demo_cases, demo_state;")
    conn.commit()


def sync_workspace_to_postgres(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_demo_schema(conn)
        reset_demo_schema(conn)
        with conn.cursor() as cur:
            for event in workspace.list_normalized_events():
                cur.execute(
                    """
                    insert into demo_events (
                      event_id, observed_at, source, principal_id,
                      resource_id, action_verb, scenario, payload
                    ) values (
                      %(event_id)s, %(observed_at)s, %(source)s, %(principal_id)s,
                      %(resource_id)s, %(action_verb)s, %(scenario)s, %(payload)s::jsonb
                    )
                    """,
                    {
                        "event_id": event["event_id"],
                        "observed_at": event["observed_at"],
                        "source": event["source"],
                        "principal_id": event["principal"]["id"],
                        "resource_id": event["resource"]["id"],
                        "action_verb": event["action"]["verb"],
                        "scenario": event["attributes"].get("scenario"),
                        "payload": json.dumps(event),
                    },
                )
            event_lookup = {event["event_id"]: event for event in workspace.list_normalized_events()}
            for detection in workspace.list_detections():
                anchor_event = event_lookup[detection["event_ids"][0]]
                cur.execute(
                    """
                    insert into demo_detections (
                      detection_id, observed_at, scenario, severity, score, title, payload
                    ) values (
                      %(detection_id)s, %(observed_at)s, %(scenario)s, %(severity)s,
                      %(score)s, %(title)s, %(payload)s::jsonb
                    )
                    """,
                    {
                        "detection_id": detection["detection_id"],
                        "observed_at": anchor_event["observed_at"],
                        "scenario": detection["scenario"],
                        "severity": detection["severity"],
                        "score": detection["score"],
                        "title": detection["title"],
                        "payload": json.dumps(detection),
                    },
                )
            for case in workspace.list_cases():
                summary = summarize_case(workspace, case["case_id"])
                cur.execute(
                    """
                    insert into demo_cases (
                      case_id, updated_at, status, disposition, payload
                    ) values (
                      %(case_id)s, %(updated_at)s, %(status)s, %(disposition)s, %(payload)s::jsonb
                    )
                    """,
                    {
                        "case_id": case["case_id"],
                        "updated_at": case["updated_at"],
                        "status": case["status"],
                        "disposition": case["disposition"],
                        "payload": json.dumps({**case, "summary": summary}),
                    },
                )
            states = {
                "stream_state": get_demo_stream_state(workspace),
                "ops_metadata": workspace.load_ops_metadata(),
                "source_capability_matrix": build_source_capability_matrix(workspace),
                "evaluation_summary_markdown": {
                    "markdown": build_evaluation_summary_markdown(workspace),
                },
            }
            for key, payload in states.items():
                cur.execute(
                    "insert into demo_state (key, payload) values (%s, %s::jsonb)",
                    (key, json.dumps(payload)),
                )
        conn.commit()
    return {
        "dsn": dsn,
        "event_count": len(workspace.list_normalized_events()),
        "detection_count": len(workspace.list_detections()),
        "case_count": len(workspace.list_cases()),
    }


def fetch_demo_overview(dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) as count from demo_events")
            event_count = cur.fetchone()["count"]
            cur.execute("select count(*) as count from demo_detections")
            detection_count = cur.fetchone()["count"]
            cur.execute("select count(*) as count from demo_cases")
            case_count = cur.fetchone()["count"]
            cur.execute("select payload from demo_state where key = 'stream_state'")
            stream_state = cur.fetchone()
            cur.execute("select payload from demo_state where key = 'ops_metadata'")
            ops_metadata = cur.fetchone()
        return {
            "event_count": event_count,
            "detection_count": detection_count,
            "case_count": case_count,
            "stream_state": stream_state["payload"] if stream_state else {},
            "ops_metadata": ops_metadata["payload"] if ops_metadata else {},
        }


def fetch_demo_events(limit: int = 50, dsn: str | None = None) -> list[dict[str, Any]]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select payload from demo_events order by observed_at desc limit %s",
                (limit,),
            )
            return [row["payload"] for row in cur.fetchall()]


def fetch_demo_detections(limit: int = 50, dsn: str | None = None) -> list[dict[str, Any]]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select payload from demo_detections order by observed_at desc limit %s",
                (limit,),
            )
            return [row["payload"] for row in cur.fetchall()]


def fetch_demo_cases(limit: int = 50, dsn: str | None = None) -> list[dict[str, Any]]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select payload from demo_cases order by updated_at desc limit %s",
                (limit,),
            )
            return [row["payload"] for row in cur.fetchall()]


def fetch_demo_state_item(key: str, dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_demo_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("select payload from demo_state where key = %s", (key,))
            row = cur.fetchone()
            return row["payload"] if row else {}


def write_demo_env_file(path: str | Path, dsn: str | None = None) -> None:
    dsn = dsn or default_demo_dsn()
    Path(path).write_text(f"SECCLOUD_DEMO_DSN={dsn}\n", encoding="utf-8")
