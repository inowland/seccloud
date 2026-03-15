from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from seccloud.pipeline import sanitize_ops_metadata
from seccloud.runtime_stream import get_runtime_stream_state
from seccloud.storage import Workspace

PROJECTED_EVENTS_TABLE = "projected_events"
PROJECTED_DETECTIONS_TABLE = "projected_detections"
PROJECTION_STATE_TABLE = "projection_state"

DEFAULT_PROJECTION_PGPORT = 55432
DEFAULT_PROJECTION_PGDATABASE = "seccloud"
DEFAULT_PROJECTION_PGUSER = "inowland"


def default_projection_dsn() -> str:
    return (
        f"dbname={DEFAULT_PROJECTION_PGDATABASE} "
        f"user={DEFAULT_PROJECTION_PGUSER} "
        f"host=127.0.0.1 "
        f"port={DEFAULT_PROJECTION_PGPORT}"
    )


def ensure_projection_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            create table if not exists {PROJECTED_EVENTS_TABLE} (
              event_id text primary key,
              observed_at timestamptz not null,
              payload jsonb not null
            );
            create table if not exists {PROJECTED_DETECTIONS_TABLE} (
              detection_id text primary key,
              observed_at timestamptz not null,
              payload jsonb not null
            );
            create table if not exists {PROJECTION_STATE_TABLE} (
              key text primary key,
              payload jsonb not null
            );
            """
        )
    conn.commit()


def reset_projection_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            drop table if exists projected_cases;
            drop table if exists {PROJECTION_STATE_TABLE};
            drop table if exists {PROJECTED_DETECTIONS_TABLE};
            drop table if exists {PROJECTED_EVENTS_TABLE};
            """
        )
    ensure_projection_schema(conn)
    conn.commit()


def sync_workspace_projection(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        reset_projection_schema(conn)
        with conn.cursor() as cur:
            for event in workspace.list_normalized_events():
                cur.execute(
                    f"""
                    insert into {PROJECTED_EVENTS_TABLE} (
                      event_id, observed_at, payload
                    ) values (
                      %(event_id)s, %(observed_at)s, %(payload)s::jsonb
                    )
                    """,
                    {
                        "event_id": event["event_id"],
                        "observed_at": event["observed_at"],
                        "payload": json.dumps(event),
                    },
                )
            event_lookup = {event["event_id"]: event for event in workspace.list_normalized_events()}
            for detection in workspace.list_detections():
                anchor_event = event_lookup[detection["event_ids"][0]]
                cur.execute(
                    f"""
                    insert into {PROJECTED_DETECTIONS_TABLE} (
                      detection_id, observed_at, payload
                    ) values (
                      %(detection_id)s, %(observed_at)s, %(payload)s::jsonb
                    )
                    """,
                    {
                        "detection_id": detection["detection_id"],
                        "observed_at": anchor_event["observed_at"],
                        "payload": json.dumps(detection),
                    },
                )
            states = {
                "stream_state": get_runtime_stream_state(workspace),
                "ops_metadata": sanitize_ops_metadata(workspace.load_ops_metadata()),
            }
            for key, payload in states.items():
                cur.execute(
                    f"insert into {PROJECTION_STATE_TABLE} (key, payload) values (%s, %s::jsonb)",
                    (key, json.dumps(payload)),
                )
        conn.commit()
    return {
        "dsn": dsn,
        "event_count": len(workspace.list_normalized_events()),
        "detection_count": len(workspace.list_detections()),
    }


def fetch_projection_overview(dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"select payload from {PROJECTION_STATE_TABLE} where key = 'stream_state'")
            stream_state = cur.fetchone()
            cur.execute(f"select payload from {PROJECTION_STATE_TABLE} where key = 'ops_metadata'")
            ops_metadata = cur.fetchone()
        return {
            "stream_state": (
                stream_state["payload"]
                if stream_state
                else {
                    "cursor": 0,
                    "total_source_events": 0,
                    "complete": False,
                    "normalized_event_count": 0,
                    "detection_count": 0,
                }
            ),
            "ops_metadata": (
                sanitize_ops_metadata(ops_metadata["payload"]) if ops_metadata else sanitize_ops_metadata(None)
            ),
        }


def _paginate_rows(
    table: str,
    order_by: str,
    limit: int,
    offset: int,
    *,
    include_total: bool = True,
    dsn: str | None = None,
) -> dict[str, Any]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            total = None
            if include_total:
                cur.execute(f"select count(*) as count from {table}")
                total = cur.fetchone()["count"]
                query_limit = limit
            else:
                query_limit = limit + 1
            cur.execute(
                f"select payload from {table} order by {order_by} desc limit %s offset %s",
                (query_limit, offset),
            )
            rows = [row["payload"] for row in cur.fetchall()]
            has_more = offset + len(rows) < total if total is not None else len(rows) > limit
            items = rows[:limit]
    return {
        "items": items,
        "page": {
            "limit": limit,
            "offset": offset,
            "returned": len(items),
            "total": total,
            "has_more": has_more,
        },
    }


def fetch_projected_events(
    limit: int = 50,
    offset: int = 0,
    dsn: str | None = None,
) -> dict[str, Any]:
    return _paginate_rows(
        table=PROJECTED_EVENTS_TABLE,
        order_by="observed_at",
        limit=limit,
        offset=offset,
        include_total=False,
        dsn=dsn,
    )


def fetch_projected_detections(
    limit: int = 50,
    offset: int = 0,
    dsn: str | None = None,
) -> dict[str, Any]:
    return _paginate_rows(
        table=PROJECTED_DETECTIONS_TABLE,
        order_by="observed_at",
        limit=limit,
        offset=offset,
        dsn=dsn,
    )


def write_projection_env_file(path: str | Path, dsn: str | None = None) -> None:
    dsn = dsn or default_projection_dsn()
    Path(path).write_text(f"SECCLOUD_PROJECTION_DSN={dsn}\n", encoding="utf-8")
