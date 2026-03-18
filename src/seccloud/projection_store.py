from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from seccloud.pipeline import sanitize_ops_metadata
from seccloud.runtime_stream import get_runtime_stream_state
from seccloud.storage import NORMALIZED_SCHEMA_VERSION, Workspace

PROJECTED_EVENTS_TABLE = "projected_events"
PROJECTED_DETECTIONS_TABLE = "projected_detections"
PROJECTION_STATE_TABLE = "projection_state"
HOT_EVENT_INDEX_TABLE = "hot_event_index"
DETECTION_EVENT_EDGE_TABLE = "detection_event_edge"

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


def _now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _tbl(name: str) -> sql.Identifier:
    return sql.Identifier(name)


def ensure_projection_schema(conn: psycopg.Connection) -> None:
    pe = _tbl(PROJECTED_EVENTS_TABLE)
    pd = _tbl(PROJECTED_DETECTIONS_TABLE)
    ps = _tbl(PROJECTION_STATE_TABLE)
    hei = _tbl(HOT_EVENT_INDEX_TABLE)
    dee = _tbl(DETECTION_EVENT_EDGE_TABLE)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
            create table if not exists {pe} (
              event_id text primary key,
              observed_at timestamptz not null,
              payload jsonb not null
            );
            create table if not exists {pd} (
              tenant_id text not null,
              detection_id text primary key,
              observed_at timestamptz not null,
              payload jsonb not null
            );
            create table if not exists {ps} (
              key text primary key,
              payload jsonb not null
            );
            create table if not exists {hei} (
              tenant_id text not null,
              event_id text primary key,
              event_key text not null,
              normalized_schema_version text not null,
              source text not null,
              integration_id text null,
              source_event_id text not null,
              observed_at timestamptz not null,
              principal_entity_id text not null,
              principal_id text not null,
              principal_display_name text not null,
              principal_email text not null,
              principal_department text not null,
              resource_entity_id text not null,
              resource_id text not null,
              resource_name text not null,
              resource_kind text not null,
              resource_sensitivity text not null,
              action_verb text not null,
              action_category text not null,
              environment jsonb not null,
              attributes jsonb not null,
              event_payload jsonb not null,
              normalized_pointer jsonb not null,
              raw_pointer jsonb null,
              raw_retention_state text not null,
              indexed_at timestamptz not null,
              updated_at timestamptz not null
            );
            create table if not exists {dee} (
              tenant_id text not null,
              detection_id text not null,
              event_id text not null references {hei}(event_id),
              ordinal integer not null,
              link_role text not null,
              observed_at timestamptz not null,
              created_at timestamptz not null,
              primary key (tenant_id, detection_id, event_id)
            );
            create unique index if not exists hot_event_index_tenant_event_key_idx
              on {hei} (tenant_id, event_key);
            create index if not exists hot_event_index_tenant_observed_idx
              on {hei} (tenant_id, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_source_observed_idx
              on {hei} (tenant_id, source, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_integration_observed_idx
              on {hei} (tenant_id, integration_id, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_principal_entity_observed_idx
              on {hei} (tenant_id, principal_entity_id, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_resource_entity_observed_idx
              on {hei} (tenant_id, resource_entity_id, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_action_category_observed_idx
              on {hei} (tenant_id, action_category, observed_at desc, event_id desc);
            create index if not exists hot_event_index_tenant_action_verb_observed_idx
              on {hei} (tenant_id, action_verb, observed_at desc, event_id desc);
            create unique index if not exists detection_event_edge_tenant_detection_ordinal_idx
              on {dee} (tenant_id, detection_id, ordinal);
            create index if not exists detection_event_edge_tenant_event_idx
              on {dee} (tenant_id, event_id, observed_at desc, detection_id desc);
            alter table {pd}
              add column if not exists tenant_id text;
            update {pd}
              set tenant_id = coalesce(tenant_id, payload->>'tenant_id')
              where tenant_id is null;
            create index if not exists projected_detections_tenant_observed_idx
              on {pd} (tenant_id, observed_at desc, detection_id desc);
            """).format(pe=pe, pd=pd, ps=ps, hei=hei, dee=dee)
        )
    conn.commit()


def reset_projection_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
            drop table if exists projected_cases;
            drop table if exists {dee};
            drop table if exists {hei};
            drop table if exists {ps};
            drop table if exists {pd};
            drop table if exists {pe};
            """).format(
                dee=_tbl(DETECTION_EVENT_EDGE_TABLE),
                hei=_tbl(HOT_EVENT_INDEX_TABLE),
                ps=_tbl(PROJECTION_STATE_TABLE),
                pd=_tbl(PROJECTED_DETECTIONS_TABLE),
                pe=_tbl(PROJECTED_EVENTS_TABLE),
            )
        )
    ensure_projection_schema(conn)
    conn.commit()


def _delete_rows_by_ids(
    cur: psycopg.Cursor,
    *,
    table: str,
    id_column: str,
    ids: list[str],
) -> None:
    if not ids:
        return
    cur.execute(
        sql.SQL("delete from {table} where {col} = any(%s)").format(table=_tbl(table), col=sql.Identifier(id_column)),
        (ids,),
    )


def _delete_tenant_rows_by_ids(
    cur: psycopg.Cursor,
    *,
    table: str,
    tenant_id: str,
    id_column: str,
    ids: list[str],
) -> None:
    if not ids:
        return
    cur.execute(
        sql.SQL("delete from {table} where tenant_id = %s and {col} = any(%s)").format(
            table=_tbl(table), col=sql.Identifier(id_column)
        ),
        (tenant_id, ids),
    )


def _select_stale_tenant_ids(
    cur: psycopg.Cursor,
    *,
    table: str,
    tenant_id: str,
    id_column: str,
    current_ids: list[str],
) -> list[str]:
    tbl = _tbl(table)
    col = sql.Identifier(id_column)
    if current_ids:
        cur.execute(
            sql.SQL("""
            select distinct {col} as id
            from {table}
            where tenant_id = %s and not ({col} = any(%s))
            """).format(col=col, table=tbl),
            (tenant_id, current_ids),
        )
    else:
        cur.execute(
            sql.SQL("""
            select distinct {col} as id
            from {table}
            where tenant_id = %s
            """).format(col=col, table=tbl),
            (tenant_id,),
        )
    return [row["id"] for row in cur.fetchall()]


def _normalized_pointer_for_event(workspace: Workspace, event: dict[str, Any]) -> dict[str, Any]:
    manifest = workspace.ensure_normalized_lake_artifacts(event)
    obj = manifest["objects"][0]
    return {
        "pointer_version": 1,
        "object_key": obj["object_key"],
        "object_format": obj["object_format"],
        "sha256": obj["sha256"],
        "manifest_key": workspace._normalized_lake_manifest_key(event),  # noqa: SLF001
        "record_locator": {
            "ordinal": 0,
            "row_group": 0,
        },
        "retention_class": "normalized_retained",
    }


def _raw_pointer_for_event(workspace: Workspace, event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    attributes = event.get("attributes", {})
    manifest_key = attributes.get("raw_manifest_key")
    object_key = attributes.get("raw_batch_object_key")
    ordinal = attributes.get("raw_record_ordinal")
    object_sha256 = attributes.get("raw_object_sha256")
    object_format = attributes.get("raw_object_format")
    if not isinstance(manifest_key, str) or not isinstance(object_key, str):
        return None, "unknown"
    manifest = workspace.object_store.get_json(manifest_key)
    descriptor = None
    if manifest is not None:
        descriptor = next(
            (item for item in manifest.get("objects", []) if item.get("object_key") == object_key),
            None,
        )
    if descriptor is not None:
        object_sha256 = descriptor["sha256"]
        object_format = descriptor["object_format"]
    if not isinstance(object_sha256, str) or not isinstance(object_format, str):
        return None, "unknown"
    pointer: dict[str, Any] = {
        "pointer_version": 1,
        "object_key": object_key,
        "object_format": object_format,
        "sha256": object_sha256,
        "manifest_key": manifest_key,
        "retention_class": "raw_hot",
    }
    if isinstance(ordinal, int):
        pointer["record_locator"] = {"ordinal": ordinal}
    raw_available = workspace.object_store.get_bytes(object_key) is not None
    return pointer, ("available" if raw_available else "expired")


def _hot_event_index_row(workspace: Workspace, event: dict[str, Any]) -> dict[str, Any]:
    indexed_at = _now_timestamp()
    normalized_pointer = _normalized_pointer_for_event(workspace, event)
    raw_pointer, raw_retention_state = _raw_pointer_for_event(workspace, event)
    return {
        "tenant_id": workspace.tenant_id,
        "event_id": event["event_id"],
        "event_key": event["event_key"],
        "normalized_schema_version": NORMALIZED_SCHEMA_VERSION,
        "source": event["source"],
        "integration_id": event.get("integration_id"),
        "source_event_id": event["source_event_id"],
        "observed_at": event["observed_at"],
        "principal_entity_id": event["principal"]["entity_id"],
        "principal_id": event["principal"]["id"],
        "principal_display_name": event["principal"]["display_name"],
        "principal_email": event["principal"]["email"],
        "principal_department": event["principal"]["department"],
        "resource_entity_id": event["resource"]["entity_id"],
        "resource_id": event["resource"]["id"],
        "resource_name": event["resource"]["name"],
        "resource_kind": event["resource"]["kind"],
        "resource_sensitivity": event["resource"]["sensitivity"],
        "action_verb": event["action"]["verb"],
        "action_category": event["action"]["category"],
        "environment": event.get("environment", {}),
        "attributes": event.get("attributes", {}),
        "event_payload": event,
        "normalized_pointer": normalized_pointer,
        "raw_pointer": raw_pointer,
        "raw_retention_state": raw_retention_state,
        "indexed_at": indexed_at,
        "updated_at": indexed_at,
    }


def upsert_event_rows(
    cur: psycopg.Cursor,
    workspace: Workspace,
    events: list[dict[str, Any]],
) -> None:
    """Upsert normalized events into hot_event_index and projected_events."""
    hei = _tbl(HOT_EVENT_INDEX_TABLE)
    pe = _tbl(PROJECTED_EVENTS_TABLE)
    for event in events:
        row = _hot_event_index_row(workspace, event)
        cur.execute(
            sql.SQL("""
            insert into {hei} (
              tenant_id, event_id, event_key, normalized_schema_version, source, integration_id,
              source_event_id, observed_at, principal_entity_id, principal_id,
              principal_display_name, principal_email, principal_department,
              resource_entity_id, resource_id, resource_name, resource_kind,
              resource_sensitivity, action_verb, action_category, environment,
              attributes, event_payload, normalized_pointer, raw_pointer,
              raw_retention_state, indexed_at, updated_at
            ) values (
              %(tenant_id)s, %(event_id)s, %(event_key)s, %(normalized_schema_version)s,
              %(source)s, %(integration_id)s,
              %(source_event_id)s, %(observed_at)s, %(principal_entity_id)s, %(principal_id)s,
              %(principal_display_name)s, %(principal_email)s, %(principal_department)s,
              %(resource_entity_id)s, %(resource_id)s, %(resource_name)s, %(resource_kind)s,
              %(resource_sensitivity)s, %(action_verb)s, %(action_category)s, %(environment)s::jsonb,
              %(attributes)s::jsonb, %(event_payload)s::jsonb,
              %(normalized_pointer)s::jsonb, %(raw_pointer)s::jsonb,
              %(raw_retention_state)s, %(indexed_at)s, %(updated_at)s
            )
            on conflict (event_id) do update set
              tenant_id = excluded.tenant_id,
              event_key = excluded.event_key,
              normalized_schema_version = excluded.normalized_schema_version,
              source = excluded.source,
              integration_id = excluded.integration_id,
              source_event_id = excluded.source_event_id,
              observed_at = excluded.observed_at,
              principal_entity_id = excluded.principal_entity_id,
              principal_id = excluded.principal_id,
              principal_display_name = excluded.principal_display_name,
              principal_email = excluded.principal_email,
              principal_department = excluded.principal_department,
              resource_entity_id = excluded.resource_entity_id,
              resource_id = excluded.resource_id,
              resource_name = excluded.resource_name,
              resource_kind = excluded.resource_kind,
              resource_sensitivity = excluded.resource_sensitivity,
              action_verb = excluded.action_verb,
              action_category = excluded.action_category,
              environment = excluded.environment,
              attributes = excluded.attributes,
              event_payload = excluded.event_payload,
              normalized_pointer = excluded.normalized_pointer,
              raw_pointer = excluded.raw_pointer,
              raw_retention_state = excluded.raw_retention_state,
              updated_at = excluded.updated_at
            """).format(hei=hei),
            {
                **row,
                "environment": json.dumps(row["environment"]),
                "attributes": json.dumps(row["attributes"]),
                "event_payload": json.dumps(row["event_payload"]),
                "normalized_pointer": json.dumps(row["normalized_pointer"]),
                "raw_pointer": json.dumps(row["raw_pointer"]) if row["raw_pointer"] is not None else None,
            },
        )
        cur.execute(
            sql.SQL("""
            insert into {pe} (
              event_id, observed_at, payload
            ) values (
              %(event_id)s, %(observed_at)s, %(payload)s::jsonb
            )
            on conflict (event_id) do update set
              observed_at = excluded.observed_at,
              payload = excluded.payload
            """).format(pe=pe),
            {
                "event_id": row["event_id"],
                "observed_at": row["observed_at"],
                "payload": json.dumps(row["event_payload"]),
            },
        )


def sync_workspace_projection(workspace: Workspace, dsn: str | None = None) -> dict[str, Any]:
    """Sync detections and stream state to postgres.

    Events are projected inline during normalization, so this function
    only handles detections, detection-event edges, projection state, and
    local backfill from the durable event index when needed.
    """
    dsn = dsn or default_projection_dsn()
    event_index = workspace.ensure_event_index()
    normalized_events = [event for event in event_index.get("events_by_id", {}).values() if isinstance(event, dict)]
    detections = workspace.list_detections()
    detection_ids = sorted(d["detection_id"] for d in detections)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            pd = _tbl(PROJECTED_DETECTIONS_TABLE)
            dee = _tbl(DETECTION_EVENT_EDGE_TABLE)
            ps = _tbl(PROJECTION_STATE_TABLE)

            # Backfill projected events from the local durable event index.
            if normalized_events:
                upsert_event_rows(cur, workspace, normalized_events)

            # Upsert detections — look up anchor timestamps from postgres
            hei = _tbl(HOT_EVENT_INDEX_TABLE)
            for detection in detections:
                if not detection["event_ids"]:
                    continue
                anchor_id = detection["event_ids"][0]
                cur.execute(
                    sql.SQL("select observed_at from {hei} where event_id = %s").format(hei=hei),
                    (anchor_id,),
                )
                row = cur.fetchone()
                observed_at = row["observed_at"] if row else None
                if observed_at is None:
                    # Fall back to evidence timestamp
                    for ev in detection.get("evidence", []):
                        if ev.get("observed_at"):
                            observed_at = ev["observed_at"]
                            break
                if observed_at is None:
                    continue
                cur.execute(
                    sql.SQL("""
                    insert into {pd} (
                      tenant_id, detection_id, observed_at, payload
                    ) values (
                      %(tenant_id)s, %(detection_id)s, %(observed_at)s, %(payload)s::jsonb
                    )
                    on conflict (detection_id) do update set
                      tenant_id = excluded.tenant_id,
                      observed_at = excluded.observed_at,
                      payload = excluded.payload
                    """).format(pd=pd),
                    {
                        "tenant_id": workspace.tenant_id,
                        "detection_id": detection["detection_id"],
                        "observed_at": observed_at,
                        "payload": json.dumps(detection),
                    },
                )
                cur.execute(
                    sql.SQL("delete from {dee} where tenant_id = %s and detection_id = %s").format(dee=dee),
                    (workspace.tenant_id, detection["detection_id"]),
                )
                for ordinal, event_id in enumerate(detection["event_ids"]):
                    cur.execute(
                        sql.SQL("select observed_at from {hei} where event_id = %s").format(hei=hei),
                        (event_id,),
                    )
                    ev_row = cur.fetchone()
                    if ev_row is None:
                        continue
                    cur.execute(
                        sql.SQL("""
                        insert into {dee} (
                          tenant_id, detection_id, event_id, ordinal, link_role, observed_at, created_at
                        ) values (
                          %(tenant_id)s, %(detection_id)s, %(event_id)s, %(ordinal)s,
                          %(link_role)s, %(observed_at)s, %(created_at)s
                        )
                        """).format(dee=dee),
                        {
                            "tenant_id": workspace.tenant_id,
                            "detection_id": detection["detection_id"],
                            "event_id": event_id,
                            "ordinal": ordinal,
                            "link_role": "anchor" if ordinal == 0 else "supporting",
                            "observed_at": ev_row["observed_at"],
                            "created_at": _now_timestamp(),
                        },
                    )
            stale_detection_ids = _select_stale_tenant_ids(
                cur,
                table=PROJECTED_DETECTIONS_TABLE,
                tenant_id=workspace.tenant_id,
                id_column="detection_id",
                current_ids=detection_ids,
            )
            _delete_tenant_rows_by_ids(
                cur,
                table=PROJECTED_DETECTIONS_TABLE,
                tenant_id=workspace.tenant_id,
                id_column="detection_id",
                ids=stale_detection_ids,
            )
            _delete_tenant_rows_by_ids(
                cur,
                table=DETECTION_EVENT_EDGE_TABLE,
                tenant_id=workspace.tenant_id,
                id_column="detection_id",
                ids=stale_detection_ids,
            )
            states = {
                "stream_state": get_runtime_stream_state(workspace),
                "ops_metadata": sanitize_ops_metadata(workspace.load_ops_metadata()),
            }
            for key, payload in states.items():
                cur.execute(
                    sql.SQL("""
                    insert into {ps} (key, payload)
                    values (%s, %s::jsonb)
                    on conflict (key) do update set payload = excluded.payload
                    """).format(ps=ps),
                    (key, json.dumps(payload)),
                )
        conn.commit()
    return {
        "dsn": dsn,
        "event_count": int(event_index.get("event_count", len(normalized_events))),
        "detection_count": len(detections),
    }


def fetch_projection_overview(dsn: str | None = None) -> dict[str, Any]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            ps = _tbl(PROJECTION_STATE_TABLE)
            cur.execute(sql.SQL("select payload from {ps} where key = 'stream_state'").format(ps=ps))
            stream_state = cur.fetchone()
            cur.execute(sql.SQL("select payload from {ps} where key = 'ops_metadata'").format(ps=ps))
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
    *,
    query: sql.Composed,
    params: tuple[Any, ...],
    limit: int,
    offset: int,
    include_total: bool = True,
    count_query: sql.Composed | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            total = None
            if include_total and count_query is not None:
                cur.execute(count_query, params[:1])
                total = cur.fetchone()["count"]
                query_limit = limit
            else:
                query_limit = limit + 1
            cur.execute(query, (*params, query_limit, offset))
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
    *,
    tenant_id: str,
    limit: int = 50,
    offset: int = 0,
    dsn: str | None = None,
) -> dict[str, Any]:
    return _paginate_rows(
        query=sql.SQL(
            "select event_payload as payload from {hei} "
            "where tenant_id = %s "
            "order by observed_at desc, event_id desc limit %s offset %s"
        ).format(hei=_tbl(HOT_EVENT_INDEX_TABLE)),
        params=(tenant_id,),
        limit=limit,
        offset=offset,
        include_total=False,
        dsn=dsn,
    )


def fetch_projected_detections(
    *,
    tenant_id: str,
    limit: int = 50,
    offset: int = 0,
    dsn: str | None = None,
) -> dict[str, Any]:
    pd = _tbl(PROJECTED_DETECTIONS_TABLE)
    return _paginate_rows(
        query=sql.SQL(
            "select payload from {pd} "
            "where tenant_id = %s and coalesce(payload->>'status', 'open') = 'open' "
            "order by observed_at desc, detection_id desc limit %s offset %s"
        ).format(pd=pd),
        params=(tenant_id,),
        limit=limit,
        offset=offset,
        count_query=sql.SQL(
            "select count(*) as count from {pd} where tenant_id = %s and coalesce(payload->>'status', 'open') = 'open'"
        ).format(pd=pd),
        dsn=dsn,
    )


def fetch_hot_event_detail(
    *,
    tenant_id: str,
    event_id: str,
    dsn: str | None = None,
) -> dict[str, Any] | None:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                select event_payload, normalized_pointer, raw_pointer, raw_retention_state
                from {hei}
                where tenant_id = %s and event_id = %s
                """).format(hei=_tbl(HOT_EVENT_INDEX_TABLE)),
                (tenant_id, event_id),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return {
        "event_payload": row["event_payload"],
        "normalized_pointer": row["normalized_pointer"],
        "raw_pointer": row["raw_pointer"],
        "raw_retention_state": row["raw_retention_state"],
    }


def fetch_detection_linked_events(
    *,
    tenant_id: str,
    detection_id: str,
    dsn: str | None = None,
) -> list[dict[str, Any]]:
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                select hei.event_payload as payload
                from {dee} dee
                join {hei} hei on hei.event_id = dee.event_id
                where dee.tenant_id = %s and dee.detection_id = %s
                order by dee.ordinal asc
                """).format(dee=_tbl(DETECTION_EVENT_EDGE_TABLE), hei=_tbl(HOT_EVENT_INDEX_TABLE)),
                (tenant_id, detection_id),
            )
            rows = cur.fetchall()
    return [row["payload"] for row in rows]


def fetch_timeline_events(
    *,
    tenant_id: str,
    principal_reference: str | None = None,
    resource_reference: str | None = None,
    limit: int = 25,
    dsn: str | None = None,
) -> list[dict[str, Any]]:
    filters = [sql.SQL("tenant_id = %s")]
    params: list[Any] = [tenant_id]
    if principal_reference is not None:
        filters.append(sql.SQL("(principal_entity_id = %s or principal_id = %s)"))
        params.extend([principal_reference, principal_reference])
    if resource_reference is not None:
        filters.append(sql.SQL("(resource_entity_id = %s or resource_id = %s)"))
        params.extend([resource_reference, resource_reference])
    dsn = dsn or default_projection_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_projection_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                select event_payload as payload
                from {hei}
                where {where}
                order by observed_at desc, event_id desc
                limit %s
                """).format(
                    hei=_tbl(HOT_EVENT_INDEX_TABLE),
                    where=sql.SQL(" and ").join(filters),
                ),
                (*params, limit),
            )
            rows = cur.fetchall()
    return [row["payload"] for row in rows]


def write_projection_env_file(path: str | Path, dsn: str | None = None) -> None:
    dsn = dsn or default_projection_dsn()
    Path(path).write_text(f"SECCLOUD_PROJECTION_DSN={dsn}\n", encoding="utf-8")
