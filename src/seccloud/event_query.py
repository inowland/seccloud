from __future__ import annotations

import base64
import json
from typing import Any
from urllib import error

from seccloud.canonical_event_store import (
    CANONICAL_EVENT_FORMAT,
    CANONICAL_EVENT_STORE,
    DETAIL_HYDRATION_STRATEGY,
    build_event_index_document,
    event_sort_key,
    get_event_by_id,
    hydrate_event_by_pointer,
    scan_events,
)
from seccloud.quickwit_client import quickwit_config_from_env, search_events
from seccloud.storage import Workspace, parse_timestamp

_QUERY_STRING_RESERVED_CHARS = set('+^`:{}"[]()~!\\*')


def _cursor_payload(observed_at: str, event_id: str, *, sort_values: list[Any] | None = None) -> str:
    payload: dict[str, Any] = {"observed_at": observed_at, "event_id": event_id}
    if sort_values is not None:
        payload["sort_values"] = sort_values
    return base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True).encode("utf-8")).decode("utf-8")


def _decode_cursor(cursor: str | None) -> dict[str, Any] | None:
    if not cursor:
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8"))
    except Exception as exc:  # pragma: no cover - defensive invalid cursor handling
        raise ValueError("Invalid event cursor.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Invalid event cursor.")
    return payload


def _parse_cursor(cursor: str | None) -> tuple[str, str] | None:
    payload = _decode_cursor(cursor)
    if payload is None:
        return None
    observed_at = payload.get("observed_at")
    event_id = payload.get("event_id")
    if not isinstance(observed_at, str) or not isinstance(event_id, str) or not observed_at or not event_id:
        raise ValueError("Invalid event cursor.")
    return observed_at, event_id


def _quickwit_search_after(cursor: str | None) -> list[Any] | None:
    payload = _decode_cursor(cursor)
    if payload is None:
        return None
    sort_values = payload.get("sort_values")
    if isinstance(sort_values, list) and sort_values:
        return sort_values
    observed_cursor = _parse_cursor(cursor)
    if observed_cursor is None:
        return None
    return [observed_cursor[0], event_sort_key(observed_cursor[1])]


def _event_query_freshness(
    *,
    query_backend: str,
    watermark_at: str | None,
    status: str = "current",
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "query_backend": query_backend,
        "canonical_store": CANONICAL_EVENT_STORE,
        "canonical_format": CANONICAL_EVENT_FORMAT,
        "status": status,
        "detail": detail,
        "watermark_at": watermark_at,
    }


def canonical_event_store_status(*, query_backend: str) -> dict[str, Any]:
    return {
        "canonical_store": CANONICAL_EVENT_STORE,
        "canonical_format": CANONICAL_EVENT_FORMAT,
        "query_backend": query_backend,
        "detail_hydration": DETAIL_HYDRATION_STRATEGY,
        "cursor_query_support": True,
        "text_query_support": True,
        "time_range_support": True,
    }


def active_event_query_backend() -> str:
    return "quickwit" if quickwit_config_from_env() is not None else "canonical_lake_scan"


def _escape_query_string(value: str) -> str:
    escaped: list[str] = []
    for char in value:
        if char in _QUERY_STRING_RESERVED_CHARS:
            escaped.append(f"\\{char}")
        else:
            escaped.append(char)
    return "".join(escaped)


def _term_query(field: str, value: str) -> dict[str, Any]:
    return {"term": {field: {"value": value}}}


def _any_term_query(fields: list[str], value: str) -> dict[str, Any]:
    return {
        "bool": {
            "should": [_term_query(field, value) for field in fields],
            "minimum_should_match": 1,
        }
    }


def _terms_filter(field: str, values: list[str]) -> dict[str, Any]:
    return {
        "bool": {
            "should": [_term_query(field, value) for value in values],
            "minimum_should_match": 1,
        }
    }


def _timestamp_seconds(value: str | None, *, ceil: bool = False) -> int | None:
    if value is None:
        return None
    timestamp = parse_timestamp(value)
    seconds = int(timestamp.timestamp())
    if ceil and timestamp.microsecond > 0:
        return seconds + 1
    return seconds


def _quickwit_query_payload(
    workspace: Workspace,
    *,
    limit: int,
    cursor: str | None,
    start_time: str | None,
    end_time: str | None,
    query_text: str | None,
    sources: list[str] | None,
    action_categories: list[str] | None,
    sensitivities: list[str] | None,
    principal_reference: str | None,
    resource_reference: str | None,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [_term_query("tenant_id", workspace.tenant_id)]
    if sources:
        filters.append(_terms_filter("source", sources))
    if action_categories:
        filters.append(_terms_filter("action_category", action_categories))
    if sensitivities:
        filters.append(_terms_filter("resource_sensitivity", sensitivities))
    if principal_reference:
        filters.append(
            _any_term_query(
                ["principal_id", "principal_entity_id", "principal_entity_key", "principal_email"],
                principal_reference,
            )
        )
    if resource_reference:
        filters.append(
            _any_term_query(
                ["resource_id", "resource_entity_id", "resource_entity_key"],
                resource_reference,
            )
        )

    must: list[dict[str, Any]] = []
    if query_text:
        must.append(
            {
                "query_string": {
                    "query": _escape_query_string(query_text),
                    "fields": ["search_text"],
                    "default_operator": "AND",
                }
            }
        )

    body: dict[str, Any] = {
        "size": limit + 1,
        "sort": [
            {"observed_at": {"order": "desc"}},
            {"event_sort_key": {"order": "desc"}},
        ],
        "query": {
            "bool": {
                "filter": filters,
            }
        },
    }
    if must:
        body["query"]["bool"]["must"] = must
    start_timestamp = _timestamp_seconds(start_time)
    end_timestamp = _timestamp_seconds(end_time, ceil=True)
    if start_timestamp is not None:
        body["query"]["bool"]["filter"].append({"range": {"observed_at": {"gte": start_timestamp}}})
    if end_timestamp is not None:
        body["query"]["bool"]["filter"].append({"range": {"observed_at": {"lt": end_timestamp}}})
    search_after = _quickwit_search_after(cursor)
    if search_after is not None:
        body["search_after"] = search_after
    return body


def _normalize_quickwit_hit(hit: dict[str, Any]) -> dict[str, Any]:
    if isinstance(hit.get("_source"), dict):
        return hit["_source"]
    if isinstance(hit.get("document"), dict):
        return hit["document"]
    return hit


def _query_quickwit_events(
    workspace: Workspace,
    *,
    limit: int,
    cursor: str | None,
    start_time: str | None,
    end_time: str | None,
    query_text: str | None,
    sources: list[str] | None,
    action_categories: list[str] | None,
    sensitivities: list[str] | None,
    principal_reference: str | None,
    resource_reference: str | None,
) -> dict[str, Any]:
    config = quickwit_config_from_env()
    if config is None:
        raise RuntimeError("Quickwit event query requested without configuration.")
    payload = _quickwit_query_payload(
        workspace,
        limit=limit,
        cursor=cursor,
        start_time=start_time,
        end_time=end_time,
        query_text=query_text,
        sources=sources,
        action_categories=action_categories,
        sensitivities=sensitivities,
        principal_reference=principal_reference,
        resource_reference=resource_reference,
    )
    try:
        result = search_events(config=config, query=payload)
    except error.HTTPError as exc:
        if exc.code != 404:
            raise
        return {
            "items": [],
            "page": {
                "limit": limit,
                "returned": 0,
                "has_more": False,
                "cursor": cursor,
                "next_cursor": None,
            },
            "freshness": _event_query_freshness(
                query_backend="quickwit",
                watermark_at=None,
                status="index_missing",
                detail="Quickwit is running, but the event index has not been created yet.",
            ),
        }
    raw_hits = result.get("hits", {})
    if isinstance(raw_hits, dict):
        hit_rows = raw_hits.get("hits", [])
    else:
        hit_rows = raw_hits
    hits = [hit for hit in hit_rows if isinstance(hit, dict)]
    has_more = len(hits) > limit
    hits = hits[:limit]
    items: list[dict[str, Any]] = []
    next_sort_values = None
    for hit in hits:
        document = _normalize_quickwit_hit(hit)
        hydrated = hydrate_event_by_pointer(
            workspace,
            document.get("normalized_pointer"),
            event_id=document.get("event_id"),
        )
        if hydrated is not None:
            items.append(hydrated)
            sort_values = hit.get("sort")
            if isinstance(sort_values, list):
                next_sort_values = sort_values
    next_cursor = None
    if has_more and items:
        last_item = items[-1]
        next_cursor = _cursor_payload(
            last_item["observed_at"],
            last_item["event_id"],
            sort_values=next_sort_values,
        )
    quickwit_state = workspace.load_quickwit_index_state()
    return {
        "items": items,
        "page": {
            "limit": limit,
            "returned": len(items),
            "has_more": has_more,
            "cursor": cursor,
            "next_cursor": next_cursor,
        },
        "freshness": _event_query_freshness(
            query_backend="quickwit",
            watermark_at=quickwit_state.get("watermark_at"),
        ),
    }


def _query_canonical_lake_events(
    workspace: Workspace,
    *,
    limit: int,
    cursor: str | None,
    start_time: str | None,
    end_time: str | None,
    query_text: str | None,
    sources: list[str] | None,
    action_categories: list[str] | None,
    sensitivities: list[str] | None,
    principal_reference: str | None,
    resource_reference: str | None,
) -> dict[str, Any]:
    cursor_payload = _parse_cursor(cursor)
    rows = scan_events(
        workspace,
        limit=limit,
        cursor=cursor_payload,
        start_time=start_time,
        end_time=end_time,
        query_text=query_text,
        sources=sources,
        action_categories=action_categories,
        sensitivities=sensitivities,
        principal_reference=principal_reference,
        resource_reference=resource_reference,
    )
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = None
    if has_more and items:
        last_item = items[-1]
        next_cursor = _cursor_payload(last_item["observed_at"], last_item["event_id"])
    watermark_at = items[0]["observed_at"] if items else None
    return {
        "items": items,
        "page": {
            "limit": limit,
            "returned": len(items),
            "has_more": has_more,
            "cursor": cursor,
            "next_cursor": next_cursor,
        },
        "freshness": _event_query_freshness(
            query_backend="canonical_lake_scan",
            watermark_at=watermark_at,
        ),
    }


def query_events(
    workspace: Workspace,
    *,
    limit: int = 50,
    cursor: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    query_text: str | None = None,
    sources: list[str] | None = None,
    action_categories: list[str] | None = None,
    sensitivities: list[str] | None = None,
    principal_reference: str | None = None,
    resource_reference: str | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    del dsn
    if quickwit_config_from_env() is not None:
        return _query_quickwit_events(
            workspace,
            limit=limit,
            cursor=cursor,
            start_time=start_time,
            end_time=end_time,
            query_text=query_text,
            sources=sources,
            action_categories=action_categories,
            sensitivities=sensitivities,
            principal_reference=principal_reference,
            resource_reference=resource_reference,
        )
    return _query_canonical_lake_events(
        workspace,
        limit=limit,
        cursor=cursor,
        start_time=start_time,
        end_time=end_time,
        query_text=query_text,
        sources=sources,
        action_categories=action_categories,
        sensitivities=sensitivities,
        principal_reference=principal_reference,
        resource_reference=resource_reference,
    )


def get_event_detail(
    workspace: Workspace,
    event_id: str,
    *,
    dsn: str | None = None,
) -> dict[str, Any] | None:
    del dsn
    return get_event_by_id(workspace, event_id)


def build_index_document(workspace: Workspace, event_id: str) -> dict[str, Any] | None:
    event = get_event_by_id(workspace, event_id)
    if event is None:
        return None
    return build_event_index_document(workspace, event)
