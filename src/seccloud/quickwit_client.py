from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, parse, request


class QuickwitError(RuntimeError):
    pass


def quickwit_config_from_env() -> dict[str, str] | None:
    base_url = os.environ.get("SECCLOUD_QUICKWIT_URL")
    index_id = os.environ.get("SECCLOUD_QUICKWIT_INDEX")
    if not base_url or not index_id:
        return None
    return {
        "base_url": base_url.rstrip("/"),
        "index_id": index_id,
    }


def _search_url(config: dict[str, str]) -> str:
    return f"{config['base_url']}/api/v1/_elastic/{parse.quote(config['index_id'], safe='')}/_search"


def _indexes_url(config: dict[str, str]) -> str:
    return f"{config['base_url']}/api/v1/indexes"


def _index_url(config: dict[str, str]) -> str:
    return f"{_indexes_url(config)}/{parse.quote(config['index_id'], safe='')}"


def _ingest_url(config: dict[str, str], *, commit: str) -> str:
    query = parse.urlencode({"commit": commit})
    return f"{config['base_url']}/api/v1/{parse.quote(config['index_id'], safe='')}/ingest?{query}"


def _request_json(
    *,
    url: str,
    method: str,
    headers: dict[str, str],
    body: bytes | None = None,
) -> dict[str, Any]:
    req = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req) as response:  # noqa: S310 - operator-provided local service URL
            payload = response.read().decode("utf-8")
    except error.HTTPError as exc:
        if exc.code == 404:
            raise
        detail = exc.read().decode("utf-8", errors="replace")
        raise QuickwitError(f"Quickwit request failed with HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise QuickwitError(f"Quickwit request failed: {exc.reason}") from exc
    if not payload:
        return {}
    return json.loads(payload)


def search_events(
    *,
    config: dict[str, str],
    query: dict[str, Any],
) -> dict[str, Any]:
    return _request_json(
        url=_search_url(config),
        method="POST",
        headers={"Content-Type": "application/json"},
        body=json.dumps(query).encode("utf-8"),
    )


def get_index_metadata(*, config: dict[str, str]) -> dict[str, Any] | None:
    try:
        return _request_json(
            url=_index_url(config),
            method="GET",
            headers={"Accept": "application/json"},
        )
    except error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def create_index(*, config: dict[str, str], index_config: dict[str, Any]) -> dict[str, Any]:
    return _request_json(
        url=_indexes_url(config),
        method="POST",
        headers={"Content-Type": "application/json"},
        body=json.dumps(index_config).encode("utf-8"),
    )


def ingest_documents(
    *,
    config: dict[str, str],
    documents: list[dict[str, Any]],
    commit: str = "wait_for",
) -> dict[str, Any]:
    payload = "\n".join(json.dumps(document, sort_keys=True) for document in documents).encode("utf-8")
    return _request_json(
        url=_ingest_url(config, commit=commit),
        method="POST",
        headers={"Content-Type": "application/x-ndjson"},
        body=payload,
    )


def get_index_stats(*, config: dict[str, str]) -> dict[str, Any]:
    response = search_events(
        config=config,
        query={
            "size": 1,
            "track_total_hits": True,
            "query": {"match_all": {}},
            "sort": [{"observed_at": {"order": "desc"}}],
            "_source": ["observed_at"],
        },
    )
    hits = response.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        indexed_event_count = int(total.get("value", 0))
    else:
        indexed_event_count = int(total or 0)
    hit_items = hits.get("hits", [])
    watermark_at = None
    if isinstance(hit_items, list) and hit_items:
        source = hit_items[0].get("_source", {})
        if isinstance(source, dict):
            observed_at = source.get("observed_at")
            if isinstance(observed_at, str):
                watermark_at = observed_at
    return {
        "indexed_event_count": indexed_event_count,
        "watermark_at": watermark_at,
    }
