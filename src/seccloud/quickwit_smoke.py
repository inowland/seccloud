from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from starlette.testclient import TestClient

from seccloud.api import create_app
from seccloud.local_quickwit import (
    local_quickwit_index_id,
    local_quickwit_url,
    read_quickwit_log_tail,
    start_local_quickwit,
    stop_local_quickwit,
)
from seccloud.pipeline import run_runtime
from seccloud.quickwit_index import sync_quickwit_event_index
from seccloud.storage import Workspace


@contextmanager
def _temporary_env(updates: dict[str, str]) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def run_quickwit_event_smoke(
    workspace: Workspace,
    *,
    runtime_root: str | Path = ".",
    limit: int = 5,
    keep_running: bool = False,
    reuse_existing_data: bool = False,
) -> dict[str, Any]:
    workspace.bootstrap()
    seeded = False
    if not reuse_existing_data or not workspace.list_normalized_events():
        run_runtime(workspace)
        seeded = True

    quickwit = start_local_quickwit(runtime_root)
    started_here = quickwit.get("status") == "started"
    try:
        env = {
            "SECCLOUD_WORKSPACE": str(workspace.root),
            "SECCLOUD_QUICKWIT_URL": local_quickwit_url(runtime_root),
            "SECCLOUD_QUICKWIT_INDEX": local_quickwit_index_id(),
        }
        with _temporary_env(env):
            sync = sync_quickwit_event_index(workspace, commit="force")
            client = TestClient(create_app())
            events_response = client.get("/api/events", params={"limit": limit})
            if events_response.status_code >= 400:
                raise RuntimeError(
                    f"/api/events failed with status {events_response.status_code}: {events_response.text}"
                )
            events_payload = events_response.json()
            items = events_payload.get("items", [])
            if not items:
                raise RuntimeError("Quickwit smoke test returned no events from /api/events.")
            first_event_id = items[0]["event_id"]
            detail_response = client.get(f"/api/events/{first_event_id}")
            if detail_response.status_code >= 400:
                raise RuntimeError(
                    f"/api/events/{first_event_id} failed with status "
                    f"{detail_response.status_code}: {detail_response.text}"
                )
            detail_payload = detail_response.json()
        return {
            "status": "ok",
            "workspace": str(workspace.root),
            "runtime_root": str(Path(runtime_root).resolve()),
            "seeded": seeded,
            "quickwit": quickwit,
            "sync": sync,
            "events_page": {
                "returned": len(items),
                "has_more": bool(events_payload.get("page", {}).get("has_more", False)),
                "query_backend": events_payload.get("freshness", {}).get("query_backend"),
                "watermark_at": events_payload.get("freshness", {}).get("watermark_at"),
            },
            "first_event_id": first_event_id,
            "detail_event_id": detail_payload.get("event_id"),
        }
    except Exception as exc:
        log_tail = read_quickwit_log_tail(runtime_root)
        if log_tail:
            log_excerpt = "\n".join(log_tail[-20:])
            raise RuntimeError(f"{exc}\n\nQuickwit log tail:\n{log_excerpt}") from exc
        raise
    finally:
        if started_here and not keep_running:
            stop_local_quickwit(runtime_root)
