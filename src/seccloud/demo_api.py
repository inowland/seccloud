from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from seccloud.demo_postgres import demo_postgres_dsn
from seccloud.demo_projection import (
    fetch_demo_cases,
    fetch_demo_detections,
    fetch_demo_events,
    fetch_demo_overview,
    fetch_demo_state_item,
    sync_workspace_to_postgres,
)
from seccloud.demo_stream import advance_demo_stream, get_demo_stream_state, initialize_demo_stream
from seccloud.investigation import (
    create_case_from_detection,
    get_case_detail,
    get_detection_detail,
    get_event_detail,
    update_case,
)
from seccloud.storage import Workspace


class CaseDispositionUpdate(BaseModel):
    disposition: str | None = None
    analyst_note: str | None = None
    feedback_label: str | None = None


def create_app() -> FastAPI:
    app = FastAPI(title="Seccloud Demo API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    workspace_root = Path(os.environ.get("SECCLOUD_DEMO_WORKSPACE", "examples/poc/demo-runtime"))
    dsn = os.environ.get("SECCLOUD_DEMO_DSN") or demo_postgres_dsn(Path.cwd())

    def workspace() -> Workspace:
        ws = Workspace(workspace_root)
        ws.bootstrap()
        return ws

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/demo/reset")
    def demo_reset() -> dict:
        ws = workspace()
        result = initialize_demo_stream(ws)
        sync = sync_workspace_to_postgres(ws, dsn)
        return {"stream": result, "projection": sync}

    @app.post("/api/demo/advance")
    def demo_advance(batch_size: int = Query(default=5, ge=1, le=20)) -> dict:
        ws = workspace()
        result = advance_demo_stream(ws, batch_size=batch_size)
        sync = sync_workspace_to_postgres(ws, dsn)
        return {"stream": result, "projection": sync}

    @app.get("/api/demo/state")
    def demo_state() -> dict:
        ws = workspace()
        return get_demo_stream_state(ws)

    @app.post("/api/demo/sync")
    def demo_sync() -> dict:
        ws = workspace()
        return sync_workspace_to_postgres(ws, dsn)

    @app.get("/api/overview")
    def overview() -> dict:
        return fetch_demo_overview(dsn)

    @app.get("/api/events")
    def events(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
        return fetch_demo_events(limit=limit, dsn=dsn)

    @app.get("/api/detections")
    def detections(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
        return fetch_demo_detections(limit=limit, dsn=dsn)

    @app.get("/api/detections/{detection_id}")
    def detection_detail(detection_id: str) -> dict:
        ws = workspace()
        return get_detection_detail(ws, detection_id)

    @app.post("/api/detections/{detection_id}/case")
    def create_case(detection_id: str) -> dict:
        ws = workspace()
        created = create_case_from_detection(ws, detection_id)
        sync_workspace_to_postgres(ws, dsn)
        return created

    @app.get("/api/cases")
    def cases(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
        return fetch_demo_cases(limit=limit, dsn=dsn)

    @app.get("/api/cases/{case_id}")
    def case_detail(case_id: str) -> dict:
        ws = workspace()
        return get_case_detail(ws, case_id)

    @app.post("/api/cases/{case_id}/disposition")
    def update_case_disposition(case_id: str, payload: CaseDispositionUpdate) -> dict:
        ws = workspace()
        updated = update_case(
            ws,
            case_id,
            disposition=payload.disposition,
            analyst_note=payload.analyst_note,
            feedback_label=payload.feedback_label,
        )
        sync_workspace_to_postgres(ws, dsn)
        return updated

    @app.get("/api/source-capability")
    def source_capability() -> dict:
        return fetch_demo_state_item("source_capability_matrix", dsn)

    @app.get("/api/evaluation-summary")
    def evaluation_summary() -> dict:
        return fetch_demo_state_item("evaluation_summary_markdown", dsn)

    @app.get("/api/events/{event_id}")
    def event_detail(event_id: str) -> dict | None:
        ws = workspace()
        return get_event_detail(ws, event_id)

    return app


app = create_app()
