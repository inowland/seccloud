from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from seccloud.api_models import (
    DetectionDetail,
    DetectionList,
    Event,
    EventList,
    Health,
    IntakeAccepted,
    IntakeRequest,
    Overview,
    SourceCapabilityMatrix,
    StreamAdvance,
    StreamReset,
    StreamState,
    WorkerState,
)
from seccloud.defaults import DEFAULT_WORKSPACE
from seccloud.investigation import (
    get_detection_detail,
    get_event_detail,
)
from seccloud.local_postgres import local_postgres_dsn
from seccloud.projection_store import (
    fetch_projected_detections,
    fetch_projected_events,
    fetch_projection_overview,
)
from seccloud.runtime_stream import (
    advance_runtime_stream,
    get_runtime_stream_state,
    initialize_runtime_stream,
)
from seccloud.source_pack import build_source_capability_matrix
from seccloud.storage import Workspace
from seccloud.workers import (
    get_worker_state,
    submit_raw_events,
)


def create_app() -> FastAPI:
    workspace_root = Path(os.environ.get("SECCLOUD_WORKSPACE", DEFAULT_WORKSPACE))
    dsn = os.environ.get("SECCLOUD_PROJECTION_DSN") or local_postgres_dsn(Path.cwd())

    def workspace() -> Workspace:
        ws = Workspace(workspace_root)
        ws.bootstrap()
        return ws

    app = FastAPI(title="Seccloud API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health", response_model=Health)
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/intake/raw-events", response_model=IntakeAccepted, status_code=202)
    def intake_raw_events(request: IntakeRequest) -> dict:
        ws = workspace()
        records: list[dict[str, object]] = []
        for record in request.records:
            if "source" in record and record["source"] != request.source:
                raise HTTPException(
                    status_code=400,
                    detail="All records in an intake batch must match the request source.",
                )
            records.append({"source": request.source, **record})
        return submit_raw_events(
            ws,
            source=request.source,
            records=records,
            intake_kind=request.intake_kind,
            integration_id=request.integration_id,
            received_at=request.received_at,
            metadata=request.metadata,
        )

    @app.get("/api/workers/state", response_model=WorkerState)
    def worker_state() -> dict:
        return get_worker_state(workspace())

    @app.post("/api/stream/reset", response_model=StreamReset)
    def stream_reset() -> dict:
        ws = workspace()
        return initialize_runtime_stream(ws)

    @app.post("/api/stream/advance", response_model=StreamAdvance)
    def stream_advance(batch_size: int = Query(default=5, ge=1, le=50)) -> dict:
        ws = workspace()
        return advance_runtime_stream(ws, batch_size=batch_size)

    @app.get("/api/stream/state", response_model=StreamState)
    def stream_state() -> dict:
        ws = workspace()
        return get_runtime_stream_state(ws)

    @app.get("/api/overview", response_model=Overview)
    def overview() -> dict:
        return fetch_projection_overview(dsn)

    @app.get("/api/events", response_model=EventList)
    def events(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        return fetch_projected_events(limit=limit, offset=offset, dsn=dsn)

    @app.get("/api/detections", response_model=DetectionList)
    def detections(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        return fetch_projected_detections(limit=limit, offset=offset, dsn=dsn)

    @app.get("/api/detections/{detection_id}", response_model=DetectionDetail)
    def detection_detail(detection_id: str) -> dict:
        ws = workspace()
        if ws.get_detection(detection_id) is None:
            raise HTTPException(status_code=404, detail=f"Detection not found: {detection_id}")
        return get_detection_detail(ws, detection_id)

    @app.get("/api/source-capability", response_model=SourceCapabilityMatrix)
    def source_capability() -> dict:
        return build_source_capability_matrix(workspace())

    @app.get("/api/events/{event_id}", response_model=Event)
    def event_detail(event_id: str) -> dict:
        ws = workspace()
        event = get_event_detail(ws, event_id)
        if event is None:
            raise HTTPException(status_code=404, detail=f"Event not found: {event_id}")
        return event

    return app


app = create_app()
