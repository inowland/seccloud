from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from seccloud.api_models import (
    Detection,
    DetectionDetail,
    DetectionList,
    Event,
    EventList,
    Health,
    IntakeAccepted,
    IntakeRequest,
    Overview,
    RuntimeStatus,
    SourceCapabilityMatrix,
    StreamAdvance,
    StreamReset,
    StreamState,
    WorkerState,
)
from seccloud.defaults import DEFAULT_WORKSPACE
from seccloud.investigation import (
    acknowledge_detection,
    get_detection_detail,
    get_event_detail,
)
from seccloud.local_postgres import local_postgres_dsn
from seccloud.projection_store import (
    fetch_projected_detections,
    fetch_projected_events,
    fetch_projection_overview,
    sync_workspace_projection,
)
from seccloud.runtime_status import build_runtime_status
from seccloud.runtime_stream import (
    advance_runtime_stream,
    get_runtime_stream_state,
    initialize_runtime_stream,
    reset_stream_cursor,
)
from seccloud.source_pack import build_source_capability_matrix
from seccloud.storage import IntakeIdempotencyConflictError, Workspace
from seccloud.workers import (
    IntakeValidationError,
    get_worker_state,
    submit_raw_events,
)


def create_app() -> FastAPI:
    workspace_root = Path(os.environ.get("SECCLOUD_WORKSPACE", DEFAULT_WORKSPACE))
    dsn = os.environ.get("SECCLOUD_PROJECTION_DSN") or local_postgres_dsn(Path.cwd())
    push_max_records = int(os.environ.get("SECCLOUD_PUSH_MAX_RECORDS", "5000"))
    push_max_body_bytes = int(os.environ.get("SECCLOUD_PUSH_MAX_BODY_BYTES", str(5 * 1024 * 1024)))

    def workspace() -> Workspace:
        ws = Workspace(workspace_root)
        ws.bootstrap()
        return ws

    def push_credentials() -> dict[str, dict[str, str]]:
        raw = os.environ.get("SECCLOUD_PUSH_AUTH_TOKENS", "{}")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=500, detail="Invalid SECCLOUD_PUSH_AUTH_TOKENS configuration.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=500, detail="Invalid SECCLOUD_PUSH_AUTH_TOKENS configuration.")
        credentials: dict[str, dict[str, str]] = {}
        for token, config in payload.items():
            if not isinstance(token, str) or not isinstance(config, dict):
                continue
            tenant_id = config.get("tenant_id")
            source = config.get("source")
            integration_id = config.get("integration_id")
            if not isinstance(tenant_id, str) or not tenant_id:
                continue
            if not isinstance(source, str) or not source:
                continue
            entry = {"tenant_id": tenant_id, "source": source}
            if isinstance(integration_id, str) and integration_id:
                entry["integration_id"] = integration_id
            credentials[token] = entry
        return credentials

    def authenticate_push_request(authorization: str | None) -> dict[str, str]:
        if not authorization:
            raise HTTPException(status_code=401, detail="Missing Authorization header.")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise HTTPException(status_code=401, detail="Expected Bearer token.")
        credentials = push_credentials()
        config = credentials.get(token)
        if config is None:
            raise HTTPException(status_code=401, detail="Invalid push ingestion token.")
        return config

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
    async def intake_raw_events(
        request: Request,
        authorization: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> dict:
        ws = workspace()
        auth_context = authenticate_push_request(authorization)
        if auth_context["tenant_id"] != ws.tenant_id:
            raise HTTPException(status_code=403, detail="Push token is not valid for this tenant.")

        body = await request.body()
        if len(body) > push_max_body_bytes:
            raise HTTPException(status_code=413, detail="Compressed request body exceeds the configured limit.")
        content_encoding = request.headers.get("content-encoding", "identity").lower()
        if content_encoding in {"", "identity"}:
            decoded_body = body
        elif content_encoding == "gzip":
            try:
                decoded_body = gzip.decompress(body)
            except gzip.BadGzipFile as exc:
                raise HTTPException(status_code=400, detail="Invalid gzip request body.") from exc
        else:
            raise HTTPException(status_code=415, detail=f"Unsupported content encoding: {content_encoding}")
        if len(decoded_body) > push_max_body_bytes:
            raise HTTPException(status_code=413, detail="Decoded request body exceeds the configured limit.")

        try:
            intake_request = IntakeRequest.model_validate_json(decoded_body)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=json.loads(exc.json())) from exc

        if intake_request.source != auth_context["source"]:
            raise HTTPException(status_code=403, detail="Push token source does not match request source.")
        if len(intake_request.records) > push_max_records:
            raise HTTPException(status_code=413, detail="Record count exceeds the configured limit.")

        integration_id = intake_request.integration_id
        auth_integration_id = auth_context.get("integration_id")
        if auth_integration_id is not None:
            if integration_id is not None and integration_id != auth_integration_id:
                raise HTTPException(status_code=403, detail="Push token integration does not match request.")
            integration_id = auth_integration_id

        try:
            return submit_raw_events(
                ws,
                source=intake_request.source,
                records=intake_request.records,
                intake_kind=intake_request.intake_kind,
                integration_id=integration_id,
                received_at=intake_request.received_at,
                metadata=intake_request.metadata,
                idempotency_key=idempotency_key,
            )
        except IntakeValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IntakeIdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/workers/state", response_model=WorkerState)
    def worker_state() -> dict:
        return get_worker_state(workspace())

    @app.get("/api/runtime-status", response_model=RuntimeStatus)
    def runtime_status() -> dict:
        return build_runtime_status(workspace(), dsn=dsn, runtime_root=Path.cwd())

    @app.post("/api/stream/reset", response_model=StreamReset)
    def stream_reset() -> dict:
        ws = workspace()
        source_path = ws.manifests_dir / "runtime_stream_source_events.json"
        if source_path.exists():
            return reset_stream_cursor(ws)
        return initialize_runtime_stream(ws)

    @app.post("/api/stream/advance", response_model=StreamAdvance)
    def stream_advance(batch_size: int = Query(default=5, ge=1, le=10000)) -> dict:
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
        ws = workspace()
        return fetch_projected_events(tenant_id=ws.tenant_id, limit=limit, offset=offset, dsn=dsn)

    @app.get("/api/detections", response_model=DetectionList)
    def detections(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        ws = workspace()
        return fetch_projected_detections(tenant_id=ws.tenant_id, limit=limit, offset=offset, dsn=dsn)

    @app.get("/api/detections/{detection_id}", response_model=DetectionDetail)
    def detection_detail(detection_id: str) -> dict:
        ws = workspace()
        if ws.get_detection(detection_id) is None:
            raise HTTPException(status_code=404, detail=f"Detection not found: {detection_id}")
        return get_detection_detail(ws, detection_id, dsn=dsn)

    @app.post("/api/detections/{detection_id}/acknowledge", response_model=Detection)
    def detection_acknowledge(detection_id: str) -> dict:
        ws = workspace()
        try:
            detection = acknowledge_detection(ws, detection_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        sync_workspace_projection(ws, dsn)
        return detection

    @app.get("/api/source-capability", response_model=SourceCapabilityMatrix)
    def source_capability() -> dict:
        return build_source_capability_matrix(workspace())

    @app.get("/api/events/{event_id}", response_model=Event)
    def event_detail(event_id: str) -> dict:
        ws = workspace()
        event = get_event_detail(ws, event_id, dsn=dsn)
        if event is None:
            raise HTTPException(status_code=404, detail=f"Event not found: {event_id}")
        return event

    return app


app = create_app()
