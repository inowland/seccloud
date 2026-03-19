from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request

from seccloud.api import create_app
from seccloud.defaults import DEFAULT_WORKSPACE
from seccloud.investigation import (
    build_evidence_bundle,
    build_peer_comparison,
    create_case_from_detection,
    get_entity_timeline,
    list_detections,
    summarize_case,
    update_case,
)
from seccloud.local_postgres import (
    init_local_postgres,
    local_postgres_dsn,
    start_local_postgres,
    stop_local_postgres,
)
from seccloud.local_quickwit import (
    ensure_local_quickwit,
    init_local_quickwit,
    local_quickwit_index_id,
    local_quickwit_url,
    start_local_quickwit,
    stop_local_quickwit,
)
from seccloud.model_artifact import (
    activate_model_artifact,
    deactivate_model_artifact,
    list_installed_model_artifacts,
    load_model_promotion_policy,
    rollback_model_artifact,
    save_model_promotion_policy,
)
from seccloud.model_training import export_workspace_model_artifact
from seccloud.onboarding import (
    build_onboarding_report_markdown,
    build_source_manifest,
    export_source_manifest,
    import_fixture_bundle,
    validate_fixture_bundle,
)
from seccloud.pipeline import (
    collect_ops_metadata,
    rebuild_derived_state,
    run_runtime,
    seed_workspace,
)
from seccloud.quickwit_index import sync_quickwit_event_index
from seccloud.quickwit_smoke import run_quickwit_event_smoke
from seccloud.reports import (
    build_conversation_pack_markdown,
    export_founder_artifacts,
)
from seccloud.runtime_status import build_runtime_status
from seccloud.runtime_stream import (
    advance_runtime_stream,
    get_runtime_stream_state,
    initialize_runtime_stream,
)
from seccloud.source_pack import build_source_capability_markdown
from seccloud.storage import Workspace, format_timestamp, parse_timestamp
from seccloud.vendor_exports import (
    build_vendor_mapping_report_markdown,
    build_vendor_source_manifest,
    export_vendor_source_manifest,
    import_vendor_fixture_bundle,
    validate_vendor_fixture_bundle,
)
from seccloud.workers import (
    get_worker_state,
    run_detection_mode_comparison,
    run_detection_mode_evaluation,
    run_detection_threshold_sweep,
    run_feature_builder,
    run_source_stats_projector,
    run_worker_service_loop,
    run_worker_service_once,
)
from seccloud.workflow_store import WORKFLOW_DSN_ENV_VAR, get_workflow_dsn

DEFAULT_DEMO_API_HOST = "127.0.0.1"
DEFAULT_DEMO_API_PORT = 8000
DEFAULT_DEMO_WEB_HOST = "127.0.0.1"
DEFAULT_DEMO_WEB_PORT = 5173
DEFAULT_DEMO_MODEL_ID = "demo-v1"
DEFAULT_DEMO_STREAM_MAX_PENDING_BATCHES = 24
DEFAULT_DEMO_STREAM_RESUME_PENDING_BATCHES = 8
WORKER_SETTLED_STATUSES = {"idle", "processed", "materialized"}


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _now_timestamp() -> str:
    return format_timestamp(datetime.now(UTC))


def export_openapi_spec(output: str | Path) -> dict[str, Any]:
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    schema = create_app().openapi()
    target.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    return {"path": str(target.resolve())}


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


def _probe_api_health(host: str = DEFAULT_DEMO_API_HOST, port: int = DEFAULT_DEMO_API_PORT) -> dict[str, Any]:
    url = f"http://{host}:{port}/api/health"
    try:
        with request.urlopen(url, timeout=1.5) as response:  # noqa: S310 - local operator endpoint
            payload = json.loads(response.read().decode("utf-8"))
        return {
            "reachable": bool(payload.get("status") == "ok"),
            "url": url,
            "payload": payload,
            "error": None,
        }
    except (OSError, error.URLError, error.HTTPError, json.JSONDecodeError) as exc:
        return {
            "reachable": False,
            "url": url,
            "payload": None,
            "error": str(exc),
        }


def _worker_recency_summary(worker_state: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    now_utc = now or datetime.now(UTC)
    last_service_at = worker_state.get("last_service_at")
    if not last_service_at:
        return {
            "recent": False,
            "age_seconds": None,
            "detail": "The worker has not reported a service iteration yet.",
        }
    parsed = parse_timestamp(str(last_service_at))
    age_seconds = max(0, int((now_utc - parsed).total_seconds()))
    recent = age_seconds <= 30
    return {
        "recent": recent,
        "age_seconds": age_seconds,
        "detail": f"Last worker activity was {age_seconds} seconds ago.",
    }


def build_demo_doctor_report(
    workspace: Workspace,
    *,
    runtime_root: str | Path = ".",
    dsn: str | None = None,
    api_host: str = DEFAULT_DEMO_API_HOST,
    api_port: int = DEFAULT_DEMO_API_PORT,
) -> dict[str, Any]:
    status = build_operator_runtime_status(
        workspace,
        dsn=dsn or local_postgres_dsn(runtime_root),
        runtime_root=runtime_root,
    )
    api = _probe_api_health(api_host, api_port)

    stream_state = status["stream_state"]
    worker_state = status["worker_state"]
    intake_backlog = int(status["intake"]["pending_batch_count"])
    processing_backlog = intake_backlog
    search_backlog = max(
        0,
        int(status["event_index"]["event_count"]) - int(status["quickwit"]["indexed_event_count"]),
    )
    model_active = status["model_runtime"]["effective_mode"] == "onnx_native"
    scoring_ready = bool(status["scoring_input"]["ready"])
    worker_recency = _worker_recency_summary(worker_state)

    blockers: list[str] = []
    warnings: list[str] = []

    if not status["quickwit"]["initialized"]:
        blockers.append("Local Quickwit has not been initialized.")
    if not api["reachable"]:
        blockers.append(f"API is not reachable at {api['url']}.")
    if not status["quickwit"]["running"]:
        blockers.append("Quickwit is not running.")
    elif not status["quickwit"]["ready"]:
        blockers.append("Quickwit is still starting.")
    if status["quickwit"]["last_sync_status"] == "failed":
        blockers.append("Quickwit sync failed.")
    if int(stream_state["total_source_events"]) == 0:
        blockers.append("The demo corpus has not been prepared yet.")
    if not status["event_index"]["available"] or not status["detection_context"]["available"]:
        blockers.append("The event plane is not materialized yet.")
    if not scoring_ready or not model_active:
        blockers.append("Model-backed scoring is not active.")
    if intake_backlog > 0 or processing_backlog > 0:
        blockers.append("The worker is still processing backlog.")
    if search_backlog > 0:
        blockers.append("Quickwit is still catching up to the event index.")

    if (intake_backlog > 0 or processing_backlog > 0) and (
        worker_state.get("last_service_status") == "idle" or not worker_recency["recent"]
    ):
        blockers.append("The worker appears stuck: backlog is present but recent progress is not visible.")

    if not status["quickwit"]["initialized"]:
        next_action = (
            "Run `npm run demo:bootstrap`, then start `npm run app:web`, `npm run app:api`, "
            "and `npm run app:worker` in separate terminals."
        )
    elif not scoring_ready or not model_active or int(stream_state["total_source_events"]) == 0:
        next_action = (
            "Run `npm run demo:prepare-model` to generate the corpus, process it, train the model, and sync search."
        )
    elif intake_backlog > 0 or processing_backlog > 0 or search_backlog > 0:
        next_action = "Leave `npm run app:worker` running and rerun `npm run demo:doctor` once the backlog clears."
    elif not api["reachable"]:
        next_action = "Start `npm run app:web`, `npm run app:api`, and `npm run app:worker` in separate terminals."
    else:
        next_action = "The demo is ready."

    ready = not blockers
    summary = "ready" if ready else "not_ready"
    return {
        "ready": ready,
        "status": summary,
        "summary": "Demo is ready." if ready else "Demo is not ready.",
        "next_action": next_action,
        "blockers": blockers,
        "warnings": warnings,
        "checks": {
            "api": api,
            "worker_recency": worker_recency,
            "backlog": {
                "intake_batches": intake_backlog,
                "processing_events": processing_backlog,
                "search_events": search_backlog,
            },
            "runtime": {
                "quickwit_running": status["quickwit"]["running"],
                "quickwit_ready": status["quickwit"]["ready"],
                "model_active": model_active,
                "scoring_ready": scoring_ready,
            },
        },
    }


def _print_demo_doctor_report(report: dict[str, Any]) -> None:
    verdict = "READY" if report["ready"] else "NOT READY"
    print(f"Demo doctor: {verdict}")
    print(report["summary"])
    print(f"Next action: {report['next_action']}")
    if report["blockers"]:
        print("")
        print("Blocking issues:")
        for item in report["blockers"]:
            print(f"- {item}")
    if report["warnings"]:
        print("")
        print("Warnings:")
        for item in report["warnings"]:
            print(f"- {item}")


def demo_prepare_model(
    workspace: Workspace,
    *,
    runtime_root: str | Path = ".",
    batch_size: int = 500,
    steps: int = 80,
    output_dir: str | None = None,
    model_id: str = DEFAULT_DEMO_MODEL_ID,
    epochs: int = 5,
    seed: int = 42,
) -> dict[str, Any]:
    bootstrap = bootstrap_local_runtime(workspace, runtime_root=runtime_root, reset_stream=False)
    dsn = str(bootstrap["api_env"]["SECCLOUD_WORKFLOW_DSN"])
    stream = run_demo_stream(
        workspace,
        batch_size=batch_size,
        interval_seconds=0.0,
        max_steps=steps,
        max_pending_batches=None,
    )
    background_worker_recent = _worker_recency_summary(get_worker_state(workspace))["recent"]

    if background_worker_recent:
        processing = wait_for_pipeline_idle(
            workspace,
            timeout_seconds=600.0,
            poll_interval_seconds=1.0,
            report_interval_seconds=10.0,
        )
        processing_mode = "background_wait"
    else:
        processing = run_worker_until_settled(
            workspace,
            dsn=dsn,
            timeout_seconds=600.0,
            report_interval_seconds=10.0,
        )
        processing_mode = "foreground_drain"

    target_output_dir = output_dir or str(workspace.root / "demo-model")
    model = export_workspace_model_artifact(
        workspace,
        target_output_dir,
        model_id=model_id,
        epochs=epochs,
        seed=seed,
        install=True,
    )
    with _temporary_env(
        {
            "SECCLOUD_QUICKWIT_URL": local_quickwit_url(runtime_root),
            "SECCLOUD_QUICKWIT_INDEX": local_quickwit_index_id(),
        }
    ):
        quickwit = sync_quickwit_event_index(workspace, commit="wait_for")

    doctor = build_demo_doctor_report(
        workspace,
        runtime_root=runtime_root,
        dsn=dsn,
    )
    return {
        "status": "prepared",
        "bootstrap": bootstrap,
        "stream": stream,
        "processing_mode": processing_mode,
        "processing": processing,
        "model": asdict(model),
        "quickwit": quickwit,
        "doctor": doctor,
    }


def build_operator_runtime_status(
    workspace: Workspace,
    *,
    dsn: str | None,
    runtime_root: str | Path = ".",
) -> dict[str, Any]:
    return build_runtime_status(workspace, dsn=dsn, runtime_root=runtime_root)


def wait_for_pipeline_idle(
    workspace: Workspace,
    *,
    timeout_seconds: float = 180.0,
    poll_interval_seconds: float = 1.0,
    report_interval_seconds: float = 10.0,
) -> dict[str, Any]:
    start_clock = time.monotonic()
    initial_state = get_worker_state(workspace)
    initial_pending = int(initial_state.get("pending_batch_count", 0))
    initial_processed = int(initial_state.get("processed_batch_count", 0))
    initial_feature_runs = int(initial_state.get("feature_runs", 0))
    initial_detection_runs = int(initial_state.get("detection_runs", 0))
    last_report_at = start_clock - report_interval_seconds

    while True:
        state = get_worker_state(workspace)
        pending_batch_count = int(state.get("pending_batch_count", 0))
        processed_batch_count = int(state.get("processed_batch_count", 0))
        feature_runs = int(state.get("feature_runs", 0))
        detection_runs = int(state.get("detection_runs", 0))
        last_service_status = str(state.get("last_service_status") or "unknown")
        settled = pending_batch_count == 0 and last_service_status in WORKER_SETTLED_STATUSES
        progressed = (
            pending_batch_count < initial_pending
            or processed_batch_count > initial_processed
            or feature_runs > initial_feature_runs
            or detection_runs > initial_detection_runs
        )

        now = time.monotonic()
        if now - last_report_at >= report_interval_seconds:
            print(
                "[seccloud] waiting for worker: "
                f"pending={pending_batch_count} "
                f"processed={processed_batch_count} "
                f"features={feature_runs} "
                f"detections={detection_runs} "
                f"status={last_service_status}",
                flush=True,
            )
            last_report_at = now

        if settled and (progressed or initial_pending == 0):
            return {
                "status": "idle" if last_service_status == "idle" else "settled",
                "waited_seconds": round(now - start_clock, 2),
                "pending_batch_count": pending_batch_count,
                "processed_batch_count": processed_batch_count,
                "worker_state": state,
            }

        if now - start_clock >= timeout_seconds:
            raise TimeoutError(
                "timed out waiting for the background worker to catch up; make sure `npm run app:worker` is running"
            )

        time.sleep(poll_interval_seconds)


def run_worker_until_settled(
    workspace: Workspace,
    *,
    dsn: str | None = None,
    timeout_seconds: float = 180.0,
    report_interval_seconds: float = 10.0,
) -> dict[str, Any]:
    start_clock = time.monotonic()
    initial_state = get_worker_state(workspace)
    initial_pending = int(initial_state.get("pending_batch_count", 0))
    initial_processed = int(initial_state.get("processed_batch_count", 0))
    initial_feature_runs = int(initial_state.get("feature_runs", 0))
    initial_detection_runs = int(initial_state.get("detection_runs", 0))
    last_report_at = start_clock - report_interval_seconds

    while True:
        service = run_worker_service_once(workspace, dsn=dsn)
        state = get_worker_state(workspace)
        pending_batch_count = int(state.get("pending_batch_count", 0))
        processed_batch_count = int(state.get("processed_batch_count", 0))
        feature_runs = int(state.get("feature_runs", 0))
        detection_runs = int(state.get("detection_runs", 0))
        status = str(service.get("status") or state.get("last_service_status") or "unknown")
        progressed = (
            pending_batch_count < initial_pending
            or processed_batch_count > initial_processed
            or feature_runs > initial_feature_runs
            or detection_runs > initial_detection_runs
        )

        now = time.monotonic()
        if now - last_report_at >= report_interval_seconds:
            print(
                "[seccloud] running worker pass: "
                f"pending={pending_batch_count} "
                f"processed={processed_batch_count} "
                f"features={feature_runs} "
                f"detections={detection_runs} "
                f"status={status}",
                flush=True,
            )
            last_report_at = now

        if pending_batch_count == 0 and status in WORKER_SETTLED_STATUSES and (progressed or initial_pending == 0):
            return {
                "status": "idle" if status == "idle" else "settled",
                "waited_seconds": round(now - start_clock, 2),
                "pending_batch_count": pending_batch_count,
                "processed_batch_count": processed_batch_count,
                "worker_state": state,
                "service": service,
            }

        if now - start_clock >= timeout_seconds:
            raise TimeoutError(
                "timed out draining the worker in foreground mode; "
                "make sure `npm run app:worker` is running and inspect the worker terminal output"
            )


def _reset_projection(dsn: str) -> None:
    del dsn


def _set_workflow_dsn_env(dsn: str | None) -> None:
    if dsn:
        os.environ[WORKFLOW_DSN_ENV_VAR] = dsn
        return
    os.environ.pop(WORKFLOW_DSN_ENV_VAR, None)


def _require_workflow_dsn(parser: argparse.ArgumentParser, command_name: str) -> str:
    workflow_dsn = get_workflow_dsn()
    if workflow_dsn:
        return workflow_dsn
    parser.error(f"{command_name} requires SECCLOUD_WORKFLOW_DSN")
    raise AssertionError("unreachable")


def bootstrap_local_runtime(
    workspace: Workspace,
    *,
    runtime_root: str | Path = ".",
    reset_stream: bool = False,
) -> dict[str, Any]:
    postgres = start_local_postgres(runtime_root)
    quickwit = start_local_quickwit(runtime_root)
    dsn = postgres.get("dsn") or local_postgres_dsn(runtime_root)
    stream_manifest_path = workspace.manifests_dir / "runtime_stream_manifest.json"
    if reset_stream or not stream_manifest_path.exists():
        _reset_projection(dsn)
        stream = initialize_runtime_stream(workspace, scaled=True, continuous=True)
    else:
        stream = {
            "status": "already_initialized",
            **get_runtime_stream_state(workspace),
        }
    return {
        "workspace": str(workspace.root),
        "tenant_id": workspace.tenant_id,
        "postgres": postgres,
        "quickwit": quickwit,
        "stream": stream,
        "api_env": {
            "SECCLOUD_WORKSPACE": str(workspace.root),
            "SECCLOUD_WORKFLOW_DSN": dsn,
            "SECCLOUD_QUICKWIT_URL": local_quickwit_url(runtime_root),
            "SECCLOUD_QUICKWIT_INDEX": local_quickwit_index_id(),
        },
    }


def run_demo_stream(
    workspace: Workspace,
    *,
    batch_size: int = 500,
    interval_seconds: float = 3.0,
    max_steps: int | None = None,
    max_pending_batches: int | None = DEFAULT_DEMO_STREAM_MAX_PENDING_BATCHES,
    resume_pending_batches: int = DEFAULT_DEMO_STREAM_RESUME_PENDING_BATCHES,
) -> dict[str, Any]:
    if max_pending_batches is not None and max_pending_batches < 0:
        raise ValueError("max_pending_batches must be >= 0")
    if resume_pending_batches < 0:
        raise ValueError("resume_pending_batches must be >= 0")
    if max_pending_batches is not None and resume_pending_batches > max_pending_batches:
        raise ValueError("resume_pending_batches must be <= max_pending_batches")

    steps = 0
    accepted_batches = 0
    accepted_records = 0
    last_result: dict[str, Any] | None = None
    throttled = False
    backpressure_wait_seconds = max(interval_seconds, 1.0)

    while True:
        state = get_runtime_stream_state(workspace)
        if state.get("complete"):
            return {
                "status": "complete" if steps > 0 else "already_complete",
                "steps": steps,
                "accepted_batches": accepted_batches,
                "accepted_records": accepted_records,
                "stream_state": state,
                "last_advance": last_result,
            }
        if max_steps is not None and steps >= max_steps:
            return {
                "status": "stopped",
                "steps": steps,
                "accepted_batches": accepted_batches,
                "accepted_records": accepted_records,
                "stream_state": state,
                "last_advance": last_result,
            }

        if max_pending_batches is not None:
            pending_batch_count = len(workspace.list_pending_intake_batches())
            if throttled:
                if pending_batch_count > resume_pending_batches:
                    print(
                        "[seccloud] stream paused for backlog: "
                        f"pending={pending_batch_count} "
                        f"resume_at<={resume_pending_batches}",
                        flush=True,
                    )
                    time.sleep(backpressure_wait_seconds)
                    continue
                throttled = False
                print(
                    f"[seccloud] stream resumed: pending={pending_batch_count} high_watermark={max_pending_batches}",
                    flush=True,
                )
            elif pending_batch_count >= max_pending_batches:
                throttled = True
                print(
                    "[seccloud] stream paused for backlog: "
                    f"pending={pending_batch_count} "
                    f"high_watermark={max_pending_batches} "
                    f"resume_at<={resume_pending_batches}",
                    flush=True,
                )
                time.sleep(backpressure_wait_seconds)
                continue

        last_result = advance_runtime_stream(workspace, batch_size=batch_size)
        steps += 1
        accepted_batches += int(last_result.get("accepted_batches", 0))
        accepted_records += int(last_result.get("accepted_records", 0))

        if last_result.get("complete"):
            return {
                "status": "complete",
                "steps": steps,
                "accepted_batches": accepted_batches,
                "accepted_records": accepted_records,
                "stream_state": get_runtime_stream_state(workspace),
                "last_advance": last_result,
            }

        time.sleep(interval_seconds)


def run_api_server(
    workspace: Workspace,
    *,
    dsn: str | None,
    runtime_root: str | Path = ".",
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
) -> None:
    import uvicorn

    os.environ["SECCLOUD_WORKSPACE"] = str(workspace.root)
    _set_workflow_dsn_env(dsn)
    os.environ.setdefault("SECCLOUD_QUICKWIT_URL", local_quickwit_url(runtime_root))
    os.environ.setdefault("SECCLOUD_QUICKWIT_INDEX", local_quickwit_index_id())
    uvicorn.run(
        "seccloud.api:app",
        host=host,
        port=port,
        reload=reload,
        timeout_graceful_shutdown=1,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seccloud",
        description="Seccloud development and operator CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_workspace_argument(command: argparse.ArgumentParser) -> argparse.ArgumentParser:
        command.add_argument("--workspace", default=DEFAULT_WORKSPACE)
        return command

    add_workspace_argument(subparsers.add_parser("seed-data"))
    service_once = add_workspace_argument(
        subparsers.add_parser(
            "run-worker-service-once",
            help="Operator-only: run the Rust local worker once.",
        )
    )
    service_once.add_argument("--dsn")
    service_once.add_argument("--runtime-root", default=".")
    service_once.add_argument("--max-batches", type=int)
    service_loop = add_workspace_argument(
        subparsers.add_parser(
            "run-worker-service",
            help="Operator-only: run the Rust local worker loop.",
        )
    )
    service_loop.add_argument("--dsn")
    service_loop.add_argument("--runtime-root", default=".")
    service_loop.add_argument("--poll-interval-seconds", type=float, default=1.0)
    service_loop.add_argument("--max-batches", type=int)
    service_loop.add_argument("--iterations", type=int)
    service_loop.add_argument(
        "--exit-when-idle",
        action="store_true",
        help="Stop the worker loop after the next idle pass.",
    )
    add_workspace_argument(
        subparsers.add_parser(
            "show-worker-state",
            help="Operator-only: inspect worker service state.",
        )
    )
    wait_idle = add_workspace_argument(
        subparsers.add_parser(
            "wait-for-pipeline-idle",
            help="Operator-only: wait until the background worker has caught up and gone idle.",
        )
    )
    wait_idle.add_argument("--timeout-seconds", type=float, default=180.0)
    wait_idle.add_argument("--poll-interval-seconds", type=float, default=1.0)
    wait_idle.add_argument("--report-interval-seconds", type=float, default=10.0)
    add_workspace_argument(
        subparsers.add_parser(
            "run-source-stats-projector",
            help="Operator-only: rebuild source stats with the Rust local worker runtime.",
        )
    )
    add_workspace_argument(
        subparsers.add_parser(
            "run-feature-builder",
            help="Operator-only: materialize Rust-backed feature lake tables from normalized events.",
        )
    )
    sync_quickwit = add_workspace_argument(
        subparsers.add_parser(
            "sync-quickwit-index",
            help="Operator-only: publish canonical normalized events into the Quickwit event index.",
        )
    )
    sync_quickwit.add_argument("--batch-size", type=int, default=500)
    sync_quickwit.add_argument(
        "--commit",
        choices=("auto", "wait_for", "force"),
        default="wait_for",
    )
    sync_quickwit.add_argument("--runtime-root", default=".")
    quickwit_smoke = add_workspace_argument(
        subparsers.add_parser(
            "smoke-quickwit-events",
            help="Operator-only: verify /api/events end-to-end against a live local Quickwit node.",
        )
    )
    quickwit_smoke.add_argument("--runtime-root", default=".")
    quickwit_smoke.add_argument("--limit", type=int, default=5)
    quickwit_smoke.add_argument("--keep-running", action="store_true")
    quickwit_smoke.add_argument("--reuse-existing-data", action="store_true")
    add_workspace_argument(
        subparsers.add_parser(
            "compare-detection-modes",
            help="Operator-only: compare heuristic vs model-backed detections on the current workspace.",
        )
    )
    add_workspace_argument(
        subparsers.add_parser(
            "evaluate-detection-modes",
            help="Operator-only: evaluate heuristic vs model-backed scoring against synthetic truth labels.",
        )
    )
    add_workspace_argument(
        subparsers.add_parser(
            "sweep-detection-thresholds",
            help="Operator-only: evaluate source-specific model thresholds against alert budget and attack recall.",
        )
    )
    train_model = add_workspace_argument(
        subparsers.add_parser(
            "train-model-artifact",
            help=(
                "Operator-only: train a local contrastive model from the "
                "durable feature lake and export a model bundle."
            ),
        )
    )
    train_model.add_argument("--output-dir", required=True)
    train_model.add_argument("--model-id", required=True)
    train_model.add_argument("--epochs", type=int, default=5)
    train_model.add_argument("--seed", type=int, default=42)
    train_model.add_argument(
        "--install",
        action="store_true",
        help="Install the exported model bundle as the active local scoring model.",
    )
    add_workspace_argument(
        subparsers.add_parser(
            "list-model-artifacts",
            help="Operator-only: list installed local model artifacts.",
        )
    )
    activate_model = add_workspace_argument(
        subparsers.add_parser(
            "activate-model-artifact",
            help="Operator-only: promote an installed local model artifact to active scoring.",
        )
    )
    activate_model.add_argument("--model-id", required=True)
    activate_model.add_argument(
        "--force",
        action="store_true",
        help="Bypass eval-gate checks when promoting a model artifact.",
    )
    add_workspace_argument(
        subparsers.add_parser(
            "deactivate-model-artifact",
            help="Operator-only: force heuristic scoring even if a model bundle is installed.",
        )
    )
    rollback_model = add_workspace_argument(
        subparsers.add_parser(
            "rollback-model-artifact",
            help="Operator-only: roll back to a previously active installed model artifact.",
        )
    )
    rollback_model.add_argument("--steps", type=int, default=1)
    rollback_model.add_argument(
        "--force",
        action="store_true",
        help="Bypass eval-gate checks when re-activating a prior model artifact.",
    )
    add_workspace_argument(
        subparsers.add_parser(
            "show-model-promotion-policy",
            help="Operator-only: inspect the local model auto-promotion policy.",
        )
    )
    set_promotion_policy = add_workspace_argument(
        subparsers.add_parser(
            "set-model-promotion-policy",
            help="Operator-only: update the local model auto-promotion policy.",
        )
    )
    set_promotion_policy.add_argument("--required-source-count", type=int)
    set_promotion_policy.add_argument("--identity-source", action="append", default=None)
    set_promotion_policy.add_argument("--resource-source", action="append", default=None)
    set_promotion_policy.add_argument(
        "--require-identity-coverage",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    set_promotion_policy.add_argument(
        "--require-resource-coverage",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    add_workspace_argument(subparsers.add_parser("run-runtime"))
    add_workspace_argument(subparsers.add_parser("list-detections"))

    timeline = add_workspace_argument(subparsers.add_parser("show-timeline"))
    timeline.add_argument("--principal")
    timeline.add_argument("--resource")
    timeline.add_argument("--limit", type=int, default=25)

    case = add_workspace_argument(subparsers.add_parser("create-case"))
    case.add_argument("--detection-id", required=True)

    update = add_workspace_argument(subparsers.add_parser("update-case"))
    update.add_argument("--case-id", required=True)
    update.add_argument("--disposition")
    update.add_argument("--note")
    update.add_argument("--feedback-label")

    evidence = add_workspace_argument(subparsers.add_parser("show-evidence"))
    evidence.add_argument("--detection-id", required=True)

    peer = add_workspace_argument(subparsers.add_parser("show-peer-comparison"))
    peer.add_argument("--detection-id", required=True)

    retention = add_workspace_argument(subparsers.add_parser("apply-retention"))
    retention.add_argument("--days", type=int, default=7)
    retention.add_argument("--reference-time", required=True)

    add_workspace_argument(subparsers.add_parser("rebuild-derived-state"))
    add_workspace_argument(subparsers.add_parser("show-conversation-pack"))
    add_workspace_argument(subparsers.add_parser("show-source-capability-matrix"))
    add_workspace_argument(subparsers.add_parser("export-founder-artifacts"))
    add_workspace_argument(subparsers.add_parser("show-ops-metadata"))
    add_workspace_argument(subparsers.add_parser("export-source-manifest"))

    validate_fixtures = subparsers.add_parser("validate-source-fixtures")
    validate_fixtures.add_argument("--fixtures-dir", required=True)

    import_fixtures = add_workspace_argument(subparsers.add_parser("import-source-fixtures"))
    import_fixtures.add_argument("--fixtures-dir", required=True)

    onboarding_report = subparsers.add_parser("show-onboarding-report")
    onboarding_report.add_argument("--fixtures-dir", required=True)

    validate_vendor = subparsers.add_parser("validate-vendor-fixtures")
    validate_vendor.add_argument("--fixtures-dir", required=True)

    import_vendor = add_workspace_argument(subparsers.add_parser("import-vendor-fixtures"))
    import_vendor.add_argument("--fixtures-dir", required=True)

    vendor_report = subparsers.add_parser("show-vendor-mapping-report")
    vendor_report.add_argument("--fixtures-dir", required=True)

    add_workspace_argument(subparsers.add_parser("export-vendor-source-manifest"))

    init_stream = add_workspace_argument(subparsers.add_parser("init-stream"))
    init_stream.set_defaults(workspace=DEFAULT_WORKSPACE)

    advance_stream = add_workspace_argument(subparsers.add_parser("advance-stream"))
    advance_stream.set_defaults(workspace=DEFAULT_WORKSPACE)
    advance_stream.add_argument("--batch-size", type=int, default=5)

    demo_stream = add_workspace_argument(
        subparsers.add_parser(
            "run-demo-stream",
            help="Operator-only: continuously advance the synthetic demo stream from the CLI.",
        )
    )
    demo_stream.add_argument("--batch-size", type=int, default=500)
    demo_stream.add_argument("--interval-seconds", type=float, default=3.0)
    demo_stream.add_argument("--steps", type=int)
    demo_stream.add_argument(
        "--max-pending-batches",
        type=int,
        default=DEFAULT_DEMO_STREAM_MAX_PENDING_BATCHES,
        help="Pause stream generation when pending intake batches reach this watermark.",
    )
    demo_stream.add_argument(
        "--resume-pending-batches",
        type=int,
        default=DEFAULT_DEMO_STREAM_RESUME_PENDING_BATCHES,
        help="Resume stream generation once pending intake batches fall to this watermark or below.",
    )
    demo_stream.add_argument(
        "--disable-backpressure",
        action="store_true",
        help="Disable backlog-aware throttling for the synthetic stream.",
    )

    stream_state = add_workspace_argument(subparsers.add_parser("stream-state"))
    stream_state.set_defaults(workspace=DEFAULT_WORKSPACE)

    runtime_status = add_workspace_argument(
        subparsers.add_parser(
            "show-runtime-status",
            help="Operator-only: inspect local runtime, queue, and projection status.",
        )
    )
    runtime_status.add_argument("--dsn")
    runtime_status.add_argument("--runtime-root", default=".")

    bootstrap = add_workspace_argument(
        subparsers.add_parser(
            "bootstrap-local-runtime",
            help="Operator-only: start local dependencies and initialize the stream runtime.",
        )
    )
    bootstrap.add_argument("--runtime-root", default=".")
    bootstrap.add_argument(
        "--reset-stream",
        action="store_true",
        help="Reset the synthetic stream state before starting the local stack.",
    )
    demo_doctor_cmd = add_workspace_argument(
        subparsers.add_parser(
            "demo-doctor",
            help="Operator-only: return a concise ready / not-ready verdict for the demo.",
        )
    )
    demo_doctor_cmd.add_argument("--runtime-root", default=".")
    demo_doctor_cmd.add_argument("--dsn")
    demo_doctor_cmd.add_argument("--api-host", default=DEFAULT_DEMO_API_HOST)
    demo_doctor_cmd.add_argument("--api-port", type=int, default=DEFAULT_DEMO_API_PORT)
    demo_prepare = add_workspace_argument(
        subparsers.add_parser(
            "demo-prepare-model",
            help="Operator-only: generate the demo corpus, process it, train the model, and sync search.",
        )
    )
    demo_prepare.add_argument("--runtime-root", default=".")
    demo_prepare.add_argument("--batch-size", type=int, default=500)
    demo_prepare.add_argument("--steps", type=int, default=80)
    demo_prepare.add_argument("--output-dir")
    demo_prepare.add_argument("--model-id", default=DEFAULT_DEMO_MODEL_ID)
    demo_prepare.add_argument("--epochs", type=int, default=5)
    demo_prepare.add_argument("--seed", type=int, default=42)

    run_api = add_workspace_argument(
        subparsers.add_parser(
            "run-api",
            help="Operator-only: run the local API server with workspace env wired in.",
        )
    )
    run_api.add_argument("--dsn")
    run_api.add_argument("--runtime-root", default=".")
    run_api.add_argument("--host", default="127.0.0.1")
    run_api.add_argument("--port", type=int, default=8000)
    run_api.add_argument("--reload", action="store_true")

    openapi_export = subparsers.add_parser(
        "export-openapi-spec",
        help="Repo maintenance: write the current FastAPI OpenAPI document to disk.",
    )
    openapi_export.add_argument("--output", default="web/openapi.json")

    init_postgres = subparsers.add_parser("init-postgres")
    init_postgres.add_argument("--runtime-root", default=".")
    start_postgres_cmd = subparsers.add_parser("start-postgres")
    start_postgres_cmd.add_argument("--runtime-root", default=".")
    stop_postgres_cmd = subparsers.add_parser("stop-postgres")
    stop_postgres_cmd.add_argument("--runtime-root", default=".")
    init_quickwit = subparsers.add_parser("init-quickwit")
    init_quickwit.add_argument("--runtime-root", default=".")
    ensure_quickwit_cmd = subparsers.add_parser("ensure-quickwit")
    ensure_quickwit_cmd.add_argument("--runtime-root", default=".")
    start_quickwit_cmd = subparsers.add_parser("start-quickwit")
    start_quickwit_cmd.add_argument("--runtime-root", default=".")
    stop_quickwit_cmd = subparsers.add_parser("stop-quickwit")
    stop_quickwit_cmd.add_argument("--runtime-root", default=".")

    case_summary = add_workspace_argument(subparsers.add_parser("show-case-summary"))
    case_summary.add_argument("--case-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    workspace = None
    if hasattr(args, "workspace"):
        workspace = Workspace(args.workspace)
        workspace.bootstrap()

    if args.command == "seed-data":
        assert workspace is not None
        _print(seed_workspace(workspace))
    elif args.command == "run-worker-service-once":
        assert workspace is not None
        _print(
            run_worker_service_once(
                workspace,
                dsn=args.dsn,
                max_batches=args.max_batches,
            )
        )
    elif args.command == "run-worker-service":
        assert workspace is not None
        _print(
            run_worker_service_loop(
                workspace,
                dsn=args.dsn,
                poll_interval_seconds=args.poll_interval_seconds,
                max_batches=args.max_batches,
                max_iterations=args.iterations,
                exit_when_idle=args.exit_when_idle,
            )
        )
    elif args.command == "show-worker-state":
        assert workspace is not None
        _print(get_worker_state(workspace))
    elif args.command == "wait-for-pipeline-idle":
        assert workspace is not None
        _print(
            wait_for_pipeline_idle(
                workspace,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                report_interval_seconds=args.report_interval_seconds,
            )
        )
    elif args.command == "show-runtime-status":
        assert workspace is not None
        _print(
            build_operator_runtime_status(
                workspace,
                dsn=args.dsn,
                runtime_root=args.runtime_root,
            )
        )
    elif args.command == "bootstrap-local-runtime":
        assert workspace is not None
        _print(
            bootstrap_local_runtime(
                workspace,
                runtime_root=args.runtime_root,
                reset_stream=args.reset_stream,
            )
        )
    elif args.command == "demo-doctor":
        assert workspace is not None
        report = build_demo_doctor_report(
            workspace,
            runtime_root=args.runtime_root,
            dsn=args.dsn,
            api_host=args.api_host,
            api_port=args.api_port,
        )
        _print_demo_doctor_report(report)
        return 0 if report["ready"] else 1
    elif args.command == "demo-prepare-model":
        assert workspace is not None
        _print(
            demo_prepare_model(
                workspace,
                runtime_root=args.runtime_root,
                batch_size=args.batch_size,
                steps=args.steps,
                output_dir=args.output_dir,
                model_id=args.model_id,
                epochs=args.epochs,
                seed=args.seed,
            )
        )
    elif args.command == "run-api":
        assert workspace is not None
        run_api_server(
            workspace,
            dsn=args.dsn,
            runtime_root=args.runtime_root,
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    elif args.command == "export-openapi-spec":
        _print(export_openapi_spec(args.output))
    elif args.command == "run-source-stats-projector":
        assert workspace is not None
        _print(run_source_stats_projector(workspace))
    elif args.command == "run-feature-builder":
        assert workspace is not None
        _print(run_feature_builder(workspace))
    elif args.command == "sync-quickwit-index":
        assert workspace is not None
        with _temporary_env(
            {
                "SECCLOUD_QUICKWIT_URL": local_quickwit_url(args.runtime_root),
                "SECCLOUD_QUICKWIT_INDEX": local_quickwit_index_id(),
            }
        ):
            _print(
                sync_quickwit_event_index(
                    workspace,
                    commit=args.commit,
                    batch_size=args.batch_size,
                )
            )
    elif args.command == "smoke-quickwit-events":
        assert workspace is not None
        _print(
            run_quickwit_event_smoke(
                workspace,
                runtime_root=args.runtime_root,
                limit=args.limit,
                keep_running=args.keep_running,
                reuse_existing_data=args.reuse_existing_data,
            )
        )
    elif args.command == "compare-detection-modes":
        assert workspace is not None
        _print(run_detection_mode_comparison(workspace))
    elif args.command == "evaluate-detection-modes":
        assert workspace is not None
        _print(run_detection_mode_evaluation(workspace))
    elif args.command == "sweep-detection-thresholds":
        assert workspace is not None
        _print(run_detection_threshold_sweep(workspace))
    elif args.command == "train-model-artifact":
        assert workspace is not None
        result = export_workspace_model_artifact(
            workspace,
            args.output_dir,
            model_id=args.model_id,
            epochs=args.epochs,
            seed=args.seed,
            install=args.install,
        )
        _print(asdict(result))
    elif args.command == "list-model-artifacts":
        assert workspace is not None
        _print(
            {
                "artifacts": list_installed_model_artifacts(workspace),
                "manifest": workspace.load_model_artifact_manifest(),
            }
        )
    elif args.command == "activate-model-artifact":
        assert workspace is not None
        _print(activate_model_artifact(workspace, args.model_id, force=args.force))
    elif args.command == "deactivate-model-artifact":
        assert workspace is not None
        _print(deactivate_model_artifact(workspace))
    elif args.command == "rollback-model-artifact":
        assert workspace is not None
        _print(rollback_model_artifact(workspace, steps=args.steps, force=args.force))
    elif args.command == "show-model-promotion-policy":
        assert workspace is not None
        _print(load_model_promotion_policy(workspace))
    elif args.command == "set-model-promotion-policy":
        assert workspace is not None
        current = load_model_promotion_policy(workspace)
        _print(
            save_model_promotion_policy(
                workspace,
                required_source_count=(
                    args.required_source_count
                    if args.required_source_count is not None
                    else int(current["required_source_count"])
                ),
                require_identity_coverage=(
                    args.require_identity_coverage
                    if args.require_identity_coverage is not None
                    else bool(current["require_identity_coverage"])
                ),
                require_resource_coverage=(
                    args.require_resource_coverage
                    if args.require_resource_coverage is not None
                    else bool(current["require_resource_coverage"])
                ),
                identity_sources=(
                    args.identity_source if args.identity_source is not None else list(current["identity_sources"])
                ),
                resource_sources=(
                    args.resource_source if args.resource_source is not None else list(current["resource_sources"])
                ),
            )
        )
    elif args.command == "run-runtime":
        assert workspace is not None
        result = run_runtime(workspace)
        result["artifacts"] = export_founder_artifacts(workspace)
        workflow_dsn = get_workflow_dsn()
        if workflow_dsn:
            created_case_ids: set[str] = set()
            for detection in list_detections(workspace):
                case = create_case_from_detection(workspace, detection["detection_id"], dsn=workflow_dsn)
                case_id = case.get("case_id")
                if isinstance(case_id, str) and case_id:
                    created_case_ids.add(case_id)
            result["case_workflow"] = {
                "status": "workflow_store_updated",
                "case_count": len(created_case_ids),
            }
        else:
            result["case_workflow"] = {
                "status": "skipped_no_workflow_store",
                "case_count": 0,
            }
        _print(result)
    elif args.command == "list-detections":
        assert workspace is not None
        _print(list_detections(workspace, dsn=get_workflow_dsn()))
    elif args.command == "show-timeline":
        assert workspace is not None
        _print(
            get_entity_timeline(
                workspace,
                principal_id=args.principal,
                resource_id=args.resource,
                limit=args.limit,
            )
        )
    elif args.command == "create-case":
        assert workspace is not None
        workflow_dsn = _require_workflow_dsn(parser, "create-case")
        _print(
            create_case_from_detection(
                workspace,
                args.detection_id,
                dsn=workflow_dsn,
            )
        )
    elif args.command == "update-case":
        assert workspace is not None
        workflow_dsn = _require_workflow_dsn(parser, "update-case")
        _print(
            update_case(
                workspace,
                args.case_id,
                disposition=args.disposition,
                analyst_note=args.note,
                feedback_label=args.feedback_label,
                dsn=workflow_dsn,
            )
        )
    elif args.command == "show-evidence":
        assert workspace is not None
        _print(build_evidence_bundle(workspace, args.detection_id))
    elif args.command == "show-peer-comparison":
        assert workspace is not None
        _print(build_peer_comparison(workspace, args.detection_id))
    elif args.command == "apply-retention":
        assert workspace is not None
        _print(workspace.apply_raw_retention(args.days, args.reference_time))
    elif args.command == "rebuild-derived-state":
        assert workspace is not None
        _print(rebuild_derived_state(workspace))
    elif args.command == "show-conversation-pack":
        assert workspace is not None
        print(build_conversation_pack_markdown(workspace), end="")
    elif args.command == "show-source-capability-matrix":
        assert workspace is not None
        print(build_source_capability_markdown(workspace), end="")
    elif args.command == "export-source-manifest":
        assert workspace is not None
        _print({"path": export_source_manifest(workspace), "manifest": build_source_manifest()})
    elif args.command == "validate-source-fixtures":
        _print(validate_fixture_bundle(args.fixtures_dir))
    elif args.command == "import-source-fixtures":
        assert workspace is not None
        _print(import_fixture_bundle(workspace, args.fixtures_dir))
    elif args.command == "show-onboarding-report":
        print(build_onboarding_report_markdown(args.fixtures_dir), end="")
    elif args.command == "validate-vendor-fixtures":
        _print(validate_vendor_fixture_bundle(args.fixtures_dir))
    elif args.command == "import-vendor-fixtures":
        assert workspace is not None
        _print(import_vendor_fixture_bundle(workspace, args.fixtures_dir))
    elif args.command == "show-vendor-mapping-report":
        print(build_vendor_mapping_report_markdown(args.fixtures_dir), end="")
    elif args.command == "export-vendor-source-manifest":
        assert workspace is not None
        _print(
            {
                "manifest": build_vendor_source_manifest(),
                "path": export_vendor_source_manifest(workspace),
            }
        )
    elif args.command == "init-stream":
        assert workspace is not None
        _print(initialize_runtime_stream(workspace))
    elif args.command == "advance-stream":
        assert workspace is not None
        _print(advance_runtime_stream(workspace, batch_size=args.batch_size))
    elif args.command == "run-demo-stream":
        assert workspace is not None
        _print(
            run_demo_stream(
                workspace,
                batch_size=args.batch_size,
                interval_seconds=args.interval_seconds,
                max_steps=args.steps,
                max_pending_batches=None if args.disable_backpressure else args.max_pending_batches,
                resume_pending_batches=args.resume_pending_batches,
            )
        )
    elif args.command == "stream-state":
        assert workspace is not None
        _print(get_runtime_stream_state(workspace))
    elif args.command == "init-postgres":
        _print(init_local_postgres(args.runtime_root))
    elif args.command == "start-postgres":
        _print(start_local_postgres(args.runtime_root))
    elif args.command == "stop-postgres":
        _print(stop_local_postgres(args.runtime_root))
    elif args.command == "init-quickwit":
        _print(init_local_quickwit(args.runtime_root))
    elif args.command == "ensure-quickwit":
        _print(ensure_local_quickwit(args.runtime_root))
    elif args.command == "start-quickwit":
        _print(start_local_quickwit(args.runtime_root))
    elif args.command == "stop-quickwit":
        _print(stop_local_quickwit(args.runtime_root))
    elif args.command == "export-founder-artifacts":
        assert workspace is not None
        _print(export_founder_artifacts(workspace))
    elif args.command == "show-ops-metadata":
        assert workspace is not None
        _print(collect_ops_metadata(workspace))
    elif args.command == "show-case-summary":
        assert workspace is not None
        _print(summarize_case(workspace, args.case_id, dsn=_require_workflow_dsn(parser, "show-case-summary")))
    else:
        parser.error(f"unsupported command: {args.command}")
    return 0
