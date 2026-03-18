from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

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
from seccloud.storage import Workspace
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


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_operator_runtime_status(
    workspace: Workspace,
    *,
    dsn: str | None,
    runtime_root: str | Path = ".",
) -> dict[str, Any]:
    return build_runtime_status(workspace, dsn=dsn, runtime_root=runtime_root)


def _reset_projection(dsn: str) -> None:
    """Clear all projected data from postgres."""
    try:
        import psycopg

        from seccloud.projection_store import reset_projection_schema

        with psycopg.connect(dsn) as conn:
            reset_projection_schema(conn)
    except Exception:
        pass


def bootstrap_local_runtime(
    workspace: Workspace,
    *,
    runtime_root: str | Path = ".",
    reset_stream: bool = False,
) -> dict[str, Any]:
    postgres = start_local_postgres(runtime_root)
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
        "stream": stream,
        "api_env": {
            "SECCLOUD_WORKSPACE": str(workspace.root),
            "SECCLOUD_PROJECTION_DSN": dsn,
        },
    }


def run_demo_stream(
    workspace: Workspace,
    *,
    batch_size: int = 500,
    interval_seconds: float = 3.0,
    max_steps: int | None = None,
) -> dict[str, Any]:
    steps = 0
    accepted_batches = 0
    accepted_records = 0
    last_result: dict[str, Any] | None = None

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
    dsn: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
) -> None:
    import uvicorn

    os.environ["SECCLOUD_WORKSPACE"] = str(workspace.root)
    os.environ["SECCLOUD_PROJECTION_DSN"] = dsn
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
            help="Operator-only: run the Rust local worker once and refresh projections if needed.",
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

    run_api = add_workspace_argument(
        subparsers.add_parser(
            "run-api",
            help="Operator-only: run the local API server with workspace and Postgres env wired in.",
        )
    )
    run_api.add_argument("--dsn")
    run_api.add_argument("--runtime-root", default=".")
    run_api.add_argument("--host", default="127.0.0.1")
    run_api.add_argument("--port", type=int, default=8000)
    run_api.add_argument("--reload", action="store_true")

    init_postgres = subparsers.add_parser("init-postgres")
    init_postgres.add_argument("--runtime-root", default=".")
    start_postgres_cmd = subparsers.add_parser("start-postgres")
    start_postgres_cmd.add_argument("--runtime-root", default=".")
    stop_postgres_cmd = subparsers.add_parser("stop-postgres")
    stop_postgres_cmd.add_argument("--runtime-root", default=".")

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
                dsn=args.dsn or local_postgres_dsn(args.runtime_root),
                max_batches=args.max_batches,
            )
        )
    elif args.command == "run-worker-service":
        assert workspace is not None
        _print(
            run_worker_service_loop(
                workspace,
                dsn=args.dsn or local_postgres_dsn(args.runtime_root),
                poll_interval_seconds=args.poll_interval_seconds,
                max_batches=args.max_batches,
                max_iterations=args.iterations,
                exit_when_idle=args.exit_when_idle,
            )
        )
    elif args.command == "show-worker-state":
        assert workspace is not None
        _print(get_worker_state(workspace))
    elif args.command == "show-runtime-status":
        assert workspace is not None
        _print(
            build_operator_runtime_status(
                workspace,
                dsn=args.dsn or local_postgres_dsn(args.runtime_root),
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
    elif args.command == "run-api":
        assert workspace is not None
        run_api_server(
            workspace,
            dsn=args.dsn or local_postgres_dsn(args.runtime_root),
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
    elif args.command == "run-source-stats-projector":
        assert workspace is not None
        _print(run_source_stats_projector(workspace))
    elif args.command == "run-feature-builder":
        assert workspace is not None
        _print(run_feature_builder(workspace))
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
        for detection in list_detections(workspace):
            create_case_from_detection(workspace, detection["detection_id"])
        _print(result)
    elif args.command == "list-detections":
        assert workspace is not None
        _print(list_detections(workspace))
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
        _print(create_case_from_detection(workspace, args.detection_id))
    elif args.command == "update-case":
        assert workspace is not None
        _print(
            update_case(
                workspace,
                args.case_id,
                disposition=args.disposition,
                analyst_note=args.note,
                feedback_label=args.feedback_label,
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
    elif args.command == "export-founder-artifacts":
        assert workspace is not None
        _print(export_founder_artifacts(workspace))
    elif args.command == "show-ops-metadata":
        assert workspace is not None
        _print(collect_ops_metadata(workspace))
    elif args.command == "show-case-summary":
        assert workspace is not None
        _print(summarize_case(workspace, args.case_id))
    else:
        parser.error(f"unsupported command: {args.command}")
    return 0
