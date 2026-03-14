from __future__ import annotations

import argparse
import json
from typing import Any

from seccloud.demo_postgres import (
    demo_postgres_dsn,
    init_demo_postgres,
    start_demo_postgres,
    stop_demo_postgres,
)
from seccloud.demo_projection import sync_workspace_to_postgres
from seccloud.demo_stream import advance_demo_stream, get_demo_stream_state, initialize_demo_stream
from seccloud.investigation import (
    build_evidence_bundle,
    build_peer_comparison,
    create_case_from_detection,
    get_entity_timeline,
    list_detections,
    summarize_case,
    update_case,
)
from seccloud.onboarding import (
    build_onboarding_report_markdown,
    build_source_manifest,
    export_source_manifest,
    import_fixture_bundle,
    validate_fixture_bundle,
)
from seccloud.pipeline import (
    build_derived_state_and_detections,
    collect_ops_metadata,
    evaluate_scenarios,
    ingest_raw_events,
    rebuild_derived_state,
    run_demo,
    seed_workspace,
)
from seccloud.reports import (
    build_conversation_pack_markdown,
    build_evaluation_summary_markdown,
    export_founder_artifacts,
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


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="seccloud")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_workspace_argument(command: argparse.ArgumentParser) -> argparse.ArgumentParser:
        command.add_argument("--workspace", default="examples/poc/runtime")
        return command

    add_workspace_argument(subparsers.add_parser("seed-data"))
    add_workspace_argument(subparsers.add_parser("run-pipeline"))
    add_workspace_argument(subparsers.add_parser("run-demo"))
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
    add_workspace_argument(subparsers.add_parser("evaluate-scenarios"))
    add_workspace_argument(subparsers.add_parser("show-evaluation-report"))
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

    demo_init_stream = add_workspace_argument(subparsers.add_parser("demo-init-stream"))
    demo_init_stream.set_defaults(workspace="examples/poc/demo-runtime")

    demo_advance_stream = add_workspace_argument(subparsers.add_parser("demo-advance-stream"))
    demo_advance_stream.set_defaults(workspace="examples/poc/demo-runtime")
    demo_advance_stream.add_argument("--batch-size", type=int, default=5)

    demo_stream_state = add_workspace_argument(subparsers.add_parser("demo-stream-state"))
    demo_stream_state.set_defaults(workspace="examples/poc/demo-runtime")

    demo_sync_postgres = add_workspace_argument(subparsers.add_parser("demo-sync-postgres"))
    demo_sync_postgres.set_defaults(workspace="examples/poc/demo-runtime")
    demo_sync_postgres.add_argument("--dsn")

    subparsers.add_parser("demo-init-postgres")
    subparsers.add_parser("demo-start-postgres")
    subparsers.add_parser("demo-stop-postgres")

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
    elif args.command == "run-pipeline":
        assert workspace is not None
        ingest = ingest_raw_events(workspace)
        detect = build_derived_state_and_detections(workspace)
        ops = collect_ops_metadata(workspace)
        _print({"ingest": ingest, "detect": detect, "ops_metadata": ops})
    elif args.command == "run-demo":
        assert workspace is not None
        result = run_demo(workspace)
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
    elif args.command == "evaluate-scenarios":
        assert workspace is not None
        _print(evaluate_scenarios(workspace))
    elif args.command == "show-evaluation-report":
        assert workspace is not None
        print(build_evaluation_summary_markdown(workspace), end="")
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
    elif args.command == "demo-init-stream":
        assert workspace is not None
        _print(initialize_demo_stream(workspace))
    elif args.command == "demo-advance-stream":
        assert workspace is not None
        _print(advance_demo_stream(workspace, batch_size=args.batch_size))
    elif args.command == "demo-stream-state":
        assert workspace is not None
        _print(get_demo_stream_state(workspace))
    elif args.command == "demo-sync-postgres":
        assert workspace is not None
        _print(sync_workspace_to_postgres(workspace, args.dsn or demo_postgres_dsn(".")))
    elif args.command == "demo-init-postgres":
        _print(init_demo_postgres("."))
    elif args.command == "demo-start-postgres":
        _print(start_demo_postgres("."))
    elif args.command == "demo-stop-postgres":
        _print(stop_demo_postgres("."))
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
