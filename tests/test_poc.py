from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import seccloud.api as seccloud_api
from seccloud.cli import bootstrap_local_runtime, build_operator_runtime_status, build_parser
from seccloud.investigation import (
    acknowledge_detection,
    build_evidence_bundle,
    build_peer_comparison,
    create_case_from_detection,
    get_entity_timeline,
    list_detections,
    summarize_case,
    update_case,
)
from seccloud.local_postgres import _cleanup_stale_runtime_files, _wait_for_socket_ready, postgres_paths
from seccloud.onboarding import (
    build_onboarding_report_markdown,
    build_source_manifest,
    import_fixture_bundle,
    validate_fixture_bundle,
)
from seccloud.pipeline import (
    collect_ops_metadata,
    ingest_raw_events,
    rebuild_derived_state,
    run_runtime,
    seed_workspace,
)
from seccloud.reports import (
    build_conversation_pack_markdown,
    export_founder_artifacts,
)
from seccloud.runtime_stream import (
    advance_runtime_stream,
    get_runtime_stream_state,
    initialize_runtime_stream,
)
from seccloud.source_pack import (
    build_source_capability_markdown,
    build_source_capability_matrix,
)
from seccloud.storage import Workspace
from seccloud.vendor_exports import (
    build_vendor_mapping_report_markdown,
    build_vendor_source_manifest,
    import_vendor_fixture_bundle,
    validate_vendor_fixture_bundle,
)
from seccloud.workers import (
    get_worker_state,
    run_all_local_workers,
    run_local_processing_workers,
    run_normalization_worker,
    run_projector_worker,
    run_source_stats_projector,
    run_worker_service_loop,
    run_worker_service_once,
    submit_grouped_raw_events,
)


class PoCTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = Path(tempfile.mkdtemp(prefix="seccloud-poc-"))
        self.workspace = Workspace(self.tempdir)
        self.workspace.bootstrap()

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_seed_and_normalize_all_sources(self) -> None:
        seed_result = seed_workspace(self.workspace)
        self.assertGreater(seed_result["raw_event_count"], 0)

        ingest_result = ingest_raw_events(self.workspace)
        normalized = self.workspace.list_normalized_events()
        sources = {event["source"] for event in normalized}

        self.assertEqual(sources, {"okta", "gworkspace", "github", "snowflake"})
        self.assertEqual(ingest_result["normalized_events_seen"], len(normalized))
        self.assertEqual(ingest_result["dead_letter_count"], 2)
        self.assertEqual(ingest_result["duplicate_semantic_events"], 1)
        self.assertEqual(ingest_result["late_arrival_count"], 1)
        for event in normalized:
            self.assertIn("event_id", event)
            self.assertIn("evidence", event)
            self.assertIn("principal", event)
            self.assertIn("resource", event)
            self.assertIn("action", event)
        self.assertEqual(len(self.workspace.list_dead_letters()), 2)

    def test_replay_is_idempotent(self) -> None:
        seed_workspace(self.workspace)
        first = ingest_raw_events(self.workspace)
        second = ingest_raw_events(self.workspace)

        self.assertGreater(first["added_normalized_events"], 0)
        self.assertEqual(second["added_normalized_events"], 0)
        self.assertEqual(second["dead_letter_count"], 0)
        self.assertEqual(second["duplicate_semantic_events"], 53)
        self.assertEqual(len(self.workspace.list_normalized_events()), first["normalized_events_seen"])

    def test_retention_and_rebuild_preserve_derived_state(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        case = create_case_from_detection(self.workspace, detections[0]["detection_id"])
        before = self.workspace.load_derived_state()

        retention = self.workspace.apply_raw_retention(7, "2026-01-10T23:59:59Z")
        after_raw = self.workspace.list_raw_events()
        rebuilt = rebuild_derived_state(self.workspace)
        after = self.workspace.load_derived_state()

        self.assertGreater(len(retention["deleted_raw_objects"]), 0)
        self.assertLess(len(after_raw), 53)
        self.assertIn("principal_profiles", after)
        self.assertEqual(before["metadata"]["normalized_event_count"], after["metadata"]["normalized_event_count"])
        self.assertEqual(rebuilt["normalized_event_count"], after["metadata"]["normalized_event_count"])
        reloaded_case = self.workspace.get_case(case["case_id"])
        self.assertTrue(reloaded_case["evidence_snapshots"])

    def test_runtime_produces_expected_detection_types(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        scenarios = {item["scenario"] for item in detections}

        self.assertEqual(
            scenarios,
            {
                "compromised_privileged_identity",
                "unusual_repo_export",
                "unusual_data_access",
                "unusual_external_sharing",
            },
        )
        self.assertEqual(len(detections), 5)
        self.assertNotIn("general_behavioral_anomaly", scenarios)

    def test_case_workflow_and_timeline(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        case = create_case_from_detection(self.workspace, detection["detection_id"])
        updated = update_case(
            self.workspace,
            case["case_id"],
            disposition="escalated",
            analyst_note="Needs review",
            feedback_label="high_value_signal",
        )
        timeline = get_entity_timeline(self.workspace, principal_id=detection["related_entity_ids"][0])
        evidence = build_evidence_bundle(self.workspace, detection["detection_id"])
        peer_comparison = build_peer_comparison(self.workspace, detection["detection_id"])
        summary = summarize_case(self.workspace, case["case_id"])

        self.assertEqual(updated["disposition"], "escalated")
        self.assertIn("high_value_signal", updated["feedback_labels"])
        self.assertTrue(timeline)
        self.assertTrue(evidence["evidence_items"])
        self.assertEqual(peer_comparison["principal_id"], detection["related_entity_ids"][0])
        self.assertEqual(summary["case_id"], case["case_id"])
        self.assertEqual(summary["primary_detection"]["detection_id"], detection["detection_id"])
        self.assertTrue(summary["case_title"])

    def test_acknowledge_detection_reduces_open_detection_count(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]

        before = get_runtime_stream_state(self.workspace)
        updated = acknowledge_detection(self.workspace, detection["detection_id"])
        after = get_runtime_stream_state(self.workspace)

        self.assertEqual(updated["status"], "acknowledged")
        self.assertEqual(after["detection_count"], before["detection_count"] - 1)

    def test_source_capability_matrix_includes_last_seen_signals(self) -> None:
        run_runtime(self.workspace)

        matrix = build_source_capability_matrix(self.workspace)
        okta = matrix["sources"]["okta"]
        source_stats = self.workspace.load_source_stats()["sources"]["okta"]

        self.assertIsNotNone(okta["recent_window_anchor_at"])
        self.assertIsNotNone(okta["raw_last_seen_at"])
        self.assertIsNotNone(okta["normalized_last_seen_at"])
        self.assertGreater(okta["raw_24h_count"], 0)
        self.assertGreater(okta["normalized_24h_count"], 0)
        self.assertGreaterEqual(okta["dead_letter_7d_count"], 0)
        self.assertIn("login", okta["seen_event_types"])
        self.assertGreater(source_stats["raw_event_count"], 0)
        self.assertGreater(source_stats["normalized_event_count"], 0)
        self.assertIn("event_type", source_stats["seen_raw_fields"])

    def test_related_detections_group_into_one_case(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        alice_detections = [item for item in detections if "alice@example.com" in item["related_entity_ids"]]

        first_case = create_case_from_detection(self.workspace, alice_detections[0]["detection_id"])
        grouped_case = create_case_from_detection(self.workspace, alice_detections[1]["detection_id"])
        summary = summarize_case(self.workspace, first_case["case_id"])

        self.assertEqual(first_case["case_id"], grouped_case["case_id"])
        self.assertEqual(len(grouped_case["detection_ids"]), 2)
        self.assertEqual(summary["detection_count"], 2)

    def test_run_runtime_is_repeatable_and_removes_stale_detections(self) -> None:
        first = run_runtime(self.workspace)
        second = run_runtime(self.workspace)

        self.assertEqual(
            first["detect"]["total_detection_count"],
            second["detect"]["total_detection_count"],
        )
        self.assertEqual(
            second["detect"]["new_detection_count"],
            second["detect"]["total_detection_count"],
        )
        self.assertNotIn("evaluation", first)
        self.assertNotIn("evaluation", second)

    def test_runtime_stream_spreads_scenarios_across_multiple_advances(self) -> None:
        initialize_runtime_stream(self.workspace)
        manifest = json.loads((self.workspace.manifests_dir / "runtime_stream_source_events.json").read_text())
        source_events = manifest["source_events"]
        nonbaseline_positions = [
            index for index, event in enumerate(source_events, start=1) if event.get("scenario") != "baseline"
        ]

        self.assertTrue(nonbaseline_positions)
        self.assertLess(nonbaseline_positions[0], 80)
        self.assertLess(nonbaseline_positions[-1], len(source_events))

        detection_advances: list[int] = []
        while True:
            advance = advance_runtime_stream(self.workspace, batch_size=25)
            worker_result = run_local_processing_workers(self.workspace)
            if worker_result["detect"]["new_detection_count"] > 0:
                detection_advances.append(advance["cursor"])
            if advance["complete"]:
                break

        self.assertGreaterEqual(len(detection_advances), 3)
        self.assertLess(detection_advances[0], 125)

    def test_ops_metadata_contains_no_raw_payloads(self) -> None:
        run_runtime(self.workspace)
        metadata = collect_ops_metadata(self.workspace)
        serialized = json.dumps(metadata)

        self.assertFalse(metadata["contains_raw_payloads"])
        self.assertNotIn("normalized_event_count", metadata)
        self.assertNotIn("detection_count", metadata)
        self.assertNotIn("actor_email", serialized)
        self.assertNotIn("resource_id", serialized)

    def test_founder_artifacts_include_conversation_pack_and_capability_matrix(self) -> None:
        run_runtime(self.workspace)
        exported = export_founder_artifacts(self.workspace)
        conversation_pack = build_conversation_pack_markdown(self.workspace)
        capability_matrix = build_source_capability_markdown(self.workspace)
        capability_data = build_source_capability_matrix(self.workspace)

        self.assertIn("conversation-pack.md", exported)
        self.assertIn("source-capability-matrix.md", exported)
        self.assertNotIn("evaluation-summary.md", exported)
        self.assertNotIn("scenario-outputs.md", exported)
        self.assertIn("What The Current PoC Proves", conversation_pack)
        self.assertIn("Total detections: `5`", conversation_pack)
        self.assertIn("Dead letters: `2`", conversation_pack)
        self.assertIn("Source contracts fully satisfied: `2/4`", conversation_pack)
        self.assertIn("## Capability Status", capability_matrix)
        self.assertEqual(capability_data["sources"]["okta"]["dead_letter_count"], 1)
        self.assertEqual(capability_data["sources"]["snowflake"]["missing_required_event_types"], [])
        self.assertTrue((self.workspace.founder_dir / "conversation-pack.md").exists())
        self.assertTrue((self.workspace.founder_dir / "source-capability-matrix.md").exists())

    def test_source_fixture_bundle_validation_and_import(self) -> None:
        fixtures_dir = ROOT / "examples" / "poc" / "fixtures" / "fixed-source-pack"
        validation = validate_fixture_bundle(fixtures_dir)
        report = build_onboarding_report_markdown(fixtures_dir)
        imported = import_fixture_bundle(self.workspace, fixtures_dir)
        worker_result = run_local_processing_workers(self.workspace)
        manifest = build_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["valid_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(imported["batch_count"], 4)
        self.assertEqual(worker_result["normalization"]["ingest"]["normalized_events_seen"], 8)
        self.assertEqual(manifest["source_pack"], ["okta", "gworkspace", "github", "snowflake"])
        self.assertIn("Validation status: `pass`", report)

    def test_source_fixture_bundle_validation_fails_for_missing_sources(self) -> None:
        fixtures_dir = self.tempdir / "fixtures"
        fixtures_dir.mkdir()
        (fixtures_dir / "okta.jsonl").write_text(
            json.dumps(
                {
                    "source": "okta",
                    "source_event_id": "okta-only-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        validation = validate_fixture_bundle(fixtures_dir)

        self.assertFalse(validation["summary"]["passes"])
        self.assertIn("gworkspace", validation["summary"]["missing_sources"])
        self.assertIn("github", validation["summary"]["missing_sources"])
        self.assertIn("snowflake", validation["summary"]["missing_sources"])

    def test_vendor_fixture_bundle_validation_and_import(self) -> None:
        fixtures_dir = ROOT / "examples" / "poc" / "vendor-fixtures" / "fixed-source-pack"
        validation = validate_vendor_fixture_bundle(fixtures_dir)
        report = build_vendor_mapping_report_markdown(fixtures_dir)
        imported = import_vendor_fixture_bundle(self.workspace, fixtures_dir)
        worker_result = run_local_processing_workers(self.workspace)
        manifest = build_vendor_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["mapped_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(imported["batch_count"], 4)
        self.assertEqual(worker_result["normalization"]["ingest"]["normalized_events_seen"], 8)
        self.assertEqual(manifest["mapping_target"], "raw-event-v1")
        self.assertIn("Validation status: `pass`", report)

    def test_vendor_fixture_bundle_validation_fails_for_unsupported_event(self) -> None:
        fixtures_dir = self.tempdir / "vendor-fixtures"
        fixtures_dir.mkdir()
        (fixtures_dir / "okta_system_log.jsonl").write_text(
            json.dumps(
                {
                    "uuid": "okta-bad-0001",
                    "published": "2026-02-12T10:00:00Z",
                    "ingested_at": "2026-02-12T10:01:00Z",
                    "eventType": "user.mfa.challenge",
                    "actor": {
                        "alternateId": "alice@example.com",
                        "displayName": "Alice Admin",
                        "department": "security",
                        "role": "security-admin",
                    },
                    "client": {
                        "ipAddress": "10.0.0.1",
                        "geographicalContext": {"country": "US", "state": "NY"},
                    },
                    "target": [{"id": "okta:admin-console", "displayName": "Admin Console", "type": "app"}],
                    "securityContext": {"isPrivileged": True},
                    "debugContext": {"sensitivity": "high"},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        validation = validate_vendor_fixture_bundle(fixtures_dir)

        self.assertFalse(validation["summary"]["passes"])
        self.assertEqual(validation["sources"]["okta"]["invalid_event_count"], 1)
        self.assertIn("gworkspace", validation["summary"]["missing_sources"])

    def test_cleanup_stale_legacy_postgres_runtime_files(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("999999\n", encoding="utf-8")
        (paths["socket"] / ".s.PGSQL.55432").write_text("", encoding="utf-8")
        (paths["socket"] / ".s.PGSQL.55432.lock").write_text("", encoding="utf-8")

        with patch("seccloud.local_postgres._process_exists", return_value=False):
            removed = _cleanup_stale_runtime_files(paths)

        self.assertEqual(len(removed), 3)
        self.assertFalse((paths["data"] / "postmaster.pid").exists())
        self.assertFalse((paths["socket"] / ".s.PGSQL.55432").exists())
        self.assertFalse((paths["socket"] / ".s.PGSQL.55432.lock").exists())

    def test_cleanup_stale_legacy_postgres_refuses_live_pid(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("12345\n", encoding="utf-8")

        with (
            patch("seccloud.local_postgres._process_exists", return_value=True),
            patch(
                "seccloud.local_postgres._process_command",
                return_value=f"postgres -D {paths['data']}",
            ),
        ):
            with self.assertRaises(RuntimeError):
                _cleanup_stale_runtime_files(paths)

    def test_cleanup_stale_legacy_postgres_allows_reused_pid(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("12345\n", encoding="utf-8")

        with (
            patch("seccloud.local_postgres._process_exists", return_value=True),
            patch(
                "seccloud.local_postgres._process_command",
                return_value="python some_other_process.py",
            ),
        ):
            removed = _cleanup_stale_runtime_files(paths)

        self.assertEqual(len(removed), 1)
        self.assertFalse((paths["data"] / "postmaster.pid").exists())

    def test_wait_for_socket_ready_succeeds_when_socket_exists(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["socket"].mkdir(parents=True)
        (paths["socket"] / ".s.PGSQL.55432").write_text("", encoding="utf-8")

        _wait_for_socket_ready(paths, timeout_seconds=0.01, poll_interval_seconds=0.001)

    def test_wait_for_socket_ready_times_out_when_socket_missing(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["socket"].mkdir(parents=True)

        with self.assertRaises(RuntimeError):
            _wait_for_socket_ready(paths, timeout_seconds=0.01, poll_interval_seconds=0.001)

    def test_start_local_postgres_recovers_from_stale_running_status(self) -> None:
        completed = subprocess.CompletedProcess(args=["pg_ctl"], returncode=0, stdout="", stderr="")

        with (
            patch("seccloud.local_postgres.init_local_postgres", return_value={"status": "already_initialized"}),
            patch("seccloud.local_postgres._run_pg_ctl", return_value=completed),
            patch(
                "seccloud.local_postgres._wait_for_socket_ready",
                side_effect=[RuntimeError("Local Postgres did not create its socket within 1.0s"), None],
            ),
            patch("seccloud.local_postgres._stop_pg_ctl"),
            patch("seccloud.local_postgres._cleanup_stale_runtime_files", return_value=["stale.pid"]),
            patch("seccloud.local_postgres._ensure_database", return_value="already_exists"),
            patch("seccloud.local_postgres._postgres_binary", return_value="/opt/homebrew/bin/pg_ctl"),
            patch("seccloud.local_postgres.subprocess.run"),
        ):
            from seccloud.local_postgres import start_local_postgres

            result = start_local_postgres(self.tempdir)

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["database_status"], "already_exists")
        self.assertEqual(result["stale_files_removed"], ["stale.pid"])

    def test_runtime_stream_advances_incrementally(self) -> None:
        initialized = initialize_runtime_stream(self.workspace)
        after_first = advance_runtime_stream(self.workspace, batch_size=7)
        run_local_processing_workers(self.workspace)
        state_after_first = get_runtime_stream_state(self.workspace)
        after_second = advance_runtime_stream(self.workspace, batch_size=7)

        self.assertEqual(initialized["cursor"], 0)
        self.assertEqual(after_first["cursor"], 7)
        self.assertEqual(after_first["accepted_records"], 7)
        self.assertEqual(state_after_first["normalized_event_count"], 7)
        self.assertEqual(state_after_first["detection_count"], len(self.workspace.list_detections()))
        self.assertGreaterEqual(after_second["cursor"], 14)

    def test_cli_only_exposes_service_level_worker_commands(self) -> None:
        parser = build_parser()
        subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))

        for command in (
            "run-pipeline",
            "run-normalization-worker",
            "run-detection-worker",
            "run-projector-worker",
            "run-workers",
            "run-all-workers",
            "sync-projection",
        ):
            self.assertNotIn(command, subparsers.choices)

        for command in (
            "run-worker-service",
            "run-worker-service-once",
            "run-source-stats-projector",
            "show-worker-state",
            "show-runtime-status",
            "bootstrap-local-runtime",
            "run-api",
        ):
            self.assertIn(command, subparsers.choices)

    def test_workers_process_shared_intake_batches(self) -> None:
        submitted = submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-worker-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                },
                {
                    "source": "github",
                    "source_event_id": "github-worker-0001",
                    "observed_at": "2026-02-11T10:05:00Z",
                    "received_at": "2026-02-11T10:06:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "archive_download",
                    "resource_id": "github:repo/seccloud",
                    "resource_name": "seccloud",
                    "resource_kind": "repo",
                    "sensitivity": "internal",
                    "bytes_transferred_mb": 44,
                },
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )

        self.assertEqual(submitted["batch_count"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 2)

        normalization = run_normalization_worker(self.workspace)
        detection = run_local_processing_workers(self.workspace)
        worker_state = get_worker_state(self.workspace)

        self.assertEqual(normalization["processed_batch_count"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 0)
        self.assertEqual(len(self.workspace.list_processed_intake_batches()), 2)
        self.assertEqual(normalization["ingest"]["added_normalized_events"], 2)
        self.assertEqual(detection["normalization"]["processed_batch_count"], 0)
        self.assertEqual(worker_state["pending_batch_count"], 0)
        self.assertEqual(worker_state["processed_batch_count"], 2)
        self.assertGreaterEqual(worker_state["normalization_runs"], 2)
        self.assertGreaterEqual(worker_state["detection_runs"], 1)

    def test_event_identity_and_object_keys_are_partition_aware(self) -> None:
        tenant_workspace = Workspace(self.tempdir / "tenant-workspace", tenant_id="acme-prod")
        tenant_workspace.bootstrap()

        submit_grouped_raw_events(
            tenant_workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-tenant-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            ],
            intake_kind="push_gateway",
            integration_id="okta-prod",
        )
        run_local_processing_workers(tenant_workspace)

        event = tenant_workspace.list_normalized_events()[0]
        self.assertTrue(event["event_id"].startswith("evt_"))
        self.assertTrue(event["event_key"].startswith("evk_"))
        self.assertEqual(event["integration_id"], "okta-prod")
        self.assertTrue(event["principal"]["entity_id"].startswith("ent_"))
        self.assertTrue(event["resource"]["entity_id"].startswith("ent_"))
        self.assertIn("tenant=acme-prod", event["evidence"]["object_key"])
        self.assertIn("integration=okta-prod", event["evidence"]["object_key"])

    def test_api_startup_does_not_sync_projection_and_read_routes_do_not_resync(self) -> None:
        overview_payload = {
            "stream_state": {
                "cursor": 0,
                "total_source_events": 0,
                "complete": False,
                "normalized_event_count": 0,
                "detection_count": 0,
            },
            "ops_metadata": {
                "workspace": str(self.workspace.root),
                "event_counts_by_source": {},
                "dead_letter_count": 0,
                "dead_letter_counts_by_source": {},
                "contains_raw_payloads": False,
            },
        }
        events_payload = {
            "items": [],
            "page": {
                "limit": 14,
                "offset": 0,
                "returned": 0,
                "total": 0,
                "has_more": False,
            },
        }

        with (
            patch.dict(
                os.environ,
                {
                    "SECCLOUD_WORKSPACE": str(self.workspace.root),
                    "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                },
                clear=False,
            ),
            patch("seccloud.api.fetch_projection_overview", return_value=overview_payload),
            patch("seccloud.api.fetch_projected_events", return_value=events_payload),
        ):
            app = seccloud_api.create_app()
            overview_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/overview"
            )
            events_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/events"
            )

            overview_response = overview_endpoint()
            events_response = events_endpoint(limit=14, offset=0)

        self.assertEqual(overview_response, overview_payload)
        self.assertEqual(events_response, events_payload)

    def test_api_intake_endpoint_enqueues_without_running_workers(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SECCLOUD_WORKSPACE": str(self.workspace.root),
                "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                "SECCLOUD_PUSH_AUTH_TOKENS": json.dumps(
                    {
                        "push-token": {
                            "tenant_id": self.workspace.tenant_id,
                            "source": "okta",
                            "integration_id": "okta-primary",
                        }
                    }
                ),
            },
            clear=False,
        ):
            app = seccloud_api.create_app()
            intake_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/intake/raw-events"
            )
            worker_state_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/workers/state"
            )
            body = gzip.compress(
                json.dumps(
                    {
                        "source": "okta",
                        "records": [
                            {
                                "source_event_id": "okta-api-0001",
                                "observed_at": "2026-02-11T10:00:00Z",
                                "received_at": "2026-02-11T10:01:00Z",
                                "actor_email": "alice@example.com",
                                "actor_name": "Alice Admin",
                                "department": "security",
                                "role": "security-admin",
                                "event_type": "login",
                                "resource_id": "okta:admin-console",
                                "resource_name": "Admin Console",
                                "resource_kind": "app",
                                "sensitivity": "high",
                                "geo": "US-NY",
                                "ip": "10.0.0.1",
                                "privileged": True,
                            }
                        ],
                    }
                ).encode("utf-8")
            )

            async def receive() -> dict[str, object]:
                return {"type": "http.request", "body": body, "more_body": False}

            intake_response = asyncio.run(
                intake_endpoint(
                    request=Request(
                        {
                            "type": "http",
                            "method": "POST",
                            "path": "/api/intake/raw-events",
                            "headers": [
                                (b"content-encoding", b"gzip"),
                                (b"content-type", b"application/json"),
                            ],
                            "query_string": b"",
                            "client": ("testclient", 123),
                            "server": ("testserver", 80),
                            "scheme": "http",
                            "http_version": "1.1",
                        },
                        receive,
                    ),
                    authorization="Bearer push-token",
                    idempotency_key=None,
                )
            )
            worker_state = worker_state_endpoint()

        self.assertEqual(intake_response["source"], "okta")
        self.assertEqual(intake_response["integration_id"], "okta-primary")
        self.assertEqual(intake_response["record_count"], 1)
        self.assertEqual(worker_state["pending_batch_count"], 1)
        self.assertEqual(worker_state["processed_batch_count"], 0)
        self.assertEqual(len(self.workspace.list_normalized_events()), 0)

    def test_api_acknowledge_detection_updates_status(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]

        with (
            patch.dict(
                os.environ,
                {
                    "SECCLOUD_WORKSPACE": str(self.workspace.root),
                    "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                },
                clear=False,
            ),
            patch("seccloud.api.sync_workspace_projection", return_value={"detection_count": 0}),
        ):
            app = seccloud_api.create_app()
            acknowledge_endpoint = next(
                route.endpoint
                for route in app.routes
                if getattr(route, "path", None) == "/api/detections/{detection_id}/acknowledge"
            )
            response = acknowledge_endpoint(detection["detection_id"])

        self.assertEqual(response["status"], "acknowledged")
        self.assertEqual(
            self.workspace.get_detection(detection["detection_id"])["status"],
            "acknowledged",
        )

    def test_api_stream_advance_enqueues_without_processing(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SECCLOUD_WORKSPACE": str(self.workspace.root),
                "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
            },
            clear=False,
        ):
            app = seccloud_api.create_app()
            reset_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/stream/reset"
            )
            advance_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/stream/advance"
            )
            worker_state_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/workers/state"
            )

            reset_endpoint()
            advance_response = advance_endpoint(batch_size=10)
            worker_state = worker_state_endpoint()

        self.assertEqual(advance_response["accepted_records"], 10)
        self.assertGreaterEqual(advance_response["accepted_batches"], 1)
        self.assertEqual(worker_state["pending_batch_count"], advance_response["accepted_batches"])
        self.assertEqual(advance_response["pending_batch_count"], advance_response["accepted_batches"])
        self.assertEqual(len(self.workspace.list_normalized_events()), 0)

    def test_projector_worker_updates_worker_state(self) -> None:
        run_runtime(self.workspace)

        with patch(
            "seccloud.projection_store.sync_workspace_projection",
            return_value={"dsn": "postgresql://projection", "event_count": 5, "detection_count": 2},
        ):
            projection = run_projector_worker(self.workspace, "postgresql://projection")

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(projection["dsn"], "postgresql://projection")
        self.assertEqual(worker_state["projection_runs"], 1)
        self.assertIsNotNone(worker_state["last_projection_at"])

    def test_source_stats_projector_updates_worker_state(self) -> None:
        run_runtime(self.workspace)

        stats = run_source_stats_projector(self.workspace)

        worker_state = get_worker_state(self.workspace)
        self.assertGreater(stats["source_count"], 0)
        self.assertEqual(worker_state["source_stats_runs"], 1)
        self.assertIsNotNone(worker_state["last_source_stats_at"])

    def test_run_all_local_workers_includes_projection(self) -> None:
        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-all-workers-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )

        with patch(
            "seccloud.projection_store.sync_workspace_projection",
            return_value={"dsn": "postgresql://projection", "event_count": 1, "detection_count": 0},
        ):
            result = run_all_local_workers(self.workspace, "postgresql://projection")

        self.assertIn("source_stats", result)
        self.assertIn("projection", result)
        self.assertGreaterEqual(result["source_stats"]["source_count"], 1)
        self.assertEqual(result["projection"]["dsn"], "postgresql://projection")

    def test_worker_service_once_processes_pending_batches(self) -> None:
        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-service-once-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )

        with patch(
            "seccloud.projection_store.sync_workspace_projection",
            return_value={"dsn": "postgresql://projection", "event_count": 1, "detection_count": 0},
        ):
            result = run_worker_service_once(self.workspace, "postgresql://projection")

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(result["status"], "processed")
        self.assertEqual(worker_state["pending_batch_count"], 0)
        self.assertEqual(worker_state["service_runs"], 1)
        self.assertEqual(worker_state["last_service_status"], "processed")
        self.assertEqual(worker_state["source_stats_runs"], 1)

    def test_worker_service_loop_reports_idle_iterations(self) -> None:
        result = run_worker_service_loop(
            self.workspace,
            dsn="postgresql://projection",
            poll_interval_seconds=0,
            max_iterations=2,
        )

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(result["iterations"], 2)
        self.assertEqual(result["idle_iterations"], 2)
        self.assertEqual(worker_state["service_runs"], 2)
        self.assertEqual(worker_state["last_service_status"], "idle")

    def test_worker_service_loop_can_exit_when_idle(self) -> None:
        result = run_worker_service_loop(
            self.workspace,
            dsn="postgresql://projection",
            poll_interval_seconds=0,
            exit_when_idle=True,
        )

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(result["iterations"], 1)
        self.assertEqual(result["idle_iterations"], 1)
        self.assertEqual(worker_state["service_runs"], 1)
        self.assertEqual(worker_state["last_service_status"], "idle")

    def test_operator_runtime_status_aggregates_runtime_state(self) -> None:
        initialize_runtime_stream(self.workspace)
        with patch(
            "seccloud.projection_store.fetch_projection_overview",
            return_value={"stream_state": {"cursor": 0}, "ops_metadata": {"dead_letter_count": 0}},
        ):
            status = build_operator_runtime_status(
                self.workspace,
                dsn="postgresql://projection",
                runtime_root=self.tempdir,
            )

        self.assertEqual(status["workspace"], str(self.workspace.root))
        self.assertEqual(status["tenant_id"], self.workspace.tenant_id)
        self.assertIn("worker_state", status)
        self.assertIn("stream_state", status)
        self.assertTrue(status["projection"]["available"])
        self.assertEqual(status["postgres"]["root"], str(self.tempdir.resolve()))

    def test_bootstrap_local_runtime_starts_postgres_and_initializes_stream(self) -> None:
        with patch(
            "seccloud.cli.start_local_postgres", return_value={"status": "started", "dsn": "postgresql://runtime"}
        ):
            result = bootstrap_local_runtime(self.workspace, runtime_root=self.tempdir)

        self.assertEqual(result["workspace"], str(self.workspace.root))
        self.assertEqual(result["postgres"]["status"], "started")
        self.assertEqual(result["stream"]["status"], "initialized")
        self.assertEqual(result["api_env"]["SECCLOUD_PROJECTION_DSN"], "postgresql://runtime")

    def test_worker_service_once_projects_after_stream_reset_without_pending_batches(self) -> None:
        initialize_runtime_stream(self.workspace)

        with patch(
            "seccloud.projection_store.sync_workspace_projection",
            return_value={"dsn": "postgresql://projection", "event_count": 0, "detection_count": 0},
        ):
            result = run_worker_service_once(self.workspace, "postgresql://projection")

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(result["status"], "projected")
        self.assertEqual(worker_state["last_service_status"], "projected")
        self.assertEqual(worker_state["source_stats_runs"], 1)
        self.assertEqual(worker_state["projection_runs"], 1)

    def test_worker_service_once_materializes_source_stats_without_pending_batches(self) -> None:
        seed_workspace(self.workspace)
        ingest_raw_events(self.workspace)
        self.workspace.save_source_stats({"sources": {}})
        self.workspace.request_source_stats_refresh()

        result = run_worker_service_once(self.workspace, "postgresql://projection")

        worker_state = get_worker_state(self.workspace)
        self.assertEqual(result["status"], "materialized")
        self.assertIn("source_stats", result["result"])
        self.assertEqual(worker_state["last_service_status"], "materialized")
        self.assertEqual(worker_state["source_stats_runs"], 1)
        self.assertEqual(worker_state["projection_runs"], 0)


if __name__ == "__main__":
    unittest.main()
