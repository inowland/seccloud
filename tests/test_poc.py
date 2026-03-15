from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import seccloud.api as seccloud_api
from seccloud.investigation import (
    build_evidence_bundle,
    build_peer_comparison,
    create_case_from_detection,
    get_entity_timeline,
    list_detections,
    summarize_case,
    update_case,
)
from seccloud.local_postgres import _cleanup_stale_runtime_files, postgres_paths
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
            if advance["detect"]["new_detection_count"] > 0:
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
        ingest = ingest_raw_events(self.workspace)
        manifest = build_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["valid_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(ingest["normalized_events_seen"], 8)
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
        ingest = ingest_raw_events(self.workspace)
        manifest = build_vendor_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["mapped_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(ingest["normalized_events_seen"], 8)
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

    def test_runtime_stream_advances_incrementally(self) -> None:
        initialized = initialize_runtime_stream(self.workspace)
        after_first = advance_runtime_stream(self.workspace, batch_size=7)
        state_after_first = get_runtime_stream_state(self.workspace)
        after_second = advance_runtime_stream(self.workspace, batch_size=7)

        self.assertEqual(initialized["cursor"], 0)
        self.assertEqual(after_first["cursor"], 7)
        self.assertEqual(state_after_first["normalized_event_count"], 7)
        self.assertEqual(state_after_first["detection_count"], len(self.workspace.list_detections()))
        self.assertGreaterEqual(after_second["cursor"], 14)

    def test_api_startup_syncs_projection_and_read_routes_do_not_resync(self) -> None:
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
            patch(
                "seccloud.api.sync_workspace_projection",
                return_value={"dsn": "postgresql://projection"},
            ) as sync_mock,
            patch("seccloud.api.fetch_projection_overview", return_value=overview_payload),
            patch("seccloud.api.fetch_projected_events", return_value=events_payload),
        ):
            app = seccloud_api.create_app()
            lifespan = app.router.lifespan_context(app)
            asyncio.run(lifespan.__aenter__())
            overview_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/overview"
            )
            events_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/events"
            )

            overview_response = overview_endpoint()
            events_response = events_endpoint(limit=14, offset=0)
            asyncio.run(lifespan.__aexit__(None, None, None))

        self.assertEqual(overview_response, overview_payload)
        self.assertEqual(events_response, events_payload)
        self.assertEqual(sync_mock.call_count, 1)
        self.assertEqual(self.workspace.load_ops_metadata()["dead_letter_count"], 0)


if __name__ == "__main__":
    unittest.main()
