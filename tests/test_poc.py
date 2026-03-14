from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

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
    import_fixture_bundle,
    validate_fixture_bundle,
)
from seccloud.pipeline import (
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
from seccloud.source_pack import build_source_capability_markdown, build_source_capability_matrix
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
        run_demo(self.workspace)
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

    def test_seeded_scenarios_detected_and_benign_control_suppressed(self) -> None:
        run_demo(self.workspace)
        evaluation = evaluate_scenarios(self.workspace)

        for scenario in (
            "compromised_privileged_identity",
            "unusual_repo_export",
            "unusual_data_access",
            "unusual_external_sharing",
        ):
            self.assertEqual(evaluation["results"][scenario]["status"], "pass")
            self.assertGreaterEqual(evaluation["results"][scenario]["matching_detection_count"], 1)
        self.assertEqual(evaluation["results"]["benign_role_change_control"]["status"], "pass")
        self.assertEqual(evaluation["results"]["benign_role_change_control"]["matching_detection_count"], 0)
        self.assertEqual(evaluation["results"]["benign_travel_with_access"]["status"], "pass")
        self.assertEqual(evaluation["results"]["benign_travel_with_access"]["matching_detection_count"], 0)
        self.assertEqual(evaluation["results"]["benign_incident_response_access"]["status"], "pass")
        self.assertEqual(evaluation["results"]["benign_incident_response_access"]["matching_detection_count"], 0)
        self.assertEqual(evaluation["results"]["benign_finance_close_access"]["status"], "pass")
        self.assertEqual(evaluation["results"]["benign_finance_close_access"]["matching_detection_count"], 0)
        self.assertEqual(evaluation["detection_count_by_event_scenario"].get("baseline", 0), 0)
        self.assertEqual(evaluation["unexpected_detections"], [])
        self.assertEqual(evaluation["missed_expected_events"], [])
        self.assertTrue(evaluation["summary"]["passes"])
        self.assertIn("okta", evaluation["source_metrics"])
        self.assertEqual(evaluation["ingest_diagnostics"]["dead_letter_count"], 2)
        self.assertEqual(evaluation["source_metrics"]["okta"]["dead_letter_count"], 1)
        self.assertEqual(evaluation["source_metrics"]["gworkspace"]["dead_letter_count"], 1)

    def test_case_workflow_and_timeline(self) -> None:
        run_demo(self.workspace)
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
        self.assertEqual(peer_comparison["detection_id"], detection["detection_id"])
        self.assertEqual(summary["case_id"], case["case_id"])
        self.assertEqual(summary["primary_detection"]["detection_id"], detection["detection_id"])
        self.assertTrue(summary["case_title"])

    def test_related_detections_group_into_one_case(self) -> None:
        run_demo(self.workspace)
        detections = list_detections(self.workspace)
        alice_detections = [item for item in detections if "alice@example.com" in item["related_entity_ids"]]

        first_case = create_case_from_detection(self.workspace, alice_detections[0]["detection_id"])
        grouped_case = create_case_from_detection(self.workspace, alice_detections[1]["detection_id"])
        summary = summarize_case(self.workspace, first_case["case_id"])

        self.assertEqual(first_case["case_id"], grouped_case["case_id"])
        self.assertEqual(len(grouped_case["detection_ids"]), 2)
        self.assertEqual(summary["detection_count"], 2)

    def test_run_demo_is_repeatable_and_removes_stale_detections(self) -> None:
        first = run_demo(self.workspace)
        second = run_demo(self.workspace)
        evaluation = evaluate_scenarios(self.workspace)

        self.assertEqual(first["detect"]["total_detection_count"], second["detect"]["total_detection_count"])
        self.assertEqual(second["detect"]["new_detection_count"], second["detect"]["total_detection_count"])
        self.assertEqual(evaluation["detection_count_by_event_scenario"].get("baseline", 0), 0)
        self.assertEqual(evaluation["unexpected_detections"], [])

    def test_ops_metadata_contains_no_raw_payloads(self) -> None:
        run_demo(self.workspace)
        metadata = collect_ops_metadata(self.workspace)
        serialized = json.dumps(metadata)

        self.assertFalse(metadata["contains_raw_payloads"])
        self.assertNotIn("actor_email", serialized)
        self.assertNotIn("resource_id", serialized)

    def test_founder_artifacts_include_evaluation_and_conversation_pack(self) -> None:
        run_demo(self.workspace)
        exported = export_founder_artifacts(self.workspace)
        evaluation_summary = build_evaluation_summary_markdown(self.workspace)
        conversation_pack = build_conversation_pack_markdown(self.workspace)
        capability_matrix = build_source_capability_markdown(self.workspace)
        capability_data = build_source_capability_matrix(self.workspace)

        self.assertIn("evaluation-summary.md", exported)
        self.assertIn("conversation-pack.md", exported)
        self.assertIn("source-capability-matrix.md", exported)
        self.assertIn("Overall evaluation status: `pass`", evaluation_summary)
        self.assertIn("## Ingest Diagnostics", evaluation_summary)
        self.assertIn("What The Current PoC Proves", conversation_pack)
        self.assertIn("Dead letters: `2`", conversation_pack)
        self.assertIn("## Capability Status", capability_matrix)
        self.assertEqual(capability_data["sources"]["okta"]["dead_letter_count"], 1)
        self.assertEqual(capability_data["sources"]["snowflake"]["missing_required_event_types"], [])
        self.assertTrue((self.workspace.founder_dir / "evaluation-summary.md").exists())
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

    def test_demo_stream_advances_incrementally(self) -> None:
        initialized = initialize_demo_stream(self.workspace)
        after_first = advance_demo_stream(self.workspace, batch_size=7)
        state_after_first = get_demo_stream_state(self.workspace)
        after_second = advance_demo_stream(self.workspace, batch_size=7)

        self.assertEqual(initialized["cursor"], 0)
        self.assertEqual(after_first["cursor"], 7)
        self.assertEqual(state_after_first["normalized_event_count"], 7)
        self.assertEqual(state_after_first["detection_count"], len(self.workspace.list_detections()))
        self.assertGreaterEqual(after_second["cursor"], 14)


if __name__ == "__main__":
    unittest.main()
