from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.investigation import (
    acknowledge_detection,
    get_detection_detail,
    get_entity_timeline,
    get_event_detail,
)
from seccloud.local_postgres import local_postgres_dsn, start_local_postgres, stop_local_postgres
from seccloud.pipeline import run_runtime
from seccloud.projection_store import (
    fetch_detection_linked_events,
    fetch_hot_event_detail,
    fetch_projected_detections,
    fetch_projected_events,
    sync_workspace_projection,
)
from seccloud.runtime_status import build_runtime_status
from seccloud.storage import Workspace
from seccloud.workers import (
    run_projector_worker,
    run_worker_service_loop,
    run_worker_service_once,
    submit_grouped_raw_events,
)


class HotEventIndexTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = Path(tempfile.mkdtemp(prefix="seccloud-hot-index-"))
        self.workspace = Workspace(self.tempdir / "workspace")
        self.workspace.bootstrap()
        try:
            start_local_postgres(self.tempdir)
        except FileNotFoundError as exc:  # pragma: no cover - environment dependent
            self.skipTest(str(exc))
        except subprocess.CalledProcessError as exc:  # pragma: no cover - sandbox dependent
            self.skipTest(f"Local Postgres unavailable in this environment: {exc}")
        self.dsn = local_postgres_dsn(self.tempdir)

    def tearDown(self) -> None:
        try:
            stop_local_postgres(self.tempdir)
        except Exception:  # pragma: no cover - best effort cleanup
            pass
        shutil.rmtree(self.tempdir)

    def _seed_projection(self) -> None:
        run_runtime(self.workspace)
        sync_workspace_projection(self.workspace, self.dsn)

    def test_hot_event_index_materializes_rows_and_edges(self) -> None:
        self._seed_projection()

        projected = fetch_projected_events(
            tenant_id=self.workspace.tenant_id,
            limit=100,
            offset=0,
            dsn=self.dsn,
        )
        normalized_events = self.workspace.list_normalized_events()
        detection = self.workspace.list_detections()[0]
        linked_events = fetch_detection_linked_events(
            tenant_id=self.workspace.tenant_id,
            detection_id=detection["detection_id"],
            dsn=self.dsn,
        )
        detail = fetch_hot_event_detail(
            tenant_id=self.workspace.tenant_id,
            event_id=normalized_events[0]["event_id"],
            dsn=self.dsn,
        )

        self.assertEqual(len(projected["items"]), len(normalized_events))
        self.assertEqual([item["event_id"] for item in linked_events], detection["event_ids"])
        self.assertIsNotNone(detail)
        self.assertEqual(detail["event_payload"]["event_id"], normalized_events[0]["event_id"])
        self.assertEqual(detail["normalized_pointer"]["retention_class"], "normalized_retained")
        self.assertEqual(detail["raw_pointer"]["retention_class"], "raw_hot")
        self.assertEqual(detail["raw_retention_state"], "available")

        normalized_object = self.workspace.object_store.get_bytes(detail["normalized_pointer"]["object_key"])
        self.assertIsNotNone(normalized_object)
        assert normalized_object is not None
        self.assertEqual(normalized_object[:4], b"PAR1")
        self.assertEqual(normalized_object[-4:], b"PAR1")

    def test_event_detail_and_detection_hydration_read_from_hot_index(self) -> None:
        self._seed_projection()
        normalized_events = self.workspace.list_normalized_events()
        detection = self.workspace.list_detections()[0]

        with patch.object(self.workspace, "list_normalized_events", side_effect=AssertionError("workspace scan")):
            event = get_event_detail(self.workspace, normalized_events[0]["event_id"], dsn=self.dsn)
            detection_detail = get_detection_detail(self.workspace, detection["detection_id"], dsn=self.dsn)

        self.assertIsNotNone(event)
        self.assertEqual(event["event_id"], normalized_events[0]["event_id"])
        self.assertEqual([item["event_id"] for item in detection_detail["events"]], detection["event_ids"])

    def test_rebuild_after_raw_retention_preserves_linkage(self) -> None:
        self._seed_projection()
        normalized_events = self.workspace.list_normalized_events()
        detection = self.workspace.list_detections()[0]

        retention = self.workspace.apply_raw_retention(0, "2026-04-30T23:59:59Z")
        sync_workspace_projection(self.workspace, self.dsn)
        detail = fetch_hot_event_detail(
            tenant_id=self.workspace.tenant_id,
            event_id=normalized_events[0]["event_id"],
            dsn=self.dsn,
        )
        linked_events = fetch_detection_linked_events(
            tenant_id=self.workspace.tenant_id,
            detection_id=detection["detection_id"],
            dsn=self.dsn,
        )

        self.assertTrue(any(item.startswith("lake/raw/") for item in retention["deleted_raw_objects"]))
        self.assertIsNotNone(detail)
        self.assertEqual(detail["raw_retention_state"], "expired")
        self.assertIsNotNone(detail["raw_pointer"])
        self.assertEqual([item["event_id"] for item in linked_events], detection["event_ids"])

    def test_projection_sync_does_not_call_full_schema_reset(self) -> None:
        run_runtime(self.workspace)

        with (
            patch("seccloud.projection_store.reset_projection_schema", side_effect=AssertionError("full reset")),
            patch.object(self.workspace, "list_normalized_events", side_effect=AssertionError("normalized scan")),
        ):
            first = sync_workspace_projection(self.workspace, self.dsn)
            second = sync_workspace_projection(self.workspace, self.dsn)

        self.assertEqual(first["event_count"], second["event_count"])
        self.assertEqual(first["detection_count"], second["detection_count"])

    def test_run_projector_worker_can_sync_with_rust_runtime(self) -> None:
        if shutil.which("cargo") is None:
            self.skipTest("cargo unavailable")

        run_runtime(self.workspace)

        with patch(
            "seccloud.projection_store.sync_workspace_projection",
            side_effect=AssertionError("python projector should not run"),
        ):
            result = run_projector_worker(self.workspace, self.dsn)

        projected = fetch_projected_events(
            tenant_id=self.workspace.tenant_id,
            limit=100,
            offset=0,
            dsn=self.dsn,
        )
        self.assertGreater(result["event_count"], 0)
        self.assertEqual(result["event_count"], projected["page"]["returned"])

    def test_run_worker_service_once_can_process_with_rust_runtime(self) -> None:
        if shutil.which("cargo") is None:
            self.skipTest("cargo unavailable")

        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-rust-service-0001",
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
            ],
            intake_kind="push_gateway",
            integration_id="okta-primary",
        )

        with (
            patch(
                "seccloud.projection_store.sync_workspace_projection",
                side_effect=AssertionError("python projector"),
            ),
            patch("seccloud.workers.run_all_local_workers", side_effect=AssertionError("python service")),
        ):
            result = run_worker_service_once(self.workspace, self.dsn)

        projected = fetch_projected_events(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )
        self.assertEqual(result["status"], "processed")
        self.assertEqual(projected["page"]["returned"], 1)

    def test_m2_acceptance_rust_worker_materializes_runtime_and_projection(self) -> None:
        if shutil.which("cargo") is None:
            self.skipTest("cargo unavailable")

        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-m2-acceptance-0001",
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
                    "source": "okta",
                    "source_event_id": "okta-m2-acceptance-0002",
                    "observed_at": "2026-02-11T10:05:00Z",
                    "received_at": "2026-02-11T10:06:00Z",
                    "actor_email": "bob@example.com",
                    "actor_name": "Bob Builder",
                    "department": "security",
                    "role": "manager",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-CA",
                    "ip": "10.0.0.2",
                    "privileged": True,
                },
            ],
            intake_kind="push_gateway",
            integration_id="okta-primary",
        )

        result = run_worker_service_once(self.workspace, self.dsn)
        status = build_runtime_status(self.workspace, dsn=self.dsn, runtime_root=self.tempdir)
        projected = fetch_projected_events(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )
        event = get_event_detail(self.workspace, projected["items"][0]["event_id"], dsn=self.dsn)

        self.assertEqual(result["status"], "processed")
        self.assertEqual(projected["page"]["returned"], 2)
        self.assertTrue(status["projection"]["available"])
        self.assertTrue(status["scoring_input"]["ready"])
        self.assertEqual(status["scoring_input"]["mode"], "feature_lake")
        self.assertEqual(status["event_index"]["event_count"], 2)
        self.assertEqual(status["detection_context"]["event_count"], 2)
        self.assertGreaterEqual(status["feature_tables"]["action_row_count"], 1)
        self.assertGreaterEqual(status["feature_tables"]["static_row_count"], 2)
        self.assertIsNotNone(event)
        self.assertEqual(event["event_id"], projected["items"][0]["event_id"])

    def test_run_worker_service_loop_can_exit_idle_with_rust_runtime(self) -> None:
        if shutil.which("cargo") is None:
            self.skipTest("cargo unavailable")

        result = run_worker_service_loop(
            self.workspace,
            dsn=self.dsn,
            poll_interval_seconds=0,
            max_iterations=2,
            exit_when_idle=True,
        )

        self.assertEqual(result["iterations"], 1)
        self.assertEqual(result["idle_iterations"], 1)

    def test_timeline_accepts_legacy_and_entity_ids_in_both_modes(self) -> None:
        self._seed_projection()
        anchor_event = self.workspace.list_normalized_events()[0]
        legacy_principal_id = anchor_event["principal"]["id"]
        principal_entity_id = anchor_event["principal"]["entity_id"]

        fallback_legacy = get_entity_timeline(self.workspace, principal_id=legacy_principal_id)
        fallback_entity = get_entity_timeline(self.workspace, principal_id=principal_entity_id)
        projected_legacy = get_entity_timeline(self.workspace, principal_id=legacy_principal_id, dsn=self.dsn)
        projected_entity = get_entity_timeline(self.workspace, principal_id=principal_entity_id, dsn=self.dsn)

        self.assertTrue(fallback_legacy)
        self.assertEqual(
            [event["event_id"] for event in projected_legacy],
            [event["event_id"] for event in fallback_legacy],
        )
        self.assertEqual(
            [event["event_id"] for event in projected_entity],
            [event["event_id"] for event in fallback_entity],
        )

    def test_empty_sync_clears_projected_detections_for_tenant(self) -> None:
        self._seed_projection()

        before = fetch_projected_detections(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )
        self.assertTrue(before["items"])

        self.workspace.reset_runtime()
        sync_workspace_projection(self.workspace, self.dsn)

        after = fetch_projected_detections(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )
        self.assertEqual(after["items"], [])

    def test_acknowledged_detection_is_removed_from_open_projection_queue(self) -> None:
        self._seed_projection()
        detection = self.workspace.list_detections()[0]

        before = fetch_projected_detections(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )
        acknowledge_detection(self.workspace, detection["detection_id"])
        sync_workspace_projection(self.workspace, self.dsn)
        after = fetch_projected_detections(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )

        self.assertEqual(after["page"]["total"], before["page"]["total"] - 1)
        self.assertNotIn(
            detection["detection_id"],
            [item["detection_id"] for item in after["items"]],
        )

    def test_projection_sync_uses_detection_lake_when_json_sidecars_are_missing(self) -> None:
        self._seed_projection()
        original = self.workspace.list_detections()
        shutil.rmtree(self.workspace.detections_dir)
        self.workspace.detections_dir.mkdir(parents=True, exist_ok=True)

        sync_workspace_projection(self.workspace, self.dsn)
        projected = fetch_projected_detections(
            tenant_id=self.workspace.tenant_id,
            limit=50,
            offset=0,
            dsn=self.dsn,
        )

        self.assertEqual(
            {item["detection_id"] for item in projected["items"]},
            {item["detection_id"] for item in original if item.get("status", "open") == "open"},
        )


if __name__ == "__main__":
    unittest.main()
