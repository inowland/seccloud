from __future__ import annotations

import json
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

from seccloud.storage import Workspace
from seccloud.workers import run_normalization_worker, run_okta_fixture_collector


def _okta_event(event_id: str, published: str) -> dict[str, object]:
    return {
        "uuid": event_id,
        "published": published,
        "ingested_at": published,
        "eventType": "user.session.start",
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


class CollectorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = Path(tempfile.mkdtemp(prefix="seccloud-collector-"))
        self.workspace = Workspace(self.tempdir)
        self.workspace.bootstrap()
        self.fixture_path = self.tempdir / "okta_system_log.jsonl"
        self.fixture_path.write_text(
            "\n".join(
                json.dumps(event)
                for event in [
                    _okta_event("okta-fixture-0001", "2026-02-11T10:00:00Z"),
                    _okta_event("okta-fixture-0002", "2026-02-11T10:01:00Z"),
                    _okta_event("okta-fixture-0003", "2026-02-11T10:02:00Z"),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _load_checkpoint(self) -> dict[str, object]:
        return self.workspace.load_collector_checkpoint(
            collector_kind="okta_system_log_fixture",
            source="okta",
            integration_id="okta-primary",
        )

    def test_collector_resumes_from_checkpoint(self) -> None:
        first = run_okta_fixture_collector(
            self.workspace,
            fixture_path=str(self.fixture_path),
            integration_id="okta-primary",
            limit=2,
        )
        second = run_okta_fixture_collector(
            self.workspace,
            fixture_path=str(self.fixture_path),
            integration_id="okta-primary",
            limit=2,
        )

        checkpoint = self._load_checkpoint()
        self.assertEqual(first["status"], "submitted")
        self.assertEqual(first["record_count"], 2)
        self.assertEqual(first["checkpoint"]["cursor"], 2)
        self.assertEqual(second["status"], "submitted")
        self.assertEqual(second["record_count"], 1)
        self.assertEqual(second["checkpoint"]["cursor"], 3)
        self.assertEqual(checkpoint["checkpoint"]["cursor"], 3)
        self.assertEqual(checkpoint["success_count"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 2)

    def test_collector_retry_preserves_checkpoint_until_success(self) -> None:
        with patch("seccloud.collectors.submit_raw_events", side_effect=RuntimeError("temporary upstream failure")):
            with self.assertRaises(RuntimeError):
                run_okta_fixture_collector(
                    self.workspace,
                    fixture_path=str(self.fixture_path),
                    integration_id="okta-primary",
                    limit=2,
                )

        failed_checkpoint = self._load_checkpoint()
        self.assertEqual(failed_checkpoint["last_run_status"], "failed")
        self.assertEqual(failed_checkpoint["checkpoint"], {})
        self.assertEqual(failed_checkpoint["last_attempted_checkpoint"]["cursor"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 0)

        retry = run_okta_fixture_collector(
            self.workspace,
            fixture_path=str(self.fixture_path),
            integration_id="okta-primary",
            limit=2,
        )

        resumed_checkpoint = self._load_checkpoint()
        self.assertEqual(retry["status"], "submitted")
        self.assertFalse(retry["duplicate"])
        self.assertEqual(resumed_checkpoint["checkpoint"]["cursor"], 2)
        self.assertEqual(resumed_checkpoint["success_count"], 1)
        self.assertEqual(resumed_checkpoint["attempt_count"], 2)

    def test_collector_batches_use_shared_intake_path(self) -> None:
        collected = run_okta_fixture_collector(
            self.workspace,
            fixture_path=str(self.fixture_path),
            integration_id="okta-primary",
            limit=1,
        )

        pending = self.workspace.list_pending_intake_batches()
        self.assertEqual(collected["status"], "submitted")
        self.assertEqual(len(pending), 1)
        self.assertIn("manifest_key", pending[0])
        self.assertIn("object_key", pending[0])

        manifest = self.workspace.object_store.get_json(collected["manifest_key"])
        self.assertEqual(manifest["producer"]["kind"], "collector_pull")
        self.assertEqual(manifest["integration_id"], "okta-primary")
        self.assertEqual(manifest["checkpoint"]["checkpoint_start"], {})
        self.assertEqual(manifest["checkpoint"]["checkpoint_end"]["cursor"], 1)

        normalization = run_normalization_worker(self.workspace)
        self.assertEqual(normalization["processed_batch_count"], 1)
        self.assertEqual(normalization["ingest"]["added_normalized_events"], 1)


if __name__ == "__main__":
    unittest.main()
