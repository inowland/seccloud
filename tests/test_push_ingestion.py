from __future__ import annotations

import gzip
import json
import os
import shutil
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
from seccloud.storage import Workspace


class PushIngestionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = Path(tempfile.mkdtemp(prefix="seccloud-push-"))
        self.workspace = Workspace(self.tempdir)
        self.workspace.bootstrap()

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _create_app(self):
        credentials = {
            "push-token": {
                "tenant_id": self.workspace.tenant_id,
                "source": "okta",
                "integration_id": "okta-primary",
            }
        }
        env = {
            "SECCLOUD_WORKSPACE": str(self.workspace.root),
            "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
            "SECCLOUD_PUSH_AUTH_TOKENS": json.dumps(credentials),
        }
        patcher = patch.dict(os.environ, env, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        return seccloud_api.create_app()

    def _build_request(self, payload: dict[str, object]) -> Request:
        body = gzip.compress(json.dumps(payload).encode("utf-8"))

        async def receive() -> dict[str, object]:
            return {"type": "http.request", "body": body, "more_body": False}

        return Request(
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
        )

    def _post_batch(
        self,
        app,
        payload: dict[str, object],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, object]:
        from starlette.testclient import TestClient

        client = TestClient(app)
        body = gzip.compress(json.dumps(payload).encode("utf-8"))
        headers = {
            "Content-Encoding": "gzip",
            "Content-Type": "application/json",
            "Authorization": "Bearer push-token",
        }
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        response = client.post("/api/intake/raw-events", content=body, headers=headers)
        if response.status_code >= 400:
            raise seccloud_api.HTTPException(status_code=response.status_code, detail=response.json().get("detail", ""))
        return response.json()

    def test_push_ingestion_accepts_valid_compressed_batch(self) -> None:
        app = self._create_app()
        payload = {
            "source": "okta",
            "records": [
                {
                    "source_event_id": "okta-push-0001",
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
                },
                {
                    "source_event_id": "okta-push-0002",
                    "observed_at": "2026-02-11T10:02:00Z",
                    "received_at": "2026-02-11T10:03:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                },
            ],
        }

        body = self._post_batch(app, payload, idempotency_key="req-1")

        self.assertFalse(body["duplicate"])
        self.assertEqual(body["tenant_id"], self.workspace.tenant_id)
        self.assertEqual(body["integration_id"], "okta-primary")
        self.assertEqual(body["record_count"], 2)
        self.assertTrue(body["object_key"].endswith(".parquet"))
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 1)
        self.assertEqual(len(self.workspace.list_normalized_events()), 0)

        manifest = self.workspace.object_store.get_json(body["manifest_key"])
        self.assertEqual(manifest["tenant_id"], self.workspace.tenant_id)
        self.assertEqual(manifest["integration_id"], "okta-primary")
        self.assertEqual(manifest["record_count"], 2)
        self.assertEqual(manifest["idempotency_key"], "req-1")
        self.assertEqual(manifest["objects"][0]["object_format"], "parquet")

        batch = self.workspace.list_pending_intake_batches()[0]
        envelopes = self.workspace.read_raw_batch_records(batch)
        self.assertEqual(len(envelopes), 2)
        self.assertEqual(envelopes[0]["tenant_id"], self.workspace.tenant_id)
        self.assertEqual(envelopes[0]["integration_id"], "okta-primary")

    def test_push_ingestion_rejects_invalid_batch_shape(self) -> None:
        from starlette.testclient import TestClient

        app = self._create_app()
        payload = {
            "source": "okta",
            "records": [
                {
                    "source": "github",
                    "source_event_id": "bad-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                }
            ],
        }

        client = TestClient(app)
        body = gzip.compress(json.dumps(payload).encode("utf-8"))
        response = client.post(
            "/api/intake/raw-events",
            content=body,
            headers={
                "Content-Encoding": "gzip",
                "Content-Type": "application/json",
                "Authorization": "Bearer push-token",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("record_source_mismatch", response.json()["detail"])
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 0)

    def test_push_ingestion_dedupes_duplicate_submission(self) -> None:
        app = self._create_app()
        payload = {
            "source": "okta",
            "records": [
                {
                    "source_event_id": "okta-dup-0001",
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
                }
            ],
        }

        first_body = self._post_batch(app, payload)
        second_body = self._post_batch(app, payload)

        self.assertFalse(first_body["duplicate"])
        self.assertTrue(second_body["duplicate"])
        self.assertEqual(first_body["batch_id"], second_body["batch_id"])
        self.assertEqual(first_body["manifest_key"], second_body["manifest_key"])
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 1)


if __name__ == "__main__":
    unittest.main()
