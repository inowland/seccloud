"""Round-trip test: verify Python can read Parquet files written by the Rust lake crate.

This test writes a Parquet file using the same schema as seccloud-lake's
`raw_event_record_batch` + `write_parquet_bytes`, then reads it back through
the Python `Workspace.read_raw_batch_records` path.

No LocalStack required — uses a temp directory as a local object store.
"""

import io
import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from seccloud.object_store import LocalObjectStore
from seccloud.storage import Workspace


def _write_test_parquet(rows: list[dict]) -> bytes:
    """Write rows using the same schema as the Rust crate."""
    schema = pa.schema(
        [
            pa.field("raw_envelope_version", pa.int32(), nullable=False),
            pa.field("tenant_id", pa.utf8(), nullable=False),
            pa.field("source", pa.utf8(), nullable=False),
            pa.field("integration_id", pa.utf8(), nullable=False),
            pa.field("intake_kind", pa.utf8(), nullable=False),
            pa.field("batch_id", pa.utf8(), nullable=False),
            pa.field("received_at", pa.utf8(), nullable=False),
            pa.field("record_ordinal", pa.int32(), nullable=False),
            pa.field("metadata_json", pa.utf8(), nullable=False),
            pa.field("record_json", pa.utf8(), nullable=False),
        ]
    )

    arrays = [
        pa.array([r["raw_envelope_version"] for r in rows], type=pa.int32()),
        pa.array([r["tenant_id"] for r in rows], type=pa.utf8()),
        pa.array([r["source"] for r in rows], type=pa.utf8()),
        pa.array([r["integration_id"] for r in rows], type=pa.utf8()),
        pa.array([r["intake_kind"] for r in rows], type=pa.utf8()),
        pa.array([r["batch_id"] for r in rows], type=pa.utf8()),
        pa.array([r["received_at"] for r in rows], type=pa.utf8()),
        pa.array([r["record_ordinal"] for r in rows], type=pa.int32()),
        pa.array([r["metadata_json"] for r in rows], type=pa.utf8()),
        pa.array([r["record_json"] for r in rows], type=pa.utf8()),
    ]

    table = pa.table(arrays, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def test_python_reads_rust_parquet_schema():
    """Verify that read_raw_batch_records correctly reads Parquet files
    with the Rust lake crate schema."""

    raw_records = [
        {
            "source_event_id": "okta-evt-001",
            "observed_at": "2026-03-15T14:00:00Z",
            "event_type": "login",
            "actor_email": "alice@example.com",
        },
        {
            "source_event_id": "okta-evt-002",
            "observed_at": "2026-03-15T14:01:00Z",
            "event_type": "logout",
            "actor_email": "bob@example.com",
        },
    ]

    rows = [
        {
            "raw_envelope_version": 1,
            "tenant_id": "tenant-1",
            "source": "okta",
            "integration_id": "okta-primary",
            "intake_kind": "push_gateway",
            "batch_id": "raw_test123",
            "received_at": "2026-03-15T14:30:00Z",
            "record_ordinal": i,
            "metadata_json": json.dumps({"origin": "test"}),
            "record_json": json.dumps(raw_records[i]),
        }
        for i in range(len(raw_records))
    ]

    parquet_bytes = _write_test_parquet(rows)

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        object_key = (
            "lake/raw/layout=v1/tenant=tenant-1/source=okta/"
            "integration=okta-primary/dt=2026-03-15/hour=14/"
            "batch=raw_test123/part-00000.parquet"
        )
        obj_path = root / object_key
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        obj_path.write_bytes(parquet_bytes)

        store = LocalObjectStore(root)
        ws = Workspace(root=root, tenant_id="tenant-1", object_store=store)

        batch_info = {"object_key": object_key}
        records = ws.read_raw_batch_records(batch_info)

    assert len(records) == 2

    # Check first record
    r0 = records[0]
    assert r0["raw_envelope_version"] == 1
    assert r0["tenant_id"] == "tenant-1"
    assert r0["source"] == "okta"
    assert r0["integration_id"] == "okta-primary"
    assert r0["intake_kind"] == "push_gateway"
    assert r0["batch_id"] == "raw_test123"
    assert r0["received_at"] == "2026-03-15T14:30:00Z"
    assert r0["record_ordinal"] == 0

    # record_json deserialized back to dict
    assert isinstance(r0["record"], dict)
    assert r0["record"]["source_event_id"] == "okta-evt-001"
    assert r0["record"]["event_type"] == "login"
    assert r0["record"]["actor_email"] == "alice@example.com"

    # metadata_json deserialized back to dict
    assert isinstance(r0["metadata"], dict)
    assert r0["metadata"]["origin"] == "test"

    # Check second record
    r1 = records[1]
    assert r1["record_ordinal"] == 1
    assert r1["record"]["source_event_id"] == "okta-evt-002"
    assert r1["record"]["event_type"] == "logout"


def test_legacy_jsonl_gz_still_works():
    """Verify the legacy gzipped JSONL path still works after the Parquet bridge."""
    import gzip

    raw_envelope = {
        "raw_envelope_version": 1,
        "tenant_id": "tenant-1",
        "source": "okta",
        "integration_id": "default",
        "intake_kind": "push_gateway",
        "batch_id": "raw_legacy",
        "received_at": "2026-03-15T14:30:00Z",
        "record_ordinal": 0,
        "metadata": {},
        "record": {"source_event_id": "e1", "observed_at": "2026-03-15T14:00:00Z"},
    }

    jsonl = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    compressed = gzip.compress(jsonl)

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        object_key = "lake/raw/layout=v1/tenant=tenant-1/source=okta/batch=raw_legacy/part-00000.jsonl.gz"
        obj_path = root / object_key
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        obj_path.write_bytes(compressed)

        store = LocalObjectStore(root)
        ws = Workspace(root=root, tenant_id="tenant-1", object_store=store)

        records = ws.read_raw_batch_records({"object_key": object_key})

    assert len(records) == 1
    assert records[0]["batch_id"] == "raw_legacy"
    assert records[0]["record"]["source_event_id"] == "e1"
