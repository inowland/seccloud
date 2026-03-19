from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from seccloud.defaults import DEFAULT_TENANT_ID
from seccloud.feature_contract import feature_scoring_contract
from seccloud.ids import event_key, new_prefixed_id
from seccloud.object_store import ObjectStore, build_object_store


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        tmp_path = Path(handle.name)
    os.replace(tmp_path, path)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _write_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

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
        pa.array([int(row["raw_envelope_version"]) for row in rows], type=pa.int32()),
        pa.array([str(row["tenant_id"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["source"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["integration_id"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["intake_kind"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["batch_id"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["received_at"]) for row in rows], type=pa.utf8()),
        pa.array([int(row["record_ordinal"]) for row in rows], type=pa.int32()),
        pa.array([str(row["metadata_json"]) for row in rows], type=pa.utf8()),
        pa.array([str(row["record_json"]) for row in rows], type=pa.utf8()),
    ]
    table = pa.table(arrays, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


class IntakeIdempotencyConflictError(ValueError):
    pass


NORMALIZED_SCHEMA_VERSION = "event.v1"
DETECTION_SCHEMA_VERSION = "detection.v1"


def normalized_event_parquet_bytes(
    event: dict[str, Any],
    *,
    tenant_id: str,
    integration_id: str,
) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    row = {
        "normalized_schema_version": NORMALIZED_SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "source": event["source"],
        "integration_id": integration_id,
        "observed_at": event["observed_at"],
        "event_id": event["event_id"],
        "event_key": event["event_key"],
        "source_event_id": event["source_event_id"],
        "principal_entity_key": event["principal"]["entity_key"],
        "resource_entity_key": event["resource"]["entity_key"],
        "action_source": event["action"]["source"],
        "action_verb": event["action"]["verb"],
        "action_category": event["action"]["category"],
        "payload_json": json.dumps(event, sort_keys=True),
    }
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([row]), sink)
    return sink.getvalue().to_pybytes()


def detection_parquet_bytes(
    detection: dict[str, Any],
    *,
    tenant_id: str,
) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    observed_at = (
        detection.get("evidence", [{}])[0].get("observed_at")
        if isinstance(detection.get("evidence"), list) and detection.get("evidence")
        else None
    )
    row = {
        "detection_schema_version": DETECTION_SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "detection_id": detection["detection_id"],
        "observed_at": observed_at or format_timestamp(datetime.now(UTC)),
        "scenario": detection["scenario"],
        "title": detection["title"],
        "score": float(detection["score"]),
        "confidence": float(detection["confidence"]),
        "severity": detection["severity"],
        "status": detection.get("status", "open"),
        "model_version": detection["model_version"],
        "primary_event_id": (
            detection["event_ids"][0]
            if isinstance(detection.get("event_ids"), list) and detection.get("event_ids")
            else ""
        ),
        "payload_json": json.dumps(detection, sort_keys=True),
    }
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([row]), sink)
    return sink.getvalue().to_pybytes()


class Workspace:
    def __init__(
        self,
        root: str | Path,
        *,
        tenant_id: str | None = None,
        object_store: ObjectStore | None = None,
    ):
        self.root = Path(root)
        self.tenant_id = tenant_id or os.environ.get("SECCLOUD_TENANT_ID", DEFAULT_TENANT_ID)
        self.object_store = object_store or build_object_store(self.root)
        self.intake_dir = self.root / "intake"
        self.intake_pending_dir = self.intake_dir / "pending"
        self.intake_processed_dir = self.intake_dir / "processed"
        self.raw_dir = self.root / "raw"
        self.dead_letters_dir = self.root / "dead_letters"
        self.normalized_dir = self.root / "normalized"
        self.manifests_dir = self.root / "manifests"
        self.derived_dir = self.root / "derived"
        self.ops_dir = self.root / "ops"
        self.founder_dir = self.root / "founder_artifacts"
        self.models_dir = self.root / "models"

    def bootstrap(self) -> None:
        for path in (
            self.intake_pending_dir,
            self.intake_processed_dir,
            self.raw_dir,
            self.dead_letters_dir,
            self.normalized_dir,
            self.manifests_dir,
            self.derived_dir,
            self.ops_dir,
            self.founder_dir,
            self.models_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        if not self.ingest_manifest_path.exists():
            write_json(
                self.ingest_manifest_path,
                {
                    "raw_event_ids": [],
                    "raw_event_keys": [],
                    "normalized_event_ids": [],
                    "normalized_event_keys": [],
                    "semantic_event_keys": [],
                    "dead_letter_ids": [],
                },
            )
        if not self.intake_manifest_path.exists():
            write_json(
                self.intake_manifest_path,
                {
                    "submitted_batch_ids": [],
                    "processed_batch_ids": [],
                    "accepted_batches_by_idempotency_key": {},
                },
            )
        if not self.worker_control_path.exists():
            write_json(
                self.worker_control_path,
                {
                    "source_stats_refresh_requested": False,
                    "source_stats_refresh_requested_at": None,
                },
            )
        if not self.worker_state_path.exists():
            write_json(
                self.worker_state_path,
                {
                    "normalization_runs": 0,
                    "feature_runs": 0,
                    "detection_runs": 0,
                    "source_stats_runs": 0,
                    "service_runs": 0,
                    "last_submitted_batch_id": None,
                    "last_processed_batch_id": None,
                    "last_normalization_at": None,
                    "last_feature_at": None,
                    "last_detection_at": None,
                    "last_source_stats_at": None,
                    "last_service_at": None,
                    "last_service_status": None,
                },
            )
        if not self.identity_manifest_path.exists():
            write_json(
                self.identity_manifest_path,
                {
                    "event_ids_by_key": {},
                    "entity_ids_by_key": {},
                },
            )
        if not self.source_stats_path.exists():
            write_json(
                self.source_stats_path,
                {
                    "sources": {},
                },
            )
        if not self.identity_profiles_path.exists():
            write_json(
                self.identity_profiles_path,
                {
                    "manifest_version": 1,
                    "source": "unknown",
                    "generated_at": None,
                    "principals": [],
                    "teams": [],
                },
            )
        if not self.collector_checkpoints_path.exists():
            write_json(
                self.collector_checkpoints_path,
                {
                    "checkpoint_version": 1,
                    "collectors": {},
                },
            )

    def reset_runtime(self) -> None:
        for path in (
            self.intake_dir,
            self.raw_dir,
            self.dead_letters_dir,
            self.normalized_dir,
            self.root / "lake",
            self.manifests_dir,
            self.derived_dir,
            self.ops_dir,
            self.founder_dir,
            self.models_dir,
        ):
            if path.exists():
                shutil.rmtree(path)
        self.bootstrap()
        self.request_source_stats_refresh()

    @property
    def ingest_manifest_path(self) -> Path:
        return self.manifests_dir / "ingest_manifest.json"

    @property
    def synthetic_manifest_path(self) -> Path:
        return self.manifests_dir / "synthetic_manifest.json"

    @property
    def intake_manifest_path(self) -> Path:
        return self.manifests_dir / "intake_manifest.json"

    @property
    def worker_state_path(self) -> Path:
        return self.manifests_dir / "worker_state.json"

    @property
    def worker_control_path(self) -> Path:
        return self.manifests_dir / "worker_control.json"

    @property
    def identity_manifest_path(self) -> Path:
        return self.manifests_dir / "identity_manifest.json"

    @property
    def identity_profiles_path(self) -> Path:
        return self.manifests_dir / "identity_profiles.json"

    @property
    def source_stats_path(self) -> Path:
        return self.manifests_dir / "source_stats.json"

    @property
    def collector_checkpoints_path(self) -> Path:
        return self.manifests_dir / "collector_checkpoints.json"

    @property
    def feature_state_path(self) -> Path:
        return self.manifests_dir / "feature_state.json"

    @property
    def feature_vocab_path(self) -> Path:
        return self.manifests_dir / "feature_vocab.json"

    @property
    def model_artifact_manifest_path(self) -> Path:
        return self.manifests_dir / "model_artifact.json"

    @property
    def model_promotion_policy_path(self) -> Path:
        return self.manifests_dir / "model_promotion_policy.json"

    @property
    def event_index_path(self) -> Path:
        return self.manifests_dir / "event_index.json"

    @property
    def quickwit_index_state_path(self) -> Path:
        return self.manifests_dir / "quickwit_index_state.json"

    @property
    def detection_context_path(self) -> Path:
        return self.manifests_dir / "detection_context.json"

    @property
    def derived_state_path(self) -> Path:
        return self.derived_dir / "derived_state.json"

    def load_ingest_manifest(self) -> dict[str, list[str]]:
        return read_json(
            self.ingest_manifest_path,
            {
                "raw_event_ids": [],
                "raw_event_keys": [],
                "normalized_event_ids": [],
                "normalized_event_keys": [],
                "semantic_event_keys": [],
                "dead_letter_ids": [],
            },
        )

    def save_ingest_manifest(self, payload: dict[str, list[str]]) -> None:
        write_json(self.ingest_manifest_path, payload)

    def load_feature_state(self) -> dict[str, Any]:
        return feature_scoring_contract(
            read_json(
                self.feature_state_path,
                {
                    "input_signature": "",
                    "normalized_event_count": 0,
                    "action_feature_row_count": 0,
                    "history_feature_row_count": 0,
                    "collaboration_feature_row_count": 0,
                    "static_feature_row_count": 0,
                    "peer_group_feature_row_count": 0,
                    "action_manifest_key": None,
                    "history_manifest_key": None,
                    "collaboration_manifest_key": None,
                    "static_manifest_key": None,
                    "peer_group_manifest_key": None,
                },
            )
        )

    def load_feature_vocab(self) -> dict[str, Any]:
        return read_json(
            self.feature_vocab_path,
            {
                "principal_entity_keys": [],
                "resource_entity_keys": [],
            },
        )

    def load_model_artifact_manifest(self) -> dict[str, Any]:
        return read_json(
            self.model_artifact_manifest_path,
            {
                "manifest_version": 2,
                "requested_mode": "heuristic",
                "active_model_id": None,
                "active_metadata_path": None,
                "activated_at": None,
                "activation_source": None,
                "activation_history": [],
            },
        )

    def save_model_artifact_manifest(self, payload: dict[str, Any]) -> None:
        write_json(self.model_artifact_manifest_path, payload)

    def load_model_promotion_policy(self) -> dict[str, Any]:
        return read_json(
            self.model_promotion_policy_path,
            {
                "manifest_version": 1,
                "required_source_count": 2,
                "require_identity_coverage": True,
                "require_resource_coverage": True,
                "identity_sources": ["okta"],
                "resource_sources": ["github", "gworkspace", "snowflake"],
            },
        )

    def save_model_promotion_policy(self, payload: dict[str, Any]) -> None:
        write_json(self.model_promotion_policy_path, payload)

    def load_event_index(self) -> dict[str, Any]:
        return read_json(
            self.event_index_path,
            {
                "index_version": 1,
                "input_signature": "",
                "event_count": 0,
                "events_by_id": {},
                "principal_event_ids": {},
                "resource_event_ids": {},
                "department_event_ids": {},
            },
        )

    def load_quickwit_index_state(self) -> dict[str, Any]:
        defaults = {
            "state_version": 1,
            "index_id": None,
            "last_sync_started_at": None,
            "last_sync_completed_at": None,
            "last_sync_duration_ms": None,
            "last_sync_status": None,
            "last_sync_error": None,
            "last_indexed_at": None,
            "watermark_at": None,
            "indexed_event_count": 0,
            "indexed_event_ids": [],
            "last_result": None,
        }
        payload = read_json(self.quickwit_index_state_path, defaults)
        if not isinstance(payload, dict):
            return defaults
        indexed_event_ids = payload.get("indexed_event_ids")
        if not isinstance(indexed_event_ids, list):
            indexed_event_ids = []
        return {
            **defaults,
            **payload,
            "indexed_event_ids": [str(event_id) for event_id in indexed_event_ids if isinstance(event_id, str)],
        }

    def save_quickwit_index_state(self, payload: dict[str, Any]) -> None:
        write_json(self.quickwit_index_state_path, payload)

    def load_detection_context(self) -> dict[str, Any]:
        return read_json(
            self.detection_context_path,
            {
                "context_version": 1,
                "input_signature": "",
                "event_count": 0,
                "ordered_event_ids": [],
                "contexts_by_event_id": {},
                "aggregates": {
                    "events_by_source": {},
                    "total_events": 0,
                },
            },
        )

    def save_detection_context(self, payload: dict[str, Any]) -> None:
        write_json(self.detection_context_path, payload)

    def load_intake_manifest(self) -> dict[str, Any]:
        return read_json(
            self.intake_manifest_path,
            {
                "submitted_batch_ids": [],
                "processed_batch_ids": [],
                "accepted_batches_by_idempotency_key": {},
            },
        )

    def save_intake_manifest(self, payload: dict[str, Any]) -> None:
        write_json(self.intake_manifest_path, payload)

    def _intake_manifest_defaults(self) -> dict[str, Any]:
        return {
            "submitted_batch_ids": [],
            "processed_batch_ids": [],
            "accepted_batches_by_idempotency_key": {},
        }

    def submit_intake_batch(self, batch: dict[str, Any]) -> str:
        path = self.intake_pending_dir / f"{batch['batch_id']}.json"
        write_json(path, batch)
        manifest = {**self._intake_manifest_defaults(), **self.load_intake_manifest()}
        submitted = set(manifest.get("submitted_batch_ids", []))
        submitted.add(batch["batch_id"])
        self.save_intake_manifest(
            {
                "submitted_batch_ids": sorted(submitted),
                "processed_batch_ids": sorted(set(manifest.get("processed_batch_ids", []))),
                "accepted_batches_by_idempotency_key": manifest.get(
                    "accepted_batches_by_idempotency_key",
                    {},
                ),
            }
        )
        return str(path.relative_to(self.root))

    def list_pending_intake_batches(self) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for path in sorted(self.intake_pending_dir.glob("*.json")):
            payload = read_json(path)
            if payload is not None:
                batches.append(payload)
        batches.sort(key=lambda item: (item.get("received_at", ""), item.get("batch_id", "")))
        return batches

    def list_processed_intake_batches(self) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for path in sorted(self.intake_processed_dir.glob("*.json")):
            payload = read_json(path)
            if payload is not None:
                batches.append(payload)
        batches.sort(key=lambda item: (item.get("received_at", ""), item.get("batch_id", "")))
        return batches

    def mark_intake_batch_processed(self, batch_id: str) -> str:
        pending_path = self.intake_pending_dir / f"{batch_id}.json"
        processed_path = self.intake_processed_dir / f"{batch_id}.json"
        if not pending_path.exists():
            raise FileNotFoundError(f"Pending intake batch not found: {batch_id}")
        processed_path.parent.mkdir(parents=True, exist_ok=True)
        pending_path.replace(processed_path)
        manifest = {**self._intake_manifest_defaults(), **self.load_intake_manifest()}
        processed = set(manifest.get("processed_batch_ids", []))
        processed.add(batch_id)
        self.save_intake_manifest(
            {
                "submitted_batch_ids": sorted(set(manifest.get("submitted_batch_ids", []))),
                "processed_batch_ids": sorted(processed),
                "accepted_batches_by_idempotency_key": manifest.get(
                    "accepted_batches_by_idempotency_key",
                    {},
                ),
            }
        )
        return str(processed_path.relative_to(self.root))

    def load_worker_state(self) -> dict[str, Any]:
        defaults = {
            "normalization_runs": 0,
            "feature_runs": 0,
            "detection_runs": 0,
            "source_stats_runs": 0,
            "service_runs": 0,
            "last_submitted_batch_id": None,
            "last_processed_batch_id": None,
            "last_normalization_at": None,
            "last_feature_at": None,
            "last_detection_at": None,
            "last_source_stats_at": None,
            "last_service_at": None,
            "last_service_status": None,
        }
        payload = read_json(self.worker_state_path, defaults)
        if not isinstance(payload, dict):
            return defaults
        return {**defaults, **payload}

    def save_worker_state(self, payload: dict[str, Any]) -> None:
        write_json(self.worker_state_path, payload)

    def load_worker_control(self) -> dict[str, Any]:
        defaults = {
            "source_stats_refresh_requested": False,
            "source_stats_refresh_requested_at": None,
        }
        payload = read_json(self.worker_control_path, defaults)
        if not isinstance(payload, dict):
            return defaults
        return {**defaults, **payload}

    def save_worker_control(self, payload: dict[str, Any]) -> None:
        write_json(self.worker_control_path, payload)

    def load_identity_manifest(self) -> dict[str, dict[str, str]]:
        return read_json(
            self.identity_manifest_path,
            {
                "event_ids_by_key": {},
                "entity_ids_by_key": {},
            },
        )

    def save_identity_manifest(self, payload: dict[str, dict[str, str]]) -> None:
        write_json(self.identity_manifest_path, payload)

    def load_identity_profiles(self) -> dict[str, Any]:
        defaults = {
            "manifest_version": 1,
            "source": "unknown",
            "generated_at": None,
            "principals": [],
            "teams": [],
        }
        payload = read_json(self.identity_profiles_path)
        if isinstance(payload, dict):
            merged = {**defaults, **payload}
            if merged["principals"] or merged["teams"]:
                return merged
        return defaults

    def save_identity_profiles(self, payload: dict[str, Any]) -> None:
        write_json(self.identity_profiles_path, payload)

    def load_source_stats(self) -> dict[str, Any]:
        return read_json(
            self.source_stats_path,
            {
                "sources": {},
            },
        )

    def save_source_stats(self, payload: dict[str, Any]) -> None:
        write_json(self.source_stats_path, payload)

    def load_collector_checkpoints(self) -> dict[str, Any]:
        return read_json(
            self.collector_checkpoints_path,
            {
                "checkpoint_version": 1,
                "collectors": {},
            },
        )

    def save_collector_checkpoints(self, payload: dict[str, Any]) -> None:
        write_json(self.collector_checkpoints_path, payload)

    def _collector_checkpoint_key(
        self,
        *,
        collector_kind: str,
        source: str,
        integration_id: str | None,
    ) -> str:
        return "::".join(
            [
                self.tenant_id,
                collector_kind,
                source,
                self._partition_value(integration_id),
            ]
        )

    def load_collector_checkpoint(
        self,
        *,
        collector_kind: str,
        source: str,
        integration_id: str | None,
    ) -> dict[str, Any]:
        normalized_integration_id = self._partition_value(integration_id)
        key = self._collector_checkpoint_key(
            collector_kind=collector_kind,
            source=source,
            integration_id=normalized_integration_id,
        )
        payload = self.load_collector_checkpoints()
        checkpoint = payload.get("collectors", {}).get(key)
        defaults = {
            "tenant_id": self.tenant_id,
            "collector_kind": collector_kind,
            "source": source,
            "integration_id": normalized_integration_id,
            "checkpoint": {},
            "last_attempted_checkpoint": None,
            "last_run_status": "never",
            "last_run_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_batch_id": None,
            "last_idempotency_key": None,
            "attempt_count": 0,
            "success_count": 0,
        }
        if not isinstance(checkpoint, dict):
            return defaults
        return {**defaults, **checkpoint}

    def save_collector_checkpoint(
        self,
        *,
        collector_kind: str,
        source: str,
        integration_id: str | None,
        checkpoint: dict[str, Any],
    ) -> None:
        normalized_integration_id = self._partition_value(integration_id)
        key = self._collector_checkpoint_key(
            collector_kind=collector_kind,
            source=source,
            integration_id=normalized_integration_id,
        )
        payload = self.load_collector_checkpoints()
        collectors = dict(payload.get("collectors", {}))
        collectors[key] = checkpoint
        self.save_collector_checkpoints(
            {
                "checkpoint_version": payload.get("checkpoint_version", 1),
                "collectors": collectors,
            }
        )

    def allocate_event_id(self, stable_event_key: str) -> str:
        manifest = self.load_identity_manifest()
        event_ids = manifest.setdefault("event_ids_by_key", {})
        event_id = event_ids.get(stable_event_key)
        if event_id is None:
            event_id = new_prefixed_id("evt")
            event_ids[stable_event_key] = event_id
            self.save_identity_manifest(manifest)
        return event_id

    def allocate_entity_id(self, stable_entity_key: str) -> str:
        manifest = self.load_identity_manifest()
        entity_ids = manifest.setdefault("entity_ids_by_key", {})
        entity_id = entity_ids.get(stable_entity_key)
        if entity_id is None:
            entity_id = new_prefixed_id("ent")
            entity_ids[stable_entity_key] = entity_id
            self.save_identity_manifest(manifest)
        return entity_id

    def request_source_stats_refresh(self, requested_at: str | None = None) -> None:
        control = self.load_worker_control()
        control["source_stats_refresh_requested"] = True
        control["source_stats_refresh_requested_at"] = requested_at or format_timestamp(datetime.now(UTC))
        self.save_worker_control(control)

    def clear_source_stats_refresh_request(self) -> None:
        control = self.load_worker_control()
        control["source_stats_refresh_requested"] = False
        control["source_stats_refresh_requested_at"] = None
        self.save_worker_control(control)

    def source_stats_refresh_requested(self) -> bool:
        return bool(self.load_worker_control().get("source_stats_refresh_requested", False))

    def _partition_value(self, value: str | None, *, default: str = "default") -> str:
        raw = (value or default).strip().lower()
        return raw.replace("/", "_").replace(" ", "_")

    def _tenant_partition(self) -> str:
        return f"tenant={self._partition_value(self.tenant_id, default=DEFAULT_TENANT_ID)}"

    def _integration_partition(self, integration_id: str | None) -> str:
        return f"integration={self._partition_value(integration_id)}"

    def _raw_batch_object_key(self, *, source: str, integration_id: str, batch_id: str, received_at: str) -> str:
        received = parse_timestamp(received_at)
        return (
            f"lake/raw/layout=v1/{self._tenant_partition()}/source={source}/"
            f"{self._integration_partition(integration_id)}/dt={received:%Y-%m-%d}/hour={received:%H}/"
            f"batch={batch_id}/part-00000.parquet"
        )

    def _raw_manifest_key(self, *, source: str, integration_id: str, batch_id: str, received_at: str) -> str:
        received = parse_timestamp(received_at)
        return (
            f"lake/manifests/layout=v1/type=raw/{self._tenant_partition()}/source={source}/"
            f"{self._integration_partition(integration_id)}/dt={received:%Y-%m-%d}/hour={received:%H}/"
            f"batch={batch_id}.json"
        )

    def lookup_accepted_batch(self, idempotency_key: str) -> dict[str, Any] | None:
        manifest = {**self._intake_manifest_defaults(), **self.load_intake_manifest()}
        entry = manifest.get("accepted_batches_by_idempotency_key", {}).get(idempotency_key)
        if not isinstance(entry, dict):
            return None
        return entry

    def register_accepted_batch(self, idempotency_key: str, entry: dict[str, Any]) -> None:
        manifest = {**self._intake_manifest_defaults(), **self.load_intake_manifest()}
        accepted = dict(manifest.get("accepted_batches_by_idempotency_key", {}))
        accepted[idempotency_key] = entry
        self.save_intake_manifest(
            {
                "submitted_batch_ids": sorted(set(manifest.get("submitted_batch_ids", []))),
                "processed_batch_ids": sorted(set(manifest.get("processed_batch_ids", []))),
                "accepted_batches_by_idempotency_key": accepted,
            }
        )

    def _raw_batch_source_event_id_hash(self, records: list[dict[str, Any]]) -> str:
        digests: list[str] = []
        for record in records:
            source_event_id = record.get("source_event_id")
            if isinstance(source_event_id, str) and source_event_id:
                digests.append(source_event_id)
            else:
                digests.append(canonical_json_bytes(record).decode("utf-8"))
        return sha256_digest("|".join(digests).encode("utf-8"))

    def land_raw_intake_batch(
        self,
        *,
        source: str,
        records: list[dict[str, Any]],
        intake_kind: str,
        integration_id: str | None,
        received_at: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        payload_sha256: str,
        producer_run_id: str,
        checkpoint_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        existing = self.lookup_accepted_batch(idempotency_key)
        if existing is not None:
            if existing.get("payload_sha256") != payload_sha256:
                raise IntakeIdempotencyConflictError(
                    f"Idempotency key already used for different payload: {idempotency_key}"
                )
            return {**existing, "duplicate": True}

        normalized_integration_id = self._partition_value(integration_id)
        batch_id = new_prefixed_id("raw")
        manifest_id = new_prefixed_id("man")
        produced_at = format_timestamp(datetime.now(UTC))
        received = parse_timestamp(received_at)
        object_key = self._raw_batch_object_key(
            source=source,
            integration_id=normalized_integration_id,
            batch_id=batch_id,
            received_at=received_at,
        )
        manifest_key = self._raw_manifest_key(
            source=source,
            integration_id=normalized_integration_id,
            batch_id=batch_id,
            received_at=received_at,
        )
        raw_envelopes = [
            {
                "raw_envelope_version": 1,
                "tenant_id": self.tenant_id,
                "source": source,
                "integration_id": normalized_integration_id,
                "intake_kind": intake_kind,
                "batch_id": batch_id,
                "received_at": received_at,
                "record_ordinal": ordinal,
                "metadata": metadata,
                "record": record,
            }
            for ordinal, record in enumerate(records)
        ]
        parquet_rows = [
            {
                "raw_envelope_version": envelope["raw_envelope_version"],
                "tenant_id": envelope["tenant_id"],
                "source": envelope["source"],
                "integration_id": envelope["integration_id"],
                "intake_kind": envelope["intake_kind"],
                "batch_id": envelope["batch_id"],
                "received_at": envelope["received_at"],
                "record_ordinal": envelope["record_ordinal"],
                "metadata_json": json.dumps(envelope["metadata"], sort_keys=True, separators=(",", ":")),
                "record_json": json.dumps(envelope["record"], sort_keys=True, separators=(",", ":")),
            }
            for envelope in raw_envelopes
        ]
        parquet_bytes = _write_parquet_bytes(parquet_rows)
        object_sha256 = sha256_digest(parquet_bytes)
        object_descriptor = {
            "object_key": object_key,
            "object_format": "parquet",
            "sha256": object_sha256,
            "size_bytes": len(parquet_bytes),
            "record_count": len(records),
            "first_record_ordinal": 0,
            "last_record_ordinal": max(len(records) - 1, 0),
        }
        manifest_payload = {
            "manifest_version": 1,
            "manifest_type": "raw",
            "layout_version": 1,
            "manifest_id": manifest_id,
            "batch_id": batch_id,
            "tenant_id": self.tenant_id,
            "source": source,
            "integration_id": normalized_integration_id,
            "produced_at": produced_at,
            "partition": {
                "dt": f"{received:%Y-%m-%d}",
                "hour": f"{received:%H}",
            },
            "time_bounds": {
                "min": format_timestamp(received.replace(minute=0, second=0, microsecond=0)),
                "max": format_timestamp(received.replace(minute=59, second=59, microsecond=0)),
            },
            "received_at_bounds": {
                "min": received_at,
                "max": received_at,
            },
            "record_count": len(records),
            "idempotency_key": idempotency_key,
            "source_event_id_hash": self._raw_batch_source_event_id_hash(records),
            "raw_envelope_version": 1,
            "producer": {
                "kind": intake_kind,
                "run_id": producer_run_id,
            },
            "checkpoint": {
                "request_payload_sha256": payload_sha256,
                **(checkpoint_payload or {}),
            },
            "objects": [object_descriptor],
        }
        self.object_store.put_bytes(object_key, parquet_bytes, content_type="application/octet-stream")
        self.object_store.put_json(manifest_key, manifest_payload)
        queue_entry = {
            "batch_id": batch_id,
            "tenant_id": self.tenant_id,
            "source": source,
            "integration_id": normalized_integration_id,
            "received_at": received_at,
            "record_count": len(records),
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_sha256,
            "manifest_key": manifest_key,
            "object_key": object_key,
            "producer": manifest_payload["producer"],
        }
        queue_path = self.submit_intake_batch(queue_entry)
        accepted = {
            "batch_id": batch_id,
            "tenant_id": self.tenant_id,
            "source": source,
            "integration_id": normalized_integration_id,
            "record_count": len(records),
            "manifest_key": manifest_key,
            "object_key": object_key,
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_sha256,
            "queue_path": queue_path,
            "duplicate": False,
        }
        self.register_accepted_batch(idempotency_key, accepted)
        return accepted

    def read_raw_batch_records(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        object_key = batch["object_key"]
        payload = self.object_store.get_bytes(object_key)
        if payload is None:
            raise FileNotFoundError(f"Raw batch object not found: {object_key}")

        if object_key.endswith(".parquet"):
            return self._read_parquet_raw_records(payload)

        # Legacy path: gzipped JSONL
        records: list[dict[str, Any]] = []
        for line in gzip.decompress(payload).decode("utf-8").splitlines():
            if not line:
                continue
            records.append(json.loads(line))
        return records

    @staticmethod
    def _read_parquet_raw_records(payload: bytes) -> list[dict[str, Any]]:
        import io

        import pyarrow.parquet as pq

        table = pq.read_table(io.BytesIO(payload))
        records: list[dict[str, Any]] = []
        for i in range(table.num_rows):
            row: dict[str, Any] = {}
            for col_name in table.column_names:
                value = table.column(col_name)[i].as_py()
                # Deserialize JSON string columns back to dicts
                if col_name in ("record_json", "metadata_json") and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        pass
                # Map Parquet column names to legacy envelope field names
                if col_name == "record_json":
                    row["record"] = value
                elif col_name == "metadata_json":
                    row["metadata"] = value
                else:
                    row[col_name] = value
            records.append(row)
        return records

    def _raw_object_key(self, source: str, raw_event: dict[str, Any], raw_event_key: str) -> str:
        received_at = parse_timestamp(raw_event.get("received_at", raw_event["observed_at"]))
        return (
            f"raw/{self._tenant_partition()}/source={source}/{self._integration_partition(raw_event.get('integration_id'))}/"
            f"dt={received_at:%Y-%m-%d}/hour={received_at:%H}/{raw_event_key}.json"
        )

    def _dead_letter_object_key(self, source: str, raw_event: dict[str, Any], dead_letter_id: str) -> str:
        received_at = parse_timestamp(
            raw_event.get(
                "received_at",
                raw_event.get("observed_at", format_timestamp(datetime.now(UTC))),
            )
        )
        return (
            f"dead_letters/{self._tenant_partition()}/source={source}/"
            f"{self._integration_partition(raw_event.get('integration_id'))}/dt={received_at:%Y-%m-%d}/"
            f"hour={received_at:%H}/{dead_letter_id}.json"
        )

    def _normalized_batch_id(self, event: dict[str, Any]) -> str:
        event_suffix = event["event_id"].split("_", 1)[1] if "_" in event["event_id"] else event["event_id"]
        return f"norm_{event_suffix}"

    def _normalized_lake_object_key(self, event: dict[str, Any]) -> str:
        observed_at = parse_timestamp(event["observed_at"])
        return (
            "lake/normalized/layout=v1/"
            f"schema={NORMALIZED_SCHEMA_VERSION}/{self._tenant_partition()}/source={event['source']}/"
            f"{self._integration_partition(event.get('integration_id'))}/dt={observed_at:%Y-%m-%d}/"
            f"hour={observed_at:%H}/batch={self._normalized_batch_id(event)}/part-00000.parquet"
        )

    def _normalized_lake_manifest_key(self, event: dict[str, Any]) -> str:
        observed_at = parse_timestamp(event["observed_at"])
        return (
            "lake/manifests/layout=v1/type=normalized/"
            f"{self._tenant_partition()}/source={event['source']}/"
            f"{self._integration_partition(event.get('integration_id'))}/dt={observed_at:%Y-%m-%d}/"
            f"hour={observed_at:%H}/batch={self._normalized_batch_id(event)}.json"
        )

    def raw_object_key(self, source: str, raw_event: dict[str, Any]) -> str:
        raw_event_key = event_key(
            source,
            raw_event["source_event_id"],
            integration_id=raw_event.get("integration_id"),
        )
        return self._raw_object_key(source, raw_event, raw_event_key)

    def write_raw_event(self, source: str, raw_event: dict[str, Any]) -> tuple[str, bool]:
        object_key = self.raw_object_key(source, raw_event)
        if self.object_store.get_json(object_key) is not None:
            return object_key, False
        self.object_store.put_json(object_key, raw_event)
        return object_key, True

    def read_raw_by_object_key(self, object_key: str) -> dict[str, Any] | None:
        return self.object_store.get_json(object_key)

    def list_raw_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for _, payload in self.object_store.list_json("raw"):
            events.append(payload)
        events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
        return events

    def save_dead_letter(self, source: str, raw_event: dict[str, Any], reason: str) -> tuple[str, bool]:
        dead_letter_id = f"{source}-{raw_event.get('source_event_id', 'unknown')}"
        object_key = self._dead_letter_object_key(source, raw_event, dead_letter_id)
        if self.object_store.get_json(object_key) is not None:
            return object_key, False
        self.object_store.put_json(
            object_key,
            {
                "dead_letter_id": dead_letter_id,
                "source": source,
                "reason": reason,
                "raw_event": raw_event,
            },
        )
        return object_key, True

    def list_dead_letters(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for _, payload in self.object_store.list_json("dead_letters"):
            events.append(payload)
        return events

    def write_normalized_event(self, event: dict[str, Any]) -> tuple[str, bool]:
        object_key = self._normalized_lake_object_key(event)
        manifest_key = self._normalized_lake_manifest_key(event)
        if self.object_store.get_json(manifest_key) is not None:
            return object_key, False
        self.ensure_normalized_lake_artifacts(event)
        return object_key, True

    def ensure_normalized_lake_artifacts(self, event: dict[str, Any]) -> dict[str, Any]:
        object_key = self._normalized_lake_object_key(event)
        manifest_key = self._normalized_lake_manifest_key(event)
        existing_manifest = self.object_store.get_json(manifest_key)
        if existing_manifest is not None:
            return existing_manifest

        integration_id = self._partition_value(event.get("integration_id"))
        payload_bytes = normalized_event_parquet_bytes(
            event,
            tenant_id=self.tenant_id,
            integration_id=integration_id,
        )
        object_sha256 = sha256_digest(payload_bytes)
        self.object_store.put_bytes(object_key, payload_bytes, content_type="application/vnd.apache.parquet")

        batch_id = self._normalized_batch_id(event)
        manifest_payload = {
            "manifest_version": 1,
            "manifest_type": "normalized",
            "layout_version": 1,
            "manifest_id": f"man_{batch_id.split('_', 1)[1]}",
            "batch_id": batch_id,
            "tenant_id": self.tenant_id,
            "source": event["source"],
            "integration_id": integration_id,
            "produced_at": event["observed_at"],
            "partition": {
                "dt": parse_timestamp(event["observed_at"]).strftime("%Y-%m-%d"),
                "hour": parse_timestamp(event["observed_at"]).strftime("%H"),
            },
            "time_bounds": {
                "min": event["observed_at"],
                "max": event["observed_at"],
            },
            "observed_at_bounds": {
                "min": event["observed_at"],
                "max": event["observed_at"],
            },
            "record_count": 1,
            "idempotency_key": (
                f"norm:{integration_id}:{event.get('attributes', {}).get('intake_batch_id', batch_id)}:"
                f"{NORMALIZED_SCHEMA_VERSION}:{event['event_id']}"
            ),
            "normalized_schema_version": NORMALIZED_SCHEMA_VERSION,
            "event_id_min": event["event_id"],
            "event_id_max": event["event_id"],
            "upstream_raw_batches": (
                [event.get("attributes", {}).get("intake_batch_id")]
                if event.get("attributes", {}).get("intake_batch_id")
                else []
            ),
            "checkpoint": {
                "event_id": event["event_id"],
            },
            "objects": [
                {
                    "object_key": object_key,
                    "object_format": "parquet",
                    "sha256": object_sha256,
                    "size_bytes": len(payload_bytes),
                    "record_count": 1,
                    "first_record_ordinal": 0,
                    "last_record_ordinal": 0,
                }
            ],
        }
        self.object_store.put_json(manifest_key, manifest_payload)
        return manifest_payload

    def list_normalized_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for _, payload in self.object_store.list_json("normalized"):
            events.append(payload)
        if not events:
            events.extend(self._list_normalized_events_from_lake())
        events.sort(key=lambda item: item["observed_at"])
        return events

    def read_normalized_event_by_pointer(
        self,
        pointer: dict[str, Any] | None,
        *,
        event_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(pointer, dict):
            return None
        object_key = pointer.get("object_key")
        if not isinstance(object_key, str) or not object_key:
            return None
        payload = self.object_store.get_bytes(object_key)
        if payload is None:
            return None

        import io

        import pyarrow.parquet as pq

        table = pq.read_table(io.BytesIO(payload))
        for row in table.to_pylist():
            if not isinstance(row, dict):
                continue
            payload_json = row.get("payload_json")
            candidate = row
            if isinstance(payload_json, str):
                try:
                    candidate = json.loads(payload_json)
                except json.JSONDecodeError:
                    candidate = row
            candidate_event_id = candidate.get("event_id")
            if event_id is None or candidate_event_id == event_id:
                return candidate
        return None

    def _list_normalized_events_from_lake(self) -> list[dict[str, Any]]:
        import io

        import pyarrow.parquet as pq

        events: list[dict[str, Any]] = []
        for _, manifest in self.object_store.list_json("lake/manifests/layout=v1/type=normalized"):
            if not isinstance(manifest, dict):
                continue
            for object_descriptor in manifest.get("objects", []):
                if not isinstance(object_descriptor, dict):
                    continue
                object_key = object_descriptor.get("object_key")
                if not isinstance(object_key, str):
                    continue
                payload = self.object_store.get_bytes(object_key)
                if payload is None:
                    continue
                table = pq.read_table(io.BytesIO(payload))
                for row in table.to_pylist():
                    if isinstance(row, dict):
                        payload_json = row.get("payload_json")
                        if isinstance(payload_json, str):
                            try:
                                events.append(json.loads(payload_json))
                                continue
                            except json.JSONDecodeError:
                                pass
                        events.append(row)
        deduped: dict[str, dict[str, Any]] = {}
        for event in events:
            event_id = event.get("event_id")
            if isinstance(event_id, str):
                deduped[event_id] = event
        return list(deduped.values())

    def _event_index_input_signature(self) -> str:
        manifest = self.load_ingest_manifest()
        payload = {
            "normalized_event_ids": sorted(manifest.get("normalized_event_ids", [])),
            "normalized_event_keys": sorted(manifest.get("normalized_event_keys", [])),
        }
        return sha256_digest(canonical_json_bytes(payload))

    def rebuild_event_index(self) -> dict[str, Any]:
        events = sorted(
            self.list_normalized_events(),
            key=lambda item: (item.get("observed_at", ""), item.get("event_id", "")),
            reverse=True,
        )
        events_by_id: dict[str, dict[str, Any]] = {}
        principal_event_ids: dict[str, list[str]] = {}
        resource_event_ids: dict[str, list[str]] = {}
        department_event_ids: dict[str, list[str]] = {}

        for event in events:
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                continue
            events_by_id[event_id] = event
            principal = event.get("principal", {})
            resource = event.get("resource", {})
            department = principal.get("department")
            for reference in (principal.get("id"), principal.get("entity_id")):
                if isinstance(reference, str) and reference:
                    principal_event_ids.setdefault(reference, []).append(event_id)
            for reference in (resource.get("id"), resource.get("entity_id")):
                if isinstance(reference, str) and reference:
                    resource_event_ids.setdefault(reference, []).append(event_id)
            if isinstance(department, str) and department:
                department_event_ids.setdefault(department, []).append(event_id)

        payload = {
            "index_version": 1,
            "input_signature": self._event_index_input_signature(),
            "event_count": len(events_by_id),
            "events_by_id": events_by_id,
            "principal_event_ids": principal_event_ids,
            "resource_event_ids": resource_event_ids,
            "department_event_ids": department_event_ids,
        }
        write_json(self.event_index_path, payload)
        return payload

    def ensure_event_index(self) -> dict[str, Any]:
        current = self.load_event_index()
        if current.get("input_signature") == self._event_index_input_signature():
            return current
        return self.rebuild_event_index()

    def get_indexed_event(self, event_id: str) -> dict[str, Any] | None:
        return self.ensure_event_index().get("events_by_id", {}).get(event_id)

    def query_indexed_events(
        self,
        *,
        principal_reference: str | None = None,
        resource_reference: str | None = None,
        department: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        index = self.ensure_event_index()
        candidate_ids: set[str] | None = None
        selectors = [
            index.get("principal_event_ids", {}).get(principal_reference) if principal_reference else None,
            index.get("resource_event_ids", {}).get(resource_reference) if resource_reference else None,
            index.get("department_event_ids", {}).get(department) if department else None,
        ]
        for selector in selectors:
            if selector is None:
                continue
            selector_ids = {item for item in selector if isinstance(item, str)}
            candidate_ids = selector_ids if candidate_ids is None else candidate_ids & selector_ids
        if candidate_ids is None:
            candidate_ids = {event_id for event_id in index.get("events_by_id", {}) if isinstance(event_id, str)}
        events = [
            event
            for event_id in candidate_ids
            if isinstance((event := index.get("events_by_id", {}).get(event_id)), dict)
        ]
        events.sort(key=lambda item: (item.get("observed_at", ""), item.get("event_id", "")), reverse=True)
        if limit is not None:
            return events[:limit]
        return events

    def save_derived_state(self, payload: dict[str, Any]) -> None:
        write_json(self.derived_state_path, payload)

    def load_derived_state(self) -> dict[str, Any]:
        return read_json(self.derived_state_path, {})

    def _detection_observed_at(self, detection: dict[str, Any]) -> str:
        evidence = detection.get("evidence")
        if isinstance(evidence, list) and evidence:
            observed_at = evidence[0].get("observed_at")
            if isinstance(observed_at, str) and observed_at:
                return observed_at
        return format_timestamp(datetime.now(UTC))

    def _detection_batch_id(self, detection: dict[str, Any]) -> str:
        detection_id = str(detection["detection_id"])
        suffix = detection_id.split("-", 1)[1] if "-" in detection_id else detection_id
        return f"det_{suffix}"

    def _detection_lake_object_key(self, detection: dict[str, Any]) -> str:
        observed_at = parse_timestamp(self._detection_observed_at(detection))
        return (
            "lake/detections/layout=v1/"
            f"schema={DETECTION_SCHEMA_VERSION}/{self._tenant_partition()}/dt={observed_at:%Y-%m-%d}/"
            f"hour={observed_at:%H}/batch={self._detection_batch_id(detection)}/part-00000.parquet"
        )

    def _detection_lake_manifest_key(self, detection: dict[str, Any]) -> str:
        observed_at = parse_timestamp(self._detection_observed_at(detection))
        return (
            "lake/manifests/layout=v1/type=detection/"
            f"{self._tenant_partition()}/dt={observed_at:%Y-%m-%d}/hour={observed_at:%H}/"
            f"batch={self._detection_batch_id(detection)}.json"
        )

    def save_detection(self, detection: dict[str, Any]) -> None:
        self.ensure_detection_lake_artifacts(detection)

    def ensure_detection_lake_artifacts(self, detection: dict[str, Any]) -> dict[str, Any]:
        object_key = self._detection_lake_object_key(detection)
        manifest_key = self._detection_lake_manifest_key(detection)
        existing_manifest = self.object_store.get_json(manifest_key)
        if existing_manifest is not None:
            return existing_manifest

        payload_bytes = detection_parquet_bytes(
            detection,
            tenant_id=self.tenant_id,
        )
        object_sha256 = sha256_digest(payload_bytes)
        observed_at = self._detection_observed_at(detection)
        batch_id = self._detection_batch_id(detection)
        self.object_store.put_bytes(object_key, payload_bytes, content_type="application/vnd.apache.parquet")
        manifest_payload = {
            "manifest_version": 1,
            "manifest_type": "detection",
            "layout_version": 1,
            "manifest_id": f"man_{batch_id.split('_', 1)[1]}",
            "batch_id": batch_id,
            "tenant_id": self.tenant_id,
            "produced_at": format_timestamp(datetime.now(UTC)),
            "partition": {
                "dt": parse_timestamp(observed_at).strftime("%Y-%m-%d"),
                "hour": parse_timestamp(observed_at).strftime("%H"),
            },
            "time_bounds": {
                "min": observed_at,
                "max": observed_at,
            },
            "record_count": 1,
            "detection_schema_version": DETECTION_SCHEMA_VERSION,
            "upstream_detection_context_signature": self.load_detection_context().get("input_signature"),
            "scoring_runtime_mode": "heuristic",
            "model_version": detection["model_version"],
            "objects": [
                {
                    "object_key": object_key,
                    "object_format": "parquet",
                    "sha256": object_sha256,
                    "size_bytes": len(payload_bytes),
                    "record_count": 1,
                    "first_record_ordinal": 0,
                    "last_record_ordinal": 0,
                }
            ],
        }
        self.object_store.put_json(manifest_key, manifest_payload)
        return manifest_payload

    def _list_detection_lake_payloads(self) -> list[dict[str, Any]]:
        import pyarrow.parquet as pq

        detections_by_id: dict[str, dict[str, Any]] = {}
        detection_manifest_root = self.root / "lake" / "manifests" / "layout=v1" / "type=detection"
        if not detection_manifest_root.exists():
            return []
        for manifest_path in sorted(detection_manifest_root.rglob("*.json")):
            manifest = read_json(manifest_path, {})
            for obj in manifest.get("objects", []):
                object_key = obj.get("object_key")
                if not isinstance(object_key, str) or not object_key:
                    continue
                object_path = self.root / object_key
                if not object_path.exists():
                    continue
                table = pq.read_table(object_path, columns=["payload_json"])
                for payload_json in table.column("payload_json").to_pylist():
                    if not isinstance(payload_json, str) or not payload_json:
                        continue
                    payload = json.loads(payload_json)
                    detection_id = payload.get("detection_id")
                    if isinstance(detection_id, str) and detection_id:
                        detections_by_id[detection_id] = payload
        return [detections_by_id[key] for key in sorted(detections_by_id)]

    def list_detections(self) -> list[dict[str, Any]]:
        return self._list_detection_lake_payloads()

    def get_detection(self, detection_id: str) -> dict[str, Any]:
        for detection in self._list_detection_lake_payloads():
            if detection.get("detection_id") == detection_id:
                return detection
        return {}

    def save_ops_metadata(self, payload: dict[str, Any]) -> None:
        write_json(self.ops_dir / "metadata.json", payload)

    def load_ops_metadata(self) -> dict[str, Any]:
        return read_json(self.ops_dir / "metadata.json", {})

    def save_founder_artifact(self, name: str, content: str) -> None:
        path = self.founder_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def apply_raw_retention(self, retention_days: int, reference_time: str) -> dict[str, Any]:
        reference = parse_timestamp(reference_time)
        cutoff = reference - timedelta(days=retention_days)
        deleted: list[str] = []
        for object_key, payload in self.object_store.list_json("raw"):
            observed_at = parse_timestamp(payload["observed_at"])
            if observed_at < cutoff:
                self.object_store.delete(object_key)
                deleted.append(object_key)
        for manifest_key, manifest in self.object_store.list_json("lake/manifests/layout=v1/type=raw"):
            received_bounds = manifest.get("received_at_bounds", {})
            max_received_at = received_bounds.get("max")
            if not isinstance(max_received_at, str):
                continue
            if parse_timestamp(max_received_at) >= cutoff:
                continue
            for obj in manifest.get("objects", []):
                object_key = obj.get("object_key")
                if isinstance(object_key, str):
                    self.object_store.delete(object_key)
                    deleted.append(object_key)
            self.object_store.delete(manifest_key)
            deleted.append(manifest_key)
        return {
            "reference_time": reference_time,
            "retention_days": retention_days,
            "deleted_raw_objects": deleted,
        }
