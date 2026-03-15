from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from seccloud.defaults import DEFAULT_TENANT_ID
from seccloud.ids import event_key, new_prefixed_id
from seccloud.object_store import ObjectStore, build_object_store


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


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
        self.detections_dir = self.root / "detections"
        self.cases_dir = self.root / "cases"
        self.ops_dir = self.root / "ops"
        self.founder_dir = self.root / "founder_artifacts"

    def bootstrap(self) -> None:
        for path in (
            self.intake_pending_dir,
            self.intake_processed_dir,
            self.raw_dir,
            self.dead_letters_dir,
            self.normalized_dir,
            self.manifests_dir,
            self.derived_dir,
            self.detections_dir,
            self.cases_dir,
            self.ops_dir,
            self.founder_dir,
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
                {"submitted_batch_ids": [], "processed_batch_ids": []},
            )
        if not self.worker_control_path.exists():
            write_json(
                self.worker_control_path,
                {
                    "projection_refresh_requested": False,
                    "projection_refresh_requested_at": None,
                    "source_stats_refresh_requested": False,
                    "source_stats_refresh_requested_at": None,
                },
            )
        if not self.worker_state_path.exists():
            write_json(
                self.worker_state_path,
                {
                    "normalization_runs": 0,
                    "detection_runs": 0,
                    "source_stats_runs": 0,
                    "projection_runs": 0,
                    "service_runs": 0,
                    "last_submitted_batch_id": None,
                    "last_processed_batch_id": None,
                    "last_normalization_at": None,
                    "last_detection_at": None,
                    "last_source_stats_at": None,
                    "last_projection_at": None,
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

    def reset_runtime(self) -> None:
        for path in (
            self.intake_dir,
            self.raw_dir,
            self.dead_letters_dir,
            self.normalized_dir,
            self.manifests_dir,
            self.derived_dir,
            self.detections_dir,
            self.cases_dir,
            self.ops_dir,
            self.founder_dir,
        ):
            if path.exists():
                shutil.rmtree(path)
        self.bootstrap()
        self.request_source_stats_refresh()
        self.request_projection_refresh()

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
    def source_stats_path(self) -> Path:
        return self.manifests_dir / "source_stats.json"

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

    def load_intake_manifest(self) -> dict[str, list[str]]:
        return read_json(
            self.intake_manifest_path,
            {"submitted_batch_ids": [], "processed_batch_ids": []},
        )

    def save_intake_manifest(self, payload: dict[str, list[str]]) -> None:
        write_json(self.intake_manifest_path, payload)

    def submit_intake_batch(self, batch: dict[str, Any]) -> str:
        path = self.intake_pending_dir / f"{batch['batch_id']}.json"
        write_json(path, batch)
        manifest = self.load_intake_manifest()
        submitted = set(manifest.get("submitted_batch_ids", []))
        submitted.add(batch["batch_id"])
        self.save_intake_manifest(
            {
                "submitted_batch_ids": sorted(submitted),
                "processed_batch_ids": sorted(set(manifest.get("processed_batch_ids", []))),
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
        manifest = self.load_intake_manifest()
        processed = set(manifest.get("processed_batch_ids", []))
        processed.add(batch_id)
        self.save_intake_manifest(
            {
                "submitted_batch_ids": sorted(set(manifest.get("submitted_batch_ids", []))),
                "processed_batch_ids": sorted(processed),
            }
        )
        return str(processed_path.relative_to(self.root))

    def load_worker_state(self) -> dict[str, Any]:
        defaults = {
            "normalization_runs": 0,
            "detection_runs": 0,
            "source_stats_runs": 0,
            "projection_runs": 0,
            "service_runs": 0,
            "last_submitted_batch_id": None,
            "last_processed_batch_id": None,
            "last_normalization_at": None,
            "last_detection_at": None,
            "last_source_stats_at": None,
            "last_projection_at": None,
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
            "projection_refresh_requested": False,
            "projection_refresh_requested_at": None,
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

    def load_source_stats(self) -> dict[str, Any]:
        return read_json(
            self.source_stats_path,
            {
                "sources": {},
            },
        )

    def save_source_stats(self, payload: dict[str, Any]) -> None:
        write_json(self.source_stats_path, payload)

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

    def request_projection_refresh(self, requested_at: str | None = None) -> None:
        control = self.load_worker_control()
        control["projection_refresh_requested"] = True
        control["projection_refresh_requested_at"] = requested_at or format_timestamp(datetime.now(UTC))
        self.save_worker_control(control)

    def clear_projection_refresh_request(self) -> None:
        control = self.load_worker_control()
        control["projection_refresh_requested"] = False
        control["projection_refresh_requested_at"] = None
        self.save_worker_control(control)

    def projection_refresh_requested(self) -> bool:
        return bool(self.load_worker_control().get("projection_refresh_requested", False))

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

    def _normalized_object_key(self, event: dict[str, Any]) -> str:
        observed_at = parse_timestamp(event["observed_at"])
        return (
            f"normalized/{self._tenant_partition()}/source={event['source']}/"
            f"{self._integration_partition(event.get('integration_id'))}/dt={observed_at:%Y-%m-%d}/"
            f"hour={observed_at:%H}/{event['event_id']}.json"
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
        object_key = self._normalized_object_key(event)
        if self.object_store.get_json(object_key) is not None:
            return object_key, False
        self.object_store.put_json(object_key, event)
        return object_key, True

    def list_normalized_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for _, payload in self.object_store.list_json("normalized"):
            events.append(payload)
        events.sort(key=lambda item: item["observed_at"])
        return events

    def save_derived_state(self, payload: dict[str, Any]) -> None:
        write_json(self.derived_state_path, payload)

    def load_derived_state(self) -> dict[str, Any]:
        return read_json(self.derived_state_path, {})

    def save_detection(self, detection: dict[str, Any]) -> None:
        write_json(self.detections_dir / f"{detection['detection_id']}.json", detection)

    def list_detections(self) -> list[dict[str, Any]]:
        detections: list[dict[str, Any]] = []
        for path in sorted(self.detections_dir.glob("*.json")):
            detections.append(read_json(path))
        detections.sort(key=lambda item: item["detection_id"])
        return detections

    def get_detection(self, detection_id: str) -> dict[str, Any]:
        return read_json(self.detections_dir / f"{detection_id}.json")

    def save_case(self, case_payload: dict[str, Any]) -> None:
        write_json(self.cases_dir / f"{case_payload['case_id']}.json", case_payload)

    def list_cases(self) -> list[dict[str, Any]]:
        cases: list[dict[str, Any]] = []
        for path in sorted(self.cases_dir.glob("*.json")):
            cases.append(read_json(path))
        cases.sort(key=lambda item: item["case_id"])
        return cases

    def get_case(self, case_id: str) -> dict[str, Any]:
        return read_json(self.cases_dir / f"{case_id}.json")

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
        return {
            "reference_time": reference_time,
            "retention_days": retention_days,
            "deleted_raw_objects": deleted,
        }
