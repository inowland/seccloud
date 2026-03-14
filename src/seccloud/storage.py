from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


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
    def __init__(self, root: str | Path):
        self.root = Path(root)
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
            write_json(self.ingest_manifest_path, {"raw_event_ids": [], "normalized_event_ids": []})

    def reset_runtime(self) -> None:
        for path in (
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

    @property
    def ingest_manifest_path(self) -> Path:
        return self.manifests_dir / "ingest_manifest.json"

    @property
    def synthetic_manifest_path(self) -> Path:
        return self.manifests_dir / "synthetic_manifest.json"

    @property
    def derived_state_path(self) -> Path:
        return self.derived_dir / "derived_state.json"

    def load_ingest_manifest(self) -> dict[str, list[str]]:
        return read_json(
            self.ingest_manifest_path,
            {
                "raw_event_ids": [],
                "normalized_event_ids": [],
                "semantic_event_keys": [],
                "dead_letter_ids": [],
            },
        )

    def save_ingest_manifest(self, payload: dict[str, list[str]]) -> None:
        write_json(self.ingest_manifest_path, payload)

    def write_raw_event(self, source: str, raw_event: dict[str, Any]) -> str:
        received_at = parse_timestamp(raw_event.get("received_at", raw_event["observed_at"]))
        object_key = f"{source}/{received_at:%Y/%m/%d}/{raw_event['source_event_id']}.json"
        path = self.raw_dir / object_key
        write_json(path, raw_event)
        return object_key

    def read_raw_by_object_key(self, object_key: str) -> dict[str, Any] | None:
        path = self.raw_dir / object_key
        if not path.exists():
            return None
        return read_json(path)

    def list_raw_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.raw_dir.rglob("*.json")):
            payload = read_json(path)
            if payload is not None:
                events.append(payload)
        events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
        return events

    def save_dead_letter(self, source: str, raw_event: dict[str, Any], reason: str) -> str:
        received_at = parse_timestamp(
            raw_event.get(
                "received_at",
                raw_event.get("observed_at", format_timestamp(datetime.now(UTC))),
            )
        )
        dead_letter_id = f"{source}-{raw_event.get('source_event_id', 'unknown')}"
        object_key = f"{source}/{received_at:%Y/%m/%d}/{dead_letter_id}.json"
        path = self.dead_letters_dir / object_key
        write_json(
            path,
            {
                "dead_letter_id": dead_letter_id,
                "source": source,
                "reason": reason,
                "raw_event": raw_event,
            },
        )
        return object_key

    def list_dead_letters(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.dead_letters_dir.rglob("*.json")):
            payload = read_json(path)
            if payload is not None:
                events.append(payload)
        return events

    def write_normalized_event(self, event: dict[str, Any]) -> None:
        observed_at = parse_timestamp(event["observed_at"])
        path = self.normalized_dir / f"{observed_at:%Y-%m-%d}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    def list_normalized_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.normalized_dir.glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    events.append(json.loads(line))
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
        for path in sorted(self.raw_dir.rglob("*.json")):
            payload = read_json(path)
            observed_at = parse_timestamp(payload["observed_at"])
            if observed_at < cutoff:
                path.unlink()
                deleted.append(str(path.relative_to(self.root)))
        return {
            "reference_time": reference_time,
            "retention_days": retention_days,
            "deleted_raw_objects": deleted,
        }
