from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from seccloud.ids import new_prefixed_id
from seccloud.storage import Workspace
from seccloud.workers import submit_raw_events


def _now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    fixture_path = Path(path)
    if not fixture_path.exists():
        return events
    for line in fixture_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


class CollectorAdapter(Protocol):
    collector_kind: str
    source: str
    integration_id: str
    checkpoint_cursor_field: str
    default_batch_size: int

    def collect_page(self, checkpoint: dict[str, Any], limit: int | None = None) -> dict[str, Any]: ...


@dataclass(slots=True)
class OktaSystemLogFixtureCollector:
    fixture_path: str | Path
    integration_id: str = "okta-primary"
    default_batch_size: int = 100

    collector_kind: str = "okta_system_log_fixture"
    source: str = "okta"
    checkpoint_cursor_field: str = "fixture_offset"

    def collect_page(self, checkpoint: dict[str, Any], limit: int | None = None) -> dict[str, Any]:
        from seccloud.vendor_exports import map_vendor_event

        fixture_events = _read_jsonl(self.fixture_path)
        cursor = int(checkpoint.get(self.checkpoint_cursor_field, 0))
        page_size = limit or self.default_batch_size
        page = fixture_events[cursor : cursor + page_size]
        mapped_records = [map_vendor_event(self.source, event) for event in page]
        next_cursor = cursor + len(page)
        return {
            "records": mapped_records,
            "checkpoint": {
                self.checkpoint_cursor_field: next_cursor,
                "cursor": next_cursor,
                "has_more": next_cursor < len(fixture_events),
                "last_source_event_id": mapped_records[-1]["source_event_id"] if mapped_records else None,
            },
            "metadata": {
                "checkpoint_start": cursor,
                "checkpoint_end": next_cursor,
                "fixture_path": str(self.fixture_path),
                "record_count": len(mapped_records),
            },
        }


def _collector_idempotency_key(
    *,
    collector_kind: str,
    source: str,
    integration_id: str,
    checkpoint_start: dict[str, Any],
    checkpoint_end: dict[str, Any],
) -> str:
    start_cursor = checkpoint_start.get("cursor", 0)
    end_cursor = checkpoint_end.get("cursor", 0)
    return f"collector:{collector_kind}:{source}:{integration_id}:{start_cursor}:{end_cursor}"


def run_collector_job(
    workspace: Workspace,
    *,
    adapter: CollectorAdapter,
    limit: int | None = None,
) -> dict[str, Any]:
    workspace.bootstrap()
    current_checkpoint = workspace.load_collector_checkpoint(
        collector_kind=adapter.collector_kind,
        source=adapter.source,
        integration_id=adapter.integration_id,
    )
    checkpoint_state = dict(current_checkpoint)
    checkpoint_state["attempt_count"] = checkpoint_state.get("attempt_count", 0) + 1
    checkpoint_state["last_run_at"] = _now_timestamp()
    run_id = new_prefixed_id("run")

    try:
        page = adapter.collect_page(checkpoint_state.get("checkpoint", {}), limit=limit)
        next_checkpoint = page["checkpoint"]
        records = page["records"]
        checkpoint_state["last_attempted_checkpoint"] = next_checkpoint

        if not records:
            checkpoint_state["last_run_status"] = "idle"
            checkpoint_state["last_error"] = None
            workspace.save_collector_checkpoint(
                collector_kind=adapter.collector_kind,
                source=adapter.source,
                integration_id=adapter.integration_id,
                checkpoint=checkpoint_state,
            )
            return {
                "status": "idle",
                "source": adapter.source,
                "integration_id": adapter.integration_id,
                "record_count": 0,
                "checkpoint": checkpoint_state["checkpoint"],
            }

        idempotency_key = _collector_idempotency_key(
            collector_kind=adapter.collector_kind,
            source=adapter.source,
            integration_id=adapter.integration_id,
            checkpoint_start=checkpoint_state.get("checkpoint", {}),
            checkpoint_end=next_checkpoint,
        )
        collector_metadata = {
            "collector_kind": adapter.collector_kind,
            **page.get("metadata", {}),
        }
        checkpoint_payload = {
            "collector_kind": adapter.collector_kind,
            "checkpoint_start": checkpoint_state.get("checkpoint", {}),
            "checkpoint_end": next_checkpoint,
        }
        submission = submit_raw_events(
            workspace,
            source=adapter.source,
            records=records,
            intake_kind="collector_pull",
            integration_id=adapter.integration_id,
            metadata=collector_metadata,
            idempotency_key=idempotency_key,
            producer_run_id=run_id,
            checkpoint_payload=checkpoint_payload,
        )
    except Exception as exc:
        checkpoint_state["last_run_status"] = "failed"
        checkpoint_state["last_error"] = str(exc)
        workspace.save_collector_checkpoint(
            collector_kind=adapter.collector_kind,
            source=adapter.source,
            integration_id=adapter.integration_id,
            checkpoint=checkpoint_state,
        )
        raise

    checkpoint_state["checkpoint"] = next_checkpoint
    checkpoint_state["last_run_status"] = "succeeded"
    checkpoint_state["last_success_at"] = checkpoint_state["last_run_at"]
    checkpoint_state["last_error"] = None
    checkpoint_state["last_batch_id"] = submission["batch_id"]
    checkpoint_state["last_idempotency_key"] = idempotency_key
    checkpoint_state["success_count"] = checkpoint_state.get("success_count", 0) + 1
    workspace.save_collector_checkpoint(
        collector_kind=adapter.collector_kind,
        source=adapter.source,
        integration_id=adapter.integration_id,
        checkpoint=checkpoint_state,
    )
    return {
        "status": "submitted",
        "source": adapter.source,
        "integration_id": adapter.integration_id,
        "record_count": len(records),
        "duplicate": submission["duplicate"],
        "batch_id": submission["batch_id"],
        "manifest_key": submission["manifest_key"],
        "object_key": submission["object_key"],
        "checkpoint": checkpoint_state["checkpoint"],
        "idempotency_key": idempotency_key,
    }
