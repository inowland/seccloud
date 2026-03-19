from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from seccloud.canonical_event_store import build_event_index_document
from seccloud.quickwit_client import create_index, get_index_metadata, ingest_documents, quickwit_config_from_env
from seccloud.storage import Workspace

QUICKWIT_CONFIG_VERSION = "0.7"
DEFAULT_INGEST_BATCH_SIZE = 500


def _now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def quickwit_event_index_config(index_id: str) -> dict[str, Any]:
    return {
        "version": QUICKWIT_CONFIG_VERSION,
        "index_id": index_id,
        "doc_mapping": {
            "mode": "dynamic",
            "field_mappings": [
                {"name": "tenant_id", "type": "text", "tokenizer": "raw", "fast": True},
                {
                    "name": "observed_at",
                    "type": "datetime",
                    "input_formats": ["rfc3339"],
                    "output_format": "rfc3339",
                    "fast": True,
                    "fast_precision": "seconds",
                },
                {"name": "event_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "event_sort_key", "type": "u64", "fast": True},
                {"name": "event_key", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "source", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "integration_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "principal_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "principal_entity_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "principal_entity_key", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "principal_display_name", "type": "text"},
                {"name": "principal_email", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "principal_department", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "resource_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "resource_entity_id", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "resource_entity_key", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "resource_name", "type": "text"},
                {"name": "resource_kind", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "resource_sensitivity", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "action_verb", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "action_category", "type": "text", "tokenizer": "raw", "fast": True},
                {"name": "search_text", "type": "text", "record": "position"},
            ],
            "timestamp_field": "observed_at",
            "partition_key": "tenant_id",
            "max_num_partitions": 200,
            "tag_fields": [
                "tenant_id",
                "source",
                "integration_id",
                "action_category",
                "resource_sensitivity",
            ],
            "store_source": True,
        },
        "search_settings": {
            "default_search_fields": ["search_text"],
        },
        "indexing_settings": {
            "commit_timeout_secs": 5,
        },
    }


def ensure_quickwit_index(*, config: dict[str, str]) -> dict[str, Any]:
    metadata = get_index_metadata(config=config)
    if metadata is not None:
        return metadata
    return create_index(
        config=config,
        index_config=quickwit_event_index_config(config["index_id"]),
    )


def index_canonical_event_batch(
    workspace: Workspace,
    events: list[dict[str, Any]],
    *,
    commit: str = "wait_for",
    batch_size: int = DEFAULT_INGEST_BATCH_SIZE,
) -> dict[str, Any]:
    config = quickwit_config_from_env()
    if config is None:
        return {
            "enabled": False,
            "status": "disabled",
            "index_id": None,
            "indexed_event_count": 0,
            "indexed_event_ids": [],
            "watermark_at": None,
        }

    ensure_quickwit_index(config=config)
    state = workspace.load_quickwit_index_state()
    indexed_event_ids = set(state.get("indexed_event_ids", []))
    pending_events = [event for event in events if event["event_id"] not in indexed_event_ids]
    started_at = _now_timestamp()
    started_clock = time.monotonic()
    if not pending_events:
        state.update(
            {
                "index_id": config["index_id"],
                "last_sync_started_at": started_at,
                "last_sync_completed_at": started_at,
                "last_sync_duration_ms": 0,
                "last_sync_status": "up_to_date",
                "last_sync_error": None,
                "last_result": {
                    "status": "up_to_date",
                    "indexed_event_count": 0,
                    "indexed_event_ids": [],
                    "watermark_at": state.get("watermark_at"),
                },
            }
        )
        workspace.save_quickwit_index_state(state)
        return {
            "enabled": True,
            "status": "up_to_date",
            "index_id": config["index_id"],
            "indexed_event_count": 0,
            "indexed_event_ids": [],
            "watermark_at": state.get("watermark_at"),
        }

    watermark_at = state.get("watermark_at")
    indexed_ids: list[str] = []
    indexed_docs = 0
    try:
        for offset in range(0, len(pending_events), batch_size):
            batch = pending_events[offset : offset + batch_size]
            documents = [build_event_index_document(workspace, event) for event in batch]
            result = ingest_documents(config=config, documents=documents, commit=commit)
            indexed_docs += int(result.get("num_docs_for_processing", len(documents)))
            indexed_ids.extend(event["event_id"] for event in batch)
            for event in batch:
                observed_at = event["observed_at"]
                if watermark_at is None or observed_at > watermark_at:
                    watermark_at = observed_at
        completed_at = _now_timestamp()
        indexed_event_ids.update(indexed_ids)
        state.update(
            {
                "index_id": config["index_id"],
                "last_sync_started_at": started_at,
                "last_sync_completed_at": completed_at,
                "last_sync_duration_ms": int((time.monotonic() - started_clock) * 1000),
                "last_sync_status": "indexed",
                "last_sync_error": None,
                "last_indexed_at": completed_at,
                "watermark_at": watermark_at,
                "indexed_event_count": len(indexed_event_ids),
                "indexed_event_ids": sorted(indexed_event_ids),
                "last_result": {
                    "status": "indexed",
                    "indexed_event_count": indexed_docs,
                    "indexed_event_ids": indexed_ids,
                    "watermark_at": watermark_at,
                },
            }
        )
        workspace.save_quickwit_index_state(state)
        return {
            "enabled": True,
            "status": "indexed",
            "index_id": config["index_id"],
            "indexed_event_count": indexed_docs,
            "indexed_event_ids": indexed_ids,
            "watermark_at": watermark_at,
        }
    except Exception as exc:
        state.update(
            {
                "index_id": config["index_id"],
                "last_sync_started_at": started_at,
                "last_sync_completed_at": _now_timestamp(),
                "last_sync_duration_ms": int((time.monotonic() - started_clock) * 1000),
                "last_sync_status": "failed",
                "last_sync_error": str(exc),
                "last_result": {
                    "status": "failed",
                    "indexed_event_count": indexed_docs,
                    "indexed_event_ids": indexed_ids,
                    "watermark_at": watermark_at,
                    "error": str(exc),
                },
            }
        )
        workspace.save_quickwit_index_state(state)
        raise


def sync_quickwit_event_index(
    workspace: Workspace,
    *,
    commit: str = "wait_for",
    batch_size: int = DEFAULT_INGEST_BATCH_SIZE,
) -> dict[str, Any]:
    return index_canonical_event_batch(
        workspace,
        workspace.list_normalized_events(),
        commit=commit,
        batch_size=batch_size,
    )
