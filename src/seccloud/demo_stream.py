from __future__ import annotations

from typing import Any

from seccloud.pipeline import (
    build_derived_state_and_detections,
    collect_ops_metadata,
    ingest_raw_events,
)
from seccloud.storage import Workspace, format_timestamp, parse_timestamp, read_json, write_json
from seccloud.synthetic import generate_synthetic_dataset


def _stream_manifest_path(workspace: Workspace):
    return workspace.manifests_dir / "demo_stream_manifest.json"


def _build_demo_source_events() -> tuple[list[dict[str, Any]], dict[str, bool]]:
    dataset = generate_synthetic_dataset()
    baseline_events = [event for event in dataset.raw_events if event.get("scenario") == "baseline"]
    expanded_history: list[dict[str, Any]] = []
    history_window = parse_timestamp("2026-01-09T00:00:00Z") - parse_timestamp("2026-01-01T00:00:00Z")

    for cycle in range(6):
        for index, event in enumerate(baseline_events):
            shift = (cycle + 1) * history_window
            shifted_observed_at = parse_timestamp(event["observed_at"]) - shift
            shifted_received_at = parse_timestamp(event.get("received_at", event["observed_at"])) - shift
            expanded_history.append(
                {
                    **event,
                    "source_event_id": f"demo-bg-{cycle + 1:02d}-{index + 1:04d}",
                    "observed_at": format_timestamp(shifted_observed_at),
                    "received_at": format_timestamp(shifted_received_at),
                }
            )

    all_events = expanded_history + dataset.raw_events
    all_events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    return all_events, dataset.expectations


def initialize_demo_stream(workspace: Workspace) -> dict[str, Any]:
    workspace.reset_runtime()
    source_events, expectations = _build_demo_source_events()
    seed = {
        "source_events": source_events,
        "expectations": expectations,
    }
    write_json(workspace.synthetic_manifest_path, {"expectations": expectations})
    write_json(
        _stream_manifest_path(workspace),
        {
            "cursor": 0,
            "total_source_events": len(source_events),
            "complete": False,
        },
    )
    write_json(workspace.manifests_dir / "demo_stream_source_events.json", seed)
    return {
        "status": "initialized",
        "total_source_events": len(source_events),
        "cursor": 0,
    }


def advance_demo_stream(workspace: Workspace, batch_size: int = 5) -> dict[str, Any]:
    workspace.bootstrap()
    manifest = read_json(_stream_manifest_path(workspace))
    if manifest is None:
        initialize_demo_stream(workspace)
        manifest = read_json(_stream_manifest_path(workspace))
    source_events = read_json(
        workspace.manifests_dir / "demo_stream_source_events.json",
        {"source_events": []},
    )["source_events"]
    cursor = manifest["cursor"]
    next_cursor = min(cursor + batch_size, len(source_events))
    batch = source_events[cursor:next_cursor]
    for raw_event in batch:
        workspace.write_raw_event(raw_event["source"], raw_event)

    ingest = ingest_raw_events(workspace)
    detect = build_derived_state_and_detections(workspace)
    ops = collect_ops_metadata(workspace)

    manifest.update(
        {
            "cursor": next_cursor,
            "complete": next_cursor >= len(source_events),
            "last_batch_size": len(batch),
        }
    )
    write_json(_stream_manifest_path(workspace), manifest)
    return {
        "status": "advanced",
        "cursor": next_cursor,
        "total_source_events": len(source_events),
        "complete": manifest["complete"],
        "batch_size": len(batch),
        "ingest": ingest,
        "detect": detect,
        "ops_metadata": ops,
    }


def get_demo_stream_state(workspace: Workspace) -> dict[str, Any]:
    manifest = read_json(
        _stream_manifest_path(workspace),
        {
            "cursor": 0,
            "total_source_events": 0,
            "complete": False,
        },
    )
    return {
        "cursor": manifest["cursor"],
        "total_source_events": manifest["total_source_events"],
        "complete": manifest["complete"],
        "normalized_event_count": len(workspace.list_normalized_events()),
        "detection_count": len(workspace.list_detections()),
        "case_count": len(workspace.list_cases()),
    }
