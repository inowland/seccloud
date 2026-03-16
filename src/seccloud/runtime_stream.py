"""Runtime stream: scaled synthetic data with ML-based detection.

The bootstrap CLI generates ~50K events from 200 principals, trains the
contrastive model, and pre-computes ML detections.  The API advances the
stream cursor in batches, feeding events through normalization and
projecting pre-computed detections as they trigger.
"""

from __future__ import annotations

import random
from typing import Any

from seccloud.storage import Workspace, format_timestamp, parse_timestamp, read_json, write_json
from seccloud.workers import submit_grouped_raw_events


def _interleave_runtime_timeline(
    background_events: list[dict[str, Any]],
    scenario_events: list[dict[str, Any]],
    warmup_events: int = 20,
) -> list[dict[str, Any]]:
    if not scenario_events:
        return background_events
    if len(background_events) <= warmup_events:
        return background_events + scenario_events

    leading_background = background_events[:warmup_events]
    trailing_background = background_events[warmup_events:]
    gap_count = len(scenario_events) + 1
    base_gap, extra_gap_events = divmod(len(trailing_background), gap_count)

    interleaved_events = list(leading_background)
    cursor = 0
    for index, scenario_event in enumerate(scenario_events):
        gap_size = base_gap + (1 if index < extra_gap_events else 0)
        interleaved_events.extend(trailing_background[cursor : cursor + gap_size])
        cursor += gap_size
        interleaved_events.append(scenario_event)

    interleaved_events.extend(trailing_background[cursor:])
    return interleaved_events


def _build_legacy_source_events() -> tuple[list[dict[str, Any]], dict[str, bool]]:
    """Build the small synthetic event stream (used by tests only)."""
    from seccloud.synthetic import generate_synthetic_dataset

    dataset = generate_synthetic_dataset()
    baseline_events = [event for event in dataset.raw_events if event.get("scenario") == "baseline"]
    scenario_events = [event for event in dataset.raw_events if event.get("scenario") != "baseline"]
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
                    "source_event_id": f"stream-bg-{cycle + 1:02d}-{index + 1:04d}",
                    "observed_at": format_timestamp(shifted_observed_at),
                    "received_at": format_timestamp(shifted_received_at),
                }
            )

    background_events = expanded_history + baseline_events
    background_events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    scenario_events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    all_events = _interleave_runtime_timeline(background_events, scenario_events)
    return all_events, dataset.expectations


def _active_detection_count(workspace: Workspace) -> int:
    return sum(1 for detection in workspace.list_detections() if detection.get("status", "open") == "open")


def _stream_manifest_path(workspace: Workspace):
    return workspace.manifests_dir / "runtime_stream_manifest.json"


def initialize_runtime_stream(
    workspace: Workspace,
    *,
    scaled: bool = False,
    num_principals: int = 200,
    num_days: int = 10,
    seed: int = 42,
) -> dict[str, Any]:
    """Generate synthetic data and optionally train ML model.

    When *scaled* is True (the default, used by ``bootstrap-local-runtime``),
    generates ~50K events from 200 principals, trains the contrastive model,
    and pre-computes ML detections.  This takes ~90 seconds.

    When *scaled* is False (used by tests), generates a small dataset from
    the legacy synthetic generator without ML training.
    """
    workspace.reset_runtime()

    if scaled:
        from seccloud.ml_scoring import precompute_detections, serialize_precomputed
        from seccloud.synthetic_scale import ScaleConfig, generate_org
        from seccloud.synthetic_scale import generate_scaled_dataset as gen_scaled

        cfg = ScaleConfig(
            num_principals=num_principals,
            num_days=num_days,
            seed=seed,
            inject_scenarios=True,
        )
        dataset = gen_scaled(cfg)
        source_events = dataset.raw_events
        expectations = dataset.expectations

        rng = random.Random(seed)
        principals, teams = generate_org(cfg, rng)
        detections = precompute_detections(
            source_events, principals, teams, epochs=10, seed=seed,
        )
        write_json(
            workspace.manifests_dir / "precomputed_detections.json",
            serialize_precomputed(detections),
        )
    else:
        source_events, expectations = _build_legacy_source_events()

    write_json(workspace.synthetic_manifest_path, {"expectations": expectations})
    write_json(
        _stream_manifest_path(workspace),
        {
            "cursor": 0,
            "total_source_events": len(source_events),
            "complete": False,
        },
    )
    write_json(
        workspace.manifests_dir / "runtime_stream_source_events.json",
        {"source_events": source_events, "expectations": expectations},
    )
    workspace.request_projection_refresh()
    return {
        "status": "initialized",
        "total_source_events": len(source_events),
        "cursor": 0,
    }


def reset_stream_cursor(workspace: Workspace) -> dict[str, Any]:
    """Reset the stream cursor to 0 without regenerating data or retraining.

    Called from the UI's "Restart stream" button.  Preserves the pre-computed
    source events and ML detections, clears all processed state so the stream
    can be replayed from the beginning.
    """
    import shutil

    source_path = workspace.manifests_dir / "runtime_stream_source_events.json"
    precomputed_path = workspace.manifests_dir / "precomputed_detections.json"

    if not source_path.exists():
        return {
            "status": "error",
            "message": "No stream data found. Run: seccloud bootstrap-local-runtime --reset-stream",
            "total_source_events": 0,
            "cursor": 0,
        }

    # Read totals before clearing
    source_data = read_json(source_path, {"source_events": []})
    total = len(source_data.get("source_events", []))

    # Clear processed state but preserve manifests
    for path in (
        workspace.intake_dir,
        workspace.raw_dir,
        workspace.dead_letters_dir,
        workspace.normalized_dir,
        workspace.derived_dir,
        workspace.detections_dir,
        workspace.cases_dir,
        workspace.ops_dir,
        workspace.founder_dir,
    ):
        if path.exists():
            shutil.rmtree(path)
    workspace.bootstrap()

    # Reset stream cursor
    write_json(
        _stream_manifest_path(workspace),
        {
            "cursor": 0,
            "total_source_events": total,
            "complete": False,
        },
    )
    workspace.request_projection_refresh()
    return {
        "status": "reset",
        "total_source_events": total,
        "cursor": 0,
    }


def advance_runtime_stream(workspace: Workspace, batch_size: int = 5) -> dict[str, Any]:
    workspace.bootstrap()
    manifest = read_json(_stream_manifest_path(workspace))
    if manifest is None:
        return {
            "status": "error",
            "message": "No stream data found. Run: seccloud bootstrap-local-runtime --reset-stream",
            "cursor": 0,
            "total_source_events": 0,
            "complete": False,
            "batch_size": 0,
            "accepted_batches": 0,
            "accepted_records": 0,
            "pending_batch_count": 0,
        }

    source_events = read_json(
        workspace.manifests_dir / "runtime_stream_source_events.json",
        {"source_events": []},
    )["source_events"]
    cursor = manifest["cursor"]
    next_cursor = min(cursor + batch_size, len(source_events))
    batch = source_events[cursor:next_cursor]
    accepted = {"batch_count": 0, "record_count": 0, "batches": []}
    if batch:
        accepted = submit_grouped_raw_events(
            workspace,
            records=batch,
            intake_kind="runtime_stream",
            integration_id="synthetic-stream",
            metadata={"cursor_start": cursor, "cursor_end": next_cursor},
        )

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
        "accepted_batches": accepted["batch_count"],
        "accepted_records": accepted["record_count"],
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
    }


def get_runtime_stream_state(workspace: Workspace) -> dict[str, Any]:
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
        "detection_count": _active_detection_count(workspace),
    }
