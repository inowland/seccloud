"""Runtime stream support for deterministic replay and continuous demo synthesis.

Finite mode keeps a deterministic source-event stream for tests and acceptance
coverage. Continuous mode persists a simulation clock and rolling buffer so the
live demo can keep generating new events across restarts.
"""

from __future__ import annotations

import random
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

from seccloud.storage import Workspace, format_timestamp, parse_timestamp, read_json, write_json
from seccloud.workers import submit_grouped_raw_events

FINITE_STREAM_MODE = "finite"
CONTINUOUS_STREAM_MODE = "continuous"


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


def _build_finite_test_source_events() -> tuple[list[dict[str, Any]], dict[str, bool]]:
    """Build the small deterministic event stream used by tests."""
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


def _normalized_event_count(workspace: Workspace) -> int:
    return len(workspace.load_ingest_manifest().get("normalized_event_ids", []))


def _active_detection_count(workspace: Workspace) -> int:
    return sum(1 for d in workspace.list_detections() if d.get("status", "open") == "open")


def _stream_manifest_path(workspace: Workspace):
    return workspace.manifests_dir / "runtime_stream_manifest.json"


def _stream_source_events_path(workspace: Workspace):
    return workspace.manifests_dir / "runtime_stream_source_events.json"


def _continuous_stream_payload(
    events: list[dict[str, Any]],
    expectations: dict[str, bool] | None = None,
) -> dict[str, Any]:
    return {
        "mode": CONTINUOUS_STREAM_MODE,
        "source_events": events,
        "expectations": expectations or {},
    }


def _build_continuous_runtime_manifest(
    *,
    num_principals: int,
    seed: int,
    start_date: str,
) -> dict[str, Any]:
    return {
        "mode": CONTINUOUS_STREAM_MODE,
        "cursor": 0,
        "total_source_events": 0,
        "complete": False,
        "num_principals": num_principals,
        "seed": seed,
        "start_date": start_date,
        "simulation_day_offset": 0,
        "event_counter": 0,
        "last_batch_size": 0,
    }


def _continuous_scale_config(
    *,
    num_principals: int,
    seed: int,
    start_date: str,
):
    from seccloud.synthetic_scale import ScaleConfig

    return ScaleConfig(
        num_principals=num_principals,
        num_days=1,
        start_date=parse_timestamp(start_date),
        seed=seed,
        inject_scenarios=False,
    )


def _initialize_continuous_runtime_stream(
    workspace: Workspace,
    *,
    num_principals: int,
    seed: int,
) -> dict[str, Any]:
    from seccloud.synthetic_scale import generate_org

    cfg = _continuous_scale_config(
        num_principals=num_principals,
        seed=seed,
        start_date=format_timestamp(datetime(2026, 1, 1, 0, 0, tzinfo=UTC)),
    )
    rng = random.Random(seed)
    principals, teams = generate_org(cfg, rng)
    identity_profiles = {
        "manifest_version": 1,
        "source": "synthetic-scale-continuous",
        "generated_at": format_timestamp(cfg.start_date),
        "principals": [asdict(principal) for principal in principals],
        "teams": [asdict(team) for team in teams],
    }
    workspace.save_identity_profiles(identity_profiles)
    write_json(workspace.synthetic_manifest_path, {"expectations": {}, "mode": CONTINUOUS_STREAM_MODE})
    write_json(
        _stream_manifest_path(workspace),
        _build_continuous_runtime_manifest(
            num_principals=num_principals,
            seed=seed,
            start_date=format_timestamp(cfg.start_date),
        ),
    )
    write_json(_stream_source_events_path(workspace), _continuous_stream_payload([]))
    return {
        "status": "initialized",
        "mode": CONTINUOUS_STREAM_MODE,
        "total_source_events": 0,
        "cursor": 0,
    }


def _build_continuous_generation_context(manifest: dict[str, Any]):
    from seccloud.synthetic_scale import (
        build_app_affinities,
        build_principal_affinities,
        generate_org,
        generate_resources,
    )

    cfg = _continuous_scale_config(
        num_principals=int(manifest.get("num_principals", 200)),
        seed=int(manifest.get("seed", 42)),
        start_date=str(manifest.get("start_date", "2026-01-01T00:00:00Z")),
    )
    rng = random.Random(cfg.seed)
    principals, teams = generate_org(cfg, rng)
    resources = generate_resources(cfg, teams, rng)
    resource_affinities = build_principal_affinities(cfg, principals, teams, resources, rng)
    app_affinities = build_app_affinities(principals, rng)
    return cfg, principals, teams, resources, resource_affinities, app_affinities


def _inject_continuous_scenarios_for_day(
    *,
    day: datetime,
    day_offset: int,
    principals: list[Any],
    resources: dict[str, list[Any]],
    rng: random.Random,
    counter: list[int],
) -> list[dict[str, Any]]:
    from seccloud.synthetic_scale import (
        LOCATIONS,
        WAREHOUSE_NAMES,
        _cross_dept_resources,
        _make_scenario_event,
        _off_hours_ts,
        _random_ts,
    )

    if day_offset < 3:
        return []

    non_managers = [principal for principal in principals if not getattr(principal, "is_manager", False)]
    principals_pool = non_managers or principals
    if not principals_pool:
        return []

    events: list[dict[str, Any]] = []
    scenario_selector = day_offset % 4

    if scenario_selector == 0:
        principal = principals_pool[day_offset % len(principals_pool)]
        other_locations = [location for location in LOCATIONS if location != principal.location]
        geo = other_locations[0] if other_locations else principal.location
        for _ in range(rng.randint(4, 7)):
            ts = _off_hours_ts(day, rng, principal.tz_offset)
            events.append(
                _make_scenario_event(
                    "okta",
                    principal,
                    ts,
                    "credential_compromise",
                    counter,
                    rng,
                    event_type="login",
                    resource_id="okta:admin-console",
                    resource_name="Admin Console",
                    resource_kind="app",
                    sensitivity="high",
                    geo=geo,
                    ip=f"172.{rng.randint(16, 31)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
                    privileged=True,
                )
            )
    elif scenario_selector == 1:
        principal = principals_pool[day_offset % len(principals_pool)]
        targets = _cross_dept_resources(resources, principal.department, "gworkspace", "high")
        if not targets:
            targets = _cross_dept_resources(resources, principal.department, "gworkspace")
        for resource in rng.sample(targets, min(len(targets), rng.randint(3, 6))):
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(
                _make_scenario_event(
                    "gworkspace",
                    principal,
                    ts,
                    "departing_employee",
                    counter,
                    rng,
                    event_type="share_external",
                    resource_id=resource.resource_id,
                    resource_name=resource.name,
                    resource_kind="document",
                    sensitivity=resource.sensitivity,
                    external=True,
                )
            )
    elif scenario_selector == 2:
        principal = principals_pool[day_offset % len(principals_pool)]
        targets = _cross_dept_resources(resources, principal.department, "github", "high")
        if not targets:
            targets = _cross_dept_resources(resources, principal.department, "github")
        for resource in rng.sample(targets, min(len(targets), rng.randint(2, 4))):
            ts = _off_hours_ts(day, rng, principal.tz_offset)
            events.append(
                _make_scenario_event(
                    "github",
                    principal,
                    ts,
                    "slow_exfiltration",
                    counter,
                    rng,
                    event_type=rng.choice(["clone", "archive_download"]),
                    resource_id=resource.resource_id,
                    resource_name=resource.name,
                    resource_kind="repo",
                    sensitivity=resource.sensitivity,
                    bytes_transferred_mb=rng.randint(200, 1200),
                )
            )
    else:
        principal = principals_pool[day_offset % len(principals_pool)]
        targets = _cross_dept_resources(resources, principal.department, "snowflake", "high")
        if not targets:
            targets = _cross_dept_resources(resources, principal.department, "snowflake")
        for resource in rng.sample(targets, min(len(targets), rng.randint(3, 5))):
            ts = _off_hours_ts(day, rng, principal.tz_offset)
            events.append(
                _make_scenario_event(
                    "snowflake",
                    principal,
                    ts,
                    "account_takeover",
                    counter,
                    rng,
                    event_type="export",
                    resource_id=resource.resource_id,
                    resource_name=resource.name,
                    resource_kind="dataset",
                    sensitivity=resource.sensitivity,
                    rows_read=rng.randint(25000, 500000),
                    warehouse=rng.choice(WAREHOUSE_NAMES),
                )
            )

    events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    return events


def _generate_continuous_day_events(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    from seccloud.synthetic_scale import _generate_day_events

    cfg, principals, teams, resources, resource_affinities, app_affinities = _build_continuous_generation_context(
        manifest
    )
    day_offset = int(manifest.get("simulation_day_offset", 0))
    day = cfg.start_date + timedelta(days=day_offset)
    baseline_rng = random.Random(cfg.seed + day_offset * 1009 + 17)
    anomaly_rng = random.Random(cfg.seed + day_offset * 2029 + 29)
    counter = [int(manifest.get("event_counter", 0))]

    baseline_events = _generate_day_events(
        day,
        principals,
        resource_affinities,
        app_affinities,
        cfg,
        baseline_rng,
        counter,
    )
    scenario_events = _inject_continuous_scenarios_for_day(
        day=day,
        day_offset=day_offset,
        principals=principals,
        resources=resources,
        rng=anomaly_rng,
        counter=counter,
    )
    events = baseline_events + scenario_events
    events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    manifest["event_counter"] = counter[0]
    manifest["simulation_day_offset"] = day_offset + 1
    return events


def _advance_continuous_runtime_stream(
    workspace: Workspace,
    manifest: dict[str, Any],
    *,
    batch_size: int,
) -> dict[str, Any]:
    buffer_payload = read_json(_stream_source_events_path(workspace), _continuous_stream_payload([]))
    source_events = list(buffer_payload.get("source_events", []))

    while len(source_events) < batch_size:
        source_events.extend(_generate_continuous_day_events(manifest))

    batch = source_events[:batch_size]
    remaining_events = source_events[batch_size:]
    accepted = {"batch_count": 0, "record_count": 0, "batches": []}
    if batch:
        accepted = submit_grouped_raw_events(
            workspace,
            records=batch,
            intake_kind="runtime_stream_continuous",
            integration_id="synthetic-stream",
            metadata={
                "cursor_start": manifest.get("cursor", 0),
                "cursor_end": int(manifest.get("cursor", 0)) + len(batch),
                "stream_mode": CONTINUOUS_STREAM_MODE,
                "simulation_day_offset": int(manifest.get("simulation_day_offset", 0)),
            },
        )

    manifest["cursor"] = int(manifest.get("cursor", 0)) + len(batch)
    manifest["last_batch_size"] = len(batch)
    manifest["complete"] = False
    manifest["total_source_events"] = manifest["cursor"] + len(remaining_events)
    write_json(_stream_manifest_path(workspace), manifest)
    write_json(_stream_source_events_path(workspace), _continuous_stream_payload(remaining_events))
    return {
        "status": "advanced",
        "cursor": manifest["cursor"],
        "total_source_events": manifest["total_source_events"],
        "complete": False,
        "batch_size": len(batch),
        "accepted_batches": accepted["batch_count"],
        "accepted_records": accepted["record_count"],
        "pending_batch_count": len(workspace.list_pending_intake_batches()),
    }


def initialize_runtime_stream(
    workspace: Workspace,
    *,
    scaled: bool = False,
    continuous: bool = False,
    num_principals: int = 200,
    num_days: int = 10,
    seed: int = 42,
) -> dict[str, Any]:
    """Generate synthetic data for finite or continuous runtime streams.

    When *scaled* is True (the default, used by ``bootstrap-local-runtime``),
    generates ~50K events from 200 principals plus identity profiles.

    When *continuous* is True, initializes a persistent scaled generator state
    instead of a finite replay tape. New events are synthesized on demand and
    the simulation clock advances across restarts.

    When *scaled* is False (used by tests), generates a small deterministic
    dataset without ML training.
    """
    workspace.reset_runtime()

    if continuous:
        return _initialize_continuous_runtime_stream(
            workspace,
            num_principals=num_principals,
            seed=seed,
        )

    if scaled:
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
        identity_profiles = {
            "manifest_version": 1,
            "source": "synthetic-scale",
            "generated_at": (
                format_timestamp(
                    parse_timestamp(source_events[-1].get("received_at", source_events[-1]["observed_at"]))
                )
                if source_events
                else None
            ),
            "principals": [asdict(principal) for principal in principals],
            "teams": [asdict(team) for team in teams],
        }
        workspace.save_identity_profiles(identity_profiles)
    else:
        source_events, expectations = _build_finite_test_source_events()

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
    return {
        "status": "initialized",
        "mode": FINITE_STREAM_MODE,
        "total_source_events": len(source_events),
        "cursor": 0,
    }


def reset_stream_cursor(workspace: Workspace) -> dict[str, Any]:
    """Reset the stream cursor to 0 without regenerating data or retraining.

    Called from the UI's "Restart stream" button. Preserves the generated
    source events, clears processed state, and replays the stream from the
    beginning through the normal runtime workers.
    """
    import shutil

    source_path = workspace.manifests_dir / "runtime_stream_source_events.json"
    manifest = read_json(_stream_manifest_path(workspace), {})

    if not source_path.exists():
        return {
            "status": "error",
            "message": "No stream data found. Run: seccloud bootstrap-local-runtime --reset-stream",
            "total_source_events": 0,
            "cursor": 0,
        }

    mode = manifest.get("mode", FINITE_STREAM_MODE)

    # Read totals before clearing
    source_data = read_json(source_path, {"source_events": []})
    total = len(source_data.get("source_events", [])) if mode == FINITE_STREAM_MODE else 0

    # Clear processed state but preserve manifests
    for path in (
        workspace.intake_dir,
        workspace.raw_dir,
        workspace.dead_letters_dir,
        workspace.normalized_dir,
        workspace.derived_dir,
        workspace.ops_dir,
        workspace.founder_dir,
    ):
        if path.exists():
            shutil.rmtree(path)
    workspace.bootstrap()

    if mode == CONTINUOUS_STREAM_MODE:
        write_json(
            _stream_manifest_path(workspace),
            _build_continuous_runtime_manifest(
                num_principals=int(manifest.get("num_principals", 200)),
                seed=int(manifest.get("seed", 42)),
                start_date=str(manifest.get("start_date", "2026-01-01T00:00:00Z")),
            ),
        )
        write_json(source_path, _continuous_stream_payload([]))
    else:
        write_json(
            _stream_manifest_path(workspace),
            {
                "cursor": 0,
                "total_source_events": total,
                "complete": False,
            },
        )
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

    if manifest.get("mode") == CONTINUOUS_STREAM_MODE:
        return _advance_continuous_runtime_stream(workspace, manifest, batch_size=batch_size)

    source_events = read_json(
        _stream_source_events_path(workspace),
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
        "normalized_event_count": _normalized_event_count(workspace),
        "detection_count": _active_detection_count(workspace),
    }
