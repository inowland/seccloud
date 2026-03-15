# Data Plane Agent Workstreams

Status: Planned

## Purpose

This document splits the next data-plane work into agent-sized execution units with explicit sequencing and conflict boundaries.

## Recommendation

Use two phases:

1. Serial contract workstreams that freeze the architecture spine.
2. Parallel implementation workstreams that build against those frozen contracts.

Do not run the implementation workstreams in parallel until the serial contract workstreams are landed and reviewed.

## Phase 1: Serial Contract Workstreams

These workstreams define the interfaces that the rest of the data plane will depend on. They should be executed by one agent at a time, in this order.

1. [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
2. [WS02 Hot Event Index Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws02-hot-event-index-contract.md)
3. [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

Reason:

- `WS01` fixes the raw and normalized object layout, batch semantics, and replay model.
- `WS02` depends on `WS01` because the event index must store stable object pointers into the lake.
- `WS03` can be reasoned about earlier, but it should still land after `WS01` and `WS02` so event and entity identity are tied to actual storage and retrieval contracts, not abstractions.

## Phase 2: Parallel Implementation Workstreams

These workstreams can run in parallel once `WS01` through `WS03` are complete.

1. [WS04 Push Ingestion Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws04-push-ingestion-implementation.md)
2. [WS05 Collector And Checkpoint Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws05-collector-and-checkpoint-implementation.md)
3. [WS06 Projection And Index Materialization Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws06-projection-and-index-materialization.md)
4. [WS07 Operator And Runtime Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws07-operator-and-runtime-implementation.md)

Reason:

- `WS04` and `WS05` can converge on the same intake contract without touching each other's source-specific logic.
- `WS06` can focus on consuming the frozen lake and identity contracts to write Postgres materializations.
- `WS07` can package and supervise the worker roles without owning object layout or indexing decisions.

## Parallelization Rules

- `WS01`, `WS02`, and `WS03` are not parallelizable.
- `WS04`, `WS05`, `WS06`, and `WS07` are parallelizable after the phase-1 contracts are frozen.
- `WS04` and `WS05` may both touch worker orchestration code, so they should merge behind the shared intake contract, not invent separate paths.
- `WS06` must not redefine event pointer or identity semantics. It consumes them.
- `WS07` must not invent new processing roles or queue semantics. It packages the agreed ones.

## Merge Order

1. `WS01`
2. `WS02`
3. `WS03`
4. `WS06`
5. `WS04`
6. `WS05`
7. `WS07`

The recommended implementation merge order prefers the Postgres event index early because the product surface will need stable event retrieval before the first real customer integration feels complete.

## Shared Assumptions

- The product-target data plane remains `object storage + async queue + Postgres`.
- API-serving processes remain stateless and separate from worker roles.
- Raw telemetry remains short-lived.
- Normalized analytical artifacts, event indexes, detections, cases, and long-lived derived state remain customer-local.
- `UUIDv7` stays the surrogate ID format for platform-assigned IDs, with stable logical keys retained where replay and deduplication require them.

## Success Condition

At the end of these workstreams, the repo should support:

- batch-oriented raw landing into object storage
- manifest-driven replay and backfills
- a hot Postgres event index linked to detections
- push and pull ingestion converging on one intake contract
- explicit operator/runtime handling for separate worker processes
