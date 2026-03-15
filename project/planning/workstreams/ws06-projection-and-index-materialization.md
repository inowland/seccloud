# WS06 Projection And Index Materialization Implementation

Status: Planned

## Goal

Implement the worker-owned materialization path for the Postgres hot event index and related read models.

## Blocked By

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [WS02 Hot Event Index Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws02-hot-event-index-contract.md)
- [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

## In Scope

- event index tables and projector logic
- detection-to-event linkage materialization
- migration of event list/detail reads toward the hot index and object pointers
- alignment between source stats projection and event/read-model projection

## Out Of Scope

- raw lake layout redesign
- ingestion auth
- collector adapter behavior
- runtime supervision

## Inputs

- [projection_store.py](/Users/inowland/Development/seccloud/src/seccloud/projection_store.py)
- [workers.py](/Users/inowland/Development/seccloud/src/seccloud/workers.py)
- [stats_projector.py](/Users/inowland/Development/seccloud/src/seccloud/stats_projector.py)
- [api.py](/Users/inowland/Development/seccloud/src/seccloud/api.py)

## Deliverables

- Postgres schema and projector implementation
- event list and detail reads backed by the hot index
- tests for replay, rebuild, and retained event linkage
- migration notes from the current full-sync projection approach

## Validation

- `/api/events` is served from Postgres-backed indexed metadata
- event detail hydration uses stable object pointers rather than workspace scans
- replay and rebuild preserve detection-to-event linkage

## May Run In Parallel With

- [WS04 Push Ingestion Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws04-push-ingestion-implementation.md)
- [WS05 Collector And Checkpoint Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws05-collector-and-checkpoint-implementation.md)
- [WS07 Operator And Runtime Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws07-operator-and-runtime-implementation.md)

## Conflict Boundary

This workstream consumes the storage and identity contracts. It must not redefine object layout, manifest semantics, or ID rules.
