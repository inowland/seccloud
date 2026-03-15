# WS02 Hot Event Index Contract

Status: Planned

## Goal

Define the Postgres event index that lets the product retrieve and search event metadata without scanning the object store.

## Why This Follows WS01

The event index must point into the lake. It cannot be defined cleanly until the lake object and manifest contract is fixed.

## In Scope

- Postgres schema for the hot event index
- event-to-detection linkage contract
- object pointer storage strategy
- required query patterns for list, filter, detail hydration, and investigation timeline use cases
- retention expectations for index rows relative to raw and normalized storage

## Out Of Scope

- lake object layout changes
- entity alias model
- push ingestion API behavior
- collector checkpoint logic

## Inputs

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [projection_store.py](/Users/inowland/Development/seccloud/src/seccloud/projection_store.py)
- [api.py](/Users/inowland/Development/seccloud/src/seccloud/api.py)
- [investigation.py](/Users/inowland/Development/seccloud/src/seccloud/investigation.py)

## Deliverables

- schema proposal for the hot event index
- list of required indexed columns and query patterns
- detection-linkage contract showing how `detection_event` edges and evidence pointers work together
- migration notes from the current projection-only event list

## Required Decisions

- exact fields stored in Postgres versus hydrated from object storage
- whether the event index is a separate table family or part of the current projection store
- which search/filter operations are product-critical in v1
- how far event detail hydration should rely on normalized batches versus raw payload fallback

## Validation

- `/api/events` and event detail can be served without object-store scans
- detections can explain their linked events after replay and rebuild
- retention behavior is defined for raw, normalized, and indexed forms of an event

## Conflict Boundary

Later workstreams may implement this contract, but should not redefine indexed columns, pointer semantics, or linkage behavior.
