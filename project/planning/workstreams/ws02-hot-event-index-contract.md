# WS02 Hot Event Index Contract

Status: Done

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

- authoritative contract spec: [hot-event-index-contract.md](/Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md)
- decision ADR: [0005-hot-event-index-contract.md](/Users/inowland/Development/seccloud/project/adr/0005-hot-event-index-contract.md)
- required indexed columns and query patterns in the spec
- detection-linkage contract showing how `detection_event_edge` rows and evidence pointers work together
- migration notes from the current projection-only event list in the spec

## Required Decisions

- exact fields stored in Postgres versus hydrated from object storage
- whether the event index is a separate table family or part of the current projection store
- which search/filter operations are product-critical in v1
- how far event detail hydration should rely on normalized batches versus raw payload fallback

## Validation

- `/api/events` and event detail can be served without object-store scans
- detections can explain their linked events after replay and rebuild
- retention behavior is defined for raw, normalized, and indexed forms of an event

## Outcome

This workstream freezes the following v1 decisions:

- the product-target event index is a separate Postgres table family, not the current `projected_events` demo table
- `hot_event_index` stores both query columns and canonical normalized `event_payload` JSON for event list and detail reads
- `detection_event_edge` is the authoritative relational join surface for ordered detection-linked events
- event rows store retained normalized pointers and optional raw pointers using the WS01 pointer envelope
- raw retention may expire raw pointers without invalidating indexed event lookup or detection linkage

## Conflict Boundary

Later workstreams may implement this contract, but should not redefine indexed columns, pointer semantics, or linkage behavior.
