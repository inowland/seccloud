# WS01 Lake Layout And Manifest Contract

Status: Planned

## Goal

Define the authoritative object-store layout and manifest model for raw and normalized event storage.

## Why This Must Go First

This workstream sets the storage and replay contracts that every later ingestion, indexing, and backfill path will depend on.

## In Scope

- raw batch object layout
- normalized batch object layout
- partitioning rules by tenant, source, integration, and time
- manifest schema for raw and normalized batches
- object pointer contract used by evidence and indexes
- replay and backfill semantics
- compaction and retention assumptions for the v1 substrate

## Out Of Scope

- authenticated ingestion API details
- collector adapter implementation
- Postgres index schema
- UI changes

## Inputs

- [product-contract.md](/Users/inowland/Development/seccloud/project/spec/product-contract.md)
- [m01.md](/Users/inowland/Development/seccloud/project/spec/m01.md)
- [0003-worker-separated-data-plane.md](/Users/inowland/Development/seccloud/project/adr/0003-worker-separated-data-plane.md)
- [object_store.py](/Users/inowland/Development/seccloud/src/seccloud/object_store.py)
- [storage.py](/Users/inowland/Development/seccloud/src/seccloud/storage.py)

## Deliverables

- an ADR or spec patch defining the lake layout
- a manifest schema document with versioning rules
- explicit examples of raw object keys, normalized object keys, and manifest entries
- implementation-plan notes listing migration steps from the current per-object JSON simulation

## Required Decisions

- whether raw landing stays JSON or moves immediately to compressed batch blobs
- whether normalized v1 is JSON batches first or Parquet immediately
- how an object pointer identifies object, batch, and row within a batch
- which manifest fields are required for replay, dedupe, retention, and checkpoints

## Validation

- sample backfill flow can be explained without ambiguity
- sample detection-to-event lookup can be explained without ambiguity
- raw-retention behavior can be described without breaking case evidence

## Conflict Boundary

No other workstream should redefine object key layout, partitioning, manifest shape, or pointer semantics once this lands.
