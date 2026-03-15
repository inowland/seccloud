# WS04 Push Ingestion Implementation

Status: Planned

## Goal

Implement the production-shaped push ingestion path against the frozen lake, intake, and identity contracts.

## Blocked By

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

## In Scope

- authenticated push ingestion endpoint
- compression and batch-size handling
- intake validation and tenant attribution
- idempotency and request-level dedupe
- writing raw landing batches and queue work against the shared intake contract

## Out Of Scope

- collector adapters
- Postgres event index materialization
- runtime supervision and deployment scripts

## Inputs

- [api.py](/Users/inowland/Development/seccloud/src/seccloud/api.py)
- [workers.py](/Users/inowland/Development/seccloud/src/seccloud/workers.py)
- [storage.py](/Users/inowland/Development/seccloud/src/seccloud/storage.py)
- [contracts.py](/Users/inowland/Development/seccloud/src/seccloud/contracts.py)

## Deliverables

- endpoint and contract updates
- auth and request-shaping rules
- tests for batched compressed submission, validation failure, and idempotent replay
- documentation for customer-side ingestion assumptions

## Validation

- push ingestion lands only raw batches and never performs processing inline
- duplicate submissions do not create duplicate event semantics
- tenant attribution and integration attribution are preserved into downstream processing

## May Run In Parallel With

- [WS05 Collector And Checkpoint Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws05-collector-and-checkpoint-implementation.md)
- [WS06 Projection And Index Materialization Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws06-projection-and-index-materialization.md)
- [WS07 Operator And Runtime Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws07-operator-and-runtime-implementation.md)

## Conflict Boundary

This workstream consumes the shared intake and lake contracts. It must not redefine them.
