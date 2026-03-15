# WS05 Collector And Checkpoint Implementation

Status: Planned

## Goal

Implement the production-shaped pull collector model with explicit checkpoints and convergence onto the shared intake path.

## Blocked By

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

## In Scope

- collector job contract
- checkpoint persistence model
- retry and incremental sync semantics
- one initial source adapter skeleton using the shared intake path

## Out Of Scope

- push ingestion auth
- Postgres event index materialization
- operator runtime packaging

## Inputs

- [workers.py](/Users/inowland/Development/seccloud/src/seccloud/workers.py)
- [storage.py](/Users/inowland/Development/seccloud/src/seccloud/storage.py)
- [onboarding.py](/Users/inowland/Development/seccloud/src/seccloud/onboarding.py)
- [vendor_exports.py](/Users/inowland/Development/seccloud/src/seccloud/vendor_exports.py)

## Deliverables

- checkpoint schema and storage design
- one collector adapter scaffold
- tests for checkpoint resume, retry, and convergence with push intake
- notes on how later source adapters should fit the same contract

## Validation

- collector output lands on the same intake path used by push ingestion
- checkpoint replay is idempotent
- failed collector runs do not corrupt checkpoint state

## May Run In Parallel With

- [WS04 Push Ingestion Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws04-push-ingestion-implementation.md)
- [WS06 Projection And Index Materialization Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws06-projection-and-index-materialization.md)
- [WS07 Operator And Runtime Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws07-operator-and-runtime-implementation.md)

## Conflict Boundary

This workstream must reuse the shared intake contract and should not introduce a collector-only raw landing path.
