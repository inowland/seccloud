# WS07 Operator And Runtime Implementation

Status: Planned

## Goal

Package the separated worker roles into a usable local and production-shaped operator/runtime experience.

## Blocked By

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [WS02 Hot Event Index Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws02-hot-event-index-contract.md)
- [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

## In Scope

- worker service packaging and startup flow
- local supervisor or dev-run scripts
- operator-only health and state inspection
- clear separation between product API surface and operator controls

## Out Of Scope

- object layout redesign
- event index schema design
- source adapter logic
- customer ingestion auth design

## Inputs

- [workers.py](/Users/inowland/Development/seccloud/src/seccloud/workers.py)
- [cli.py](/Users/inowland/Development/seccloud/src/seccloud/cli.py)
- [README.md](/Users/inowland/Development/seccloud/README.md)

## Deliverables

- documented local run flow for API, worker service, and dependencies
- improved operator commands and health surfaces
- optional supervisor/dev helper scripts
- tests or verification notes for worker loop behavior and recovery paths

## Validation

- the demo and local dev flow do not rely on the browser triggering worker execution
- workers can be started, stopped, and observed independently of the API
- operator-only controls remain separate from the product API path

## May Run In Parallel With

- [WS04 Push Ingestion Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws04-push-ingestion-implementation.md)
- [WS05 Collector And Checkpoint Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws05-collector-and-checkpoint-implementation.md)
- [WS06 Projection And Index Materialization Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws06-projection-and-index-materialization.md)

## Conflict Boundary

This workstream packages the agreed architecture. It should not invent new product-path APIs, queue semantics, or storage contracts.
