# M01 Implementation Plan

Status: Planned

## Objective And Linked Requirements

- Implement the substrate defined in [m01.md](/Users/inowland/Development/seccloud/project/spec/m01.md).
- Preserve the retention and evidence contract defined in [product-contract.md](/Users/inowland/Development/seccloud/project/spec/product-contract.md).
- Lock the active PoC source pack to `Okta`, `Google Workspace`, `GitHub`, and `Snowflake`.

## Exact Implementation Steps

1. Define the raw intake envelope, versioned canonical event schema, and evidence pointer contract shared by both pull and push ingestion.
2. Split the runtime into explicit roles: stateless API service, scheduled collector workers, push ingestion gateway, normalization workers, and detection/materialization workers.
3. Implement source adapter scaffolds for `Okta`, `Google Workspace`, `GitHub`, and `Snowflake` with source-specific checkpoints.
4. Build append-only raw event landing storage in object storage with immutable evidence references.
5. Add asynchronous queue semantics for normalization, replay, dead-letter handling, and downstream materialization work.
6. Build manifest and catalog metadata supporting idempotency, replay, retention, and worker checkpoints.
7. Materialize normalized analytical event batches back to object storage in a columnar format and maintain a hot event index for event lookup and detection linkage.
8. Persist detections, cases, projection state, manifests, and checkpoints in `Postgres`.
9. Implement derived-state and detection materialization jobs for the first required state categories.
10. Implement retention enforcement that deletes raw data while preserving allowed retained analytical artifacts, derived state, and case evidence.
11. Build replay and rebuild workflows and document operational expectations.

## Dependency Order

1. Intake and canonical schemas before worker separation.
2. Worker separation before source adapters and ingestion gateway rollout.
3. Raw storage and queue semantics before replay validation.
4. Event indexing and normalized batch storage before investigation-linkage validation.
5. Derived-state materialization before rebuild testing.

## Agent Workstream Split

The remaining data-plane work should be executed as explicit agent workstreams with frozen dependency boundaries.

Serial contract phase:

1. [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
2. [WS02 Hot Event Index Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws02-hot-event-index-contract.md)
3. [WS03 Identity Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws03-identity-contract.md)

Parallel implementation phase after those contracts land:

1. [WS04 Push Ingestion Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws04-push-ingestion-implementation.md)
2. [WS05 Collector And Checkpoint Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws05-collector-and-checkpoint-implementation.md)
3. [WS06 Projection And Index Materialization Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws06-projection-and-index-materialization.md)
4. [WS07 Operator And Runtime Implementation](/Users/inowland/Development/seccloud/project/planning/workstreams/ws07-operator-and-runtime-implementation.md)

See the umbrella sequencing note in [data-plane-agent-workstreams.md](/Users/inowland/Development/seccloud/project/planning/data-plane-agent-workstreams.md).

## Requirement To Test To Evidence

| Requirement                                             | Test Or Validation                                                          | Evidence                           |
| ------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------- |
| Canonical event schema is stable and versioned          | Schema validation tests and fixture replay                                  | Schema test output and fixtures    |
| Supported sources ingest correctly                      | Adapter correctness tests with source fixtures                              | Adapter test report                |
| Pull and push ingestion converge on one intake contract | Gateway and collector integration tests feeding the same normalization path | Intake convergence report          |
| API is decoupled from processing                        | Validation that read APIs succeed while workers are paused or restarted     | Service-boundary validation report |
| Replay is idempotent                                    | Duplicate and late-event replay tests                                       | Replay validation report           |
| Raw retention preserves valid derived state             | Retention deletion and rebuild tests                                        | Retention and rebuild report       |
| Detections remain linked to retained events             | Investigation and evidence tests after raw-retention enforcement            | Detection linkage report           |

## Required Tests

- schema and adapter correctness
- ingestion gateway and collector convergence
- service-boundary validation between API and worker roles
- replay and idempotency
- queue retry and dead-letter handling
- retained-event linkage for investigations
- retention deletion and rebuildability
- deployment and bootstrap validation for the substrate services

## Required Evidence Artifacts

- schema fixtures
- adapter fixture reports
- intake convergence report
- service-boundary validation report
- replay validation report
- detection linkage report
- retention deletion report
- derived-state rebuild report
- synthetic source-pack fixture manifest

## Open Risks

- source schemas may drift quickly
- retained normalized event storage may grow quickly without clear compaction and retention rules
- evidence-pointer and event-index design may be insufficient for later case workflows
- queue semantics may be implemented too narrowly for backfill and replay pressure
- retention enforcement may expose hidden dependencies on raw data

## Temporary Architecture And Retirement Gate

- Temporary element: a narrow AWS-first data plane using object storage, an asynchronous queue, and a hot `Postgres` index optimized for the approved v1 pack
- Replacement: broader reusable substrate only if validated by later milestones or if customer search requirements force a dedicated analytics engine
- Retirement gate: revisit after `m02` and `m03` prove workflow and scoring value and after the first real customer integration validates the ingestion split
