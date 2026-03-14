# M01 Implementation Plan

Status: Planned

## Objective And Linked Requirements
- Implement the substrate defined in [m01.md](/Users/inowland/Development/seccloud/.project/spec/m01.md).
- Preserve the retention and evidence contract defined in [product-contract.md](/Users/inowland/Development/seccloud/.project/spec/product-contract.md).
- Lock the active PoC source pack to `Okta`, `Google Workspace`, `GitHub`, and `Snowflake`.

## Exact Implementation Steps
1. Define the versioned canonical event schema and evidence pointer contract.
2. Implement source adapter scaffolds for `Okta`, `Google Workspace`, `GitHub`, and `Snowflake`.
3. Build append-only raw event storage in object storage with immutable evidence references.
4. Build manifest and catalog metadata supporting idempotency, replay, and retention.
5. Implement derived-state materialization jobs for the first required state categories.
6. Implement retention enforcement that deletes raw data while preserving allowed derived state.
7. Build replay and rebuild workflows and document operational expectations.

## Dependency Order
1. Canonical schema before adapters.
2. Adapters before replay validation.
3. Raw storage and metadata before retention enforcement.
4. Derived-state materialization before rebuild testing.

## Requirement To Test To Evidence

| Requirement | Test Or Validation | Evidence |
| --- | --- | --- |
| Canonical event schema is stable and versioned | Schema validation tests and fixture replay | Schema test output and fixtures |
| Supported sources ingest correctly | Adapter correctness tests with source fixtures | Adapter test report |
| Replay is idempotent | Duplicate and late-event replay tests | Replay validation report |
| Raw retention preserves valid derived state | Retention deletion and rebuild tests | Retention and rebuild report |

## Required Tests
- schema and adapter correctness
- replay and idempotency
- retention deletion and rebuildability
- deployment and bootstrap validation for the substrate services

## Required Evidence Artifacts
- schema fixtures
- adapter fixture reports
- replay validation report
- retention deletion report
- derived-state rebuild report
- synthetic source-pack fixture manifest

## Open Risks
- source schemas may drift quickly
- evidence-pointer design may be insufficient for later case workflows
- retention enforcement may expose hidden dependencies on raw data

## Temporary Architecture And Retirement Gate
- Temporary element: a narrow object-store-native substrate optimized for the approved v1 pack
- Replacement: broader reusable substrate only if validated by later milestones
- Retirement gate: revisit after `m02` and `m03` prove workflow and scoring value
