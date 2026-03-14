# M01 Test Retrospective

Status: In Progress

## Failure Or Gap
- The substrate is validated only against synthetic data and a filesystem-backed runtime.

## Root Cause
- The PoC was intentionally optimized for local reproducibility and contract validation before cloud deployment and real customer fixtures.

## Fix Applied Or Planned
- Added ADR 0001 to record the filesystem-backed substrate decision.
- Kept object-store, replay, retention, and derived-state interfaces portable so real cloud adapters can replace the local runtime later.

## Durable Lesson
- The substrate can be de-risked locally, but source drift and retention posture are not credibly validated until real customer fixtures are introduced.

## Spec Or Plan Updates Triggered
- Patched the active PoC source pack into the product contract and `m01`.
