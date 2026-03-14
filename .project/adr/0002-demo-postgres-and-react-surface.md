# ADR 0002: Demo Postgres Projection And React Surface

Status: Accepted
Owner: Codex
Last Updated: 2026-03-14
Related Milestone: m01, m02
Related Spec: /Users/inowland/Development/seccloud/.project/spec/product-contract.md
Retirement Gate: Replace when the PoC transitions from a local demo environment to a customer-local service stack with durable app-facing stores and a supported analyst product surface.

## Title
Use a workspace-local Postgres projection store, FastAPI demo service, and React frontend to exercise the analyst workflow against streamed PoC state.

## Context
The repo had reached the point where CLI-only workflows were no longer sufficient to answer productization questions. The next risk was not whether detections could be generated, but whether the system could be made legible as a live product surface with evolving state, projected data, and a repeatable analyst-facing demo.

The existing filesystem-backed substrate remains the source of truth for the PoC, but a demo-friendly read model and web surface are now needed to simulate a more realistic product environment.

## Decision
Implement a temporary hybrid demo architecture with:
- the existing Python ingest and scoring engine remaining the write path
- a workspace-local Postgres instance used as a projected read model for demo views
- a FastAPI service exposing the projected demo state
- a React frontend consuming the API and rendering events, detections, cases, source capability, and stream controls
- a stream simulator that advances raw events in batches so the UI can show state changes over time

## Consequences
- Positive: the team can evaluate a more realistic analyst-facing flow without replacing the existing PoC engine.
- Positive: the demo now exercises projected data, incremental state changes, and a real web UI.
- Negative: the Postgres and React layers are demo architecture, not yet the supported BYOC product deployment model.
- Negative: there is now a temporary dual-store setup: filesystem-backed source-of-truth state and Postgres-backed demo projections.

## Spec Links
- `/Users/inowland/Development/seccloud/.project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/.project/spec/m01.md`
- `/Users/inowland/Development/seccloud/.project/spec/m02.md`
