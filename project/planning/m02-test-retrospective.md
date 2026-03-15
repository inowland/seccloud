# M02 Test Retrospective

Status: In Progress

## Failure Or Gap

- The investigation workflow is only exposed through an internal CLI and preserved case snapshots, not a polished analyst surface or external immutable evidence system.

## Root Cause

- The PoC prioritized stable investigation contracts and retention-safe evidence preservation over UI work and external system integrations.

## Fix Applied Or Planned

- Implemented evidence bundle lookup, timelines, cases, disposition updates, and feedback capture through the CLI.
- Preserved case snapshots so evidence remains available after raw-retention deletion in the PoC.

## Durable Lesson

- For early customer conversations, a thin CLI is sufficient if the evidence contract is strong and the workflow outputs are inspectable.

## Spec Or Plan Updates Triggered

- Patched the privileged behavior scenario pack and internal API or CLI PoC surface into `m02`.
