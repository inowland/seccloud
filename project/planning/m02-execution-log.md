# M02 Execution Log

Status: In Progress

## Date

- 2026-03-14

## Work Actually Completed

- Patched the privileged behavior scenario pack into the product contract and milestone specs.
- Implemented a thin internal CLI for seeded scenario evaluation, evidence lookup, timeline retrieval, case creation, disposition updates, feedback capture, retention, and founder artifact export.
- Implemented evidence bundle construction and entity timelines using normalized events and retained case artifacts.
- Implemented founder and design-partner artifacts derived from the same PoC contracts: source inventory, retention matrix, deployment boundary, scenario outputs, evaluation summary, and conversation pack.
- Added investigation peer-comparison and case-summary surfaces to make detections more explainable during founder and design-partner walkthroughs.
- Added a temporary hybrid demo surface: incremental stream controls, a Postgres-backed projection layer, a FastAPI demo service, and a React frontend that renders projected events, detections, cases, and source capability state.
- Restructured the demo UI around open investigations, detection queue, and clickable detail panes, and expanded the demo stream so the UI now advances through a materially larger event set.
- Changed the demo semantics so detections no longer auto-create cases, and related detections for the same principal can now group into a single analyst investigation.

## Deviations From Plan

- Built the first usable web demo only after the CLI and case contracts were stable enough to project into a read model. The demo UI remains temporary and is tracked in ADR 0002.

## Evidence Produced

- `/Users/inowland/Development/seccloud/src/seccloud/cli.py`
- `/Users/inowland/Development/seccloud/src/seccloud/demo_stream.py`
- `/Users/inowland/Development/seccloud/src/seccloud/demo_projection.py`
- `/Users/inowland/Development/seccloud/src/seccloud/demo_postgres.py`
- `/Users/inowland/Development/seccloud/src/seccloud/demo_api.py`
- `/Users/inowland/Development/seccloud/src/seccloud/investigation.py`
- `/Users/inowland/Development/seccloud/src/seccloud/reports.py`
- `/Users/inowland/Development/seccloud/web`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/cases`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/founder_artifacts`

## Blockers And Follow-Ups

- The current demo UI is temporary and local-environment-oriented; it is not yet the supported analyst product surface.
- Investigation workflows have only been exercised on synthetic and fixture-based scenarios.
- The demo now has detail views and lightweight case actions, but it still lacks richer multi-detection case grouping and deeper analyst workflows.
