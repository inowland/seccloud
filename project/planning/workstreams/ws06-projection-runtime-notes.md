# WS06 Projection Runtime Notes

This note documents the current local-runtime materialization shape for the hot event index. It does not redefine the frozen lake, hot-index, or identity contracts.

## Materialized Tables

- `hot_event_index` stores one row per retained normalized event with:
  - flattened query columns
  - compatibility `event_payload`
  - `normalized_pointer`
  - `raw_pointer`
  - `raw_retention_state`
- `detection_event_edge` stores ordered `(tenant_id, detection_id, event_id)` linkage.

## Read Path Migration

- `/api/events` now reads from `hot_event_index`.
- `/api/events/{event_id}` resolves through `hot_event_index` when a projection DSN is available.
- Detection detail event hydration resolves through `detection_event_edge -> hot_event_index` when a projection DSN is available.
- Workspace-scan fallback remains only as a compatibility path when the Postgres projection has not yet been materialized.

## Local Runtime Adapter

- The current local runtime still keeps legacy normalized JSON objects for compatibility.
- To satisfy the hot-index pointer contract, the runtime also emits deterministic local normalized-lake artifacts and manifests under `lake/normalized/...` and `lake/manifests/...`.
- The local artifact payload is an adapter for development and replay tests; the product-target contract remains the frozen lake spec.
