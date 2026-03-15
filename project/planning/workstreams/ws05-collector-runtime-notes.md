# WS05 Collector Runtime Notes

This note documents the current local-runtime shape for the pull collector path. It does not redefine the frozen lake or identity contracts.

## Checkpoint Persistence

- Local collector checkpoints are persisted in `manifests/collector_checkpoints.json`.
- The file stores one logical checkpoint record per `(tenant_id, collector_kind, source, integration_id)`.
- Each record keeps:
  - stable collector identity fields
  - the last committed opaque `checkpoint` object
  - `last_attempted_checkpoint`
  - last run status and error
  - attempt and success counters
  - last submitted batch and idempotency key

## Shared Intake Convergence

- Pull collectors submit raw batches through the same `submit_raw_events(...)` path used by push ingestion.
- Collector batches land under the same `lake/raw/...` and `lake/manifests/...` contracts.
- The raw manifest `checkpoint` payload carries collector-specific cursor metadata while staying inside the existing opaque checkpoint field from the lake contract.

## Initial Adapter Scaffold

- The initial adapter is a fixture-backed Okta System Log collector scaffold.
- It models paginated pull behavior with an opaque checkpoint object whose primary cursor field is `fixture_offset`.
- Later source adapters should preserve the same pattern:
  - read from the stored opaque checkpoint
  - build a stable request-level idempotency key for the logical page
  - submit through the shared intake path
  - advance the persisted checkpoint only after successful submission
