# Normalized Event Schema v1

This document freezes the current normalized-event contract that the Rust M2
normalization worker must reproduce before we evolve the schema further.

The source of truth for the current behavior is:

- `src/seccloud/contracts.py`
- `src/seccloud/pipeline.py`
- `src/seccloud/storage.py`

## Scope

Schema v1 describes the event payload produced by the current Python normalization
path and persisted via `Workspace.write_normalized_event()` /
`Workspace.ensure_normalized_lake_artifacts()`.

This is a compatibility contract, not the final idealized end-state schema.

## Top-Level Fields

### `event_id: str`

Opaque event identifier allocated from the stable `event_key`.

- Current behavior: replay-stable within a workspace because `event_key ->
event_id` is stored in the identity manifest
- Requirement for parity: Rust must preserve the same allocation semantics even if
  the concrete ID generator differs internally

### `event_key: str`

Stable semantic key computed from:

- `source`
- `integration_id` or empty string
- `source_event_id`

This is the cross-run stable event identity.

### `integration_id: str | null`

Logical source integration partition. `null` is valid.

### `source: str`

Normalized source name. Current allowed values:

- `okta`
- `gworkspace`
- `github`
- `snowflake`

### `source_event_id: str`

Vendor-native event identifier after raw mapping.

### `observed_at: str`

UTC timestamp string in ISO-8601 form. This is event time, not ingestion time.

### `environment: dict[str, Any]`

Current required content:

- `source_kind`

Current source-kind mapping:

- `okta -> identity`
- `gworkspace -> document`
- `github -> repo`
- `snowflake -> dataset`

### `attributes: dict[str, Any]`

Extensible bag of source-specific fields not promoted into the principal, resource,
action, or top-level event fields.

Current behavior also leaves `received_at` in `attributes`.

### `evidence: EvidencePointer`

Pointer back to the raw event object used to produce the normalized event.

Required fields:

- `source`
- `object_key`
- `raw_event_id`
- `observed_at`

## Nested Objects

### `principal`

Required fields:

- `id: str`
- `entity_id: str`
- `entity_key: str`
- `kind: str`
- `provider: str`
- `email: str`
- `display_name: str`
- `department: str`
- `attributes: dict[str, Any]`

Current behavior:

- `id` is the raw `actor_email`
- `kind` is always `human`
- `provider` is the normalized source name
- `attributes.role` stores the raw `role`

`entity_key` is the stable principal identity key. `entity_id` is an opaque allocated
identifier derived from that key.

### `resource`

Required fields:

- `id: str`
- `entity_id: str`
- `entity_key: str`
- `kind: str`
- `provider: str`
- `name: str`
- `sensitivity: str`
- `attributes: dict[str, Any]`

Current behavior:

- `provider` is taken from the source mapping, not always equal to `source`
- `attributes` is currently empty for the normalized PoC path

### `action`

Required fields:

- `source: str`
- `verb: str`
- `category: str`

Current supported action taxonomy:

- `okta/login -> (login, authentication)`
- `okta/admin_change -> (admin_change, administration)`
- `gworkspace/view -> (read, access)`
- `gworkspace/share_external -> (share_external, sharing)`
- `github/view -> (read, access)`
- `github/clone -> (clone, code_access)`
- `github/archive_download -> (archive_download, code_access)`
- `snowflake/query -> (query, data_access)`
- `snowflake/export -> (export, data_access)`

## Validation Contract

The current raw validation contract requires these fields before normalization:

- `source`
- `source_event_id`
- `observed_at`
- `actor_email`
- `actor_name`
- `department`
- `role`
- `event_type`
- `resource_id`
- `resource_name`
- `resource_kind`
- `sensitivity`

Current validation failures used by downstream code:

- `missing_required_fields:<csv>`
- `unsupported_source`
- `unsupported_event_type`

## Stability Notes

- `event_key` and `entity_key` are the stable identities to preserve across
  implementations.
- `event_id` and `entity_id` are intentionally opaque. In parity fixtures we
  canonicalize them as placeholders derived from the stable keys so the fixtures do
  not depend on a specific UUID implementation.
- Any Rust implementation should first match this schema exactly before adding new
  fields or changing the physical Parquet layout.
