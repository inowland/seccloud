# Hot Event Index Contract

## Purpose

This document defines the authoritative v1 Postgres hot event index contract that serves event list, event detail, and detection-linked event lookup without scanning the object store.

This contract consumes the frozen lake layout and object-pointer semantics from `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`. It does not redefine object layout, manifest shape, or ID rules.

## Scope

In scope:

- Postgres schema for indexed event metadata and event detail payloads
- detection-to-event linkage schema
- query patterns required by `/api/events`, event detail, and investigation timelines
- retention and rebuild rules for hot index rows
- migration guidance from the current projection-only event list

Out of scope:

- lake object layout changes
- normalized schema redesign
- push-ingestion behavior
- collector checkpoint logic
- identity alias resolution

## Design Constraints

- The hot event index is a rebuildable read-optimized projection, not the system of record.
- The retained normalized lake object remains the source of truth for replay and rebuild.
- Event list and event detail must not require object-store scans in the steady state.
- Detection linkage must survive replay and raw-retention windows as long as the normalized event is retained.

## Table Family

The v1 contract uses a separate Postgres table family, not the current `projected_events` payload table in `src/seccloud/projection_store.py`.

Required tables:

- `hot_event_index`
- `detection_event_edge`

Existing projection-state or detection read-model tables may continue temporarily during migration, but they are not the authoritative event-index contract.

## `hot_event_index` Schema

`hot_event_index` stores one row per normalized event retained in the hot query window.

Required columns:

- `tenant_id text not null`
- `event_id text primary key`
- `event_key text not null`
- `normalized_schema_version text not null`
- `source text not null`
- `integration_id text null`
- `source_event_id text not null`
- `observed_at timestamptz not null`
- `principal_entity_id text not null`
- `principal_id text not null`
- `principal_display_name text not null`
- `principal_email text not null`
- `principal_department text not null`
- `resource_entity_id text not null`
- `resource_id text not null`
- `resource_name text not null`
- `resource_kind text not null`
- `resource_sensitivity text not null`
- `action_verb text not null`
- `action_category text not null`
- `environment jsonb not null`
- `attributes jsonb not null`
- `event_payload jsonb not null`
- `normalized_pointer jsonb not null`
- `raw_pointer jsonb null`
- `raw_retention_state text not null`
- `indexed_at timestamptz not null`
- `updated_at timestamptz not null`

Required semantic rules:

- `event_id` and `event_key` use the identity contract defined in `/Users/inowland/Development/seccloud/project/spec/identity-contract.md` and are not redefined here.
- `principal_entity_id` and `resource_entity_id` are the canonical query and join keys for principals and resources in the hot event index.
- `principal_id` and `resource_id` are retained only as source-local compatibility aliases copied from the current event payload shape; they are not the canonical join surface for long-lived reads.
- `event_payload` stores the compatibility event document served by the current API model in `/Users/inowland/Development/seccloud/src/seccloud/api_models.py`.
- `event_payload` is intentionally compatibility-shaped for the current API transition period; it is not the place where the full WS01 object-pointer envelope is stored.
- `normalized_pointer` stores the full `ObjectPointer` envelope from `/Users/inowland/Development/seccloud/project/spec/lake-contract.md` and must point to the retained normalized batch row for this event.
- `raw_pointer`, when present, stores the corresponding raw `ObjectPointer` envelope for source-fidelity lookups. It may outlive the raw object only as a historical reference.
- `raw_retention_state` is one of `available`, `expired`, or `unknown`.
- `environment` and `attributes` duplicate the normalized event fields for targeted reads without unpacking `event_payload`.

Example row shape:

```json
{
  "tenant_id": "acme-prod",
  "event_id": "evt_01hq9rcgmqf6j8ncm33pa40m3j",
  "event_key": "evk_5f0d78ce7f0b6a5d4c8a63f1",
  "normalized_schema_version": "event.v1",
  "source": "okta",
  "integration_id": "okta-primary",
  "source_event_id": "00u9example",
  "observed_at": "2026-03-15T14:04:21Z",
  "principal_id": "usr_okta_alice",
  "principal_entity_id": "ent_01hq9r9ys5cb6ww1n4pfmxf9z9",
  "principal_display_name": "Alice Wong",
  "principal_email": "alice@acme.example",
  "principal_department": "engineering",
  "resource_id": "app_okta_admin_console",
  "resource_entity_id": "ent_01hq9ra5s2zdr6ctmt90b6h4x1",
  "resource_name": "Okta Admin Console",
  "resource_kind": "app",
  "resource_sensitivity": "high",
  "action_verb": "admin-change",
  "action_category": "admin",
  "environment": {
    "source_kind": "identity_provider"
  },
  "attributes": {
    "ip_address": "203.0.113.14"
  },
  "event_payload": {
    "event_id": "evt_01hq9rcgmqf6j8ncm33pa40m3j",
    "event_key": "evk_5f0d78ce7f0b6a5d4c8a63f1",
    "integration_id": "okta-primary",
    "source": "okta",
    "source_event_id": "00u9example",
    "principal": {
      "id": "usr_okta_alice",
      "entity_id": "ent_01hq9r9ys5cb6ww1n4pfmxf9z9",
      "entity_key": "enk_91a7c3f07c99d6af6f2b2d88",
      "kind": "human",
      "provider": "okta",
      "email": "alice@acme.example",
      "display_name": "Alice Wong",
      "department": "engineering",
      "attributes": {}
    },
    "resource": {
      "id": "app_okta_admin_console",
      "entity_id": "ent_01hq9ra5s2zdr6ctmt90b6h4x1",
      "entity_key": "enk_3ac1640d7f4b527db4216ad2",
      "kind": "app",
      "provider": "okta",
      "name": "Okta Admin Console",
      "sensitivity": "high",
      "attributes": {}
    },
    "action": {
      "source": "okta",
      "verb": "admin-change",
      "category": "admin"
    },
    "observed_at": "2026-03-15T14:04:21Z",
    "environment": {
      "source_kind": "identity_provider"
    },
    "attributes": {
      "ip_address": "203.0.113.14"
    },
    "evidence": {
      "source": "okta",
      "object_key": "lake/raw/layout=v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f/part-00000.jsonl.gz",
      "raw_event_id": "00u9example",
      "observed_at": "2026-03-15T14:04:21Z"
    }
  },
  "normalized_pointer": {
    "pointer_version": 1,
    "object_key": "lake/normalized/layout=v1/schema=event.v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=norm_01hq9reb0p1d4t6msw3j0s6pk4/part-00000.parquet",
    "object_format": "parquet",
    "sha256": "sha256:57c603b33baf0d165c8cf2461ecf2fb0fd4a6a54a3f7bde6498ae36d1dbfd51a",
    "manifest_key": "lake/manifests/layout=v1/type=normalized/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=norm_01hq9reb0p1d4t6msw3j0s6pk4.json",
    "record_locator": {
      "ordinal": 417,
      "row_group": 0
    },
    "retention_class": "normalized_retained"
  },
  "raw_pointer": {
    "pointer_version": 1,
    "object_key": "lake/raw/layout=v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f/part-00000.jsonl.gz",
    "object_format": "jsonl.gz",
    "sha256": "sha256:7fc1f1b0cf0f698aa91d55f12389f7b12908c648e5f2cd2254a9fe0f3416f88a",
    "manifest_key": "lake/manifests/layout=v1/type=raw/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f.json",
    "record_locator": {
      "ordinal": 417
    },
    "retention_class": "raw_hot"
  },
  "raw_retention_state": "available",
  "indexed_at": "2026-03-15T14:08:41Z",
  "updated_at": "2026-03-15T14:08:41Z"
}
```

## `detection_event_edge` Schema

`detection_event_edge` materializes the many-to-many linkage between detections and indexed events.

Required columns:

- `tenant_id text not null`
- `detection_id text not null`
- `event_id text not null`
- `ordinal integer not null`
- `link_role text not null`
- `observed_at timestamptz not null`
- `created_at timestamptz not null`

Required semantic rules:

- Primary key: `(tenant_id, detection_id, event_id)`
- Unique constraint: `(tenant_id, detection_id, ordinal)`
- Foreign key: `(event_id)` must reference `hot_event_index(event_id)`
- `ordinal` preserves the order of `detection.event_ids`
- `ordinal = 0` must identify the anchor event for the detection
- `link_role` is `anchor` for ordinal `0`; later edges use `supporting` in v1 unless a later version adds finer semantics

Example row shape:

```json
{
  "tenant_id": "acme-prod",
  "detection_id": "det_01hq9rj0tpxffxg97zh2j5a4q8",
  "event_id": "evt_01hq9rcgmqf6j8ncm33pa40m3j",
  "ordinal": 0,
  "link_role": "anchor",
  "observed_at": "2026-03-15T14:04:21Z",
  "created_at": "2026-03-15T14:10:12Z"
}
```

## Required Query Patterns

The hot event index must support the following product-critical queries in v1:

1. Recent events list for `/api/events`
   - Filters: `tenant_id`
   - Optional filters: `source`, `integration_id`, `principal_entity_id`, `resource_entity_id`, `action_category`, `action_verb`, `observed_at >=`, `observed_at <=`
   - Order: `observed_at desc, event_id desc`

2. Event detail for `/api/events/{event_id}`
   - Lookup: `tenant_id, event_id`
   - Result: `event_payload`, `normalized_pointer`, `raw_pointer`, `raw_retention_state`

3. Detection detail event hydration
   - Join `detection_event_edge` to `hot_event_index`
   - Filter: `tenant_id, detection_id`
   - Order: `ordinal asc`

4. Investigation timeline by principal
   - Filters: `tenant_id, principal_entity_id`
   - Optional filters: `resource_entity_id`, `observed_at >=`, `observed_at <=`
   - Order: `observed_at desc, event_id desc`

5. Investigation timeline by resource
   - Filters: `tenant_id, resource_entity_id`
   - Optional filters: `principal_entity_id`, `observed_at >=`, `observed_at <=`
   - Order: `observed_at desc, event_id desc`

6. Reverse lookup from event to linked detections
   - Join `detection_event_edge`
   - Filter: `tenant_id, event_id`
   - Order: `observed_at desc, detection_id desc`

Full-text search, arbitrary JSON-attribute search, and SIEM-style faceting are out of scope for v1.

## Required Indexes

`hot_event_index`:

- primary key on `event_id`
- unique index on `(tenant_id, event_key)`
- btree index on `(tenant_id, observed_at desc, event_id desc)`
- btree index on `(tenant_id, source, observed_at desc, event_id desc)`
- btree index on `(tenant_id, integration_id, observed_at desc, event_id desc)`
- btree index on `(tenant_id, principal_entity_id, observed_at desc, event_id desc)`
- btree index on `(tenant_id, resource_entity_id, observed_at desc, event_id desc)`
- btree index on `(tenant_id, action_category, observed_at desc, event_id desc)`
- btree index on `(tenant_id, action_verb, observed_at desc, event_id desc)`

Compatibility alias indexes on `principal_id` or `resource_id` are optional implementation details during migration and are not part of the frozen v1 query contract.

`detection_event_edge`:

- primary key on `(tenant_id, detection_id, event_id)`
- unique index on `(tenant_id, detection_id, ordinal)`
- btree index on `(tenant_id, event_id, observed_at desc, detection_id desc)`

## Detection-To-Event Linkage Contract

The detection linkage contract is intentionally split across relational edges and immutable object pointers:

- `detection_event_edge` answers which normalized indexed events are linked to a detection.
- `hot_event_index.normalized_pointer` answers where the retained normalized source of truth for that event lives in the lake.
- `hot_event_index.raw_pointer` answers where the corresponding raw evidence lived during the raw hot window.
- Detection read models may continue to carry an ordered `event_ids` array for compatibility, but the relational edge table is the authoritative join surface for Postgres-backed reads.

Rules:

- Every `detection.event_ids[n]` entry must materialize exactly one `detection_event_edge` row with the same ordering in `ordinal`.
- Every `detection_event_edge.event_id` must resolve to exactly one `hot_event_index` row.
- Event detail and detection-linked event hydration use `event_payload` from `hot_event_index`, not workspace scans.
- Raw evidence lookup for case evidence bundles may use `raw_pointer` when `raw_retention_state = available`. If the state is `expired`, the detection remains valid through the event row, normalized pointer, and structured detection evidence.
- Detection evidence pointers and detection-event edges are complementary, not interchangeable. Evidence pointers identify immutable retained artifacts; edges identify indexed event membership and order.
- Principal and resource timeline joins in the product-target read path use canonical entity columns, not source-local compatibility alias columns.

## Retention And Rebuild Rules

- A `hot_event_index` row must exist for every normalized event retained in the hot query window.
- A `hot_event_index` row must not outlive the retained normalized event it points to unless an explicit future archival contract is added.
- Raw retention must not delete the event row. It only changes `raw_retention_state` and may leave `raw_pointer` as a historical reference.
- Rebuild proceeds from retained normalized manifests and objects plus detection read models. The index must be fully reconstructable without raw objects.
- Replaying normalization for an existing `event_id` must upsert the same event row and preserve the same `event_id`, `event_key`, and normalized pointer semantics.

## Migration Notes From The Current Projection-Only Event List

The current implementation stores full event payloads in `projected_events` and serves:

- `/api/events` from `src/seccloud/projection_store.py`
- `/api/events/{event_id}` by scanning `workspace.list_normalized_events()` in `src/seccloud/investigation.py`

Migration rules:

- `projected_events` is a temporary demo table and is not the v1 event-index contract.
- Replace `projected_events` reads with `hot_event_index` reads for event list and event detail.
- Replace workspace scans in `get_event_detail()` and detection-detail event hydration with indexed lookups by `event_id`.
- Materialize `detection_event_edge` from existing ordered `detection.event_ids` arrays without changing detection IDs or event IDs.
- Continue serving the current event API response shape from `event_payload` during the migration window.
- Keep the full WS01 pointer envelopes in `normalized_pointer` and `raw_pointer`; do not overload `event_payload.evidence` with the new pointer shape until the API model itself is versioned.
- Migrate principal and resource timeline or filter joins from source-local `principal.id` and `resource.id` semantics toward canonical `principal_entity_id` and `resource_entity_id`.
