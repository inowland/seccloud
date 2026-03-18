# Feature Schema v1

## Goal

Freeze the first durable Rust feature representation for M2b based on the actual
M0 model inputs, while avoiding accidental coupling to Python-only synthetic-org
internals such as `principal_idx`.

## Source Of Truth

- [src/seccloud/feature_pipeline.py](../../src/seccloud/feature_pipeline.py)
- [project/plan/m0-ml-validation.md](./m0-ml-validation.md)
- [project/plan/normalized-event-schema-v1.md](./normalized-event-schema-v1.md)

## Decision

Persist stable keys, not synthetic dense indices.

The Python M0 pipeline uses `principal_idx` because it is feeding tensors directly
into the model. The durable lake representation should preserve the same semantics
using stable normalized identities:

- `principal_entity_key`
- `resource_entity_key`

A later vocab-building step can derive dense indices from these stable keys for a
specific training or inference run.

## Durable Tables

### `action-accessor-set.v1`

Semantics: weighted accessor set for each resource, matching M0 action features.

One row per `(resource_entity_key, principal_entity_key)` pair.

Columns:

- `feature_schema_version`
- `tenant_id`
- `source`
- `resource_entity_key`
- `principal_entity_key`
- `access_count`
- `accessor_weight`

Invariants:

- `access_count >= 1`
- for each `resource_entity_key`, `sum(accessor_weight) == 1`
- `accessor_weight = access_count / total_accesses_for_resource`

### `history-window.v1`

Semantics: per-principal non-overlapping 2-hour windows over normalized event
history, matching M0 history profiles.

One row per `(principal_entity_key, window_start, resource_entity_key)` triple.

Columns:

- `feature_schema_version`
- `tenant_id`
- `principal_entity_key`
- `window_start`
- `resource_entity_key`

Invariants:

- `window_start` is the start of the containing 2-hour UTC window
- resources are distinct within a window for a given principal
- empty windows are omitted

### `collaboration.v1`

Semantics: inferred collaboration weights from shared resource access,
matching the M0 collaboration features.

One row per `(principal_entity_key, collaborator_entity_key)` pair.

Columns:

- `feature_schema_version`
- `tenant_id`
- `principal_entity_key`
- `collaborator_entity_key`
- `co_access_weight`

Invariants:

- rows are directional but symmetric in value across both directions
- `co_access_weight > 0`
- weight is the sum over shared resources of
  `min(access_count_i, access_count_j) / unique_accessors(resource)`

### `principal-static.v1`

Semantics: stable per-principal categorical context derived from normalized
events and, when present at build time, the durable identity profile manifest.
This matches the static feature portion of the M0 context features without
requiring Python to patch the rows after the fact.

One row per `principal_entity_key`.

Columns:

- `feature_schema_version`
- `tenant_id`
- `principal_entity_key`
- `principal_id`
- `department`
- `role`
- `location`
- `employment_duration_bucket`
- `privilege_level`

Invariants:

- `department`, `role`, and `location` reflect the latest observed normalized
  principal metadata for that principal, unless a richer identity profile
  overrides those fields at feature-build time
- `principal_id` preserves the current normalized principal identifier so
  downstream consumers can join to richer org/profile manifests when available
- `privilege_level` is derived from role using the existing M0 privilege mapping
- `employment_duration_bucket` uses identity-profile tenure bucketing when
  identity profile data is present, otherwise it falls back to a stable default

### `peer-group.v1`

Semantics: durable peer relationships for M0 context features, covering the
department, manager, and group peer sets that were previously reconstructed in
Python at read time.

One row per `(principal_entity_key, peer_entity_key, peer_type)` triple.

Columns:

- `feature_schema_version`
- `tenant_id`
- `principal_entity_key`
- `peer_entity_key`
- `peer_type`
- `peer_weight`

Invariants:

- `peer_type` is one of `department`, `manager`, or `group`
- department peers are emitted whenever two principals share a department in
  `principal-static.v1`
- manager and group peers are emitted when identity profile data is present during
  feature materialization
- `peer_weight > 0`
- rows are directional so consumers can build per-principal peer maps directly

## Deferred Tables

These remain part of feature schema v1 semantically, but are deferred from the
current Rust storage slice:

- derived dense vocab manifests for model tensorization

## Mapping To M0

- Python `ActionFeatures.accessor_weights` maps to `action-accessor-set.v1`
- Python `HistoryWindow.resource_ids` maps to `history-window.v1`
- Python `CollaborationFeatures.co_access` maps to `collaboration.v1`
- Python `StaticFeatures` maps to `principal-static.v1`
- Python `PeerFeatures.department_peers`, `manager_peers`, and `group_peers`
  map to `peer-group.v1`
- Python `principal_idx` becomes a later consumer concern derived from
  `principal_entity_key`
- Python `resource_vocab` becomes a later consumer concern derived from stable
  resource keys

## Physical Layout

Rust feature lake objects should live under:

- `lake/features/layout=v1/table=action-accessor-set/schema=feature.v1/...`
- `lake/features/layout=v1/table=history-window/schema=feature.v1/...`
- `lake/features/layout=v1/table=collaboration/schema=feature.v1/...`
- `lake/features/layout=v1/table=principal-static/schema=feature.v1/...`
- `lake/features/layout=v1/table=peer-group/schema=feature.v1/...`

Feature manifests should live under:

- `lake/manifests/layout=v1/type=feature/...`

## Acceptance For The First Rust Slice

1. Rust can serialize and deserialize all durable tables as Parquet.
2. Rust can write feature lake objects and manifests for those tables.
3. Python scoring and feature-bridge consumers can prefer those durable tables
   over ad hoc runtime reconstruction when they are present.
4. A later extractor worker can target this storage contract without changing the
   file shapes again.
