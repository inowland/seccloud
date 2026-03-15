# Lake Layout And Manifest Contract

## Purpose

This document defines the authoritative v1 object-store contract for raw and normalized event batches, their manifests, and the object pointers used by evidence and indexes.

This is the stable storage contract for `m01`. Later workstreams may build ingestion, indexing, replay, and retention enforcement on top of it, but they must not redefine the layout, manifest shape, or pointer semantics established here.

## Scope

In scope:

- raw object layout
- normalized object layout
- dead-letter object layout
- partitioning by tenant, source, integration, and UTC time
- manifest schema and versioning rules
- object pointer semantics for whole-object and per-record references
- replay, backfill, and retention expectations

Out of scope:

- authenticated push-ingestion API shape
- collector-specific checkpoint payloads beyond the manifest placeholder contract
- Postgres table design
- UI or analyst workflow behavior

## Root Prefixes

All product-target lake objects live under a `lake/` root in the tenant-local object store.

- `lake/raw/`: immutable raw intake batches
- `lake/normalized/`: immutable normalized analytical event batches
- `lake/dead_letters/`: immutable normalization failures with raw context
- `lake/manifests/`: immutable per-batch manifests

The current local filesystem runtime may continue to simulate these keys under a workspace root, but the path contract is defined as object-store keys, not local directories.

## Partitioning Rules

- Partition segments are lowercase ASCII `name=value` pairs.
- `tenant`, `source`, and `integration` values must be stable identifiers using only `a-z`, `0-9`, `.`, `_`, and `-`.
- Missing integration identifiers must use `integration=default`.
- Raw and dead-letter batches partition by `received_at` in UTC.
- Normalized batches partition by `observed_at` in UTC.
- A single batch object must not cross a UTC hour boundary for its partitioning timestamp.
- A single batch object must contain records for exactly one `tenant`, `source`, and `integration`.

## Object Key Contract

### Raw Batch Objects

Raw landing uses compressed JSON Lines immediately in v1.

Key pattern:

```text
lake/raw/layout=v1/tenant=<tenant>/source=<source>/integration=<integration>/dt=<YYYY-MM-DD>/hour=<HH>/batch=<batch_id>/part-<nnnnn>.jsonl.gz
```

Rules:

- Each line is one raw intake record envelope.
- The envelope preserves the source payload exactly as received plus ingestion metadata.
- Objects are append-only and immutable after the manifest is published.
- `part-00000` is valid for single-object batches; multiple parts are allowed for large batches.

Examples:

```text
lake/raw/layout=v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f/part-00000.jsonl.gz
lake/raw/layout=v1/tenant=acme-prod/source=github/integration=github-audit/dt=2026-03-15/hour=14/batch=raw_01hq9ra1c8m4wx0h8kv2e9b4qg/part-00000.jsonl.gz
```

### Normalized Batch Objects

Normalized analytical storage uses `Parquet` immediately in v1.

Key pattern:

```text
lake/normalized/layout=v1/schema=<normalized_schema_version>/tenant=<tenant>/source=<source>/integration=<integration>/dt=<YYYY-MM-DD>/hour=<HH>/batch=<batch_id>/part-<nnnnn>.parquet
```

Rules:

- Every row is one canonical normalized event.
- All rows in an object must share the same normalized schema version.
- A normalized batch may be produced from one or more raw batches, but it must still obey the single-hour partition rule.
- Late-arriving events are written into the partition determined by their normalized `observed_at`, not their raw `received_at`.

Examples:

```text
lake/normalized/layout=v1/schema=event.v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=norm_01hq9reb0p1d4t6msw3j0s6pk4/part-00000.parquet
lake/normalized/layout=v1/schema=event.v1/tenant=acme-prod/source=snowflake/integration=snowflake-access/dt=2026-03-15/hour=13/batch=norm_01hq9rfyl0jbepg3e3n7ap9sh8/part-00000.parquet
```

### Dead-Letter Objects

Dead letters stay JSON for operator readability in v1.

Key pattern:

```text
lake/dead_letters/layout=v1/tenant=<tenant>/source=<source>/integration=<integration>/dt=<YYYY-MM-DD>/hour=<HH>/batch=<batch_id>/dead-letter-<dead_letter_id>.json
```

## Manifest Contract

Every raw and normalized batch must publish exactly one immutable manifest JSON object after all data objects for that batch are durable.

Manifest key pattern:

```text
lake/manifests/layout=v1/type=<raw|normalized>/tenant=<tenant>/source=<source>/integration=<integration>/dt=<YYYY-MM-DD>/hour=<HH>/batch=<batch_id>.json
```

### Common Manifest Schema

Required top-level fields:

- `manifest_version`: manifest schema version, initially `1`
- `manifest_type`: `raw` or `normalized`
- `layout_version`: object-key layout version, initially `1`
- `manifest_id`: immutable unique identifier for this manifest object
- `batch_id`: immutable logical batch identifier
- `tenant_id`
- `source`
- `integration_id`
- `produced_at`: UTC timestamp when the manifest was written
- `partition`: object with `dt` and `hour`
- `time_bounds`: object with `min` and `max` timestamps for the batch's partitioning clock
- `record_count`
- `objects`: ordered array of object descriptors
- `idempotency_key`: stable dedupe key for the logical batch
- `checkpoint`: opaque JSON object recorded from the producer or replay coordinator

Required `objects[]` fields:

- `object_key`
- `object_format`
- `sha256`
- `size_bytes`
- `record_count`
- `first_record_ordinal`
- `last_record_ordinal`

`first_record_ordinal` and `last_record_ordinal` are inclusive, zero-based ordinals local to the referenced object only.

- They do not describe batch-global ordering.
- For a single-object batch, the first object normally uses `0..record_count-1`.
- For a multi-object batch, each object starts its own local ordinal range again unless a future version introduces separate batch-global ordinal fields.
- Writers must not encode batch-global offsets into these fields.

### Raw Manifest Fields

Raw manifests add the following required fields:

- `raw_envelope_version`: currently `1`
- `received_at_bounds`: UTC `min` and `max`
- `source_event_id_hash`: deterministic hash of the ordered source event IDs or raw line digests used for batch-level dedupe
- `producer`: object describing the upstream writer, with at least `kind` and `run_id`

### Normalized Manifest Fields

Normalized manifests add the following required fields:

- `normalized_schema_version`: currently `event.v1`
- `observed_at_bounds`: UTC `min` and `max`
- `event_id_min`
- `event_id_max`
- `upstream_raw_batches`: ordered array of source `batch_id` values used to build this normalized batch

## Manifest Examples

Raw manifest example:

```json
{
  "manifest_version": 1,
  "manifest_type": "raw",
  "layout_version": 1,
  "manifest_id": "man_01hq9r8c2n2s3r7d8akd3s4x1p",
  "batch_id": "raw_01hq9r7m6v4gn8xsm3h7q3j29f",
  "tenant_id": "acme-prod",
  "source": "okta",
  "integration_id": "okta-primary",
  "produced_at": "2026-03-15T14:07:12Z",
  "partition": {
    "dt": "2026-03-15",
    "hour": "14"
  },
  "time_bounds": {
    "min": "2026-03-15T14:00:00Z",
    "max": "2026-03-15T14:59:59Z"
  },
  "received_at_bounds": {
    "min": "2026-03-15T14:02:01Z",
    "max": "2026-03-15T14:06:58Z"
  },
  "record_count": 2500,
  "idempotency_key": "okta-primary:2026-03-15T14:00Z:cursor-000184",
  "source_event_id_hash": "sha256:1dbb0d4f1f4d4d3ac9a6820e4322f6d8f6d6296e5bbf7659e30b77ea7ab9d78c",
  "raw_envelope_version": 1,
  "producer": {
    "kind": "collector_pull",
    "run_id": "run_01hq9r75gn3e2dv0h8tfrwz42m"
  },
  "checkpoint": {
    "cursor": "000184"
  },
  "objects": [
    {
      "object_key": "lake/raw/layout=v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f/part-00000.jsonl.gz",
      "object_format": "jsonl.gz",
      "sha256": "sha256:7fc1f1b0cf0f698aa91d55f12389f7b12908c648e5f2cd2254a9fe0f3416f88a",
      "size_bytes": 185522,
      "record_count": 2500,
      "first_record_ordinal": 0,
      "last_record_ordinal": 2499
    }
  ]
}
```

Normalized manifest example:

```json
{
  "manifest_version": 1,
  "manifest_type": "normalized",
  "layout_version": 1,
  "manifest_id": "man_01hq9rfzmyv4s3bk4prg1jv2x5",
  "batch_id": "norm_01hq9reb0p1d4t6msw3j0s6pk4",
  "tenant_id": "acme-prod",
  "source": "okta",
  "integration_id": "okta-primary",
  "produced_at": "2026-03-15T14:08:40Z",
  "partition": {
    "dt": "2026-03-15",
    "hour": "14"
  },
  "time_bounds": {
    "min": "2026-03-15T14:00:00Z",
    "max": "2026-03-15T14:59:59Z"
  },
  "observed_at_bounds": {
    "min": "2026-03-15T14:01:18Z",
    "max": "2026-03-15T14:06:44Z"
  },
  "record_count": 2489,
  "idempotency_key": "norm:okta-primary:raw_01hq9r7m6v4gn8xsm3h7q3j29f:event.v1",
  "normalized_schema_version": "event.v1",
  "event_id_min": "evt_01hq9rcgmqf6j8ncm33pa40m3j",
  "event_id_max": "evt_01hq9rdz52pxmsay1g1we2a4f9",
  "upstream_raw_batches": ["raw_01hq9r7m6v4gn8xsm3h7q3j29f"],
  "checkpoint": {
    "normalizer_run_id": "run_01hq9re0xgx3krb3mz2qkyb61r"
  },
  "objects": [
    {
      "object_key": "lake/normalized/layout=v1/schema=event.v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=norm_01hq9reb0p1d4t6msw3j0s6pk4/part-00000.parquet",
      "object_format": "parquet",
      "sha256": "sha256:57c603b33baf0d165c8cf2461ecf2fb0fd4a6a54a3f7bde6498ae36d1dbfd51a",
      "size_bytes": 93548,
      "record_count": 2489,
      "first_record_ordinal": 0,
      "last_record_ordinal": 2488
    }
  ]
}
```

## Object Pointer Contract

Evidence, event indexes, and replay metadata must use a versioned pointer envelope instead of storing a bare object key.

Pointer schema:

- `pointer_version`: initially `1`
- `object_key`: lake-relative object key
- `object_format`: `jsonl.gz`, `parquet`, or `json`
- `sha256`: object digest copied from the manifest
- `manifest_key`: manifest object key for the batch containing the object
- `record_locator`: optional locator for a single record
- `retention_class`: `raw_hot`, `normalized_retained`, or `derived_long_lived`

`record_locator` rules:

- If absent, the pointer refers to the whole object.
- `ordinal` is zero-based within the referenced object.
- `row_group` is optional and only advisory for `Parquet`.
- Consumers must treat `ordinal` as the stable logical locator even if physical file internals change between layout versions.

Raw record pointer example:

```json
{
  "pointer_version": 1,
  "object_key": "lake/raw/layout=v1/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f/part-00000.jsonl.gz",
  "object_format": "jsonl.gz",
  "sha256": "sha256:7fc1f1b0cf0f698aa91d55f12389f7b12908c648e5f2cd2254a9fe0f3416f88a",
  "manifest_key": "lake/manifests/layout=v1/type=raw/tenant=acme-prod/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_01hq9r7m6v4gn8xsm3h7q3j29f.json",
  "record_locator": {
    "ordinal": 417
  },
  "retention_class": "raw_hot"
}
```

Normalized record pointer example:

```json
{
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
}
```

## Replay, Backfill, And Retention Semantics

- Replay and backfill operate from manifests, not from prefix listing alone.
- Re-running the same logical batch must preserve `batch_id` and `idempotency_key`; a duplicate physical write is invalid once a manifest for that `batch_id` is published.
- Normalization may emit fewer records than the raw batch count because invalid rows may move to dead letters. This must be reflected explicitly in the normalized manifest and dead-letter objects.
- Retention deletes raw objects and raw manifests on schedule according to the raw hot-window policy.
- Detections, cases, and read models must not rely on raw pointers as their only surviving evidence reference after retention. They must retain at least one normalized or derived pointer plus the structured evidence fields needed for investigation.
- A missing raw pointer after retention is a valid and expected outcome for `retention_class=raw_hot`.

## Versioning Rules

- `layout_version=1` freezes the key patterns in this document.
- `manifest_version=1` freezes the manifest fields and their meanings in this document.
- `pointer_version=1` freezes the pointer envelope and locator semantics in this document.
- `normalized_schema_version` is versioned independently from the lake layout and starts at `event.v1`.
- Adding optional manifest or pointer fields is a backward-compatible change within the same version.
- Renaming fields, changing required semantics, or changing key partition rules requires a new major version and a dual-read migration plan.
- Writers must emit exactly one layout version per object. Mixed-version reads are allowed only during an explicit migration window.

## Migration Notes From The Current Per-Object JSON Simulation

The current local simulation in `src/seccloud/storage.py` writes one JSON object per raw event and one JSON object per normalized event:

- `raw/.../<raw_event_key>.json`
- `normalized/.../<event_id>.json`
- shared manifest files such as `manifests/ingest_manifest.json` and `manifests/intake_manifest.json`

Migration rules:

- Raw single-event JSON objects become batched `jsonl.gz` objects under `lake/raw/.../batch=<batch_id>/part-00000.jsonl.gz`.
- Normalized single-event JSON objects become batched `Parquet` objects under `lake/normalized/.../batch=<batch_id>/part-00000.parquet`.
- Shared mutable manifest files are replaced by immutable per-batch manifest objects under `lake/manifests/...`.
- Evidence references and future index rows must store the pointer envelope from this document rather than a bare raw or normalized object key.
- The local filesystem runtime may continue to exist as a development adapter, but it is no longer the authoritative storage contract once the lake layout is adopted.
