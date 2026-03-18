# M2: Normalization & Feature Engineering

## Goal

Build the Rust processing pipeline that reads raw Parquet from S3, normalizes events
into a unified schema, computes ML-ready feature vectors, and writes them back to S3
as Parquet.

In the current repo, this milestone is a migration from the Python PoC normalization
and feature code to the Rust data plane introduced in M1. The first job is parity
with the existing normalized-event contract; only then do we promote the pipeline to
the fuller declarative and columnar design described below.

## Success Criteria

1. **Schema coverage**: normalize all 4 initial sources into the unified event schema.
2. **Feature completeness**: produce the feature vector schema required by the M0 model.
3. **Throughput**: process raw events at 10K+ eps per worker pod.
4. **Exactly-once semantics**: no duplicate normalized events or features, even after
   worker restarts.
5. **Incremental processing**: workers process only new raw batches, not the full
   history on each run.

## Current Repo State

The codebase already has working Python implementations for the behavior M2 needs to
preserve:

- [src/seccloud/pipeline.py](../../src/seccloud/pipeline.py) defines the current raw
  validation rules, action taxonomy, entity resolution, and normalized-event
  production path.
- [src/seccloud/contracts.py](../../src/seccloud/contracts.py) defines the current
  normalized-event contract that downstream code already consumes.
- [src/seccloud/workers.py](../../src/seccloud/workers.py) still runs normalization in
  Python after the Rust ingestion tier lands raw Parquet batches.
- [src/seccloud/feature_pipeline.py](../../src/seccloud/feature_pipeline.py) defines
  the M0 feature semantics, but in model-oriented Python data structures rather than a
  production Rust feature store.

That means M2 must be staged. Treat the current Python normalized-event behavior as
the compatibility oracle for the first Rust implementation.

## Execution Stages

### M2a: Rust Normalization Parity

Deliver a Rust worker that reproduces the current Python normalization semantics for
the initial 4 sources and writes normalized lake artifacts with exactly-once behavior.

### M2b: Rust Feature Engineering

Build the production feature pipeline on top of normalized Parquet once the
normalization contract is frozen and testable in Rust.

## Architecture

### Normalization Workers (Rust)

Normalization is a transform chain built on the M1 `seccloud-pipeline` core crate:

```
Durable intake queue ──► Worker claims batch
                              │
                              ReadRawParquet
                              │
                              Validate ──► (on failure) ──► DeadLetterSink
                              │
                              NormalizeSchema (source mapping config)
                              │
                              ResolveEntities (principal/resource keying)
                              │
                              WriteNormalizedParquet (S3)
                              │
                              UpdateManifest (durable manifest/progress state)
```

- Workers run as EKS Deployments with configurable replica count
- Work claiming via durable manifest/Postgres state with optimistic locking semantics
- Each stage is an independent `Transform` implementation — testable in isolation
  and reorderable without changing adjacent stages
- Dead-letter routing uses the same pipeline (validation failures produce an envelope
  with `metadata.dead_letter_reason`, routed to a dead-letter sink transform)

### Declarative Source Mapping Registry

Per-source normalization logic lives in config files, not Rust code:

```
sources/
  okta.toml         # field mappings, action taxonomy, required fields, sensitivity rules
  gworkspace.toml
  github.toml
  snowflake.toml
```

Each mapping config declares:

- Field mapping: vendor field names → unified schema fields
- Action taxonomy: (source, event_type) → (verb, category)
- Required fields: which vendor fields must be present (else dead-letter)
- Entity keying rules: how to derive principal_key and resource_key
- Source-specific enrichment: e.g., Okta geo fields, GitHub bytes_transferred

The `NormalizeSchema` transform reads the registry at startup. Adding a new source
(M6 agent monitoring, future integrations) is "write a TOML config + test fixtures,"
not "modify the normalization worker."

For migration, do not start with TOML as the first step. First prove Rust parity
against the Python mappings with code and fixture tests. Promote those rules into a
declarative registry only after the semantics are stable.

### Feature Engineering Workers (Rust)

Feature engineering is also a transform chain, composable with normalization (fused
in one pod or split across separate pools with a queue in between — same code either
way):

```
Normalized events in S3 ──► Feature worker reads time windows
                                 │
                                 ComputePrincipalProfile (action counts, resource diversity,
                                 │   time-of-day distribution, geo history)
                                 │
                                 ComputePeerAggregates (same role/team/org-unit)
                                 │
                                 ComputeResourcePatterns (who else accesses this,
                                 │   sensitivity signals)
                                 │
                                 ComputeTemporalFeatures (velocity, acceleration, periodicity)
                                 │
                                 WriteFeatureParquet (S3)
```

- Feature computation is windowed: each feature vector covers a configurable time
  window (e.g., 1 hour of activity with 30 days of history context)
- Principal profiles are maintained as rolling aggregates in Postgres or durable
  manifests (hot) backed by full history in S3 (cold)
- Peer group definitions come from identity source data (Okta groups, org chart)
- Each feature computation stage is an independent transform — can be tested against
  fixture data without running the full pipeline

### S3 Layout (additions to v1)

```
s3://{bucket}/{tenant}/v1/normalized/{YYYY}/{MM}/{DD}/{HH}/{partition-id}.parquet
s3://{bucket}/{tenant}/v1/features/{YYYY}/{MM}/{DD}/{HH}/{partition-id}.parquet
s3://{bucket}/{tenant}/v1/profiles/{principal-key}.parquet  (periodic snapshots)
```

### Unified Event Schema

The initial Rust target is the existing PoC contract in
[src/seccloud/contracts.py](../../src/seccloud/contracts.py). Schema evolution beyond
that contract should happen only after parity tests exist.

The normalized schema must cover at least:

- `event_id`: stable content-hash key
- `event_key`: stable semantic event key
- `integration_id`: source integration partition
- `source_event_id`: vendor-native event identifier
- `observed_at`: event time (not ingestion time)
- `principal`: normalized principal object including stable entity identity
- `resource`: normalized resource object including stable entity identity
- `action`: normalized source, verb, and category
- `source`: originating integration
- `environment`: normalized environment metadata
- `attributes`: source-specific extensible fields
- `evidence`: pointer back to raw lake objects

### Feature Vector Schema

The feature schema should be derived from the actual M0 model inputs in
[src/seccloud/feature_pipeline.py](../../src/seccloud/feature_pipeline.py), not from
an aspirational generic summary alone.

At minimum the production representation must preserve:

- per-resource weighted accessor sets for action embeddings
- per-principal history windows over trailing activity
- peer-group features derived from identity and org structure
- collaboration features inferred from shared-resource behavior
- static categorical principal features
- stable references back to normalized events or windows for evidence and replay

The final physical layout may be multiple Parquet tables rather than one wide row if
that better preserves the structure of the M0 features.

## Key Decisions

- **Entity resolution at normalization time**: principal and resource keys are
  resolved during normalization, not downstream. This means the identity model
  must be available to normalization workers.
- **Rolling profiles in durable state**: maintaining per-principal profiles as rolling
  aggregates avoids re-scanning full history for each feature computation. Keep this
  on Postgres/manifests until that is no longer sufficient.
- **Arrow-native processing**: use arrow-rs and datafusion for all Parquet I/O and
  columnar computation. No row-by-row processing.
- **Parity before abstraction**: the first Rust normalization worker should match the
  existing Python contract and tests before introducing a TOML mapping registry or
  broader schema evolution.
- **Declarative source mappings**: per-source normalization logic is config, not code.
  This keeps the normalization worker generic and makes adding sources a config change.
  The trade-off is a small runtime config-parsing cost at startup, which is negligible
  compared to the Parquet I/O.
- **Data-driven transform tests**: each source has a fixtures directory with
  input/expected-output JSON pairs. The test harness validates every fixture
  automatically. This catches field-mapping regressions without writing new test
  code per source.

## Dependencies

- M0: feature vector schema (what the model needs as input)
- M1: raw Parquet in S3 plus durable local/Postgres coordination state

## Deliverables

1. Unified event schema v1 specification, explicitly grounded in the current Python
   contract.
2. Rust normalization fixture harness that uses Python normalization outputs as the
   parity oracle for Okta, Google Workspace, GitHub, and Snowflake.
3. Rust normalization worker that reads raw Parquet batches and writes normalized
   Parquet/manifests with exactly-once semantics.
4. Normalized lake storage primitives in Rust, including manifests, object keys, and
   batch progress tracking.
5. Declarative source mapping registry for the 4 initial sources, introduced only
   after parity behavior is proven.
6. Feature vector schema v1 specification grounded in the M0 feature pipeline.
7. Rust feature pipeline implemented in stages: action/access history, history
   windows, collaboration features, peer features, and static features.
8. Rolling profile/progress state design for incremental feature computation.
9. Integration tests validating end-to-end raw -> normalized -> features behavior,
   including restart and idempotency cases.

## Implementation Sequence

1. Freeze normalized-event schema v1 from the current Python contract.
2. Add Rust tests and fixtures that compare raw inputs to Python-produced normalized
   outputs and dead letters.
3. Add normalized lake object-key, manifest, and writer helpers to `seccloud-lake`.
4. Implement the Rust normalization worker end to end, including entity resolution and
   exactly-once progress updates.
5. Keep the local normalization path Rust-first once parity is proven, and treat the
   old Python path as historical migration context rather than an active runtime goal.
6. Extract source-specific normalization rules behind a registry and convert them to
   TOML-backed configuration.
7. Freeze feature schema v1 from the actual M0 action/context feature requirements.
8. Implement Rust feature computation in incremental stages, validating each stage
   against Python feature fixtures where practical.
9. Add end-to-end raw -> normalized -> features tests, plus restart and replay tests.

## Progress Notes

- Normalized-event schema v1 is frozen in
  [normalized-event-schema-v1.md](./normalized-event-schema-v1.md).
- Feature schema v1 is frozen in
  [feature-schema-v1.md](./feature-schema-v1.md).
- The current durable Rust feature slice now targets stable-key
  `action-accessor-set`, `history-window`, `collaboration`, `principal-static`,
  and `peer-group` tables.
- Local Python scoring now prefers those durable feature tables when the feature
  lake is complete, instead of rebuilding action/context tensors from scratch.
- `principal-static` and `peer-group` can now be enriched from the durable
  `identity_profiles.json` manifest at feature-build time.
- Python can read those durable feature tables back from the workspace lake via
  [src/seccloud/feature_lake.py](../../src/seccloud/feature_lake.py), including a
  bridge into the existing model-oriented `FeatureSet` shape. Static
  categoricals and peer groups now come from the durable lake tables, with
  richer org context sourced from `identity_profiles.json` during feature
  materialization rather than reconstructed ad hoc in Python.
- The existing ML scoring path in
  [src/seccloud/ml_scoring.py](../../src/seccloud/ml_scoring.py) now prefers the
  durable feature lake when those manifests are present and falls back to the
  Python feature builder otherwise.
- Local normalized storage is now lake-first for both Rust and Python paths.
  Normalized parquet/manifests are the canonical artifacts, while direct
  `normalized/*.json` segments are no longer required in the default local path.
- The local runtime now maintains a durable event index in
  `manifests/event_index.json`. Investigation detail views, local timelines, case
  timeline construction, source-stats rebuilds, heuristic derived-state rebuilds,
  runtime status, and projection sync all use that index instead of ad hoc
  normalized-event scans.
- Heuristic detection rebuilds no longer reconstruct in-memory replay state on
  every pass. The local runtime now persists `manifests/detection_context.json`,
  which captures per-event prior baseline context from the event index and is
  reused by the detection worker and investigation detail views.
- The local worker path now treats detection-context materialization as an
  explicit stage between feature building and detection scoring, instead of only
  deriving that context lazily inside status or investigation reads.
- Runtime substrate reporting is now centralized in
  [src/seccloud/runtime_status.py](../../src/seccloud/runtime_status.py), so the
  CLI, API, founder artifacts, and web UI all read the same summary of event
  index coverage, feature-table materialization, identity-profile availability,
  detection-context readiness, and projection readiness.
- The demo/reporting surface now exposes this durable data plane more directly:
  the `/api/runtime-status` route feeds the integration detail UI, and founder
  artifacts report indexed coverage plus projected-event/detection coverage
  alongside feature and identity coverage.
- Detection detail peer context now includes durable “known before this event”
  baseline fields such as prior event count, prior action count, prior resource
  count, prior peer-group resource count, and whether the geography was already
  seen before the triggering event.

## Out Of Scope For Initial M2a Slice

- Cross-source identity unification beyond the current Python entity-key semantics
- A one-shot generic feature row if multiple Parquet outputs are a cleaner fit
- Full BYOC cloud adapters beyond the local/shared-filesystem bridge already used for
  development
