# M2: Normalization & Feature Engineering

## Goal

Build the Rust processing pipeline that reads raw Parquet from S3, normalizes events
into a unified schema, computes ML-ready feature vectors, and writes them back to S3
as Parquet.

## Success Criteria

1. **Schema coverage**: normalize all 4 initial sources into the unified event schema.
2. **Feature completeness**: produce the feature vector schema required by the M0 model.
3. **Throughput**: process raw events at 10K+ eps per worker pod.
4. **Exactly-once semantics**: no duplicate normalized events or features, even after
   worker restarts.
5. **Incremental processing**: workers process only new raw batches, not the full
   history on each run.

## Architecture

### Normalization Workers (Rust)

Normalization is a transform chain built on the M1 `seccloud-pipeline` core crate:

```
DynamoDB intake-queue ──► Worker claims batch
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
                              UpdateManifest (DynamoDB)
```

- Workers run as EKS Deployments with configurable replica count
- Work claiming via DynamoDB conditional writes (optimistic locking)
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
- Principal profiles are maintained as rolling aggregates in DynamoDB (hot) backed by
  full history in S3 (cold)
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

Extends the PoC contract with ML-relevant fields:

- `event_id`: stable content-hash key
- `timestamp`: event time (not ingestion time)
- `principal_key`: stable identity (resolved across sources)
- `principal_type`: human | service_account | agent
- `resource_key`: stable resource identity
- `action_type`: normalized action taxonomy
- `source`: originating integration
- `geo`: country, region, city, ASN (when available)
- `session_key`: session or correlation ID (when available)
- `evidence`: typed key-value bag for source-specific fields

### Feature Vector Schema

Output of feature engineering, input to ML:

- `window_id`: time window identifier
- `principal_key`: who
- `action_summary`: action type counts within window
- `resource_summary`: resource diversity and sensitivity metrics
- `temporal_features`: hour-of-day, day-of-week, velocity, acceleration
- `baseline_deviation`: delta from principal's rolling baseline
- `peer_deviation`: delta from peer group aggregate
- `geo_features`: location novelty, impossible travel
- `social_features`: collaboration graph metrics
- `raw_action_ids`: references back to normalized events (for evidence)

## Key Decisions

- **Entity resolution at normalization time**: principal and resource keys are
  resolved during normalization, not downstream. This means the identity model
  must be available to normalization workers.
- **Rolling profiles in DynamoDB**: maintaining per-principal profiles as rolling
  aggregates avoids re-scanning full history for each feature computation. Trade-off:
  profile state is eventually consistent.
- **Arrow-native processing**: use arrow-rs and datafusion for all Parquet I/O and
  columnar computation. No row-by-row processing.
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
- M1: raw Parquet in S3, DynamoDB coordination tables

## Deliverables

1. Rust normalization worker as a transform chain, with schema mapping for 4 sources.
2. Declarative source mapping configs (TOML) for Okta, Google Workspace, GitHub,
   Snowflake.
3. Data-driven fixture test harness with input/expected-output pairs per source.
4. Rust feature engineering worker as a transform chain, producing ML-ready feature
   vectors.
5. Unified event schema v1 specification.
6. Feature vector schema v1 specification.
7. DynamoDB principal profile table design.
8. Integration tests validating end-to-end raw → features pipeline.
