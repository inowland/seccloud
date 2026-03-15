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

```
DynamoDB intake-queue ──► Worker claims batch
                              │
                              ├── Read raw Parquet from S3
                              ├── Schema mapping (vendor → unified)
                              ├── Entity resolution (principal/resource keying)
                              ├── Write normalized Parquet to S3
                              └── Update DynamoDB (mark complete, write manifest)
```

- Workers run as EKS Deployments with configurable replica count
- Work claiming via DynamoDB conditional writes (optimistic locking)
- Schema mapping registry: declarative mapping from vendor schemas to unified schema
- Dead-letter output for events that fail normalization

### Feature Engineering Workers (Rust)

```
Normalized events in S3 ──► Feature worker reads time windows
                                 │
                                 ├── Principal profile (action counts, resource diversity,
                                 │   time-of-day distribution, geo history)
                                 ├── Peer group aggregates (same role/team/org-unit)
                                 ├── Resource access patterns (who else accesses this,
                                 │   sensitivity signals)
                                 ├── Temporal features (velocity, acceleration, periodicity)
                                 └── Write feature vectors as Parquet to S3
```

- Feature computation is windowed: each feature vector covers a configurable time
  window (e.g., 1 hour of activity with 30 days of history context)
- Principal profiles are maintained as rolling aggregates in DynamoDB (hot) backed by
  full history in S3 (cold)
- Peer group definitions come from identity source data (Okta groups, org chart)

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

## Dependencies

- M0: feature vector schema (what the model needs as input)
- M1: raw Parquet in S3, DynamoDB coordination tables

## Deliverables

1. Rust normalization worker with schema mapping for 4 sources.
2. Rust feature engineering worker producing ML-ready feature vectors.
3. Unified event schema v1 specification.
4. Feature vector schema v1 specification.
5. DynamoDB principal profile table design.
6. Integration tests validating end-to-end raw → features pipeline.
