# M1: Production Ingestion Layer

## Goal

Replace the Python push gateway with a Rust ingestion tier on EKS that can handle
production enterprise log volumes, and establish the S3 lake layout that the rest of
the system will build on.

## Success Criteria

1. **Throughput**: sustain 10K events/sec per tenant on a single ingestion pod.
2. **Durability**: no event loss once an HTTP 200 is returned to the client.
3. **Lake layout v1**: S3 key structure and Parquet schema are versioned and documented.
4. **Backpressure**: graceful degradation under load (reject with 429, don't OOM).
5. **Dead-letter handling**: malformed events are quarantined, not dropped.
6. **Collector framework**: at least 4 pull-based collectors (Okta, Google Workspace,
   GitHub, Snowflake) with checkpoint-based resumption.

## Architecture

### Core Pipeline Crate (`seccloud-pipeline`)

All data plane services (ingestion, normalization, scoring) share a core crate that
defines the composable pipeline primitives. Every processing stage has the same shape:

```rust
/// Every stage — validation, normalization, routing, sinking — is a Transform.
trait Transform: Send + Sync {
    fn apply(&self, ctx: &Context, msg: Envelope) -> Result<Vec<Envelope>>;
}

/// Payload stays immutable after ingestion; metadata accumulates processing state.
struct Envelope {
    payload: Bytes,
    metadata: EventMetadata,         // batch_id, source, intake_ts, attempt_count, lineage
    control: Option<ControlSignal>,  // pipeline flow control
}

enum ControlSignal { Flush, BatchComplete, Shutdown }
```

Key design properties (inspired by Substation):

- **Fan-out**: a transform returns `Vec<Envelope>` — it can produce 0 (drop/filter),
  1 (map), or N (split) messages.
- **Data/metadata separation**: the raw event payload is immutable after ingestion.
  Processing state (dead-letter reason, retry count, lineage pointers) accumulates in
  metadata without polluting the event.
- **Control messages**: `Flush`, `BatchComplete`, and `Shutdown` signals flow through
  the same channel as data. This gives clean batch boundary semantics (for hitting the
  64-128 MB Parquet target) and graceful drain (for zero-downtime upgrades in M5).
- **Routing is transformation**: dead-letter handling, S3 writes, and DynamoDB updates
  are all transforms in the chain, not special-cased branches. This eliminates a class
  of bugs where the error path diverges from the happy path.

### Push Gateway (Rust)

The push gateway is a transform chain:

```
Client/Webhook ──► ALB ──► Rust HTTP Service (axum/tonic)
                              │
                              Authenticate
                              │
                              Validate ──► (on failure) ──► DeadLetterSink
                              │
                              AssignBatch
                              │
                              Buffer (bounded, flushes on size/interval/Flush signal)
                              │
                              FlushToS3 (Parquet)
                              │
                              WriteIntakeManifest (DynamoDB)
```

- Stateless: no local disk, no persistent connections between pods
- Horizontal scaling via EKS HPA on CPU/memory/request-rate
- Authentication: bearer token per integration (rotate via DynamoDB)
- Schema validation: reject malformed payloads before buffering
- Dead-letter routing: validation failures produce an envelope with
  `metadata.dead_letter_reason` set, routed to a dead-letter sink transform
  (same pipeline, not a separate codepath)

### Pull Collectors (Rust)

- Run as EKS CronJobs or long-polling pods
- Each collector implements a trait: `poll() → Vec<RawEvent>`, `checkpoint() → CheckpointState`
- Checkpoints stored in DynamoDB (not S3, for atomic CAS)
- Rate limiting and backoff per source API
- Initial sources: Okta System Log, Google Workspace Activity, GitHub Audit Log,
  Snowflake Access History

### S3 Lake Layout v1

```
s3://{bucket}/{tenant}/v1/raw/{source}/{YYYY}/{MM}/{DD}/{HH}/{batch-id}.parquet
s3://{bucket}/{tenant}/v1/dead-letter/{source}/{YYYY}/{MM}/{DD}/{batch-id}.parquet
s3://{bucket}/{tenant}/v1/manifests/intake/{batch-id}.json
```

- Partition by tenant, source, and hour for efficient scan and lifecycle management
- Parquet files target 64-128 MB (batch multiple flushes if needed)
- Intake manifests reference their Parquet files and record row counts, byte sizes,
  and schema version

### DynamoDB Tables

- `intake-queue`: pending batches awaiting normalization (partition: tenant, sort: batch-id)
- `collector-checkpoints`: per-source checkpoint state (partition: tenant#source)
- `integration-config`: per-source configuration and credentials references

## Key Decisions

- **Parquet from ingestion**: raw events are written as Parquet, not JSON/JSONL. This
  means downstream processing never parses JSON again. Cost: slightly more complex
  ingestion code. Benefit: massive downstream performance win.
- **No SQS**: DynamoDB intake-queue table replaces SQS for work discovery. Workers
  poll the table. This keeps the cloud-service surface minimal and is portable.
- **Tenant isolation at the S3 key level**: no shared prefixes between tenants. A
  tenant's data can be deleted by prefix. IAM policies can scope to tenant prefix.
- **Composable transform pipeline**: the `Transform` trait and `Envelope` type are
  defined in M1 and used through M2-M6. This is a one-time design cost (~1 week) that
  pays dividends: M2 normalization, M3 scoring, and M6 multi-cloud sinks all compose
  over the same abstraction. Control messages reuse the same channel for batch
  boundaries (M1), graceful drain (M5), and adaptive batching (M6).

## Dependencies

- M0 informs the raw event schema (what fields are needed for ML features), but M1
  can begin with the existing PoC schema and evolve.

## Deliverables

1. `seccloud-pipeline` core crate: `Transform` trait, `Envelope`, `ControlSignal`,
   `Chain` executor, and sink/dead-letter primitives.
2. Rust ingestion service (push gateway) deployable on EKS, built as a transform chain.
3. Rust collector framework with 4 source implementations.
4. S3 lake layout v1 specification and Parquet schemas.
5. DynamoDB table definitions and access patterns.
6. Terraform module for ingestion tier (EKS, ALB, DynamoDB, S3).
7. Load test demonstrating 10K eps sustained.
