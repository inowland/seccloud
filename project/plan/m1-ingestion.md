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

### Push Gateway (Rust)

```
Client/Webhook ──► ALB ──► Rust HTTP Service (axum/tonic)
                              │
                              ├── Validate schema
                              ├── Assign intake batch ID
                              ├── Buffer in memory (bounded)
                              ├── Flush to S3 as Parquet on interval or size threshold
                              └── Write intake manifest to DynamoDB
```

- Stateless: no local disk, no persistent connections between pods
- Horizontal scaling via EKS HPA on CPU/memory/request-rate
- Authentication: bearer token per integration (rotate via DynamoDB)
- Schema validation: reject malformed payloads before buffering

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

## Dependencies

- M0 informs the raw event schema (what fields are needed for ML features), but M1
  can begin with the existing PoC schema and evolve.

## Deliverables

1. Rust ingestion service (push gateway) deployable on EKS.
2. Rust collector framework with 4 source implementations.
3. S3 lake layout v1 specification and Parquet schemas.
4. DynamoDB table definitions and access patterns.
5. Terraform module for ingestion tier (EKS, ALB, DynamoDB, S3).
6. Load test demonstrating 10K eps sustained.
