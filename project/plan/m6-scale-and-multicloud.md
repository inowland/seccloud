# M6: Scale & Multi-Cloud

## Goal

Handle enterprise-scale log volumes (100K+ events/sec per tenant), add advanced
query capabilities (vector search, SQL-style analytics), and expand beyond AWS to
GCP and Azure.

## Success Criteria

1. **Ingestion throughput**: 100K+ events/sec sustained per tenant.
2. **Query performance**: entity timeline queries < 1s for 90-day windows; full-text
   and vector search across normalized events.
3. **Storage efficiency**: tiered storage with automated lifecycle (hot → warm → cold
   → delete per retention policy).
4. **Multi-cloud**: deploy data plane on GCP (GKE + GCS) and Azure (AKS + Blob).
5. **Agent monitoring**: ingest and analyze AI agent activity logs as a first-class
   source type.

## Architecture

### Ingestion Scaling

- Partition ingestion across multiple pods by source or hash-ring assignment
- In-memory write-ahead buffer with S3 flush (Warpstream-style)
- Adaptive batching: larger Parquet files under high throughput, faster flushes under
  low throughput
- Consider io_uring for high-throughput network I/O on Linux

### Storage Tiering

```
Hot (< 24h):    DynamoDB + in-memory indices on query pods
Warm (1-30d):   S3 Standard, columnar indices, bloom filters, partition pruning
Cold (30d-1y):  S3 Infrequent Access, metadata-only indices
Archive (> 1y): S3 Glacier or customer-managed (if retention allows)
Delete:         Automated per retention policy
```

- Columnar indices built during feature engineering: min/max per column per Parquet
  row group, bloom filters for principal_key and resource_key
- Turbopuffer-style approach: small index files in S3 alongside data files, loaded
  on-demand by query workers

### Advanced Query Capabilities

- **Vector search**: embed queries and events in the same space, find semantically
  similar activity patterns. Useful for "show me other activity that looks like this
  detection."
- **SQL-style analytics**: DataFusion-based query engine for ad-hoc analysis over
  Parquet in S3. Enables TAM customizations and advanced investigation.
- **Full-text search**: inverted indices over event descriptions and evidence fields,
  stored as Parquet-adjacent index files in S3.

### Multi-Cloud Abstraction

Three abstraction points:

| Capability | AWS | GCP | Azure |
|---|---|---|---|
| Object storage | S3 | GCS | Azure Blob |
| Coordination store | DynamoDB | Bigtable or Firestore | CosmosDB |
| Compute orchestration | EKS | GKE | AKS |

- Rust trait-based abstraction for object store and coordination store
- Kubernetes manifests are cloud-agnostic (already)
- Terraform provider modules per cloud, shared module structure
- CI builds and tests against all three providers

### Agent Monitoring

AI agents as a new principal type with source-specific collectors:

- **Coding agents**: tool calls, file access, API requests, git operations
- **Customer service agents**: conversation actions, data lookups, escalations
- **Data analysis agents**: query execution, data access, export actions
- **Orchestration agents**: inter-service calls, permission usage, resource creation

Agent activity shares the same normalized schema (principal + action + resource +
context) but with agent-specific features: tool call sequences, permission scope
usage, delegation chains.

## Key Decisions

- **DataFusion for SQL**: embedded query engine, no external dependency. Reads
  Parquet natively. Can be extended with custom functions.
- **No managed search service**: build search indices in S3, not Elasticsearch or
  OpenSearch. More operational complexity to build, but zero to operate in BYOC.
- **Agent monitoring uses the same pipeline**: not a separate product. Agents are
  principals in the same model. This is architecturally elegant and lets the model
  learn human-agent interaction patterns.

## Dependencies

- M5: stable BYOC deployment foundation
- M3: model architecture that can incorporate new source types and principal types

## Deliverables

1. Ingestion tier scaled to 100K+ eps with adaptive batching.
2. Storage tiering with automated lifecycle management.
3. DataFusion-based SQL query engine over S3 Parquet.
4. Vector search for similarity-based investigation.
5. GCP and Azure deployment modules and abstraction layer.
6. Agent activity collectors and agent-specific features.
7. Multi-cloud CI/CD pipeline.
