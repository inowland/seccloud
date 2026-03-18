# M6: BYOC Deployment & Multi-Cloud

## Goal

Deploy the already-proven product into customer-owned environments when the demo,
scale, resilience, and operator story are mature enough to justify the extra
operational complexity. Multi-cloud remains an even later extension of the same
deployment work.

## Success Criteria

1. **Deployment repeatability**: provision a customer-style environment and ingest
   data with minimal manual intervention.
2. **Upgrade safety**: rolling deployment, rollback, and recovery work without event
   loss or detection gaps.
3. **Data-plane isolation**: no customer event or detection payloads leave the
   customer environment.
4. **Operational visibility**: control-plane-visible health metadata without pulling
   customer data across the boundary.
5. **Cloud portability**: the object-store, serving-store, and orchestration
   abstractions are clean enough that AWS-first deployment does not permanently
   trap the architecture.

## Architecture

### Control Plane / Data Plane Split

```
┌─────────────────────────────────────────┐
│ Control Plane (our AWS account)         │
│                                         │
│  ├── Deployment orchestration           │
│  ├── Configuration management           │
│  ├── Health monitoring dashboard        │
│  ├── Model artifact registry            │
│  └── Customer management                │
└────────────────┬────────────────────────┘
                 │ metadata only — no customer data
┌────────────────▼────────────────────────┐
│ Data Plane (customer account)           │
│                                         │
│  ├── EKS / equivalent cluster           │
│  ├── Object store                       │
│  ├── Postgres / projection store        │
│  ├── Rust worker-service + scoring      │
│  └── API / query service                │
└─────────────────────────────────────────┘
```

### What Crosses The Boundary

Control plane → data plane:

- container image references
- configuration and feature-flag updates
- model version rollout commands
- deployment / rollback commands

Data plane → control plane:

- health metrics
- queue depth and lag summaries
- deployment status
- aggregate counts that contain no customer-identifying payloads

Never crosses the boundary:

- raw events
- normalized events
- feature values
- detections and investigation payloads
- user identities

### Infrastructure As Code

- Terraform modules for the full data plane stack
- Parameterized by region, storage class, instance type, retention, and enabled
  integrations
- Customer-facing deployment should reuse the same object-store and projection
  contracts proven in earlier milestones

### Multi-Cloud Abstraction

Three abstraction points remain the same:

| Capability                   | AWS      | GCP                              | Azure               |
| ---------------------------- | -------- | -------------------------------- | ------------------- |
| Object storage               | S3       | GCS                              | Azure Blob          |
| Serving / coordination store | Postgres | Cloud SQL / Bigtable / Firestore | Postgres / CosmosDB |
| Compute orchestration        | EKS      | GKE                              | AKS                 |

- Cloud-specific sinks (S3, GCS, Azure Blob) are each a `Transform` implementation
  from the M1 core crate. Swapping clouds means swapping a transform in the pipeline
  config — pipeline logic and upstream transforms are untouched.
- Rust trait-based abstraction for object store and serving/coordination store
- Kubernetes manifests are cloud-agnostic (already)
- Terraform provider modules per cloud, shared module structure
- CI builds and tests against all three providers

## Key Decisions

- **BYOC is late on purpose**: deployment complexity should follow product clarity,
  not lead it.
- **AWS first, multi-cloud later**: do not pay abstraction tax until the AWS path is
  truly solid.
- **Control-plane metadata only**: customer payload isolation is a first-order
  architectural rule, not a policy note.

## Dependencies

- M5: demonstrated load, resilience, and operator readiness
- M1-M4: stable runtime, scoring, projection, and demo surfaces

## Deliverables

1. Terraform module suite for the data plane deployment.
2. Control-plane service for deployment orchestration and health monitoring.
3. Upgrade, rollback, and recovery runbooks.
4. Cross-account IAM and networking design.
5. AWS-first deployment path, with later GCP/Azure modules if still justified.
