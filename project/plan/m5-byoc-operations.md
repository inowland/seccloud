# M5: BYOC Deployment & Operations

## Goal

Make the system reliably deployable and operable in customer AWS accounts with
minimal manual intervention. A new customer deployment should take less than 1 day,
and upgrades should be zero-downtime.

## Success Criteria

1. **Deployment time**: new customer environment provisioned and ingesting data
   within 1 business day.
2. **Zero-downtime upgrades**: rolling EKS deployments with no event loss or
   detection gap.
3. **Self-healing**: workers recover automatically from transient failures (S3
   throttling, DynamoDB capacity, pod eviction).
4. **Observability**: control plane has visibility into data plane health across
   all customer deployments.
5. **Data isolation**: cryptographic guarantee that no customer data leaves their
   AWS account.

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
                 │ (TLS, mutual auth)
                 │ metadata only — no customer data
                 │
┌────────────────▼────────────────────────┐
│ Data Plane (customer AWS account)       │
│                                         │
│  ├── EKS cluster                        │
│  │   ├── Ingestion pods                 │
│  │   ├── Normalization workers          │
│  │   ├── Feature engineering workers    │
│  │   ├── Scoring workers                │
│  │   ├── Projection workers             │
│  │   ├── Training jobs (GPU)            │
│  │   └── API / query service            │
│  ├── S3 bucket (all data)               │
│  ├── DynamoDB tables (coordination)     │
│  ├── RDS Postgres (projection store)    │
│  └── ALB (ingress)                      │
└─────────────────────────────────────────┘
```

### What Crosses the Boundary

Control plane → data plane:

- Container image references (ECR in our account, cross-account pull)
- Configuration updates (feature flags, model versions, schema versions)
- Deployment commands (kubectl apply via assumed role)

Data plane → control plane:

- Health metrics (pod status, queue depth, error rates)
- Deployment status (current versions, rollout state)
- Aggregate statistics (event volume, detection count — no PII)

**Never crosses the boundary**: raw events, normalized events, feature vectors,
detection details, user identities, or any data that could identify individuals.

### Infrastructure as Code

- Terraform modules for the full data plane stack
- Parameterized by: AWS region, VPC configuration, instance types, retention
  policies, source integrations
- Modules:
  - `seccloud-eks`: EKS cluster with node groups (CPU + GPU)
  - `seccloud-storage`: S3 bucket, DynamoDB tables, RDS instance
  - `seccloud-networking`: VPC, subnets, ALB, security groups
  - `seccloud-iam`: roles, policies, cross-account trust
  - `seccloud-workloads`: Kubernetes manifests for all services

### Upgrade Path

1. Build and push new container images to ECR
2. Update Kubernetes manifests in customer deployment (via control plane)
3. Rolling deployment: new pods come up, old pods drain using the control message
   protocol from the M1 core crate:
   - `Shutdown` signal injected into the pipeline — stops accepting new events
   - `Flush` signal drains in-memory buffers to S3
   - `BatchComplete` confirms the last batch is written and manifested
   - Pod reports ready-to-terminate via readiness probe
4. Health check gates: new pods must pass readiness checks before old pods terminate
5. Rollback: revert to previous manifest if health checks fail

### Self-Healing

- Worker pods: liveness and readiness probes, restart on failure
- S3 throttling: exponential backoff with jitter (built into Rust workers)
- DynamoDB capacity: on-demand capacity mode (no provisioned throughput to manage)
- Stale work: DynamoDB TTL on work queue items, workers re-claim stale items
- State recovery: any hot store can be rebuilt from S3 source of truth. Envelope
  metadata (batch_id, lineage pointers) provides the checkpoint state needed to
  resume interrupted pipeline stages without re-processing

## Key Decisions

- **No SSH access to customer environments**: all operations via Kubernetes API
  with assumed IAM roles. Audit trail on every action.
- **ECR cross-account image pull**: images built in our CI, pulled by customer EKS.
  Customer controls the IAM trust policy.
- **Terraform, not Pulumi**: broader enterprise adoption, customer infra teams are
  more likely to be familiar with it.

## Dependencies

- M1-M4: all data plane components deployable on EKS
- Design partner agreements for AWS account access

## Deliverables

1. Terraform module suite for full data plane deployment.
2. Control plane service for deployment orchestration and health monitoring.
3. Cross-account IAM and networking design.
4. Upgrade and rollback runbook.
5. Self-healing test suite (fault injection).
6. Customer onboarding automation (provision → configure → ingest in < 1 day).
