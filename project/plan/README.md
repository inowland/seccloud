# Seccloud Milestone Plan

This plan sequences the build-out from the current Python PoC to a production BYOC
insider-threat detection platform with a Rust data plane and ML scoring pipeline.

## Guiding Principles

1. **Validate ML before infrastructure.** The contrastive-learning approach must
   produce reliably low false-positive rates without per-customer ML engineering
   before we invest in production infrastructure.
2. **S3 is the source of truth.** Every other store (Postgres, DynamoDB, in-process
   indices) is a projection that can be rebuilt. This makes BYOC deployments
   recoverable by default.
3. **Stateless compute, disposable workers.** No worker holds state that cannot be
   reconstructed from S3 + DynamoDB coordination metadata. Upgrades are image swaps.
4. **Rust for data plane, Python for ML.** The interface between them is Parquet
   files in S3 and ONNX model artifacts. No cross-language RPC in the hot path.
5. **No distributed open-source infrastructure.** No Kafka, Spark, Flink, or
   Cassandra in customer environments. Build simpler equivalents on S3 + DynamoDB +
   EKS.
6. **Build for multi-cloud, deploy on AWS first.** Abstract at three points: object
   storage, coordination store, compute orchestration (Kubernetes). Everything else
   is cluster-native.
7. **Ship to design partners early and continuously.** Each milestone produces a
   deployable artifact, not a whiteboard diagram.

## Milestone Sequence

| Milestone | Title | Key Deliverable |
|-----------|-------|-----------------|
| [M0](m0-ml-validation.md) | ML Validation | Proven detection quality on realistic data |
| [M1](m1-ingestion.md) | Production Ingestion | Rust ingestion tier, S3 lake layout v1 |
| [M2](m2-feature-engineering.md) | Normalization & Feature Engineering | Raw-to-features pipeline in Rust on EKS |
| [M3](m3-scoring-pipeline.md) | Training Pipeline & Real-Time Scoring | End-to-end ML loop, first design partner on real data |
| [M4](m4-query-and-ux.md) | Query Layer & Investigation UX | Actionable investigation experience |
| [M5](m5-byoc-operations.md) | BYOC Deployment & Operations | Repeatable customer deployment in < 1 day |
| [M6](m6-scale-and-multicloud.md) | Scale & Multi-Cloud | 100K+ eps per tenant, first non-AWS deployment |

## Dependency Graph

```
M0 (ML Validation)
 │
 ├──► M1 (Ingestion) ──► M2 (Features) ──► M3 (Scoring)
 │                                              │
 │                                              ├──► M4 (Query & UX)
 │                                              │
 │                                              └──► M5 (BYOC Ops)
 │                                                       │
 │                                                       └──► M6 (Scale)
 │
 └──► (M0 runs in parallel with M1; M1-M2 can overlap)
```

M0 is the gate. If contrastive learning does not produce acceptable detection quality,
we revisit the ML approach before proceeding. M1 can begin in parallel with M0 since
the ingestion layer is needed regardless of the ML approach.

## AWS Services Used

Limited to the following in customer data-plane accounts:

- **S3** — object storage (source of truth)
- **DynamoDB** — coordination, work queues, checkpoints, hot metadata
- **EKS** — compute orchestration for Rust workers and Python training jobs
- **EC2** — underlying EKS nodes (including GPU instances for training)
- **ALB** — ingress for push gateway and API
- **RDS Postgres / DSQL** — projection store for investigations and case management

No SQS, SNS, Kinesis, Lambda, Glue, EMR, or managed streaming services in the
critical path. This keeps the cloud-abstraction surface small for future GCP/Azure
portability.
