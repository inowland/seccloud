# Seccloud Milestone Plan

This plan sequences the build-out from the current Python PoC to a production-scale
insider-threat demo backed by a Rust data plane and ML scoring pipeline, with BYOC
deployment intentionally deferred until the demo, scale, and operator story are
already strong.

## Guiding Principles

1. **Validate ML before infrastructure.** The contrastive-learning approach must
   produce reliably low false-positive rates without per-customer ML engineering
   before we invest in production infrastructure.
2. **S3 is the source of truth.** Every other store (Postgres, local manifest state, in-process
   indices) is a projection that can be rebuilt. This makes BYOC deployments
   recoverable by default.
3. **Stateless compute, disposable workers.** No worker holds state that cannot be
   reconstructed from S3 + durable manifests/projections. Upgrades are image swaps.
4. **Rust for data plane, Python for ML.** The interface between them is Parquet
   files in the object-store layout and ONNX model artifacts. In local/dev mode the
   bridge is a shared workspace filesystem; in BYOC mode it is S3. No cross-language
   RPC in the hot path.
5. **No distributed open-source infrastructure.** No Kafka, Spark, Flink, or
   Cassandra in customer environments. Build simpler equivalents on S3 + Postgres +
   EKS until a separate coordination store is actually required.
6. **Build for multi-cloud, deploy on AWS first.** Abstract at three points: object
   storage, serving/query store, compute orchestration (Kubernetes). Defer any
   separate coordination-store choice until Postgres and durable manifests stop
   serving the product well.
7. **Build the strongest demo before the hardest deployment.** Prove the product
   through investigation UX, model trust, and scale/resilience before investing in
   customer-account deployment workflows.

## Milestone Sequence

| Milestone                        | Title                               | Key Deliverable                                                |
| -------------------------------- | ----------------------------------- | -------------------------------------------------------------- |
| [M0](m0-ml-validation.md)        | ML Validation                       | Proven detection quality on realistic data                     |
| [M1](m1-ingestion.md)            | Production Ingestion                | Rust ingestion tier and lake layout v1                         |
| [M2](m2-feature-engineering.md)  | Normalization & Feature Engineering | Durable raw-to-features runtime in Rust                        |
| [M3](m3-scoring-pipeline.md)     | Model-Backed Detection Runtime      | End-to-end local ML loop with native Rust ONNX scoring         |
| [M4](m4-query-and-ux.md)         | Investigation UX & Demo Experience  | A compelling live analyst demo                                 |
| [M5](m5-byoc-operations.md)      | Scale, Resilience & Benchmarking    | Production credibility under load and failure                  |
| [M6](m6-scale-and-multicloud.md) | BYOC Deployment & Multi-Cloud       | Deferred customer-account deployment and later cloud expansion |

## Dependency Graph

```
M0 (ML Validation)
 │
 ├──► M1 (Ingestion) ──► M2 (Features) ──► M3 (Model Runtime)
 │                                              │
 │                                              └──► M4 (Demo UX)
 │                                                       │
 │                                                       └──► M5 (Scale / Resilience)
 │                                                                │
 │                                                                └──► M6 (BYOC / Multi-Cloud)
 │
 └──► (M0 runs in parallel with M1; M1-M2 can overlap)
```

M0 is the gate. If contrastive learning does not produce acceptable detection quality,
we revisit the ML approach before proceeding. M1 can begin in parallel with M0 since
the ingestion layer is needed regardless of the ML approach.

The current product focus after M3 is M4: turn the existing runtime into the
strongest possible investigation demo. M5 then proves that the demo is backed by
production-shaped software through load, recovery, and benchmark evidence. BYOC
deployment is intentionally later.

## AWS Services Used

Limited to the following in customer data-plane accounts:

- **S3** — object storage (source of truth)
- **RDS Postgres / DSQL** — projection store, hot query serving, and any structured
  coordination state that outgrows local manifest files
- **EKS** — compute orchestration for Rust workers and Python training jobs
- **EC2** — underlying EKS nodes (including GPU instances for training)
- **ALB** — ingress for push gateway and API

No SNS, Kinesis, Lambda, Glue, EMR, or managed streaming services in the
critical path. This keeps the cloud-abstraction surface small for future GCP/Azure
portability.
