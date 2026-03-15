# ADR 0003: Worker-Separated Data Plane On Object Storage And Queue

Status: Accepted
Owner: Codex
Last Updated: 2026-03-15
Related Milestone: m01, m02, m03
Related Spec: /Users/inowland/Development/seccloud/project/spec/product-contract.md
Retirement Gate: Revisit only if first-customer integrations prove that the object-storage-plus-queue substrate cannot satisfy ingest volume, replay, or investigation latency requirements.

## Title

Use separate collector and processing workers, customer-local object storage, and an asynchronous queue for the product data plane.

## Context

The current repo proves the core contracts with a local filesystem-backed runtime and a demo API surface. That was the right first step, but it is no longer enough for the next product question.

The next risk is not whether the system can normalize and score synthetic events. The next risk is whether the product can ingest real customer telemetry through both pull-based integrations and high-volume pushed logs without coupling processing work to the stateless API tier.

The architecture must preserve the product contract:

- vendor-run, single-tenant BYOC inside the customer account
- short-lived raw telemetry
- long-lived retained analytical state needed for replay, detections, and investigations
- evidence-backed detections that link back to immutable source material

## Decision

Adopt the following product-target data plane:

- The API service is stateless and read-oriented. It serves projections, detections, cases, event lookup, and evidence retrieval, but it does not perform ingestion or detection work inline.
- Scheduled collector workers call supported customer APIs and write raw intake batches into customer-local object storage.
- An authenticated ingestion gateway accepts high-volume pushed telemetry, such as Vector-originated logs, and writes the same raw intake batches into customer-local object storage.
- A shared asynchronous work queue coordinates normalization, replay, enrichment, detection, and projection work. For the first AWS target this means `S3` for durable objects and `SQS` for queue semantics.
- Normalization workers convert all inbound data into a versioned canonical event contract, persist dead letters, and materialize normalized analytical event batches back to object storage in columnar form.
- Detection and materialization workers update derived state, create detections, and maintain the application-facing read models.
- `Postgres` stores detections, cases, checkpoints, manifests, projection state, and a hot event index that links stable `event_id` values to object-storage locations and investigation metadata.
- The system does not require a dedicated full-text or observability search engine in v1. Broader arbitrary search remains a later addition if customer workflows prove it necessary.

## Consequences

- Positive: ingestion and detection scale independently from the stateless API tier.
- Positive: the product supports both pull-based source adapters and push-based log shipping without bespoke pipelines per customer.
- Positive: replay and rebuild stay aligned with the retention and evidence contract because raw and normalized artifacts remain in customer-local object storage.
- Positive: `S3` is used for durable storage, but queue semantics come from `SQS`, which avoids forcing `S3` alone to act as a queue.
- Negative: the product-target architecture is now explicitly more complex than the local PoC runtime.
- Negative: the repo must continue to distinguish between the current local simulation and the intended customer deployment architecture.

## Spec Links

- `/Users/inowland/Development/seccloud/project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/m01.md`
