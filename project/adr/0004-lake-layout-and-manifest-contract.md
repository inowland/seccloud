# ADR 0004: Freeze The V1 Lake Layout And Manifest Contract

Status: Accepted
Owner: Codex
Last Updated: 2026-03-15
Related Milestone: m01
Related Spec: /Users/inowland/Development/seccloud/project/spec/lake-contract.md
Retirement Gate: Revisit only if the v1 object-store layout cannot support replay, raw-retention enforcement, or evidence lookup without unacceptable operational cost.

## Title

Freeze the v1 raw and normalized lake layout, manifest contract, and object-pointer semantics before implementing ingestion or indexing.

## Context

The repo currently simulates the data plane with per-object JSON files and shared mutable manifest files in `src/seccloud/storage.py`. That simulation was useful for proving the first workflows, but it leaves several storage contracts ambiguous:

- whether raw landing is per-event JSON or batch-oriented
- whether normalized storage is JSON or columnar
- how detections and indexes identify a specific record inside a retained batch
- which manifest fields are required for idempotency, replay, and retention

If those contracts remain implicit, later workstreams will re-interpret them independently and drift the data plane before the first product-target implementation exists.

## Decision

Adopt the following v1 contract:

- Raw landing uses immutable `jsonl.gz` batch objects under the `lake/raw/` prefix.
- Normalized analytical storage uses immutable `Parquet` batch objects under the `lake/normalized/` prefix.
- Every raw and normalized batch publishes one immutable manifest JSON object under `lake/manifests/`.
- Manifests carry the minimum required fields for replay and dedupe: stable `batch_id`, `idempotency_key`, time bounds, ordered object descriptors, and an opaque checkpoint payload.
- Evidence and future event indexes use a versioned object-pointer envelope that identifies the object, the manifest, and an optional zero-based record ordinal within the object.
- Raw pointers are explicitly short-lived. Any workflow that must survive raw retention must also keep a retained normalized or derived pointer and structured evidence fields.

The normative schema, key patterns, examples, and migration notes live in `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`.

## Consequences

- Positive: later ingestion, replay, and indexing work can share one stable object contract instead of inferring it from the current simulation.
- Positive: raw landing becomes batch-oriented without losing source-fidelity or replay semantics.
- Positive: normalized storage aligns with the milestone's columnar-storage requirement from day one.
- Positive: evidence and index records can identify a specific retained event without depending on per-event JSON objects.
- Negative: the local runtime now clearly diverges from the product-target storage contract and will need an adapter or migration path.
- Negative: raw evidence retrieval becomes stream-or-scan based within a compressed batch object instead of direct single-file lookup.

## Spec Links

- `/Users/inowland/Development/seccloud/project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/m01.md`
- `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`
