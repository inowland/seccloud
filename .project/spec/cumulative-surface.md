# Cumulative Surface

## Purpose
This document lists the approved product surface and the current state of each surface area. It is the authoritative inventory of what exists conceptually and whether it is planned, implemented, superseded, missing, or obsolete.

## Status Vocabulary
- `planned`: approved in product truth but not implemented
- `implemented`: implemented and supported
- `superseded`: replaced by a newer supported surface
- `missing`: required for current milestone goals but not implemented
- `obsolete`: intentionally retired and no longer supported

## Surface Inventory

| Surface | Description | State | Owning Milestone |
| --- | --- | --- | --- |
| Source adapters | Supported collectors and normalizers for the fixed PoC source pack. | `implemented` | `m01` |
| Normalized event schema | Versioned canonical event contract for ingestion and downstream consumers. | `implemented` | `m01` |
| Raw event object storage | Append-only short-lived storage for immutable source evidence. | `implemented` | `m01` |
| Manifest and catalog metadata | Metadata required for replay, idempotency, retention, and evidence lookup. | `implemented` | `m01` |
| Source manifests and fixture validation | Productized onboarding contract and compatibility checks for the fixed source pack. | `implemented` | `m01` |
| Vendor export mapping and validation | Translation and compatibility checks from vendor-shaped exports into the raw-event contract. | `implemented` | `m01` |
| Derived-state materialization | Customer-local long-lived state required for scoring and investigations. | `implemented` | `m01` |
| Entity timeline retrieval | Timeline assembly by principal, resource, or case context. | `implemented` | `m02` |
| Evidence retrieval | Retrieval of immutable evidence pointers and related context. | `implemented` | `m02` |
| Case workflow | Detection-backed case creation, disposition, and analyst feedback. | `implemented` | `m02` |
| Investigation summaries and peer comparison | Analyst-facing summary and peer-context views for explainable review. | `implemented` | `m02` |
| Demo stream simulator | Incremental event-release controls for the internal demo environment. | `implemented` | `m02` |
| Demo Postgres projection store | Temporary demo read model projected from the PoC write path. | `implemented` | `m02` |
| Demo API surface | Temporary API exposing the projected demo state for the web UI. | `implemented` | `m02` |
| Demo React UI | Temporary analyst-facing web demo rendered from projected demo state. | `implemented` | `m02` |
| Baseline policy packs | Productized scenario packs for the privileged behavior PoC workflows. | `implemented` | `m02` |
| Scoring service | Rules and heuristics scoring interface and execution path for the PoC. | `implemented` | `m03` |
| Source-pack capability matrix | Runtime-derived view of required source signals, observed coverage, and ingest failures. | `implemented` | `m01` |
| Feature extraction and model registry | Stable feature and model artifact contracts for customer-local scoring. | `missing` | `m03` |
| Response actions | Narrow high-confidence actions such as step-up auth or approval gating. | `planned` | `m04` |
| Agent identities | Non-human principals modeled as first-class identities. | `planned` | `m05` |
| Agent-aware case timelines | Mixed human and agent investigations in one workflow. | `planned` | `m05` |

## Update Rules
- Do not use this document to speculate about future features beyond approved product truth.
- Do not mark a surface as `implemented` until execution artifacts exist.
- If a surface is replaced, mark the old entry `superseded` or `obsolete` and add the replacement explicitly.
