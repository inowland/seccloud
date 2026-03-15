# ADR 0001: Filesystem-Backed PoC Substrate

Status: Accepted
Owner: Codex
Last Updated: 2026-03-14
Related Milestone: m01, m02, m03
Related Spec: /Users/inowland/Development/seccloud/project/spec/product-contract.md
Retirement Gate: Replace when the PoC moves from local simulation to real cloud object storage and runtime services.

## Title

Use a filesystem-backed object-store simulation for the PoC while preserving cloud-portable interfaces.

## Context

The repo had no existing implementation. The first build needs to de-risk canonical contracts, replay, retention, derived-state rebuild, evidence lookup, and case flow before real cloud deployment work.

The product contract remains AWS-first and BYOC, but the PoC must be runnable locally with synthetic data and no external dependencies.

## Decision

Implement the first substrate as a local filesystem-backed runtime with:

- raw object storage directories that simulate immutable object storage
- normalized event segments and ingest manifests
- JSON-backed retained derived state, detections, and cases
- a stable set of interfaces for ingest, scoring, investigation, retention, and reporting

This keeps the contracts portable while avoiding premature cloud provisioning work.

## Consequences

- Positive: the PoC is runnable in any local environment with Python alone.
- Positive: replay, retention, and investigation logic can be tested deterministically.
- Negative: operational characteristics of real cloud services are not exercised yet.
- Negative: this substrate must not be mistaken for the final deployment architecture.

## Spec Links

- `/Users/inowland/Development/seccloud/project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/m01.md`
- `/Users/inowland/Development/seccloud/project/spec/m02.md`
