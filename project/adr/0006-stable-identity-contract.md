# ADR 0006: Freeze The V1 Identity Contract

Status: Accepted
Owner: Codex
Last Updated: 2026-03-15
Related Milestone: m01
Related Spec: /Users/inowland/Development/seccloud/project/spec/identity-contract.md
Retirement Gate: Revisit only if first-customer integrations show that replay-safe event identity or canonical entity binding cannot be maintained without materially different semantics.

## Title

Freeze the v1 identity contract for tenants, integrations, events, entities, and alias bindings.

## Context

The repo currently has deterministic helpers in `src/seccloud/ids.py` and generated IDs in `src/seccloud/storage.py`, but the semantics are still implicit:

- `event_key` is deterministic, but only for the current `source_event_id`-based path
- `event_id` is generated from a local manifest without a written replay contract
- `entity_key` is deterministic, but it is effectively acting as both alias key and canonical identity in different parts of the runtime
- `principal.id` and `resource.id` are source-local values that can be mistaken for canonical identities
- `tenant_id` and `integration_id` participate in lake and hot-index contracts, but their identity rules were not yet frozen

If later workstreams implement ingestion, indexing, and case linkage without a stable identity contract, replay and rename behavior will drift.

## Decision

Adopt the following v1 contract:

- `event_key` is the deterministic tenant-local replay key; `event_id` is the immutable platform event reference allocated from `(tenant_id, event_key)`.
- `event_id` and `entity_id` are opaque surrogate IDs, currently generated as prefixed `UUIDv7` values. Their time-sortable component is intentional and does not make them business-semantic IDs.
- `entity_id` is the immutable canonical entity reference; `entity_key` is the deterministic alias-binding key for the alias used to resolve that entity in a given event.
- `principal.id` and `resource.id` remain compatibility fields for source-local aliases and readability, but canonical joins must use `entity_id`.
- `tenant_id` scopes all deterministic keys, manifests, lake objects, and hot-index rows.
- `integration_id` is stable within a tenant and participates in event identity to prevent collisions across configured source instances.
- Alias bindings are first-class logical records with binding-strength precedence so renames and provider alias changes do not create duplicate canonical entities.

The normative rules, examples, and migration notes live in `/Users/inowland/Development/seccloud/project/spec/identity-contract.md`.

## Consequences

- Positive: replay and rebuild can preserve event identity independently from the current local manifest implementation.
- Positive: principal and resource renames no longer require inventing new canonical IDs.
- Positive: hot event index rows and future graph or case surfaces can join on canonical entity IDs while still retaining source-local alias data.
- Positive: the current runtime can migrate incrementally because the existing `entity_key` field is preserved as an alias-binding key.
- Negative: downstream implementations must explicitly distinguish canonical IDs from source-local aliases instead of treating all ID-like fields interchangeably.
- Negative: alias-binding persistence becomes a required subsystem even though this workstream does not implement it.

## Spec Links

- `/Users/inowland/Development/seccloud/project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/m01.md`
- `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/identity-contract.md`
