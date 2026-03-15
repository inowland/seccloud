# WS03 Identity Contract

Status: Done

## Goal

Define the stable identity model for events, entities, integrations, and customer-visible references.

## Why This Is Still Serial

Identity touches every major store and interface. If multiple agents invent ID ownership independently, replay, evidence, and investigation links will drift.

## In Scope

- `event_id` versus `event_key`
- `entity_id` versus alias and provider-native bindings
- tenant and integration identity rules
- which IDs are internal-only versus product-visible
- naming and prefix conventions for platform-assigned IDs

## Out Of Scope

- lake batch format
- Postgres index schema details beyond ID usage
- auth and request signing
- source-specific collector implementations

## Inputs

- [WS01 Lake Layout And Manifest Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws01-lake-layout-and-manifest-contract.md)
- [WS02 Hot Event Index Contract](/Users/inowland/Development/seccloud/project/planning/workstreams/ws02-hot-event-index-contract.md)
- [ids.py](/Users/inowland/Development/seccloud/src/seccloud/ids.py)
- [pipeline.py](/Users/inowland/Development/seccloud/src/seccloud/pipeline.py)
- [contracts.py](/Users/inowland/Development/seccloud/src/seccloud/contracts.py)

## Deliverables

- authoritative contract spec: [identity-contract.md](/Users/inowland/Development/seccloud/project/spec/identity-contract.md)
- decision ADR: [0006-stable-identity-contract.md](/Users/inowland/Development/seccloud/project/adr/0006-stable-identity-contract.md)
- alias-binding model for principals and resources in the spec
- examples showing replay, rename, and source-ID collision handling in the spec
- migration notes for current principal/resource identity usage in the spec

## Required Decisions

- event-key construction priority order when provider-native IDs are weak or absent
- alias-binding persistence model for entities
- whether customer-facing APIs should expose `entity_id`, alias data, or both
- how tenant and integration IDs participate in stable keys

## Validation

- replay of the same source event preserves event identity semantics
- principal rename and provider alias changes do not create duplicate canonical entities
- detection and case references remain stable after rebuilds

## Outcome

This workstream freezes the following v1 decisions:

- `event_key` is the deterministic tenant-local replay key and `event_id` is the canonical immutable event reference
- `entity_id` is canonical, while `entity_key` is the deterministic alias-binding key used to resolve that entity in an event
- `principal.id` and `resource.id` remain source-local compatibility aliases, not canonical cross-event identities
- `tenant_id` scopes deterministic identity and `integration_id` participates in event identity to prevent source-instance collisions
- alias bindings are first-class logical records with strength ordering so renames do not create duplicate canonical entities

## Conflict Boundary

No later workstream should invent alternate event or entity identity rules.
