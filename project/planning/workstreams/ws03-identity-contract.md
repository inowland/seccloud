# WS03 Identity Contract

Status: Planned

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

- identity spec or ADR defining each stable ID type
- alias-binding model for principals and resources
- examples showing replay, rename, and source-ID collision handling
- migration notes for current principal/resource identity usage

## Required Decisions

- event-key construction priority order when provider-native IDs are weak or absent
- alias-binding persistence model for entities
- whether customer-facing APIs should expose `entity_id`, alias data, or both
- how tenant and integration IDs participate in stable keys

## Validation

- replay of the same source event preserves event identity semantics
- principal rename and provider alias changes do not create duplicate canonical entities
- detection and case references remain stable after rebuilds

## Conflict Boundary

No later workstream should invent alternate event or entity identity rules.
