# ADR 0005: Freeze The V1 Hot Event Index Contract

Status: Accepted
Owner: Codex
Last Updated: 2026-03-15
Related Milestone: m01
Related Spec: /Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md
Retirement Gate: Revisit only if the Postgres hot index cannot serve event list, event detail, or detection-linked event lookup at acceptable latency without unacceptable storage duplication.

## Title

Freeze the v1 Postgres hot event index schema, query surface, and detection-to-event linkage contract.

## Context

The repo currently uses a temporary projection table, `projected_events`, that stores whole event payloads and supports a demo list view. Event detail and investigation workflows still scan normalized events directly in workspace storage.

That is no longer sufficient for the product-target data plane. The API tier must serve event list, event detail, and detection-linked event lookup from Postgres-backed read surfaces without object-store scans, while still pointing back to immutable retained artifacts in the lake.

WS01 already froze the object-store layout, manifests, and pointer semantics. The remaining missing contract is how Postgres indexes events and links them to detections.

## Decision

Adopt the following v1 contract:

- Use a separate Postgres table family, `hot_event_index` and `detection_event_edge`, rather than treating the current `projected_events` table as the product-target schema.
- Store both flattened query columns and the canonical normalized `event_payload` JSON in `hot_event_index` so `/api/events` and `/api/events/{event_id}` can be served without object-store scans.
- Store the retained normalized pointer and optional raw pointer on the event row using the `ObjectPointer` envelope frozen by WS01.
- Materialize ordered detection-to-event edges in `detection_event_edge`; `ordinal = 0` defines the anchor event.
- Treat the hot event index as rebuildable from retained normalized lake artifacts and detection read models. It is a query surface, not the source of truth.

The normative schema, index requirements, linkage rules, and migration notes live in `/Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md`.

## Consequences

- Positive: the API tier can serve event list and detail from Postgres without workspace scans.
- Positive: detection detail and investigation timelines get a stable relational join surface over retained events.
- Positive: raw-retention behavior is explicit because raw pointers can expire while normalized event rows remain queryable.
- Positive: migration from the current demo projection store can happen incrementally without redefining event IDs or lake pointers.
- Negative: the hot index duplicates normalized event payloads in Postgres for latency and API compatibility.
- Negative: the repo will temporarily carry both the legacy projection table and the new product-target event-index contract until implementation catches up.

## Spec Links

- `/Users/inowland/Development/seccloud/project/spec/product-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/m01.md`
- `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`
- `/Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md`
