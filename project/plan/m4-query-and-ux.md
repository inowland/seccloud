# M4: Investigation UX & Demo Experience

## Goal

Build the investigation experience that turns the runtime into a genuinely compelling
demo. A detection is only valuable if an analyst can quickly understand what
happened, assess severity, decide on a response, and feel that the system is alive,
fast, and coherent.

The starting point for M4 is stronger than the original draft assumed:

- the API already exposes detection detail, timelines, peer comparison, runtime
  status, and case flows
- Postgres-backed hot projection and hot event index already exist
- the UI already consumes durable event-index, feature-table, and detection-context
  state for the demo

So M4 is not "invent the query substrate." It is "turn the existing projected and
indexed runtime into a production-grade analyst experience and the strongest
possible live demo."

## Success Criteria

1. **Detection-to-investigation time**: analyst can view full context for a detection
   within 3 seconds of clicking it.
2. **Entity timeline**: view all activity for a principal across sources within a
   configurable time window.
3. **Peer comparison**: compare a principal's behavior to their peer group in the
   same view.
4. **Evidence bundling**: every detection links to the raw events, feature values,
   and model attributions that produced it.
5. **Case management**: analysts can group detections, add notes, assign cases, and
   track resolution.
6. **Demo narrative**: the UI supports a guided “what changed, why it fired, what
   else is related, what should I do next” flow without needing backend-only
   explanations.

## Architecture

### Query Service

```
API Request ──► Query Service
                    │
                    ├── Hot path: Postgres for recent detections and events
                    ├── Warm path: object-store Parquet scan for historical data
                    └── Cold path: object-store scan with partition pruning for deep investigation
```

- The current implementation is Python/FastAPI backed by Postgres projection plus
  workspace/lake readers; a Rust query service remains optional, not assumed
- Stateless HTTP/gRPC service on EKS remains a valid future target if Python query
  handling becomes the bottleneck
- Query routing: recent data (< 24h) from Postgres, historical from S3 Parquet
- Partition pruning: use lake layout (tenant/date/hour) to minimize S3 scans
- Result caching: in-memory LRU cache per pod (not shared cache — keeps it stateless)

### Hot Event Index

- Postgres-backed hot event index with recent normalized events (retention: 7-14 days)
- Partitioned and indexed by tenant + principal_key + timestamp
- Supports: entity timeline queries, peer comparison lookups, detection evidence
  resolution
- Populated by projection workers (write-behind from S3)

### Investigation API

Extends the current FastAPI endpoints (may migrate selective hot paths later if needed):

- `GET /detections` — paginated detection list with filters
- `GET /detections/{id}` — full detection with evidence, attributions, entity context
- `GET /entities/{principal_key}/timeline` — activity timeline across sources
- `GET /entities/{principal_key}/peers` — peer group comparison
- `POST /cases` — create investigation case from detections
- `PATCH /cases/{id}` — update case status, add notes
- `GET /cases/{id}/evidence` — bundled evidence for a case

### Frontend (React)

Evolve the current frontend rather than replacing it:

- **Detection queue**: prioritized by severity and score, filterable by source,
  scenario type, principal
- **Detection detail**: split-pane with detection summary, evidence timeline,
  feature attributions, peer comparison
- **Entity view**: cross-source activity timeline for a principal, baseline vs
  current behavior visualization
- **Case workspace**: group related detections, collaborative notes, status tracking
- **Dashboard**: detection volume trends, model health, source coverage
- **Guided demo flow**: support a presenter path that can move from queue →
  detection → entity → case while surfacing model/runtime health alongside the
  investigation itself

## Key Decisions

- **Postgres for hot index and cases**: keep the recent event index, case
  management, analyst workflows, and complex queries on one serving substrate
  until it is clearly the bottleneck.
- **API/query layer stays Python until proven otherwise**: the current FastAPI
  layer already serves the demo well, and the hot-query substrate is Postgres.
  Migrate specific hot paths to Rust only if latency or concurrency data says it is
  necessary.
- **Demo polish outranks backend replacement**: if a choice is between a brand-new
  query stack and a more legible, faster, better explained investigation flow, pick
  the latter.

## Dependencies

- M3: durable scored detections and model-backed scoring outputs
- M2: projected events, hot event index, entity keys, and durable runtime status

## Deliverables

1. Investigation/query contract refresh grounded in the existing Postgres and
   event-index runtime.
2. Postgres hot event index hardening and projection/query optimizations.
3. Investigation API (detection detail, entity timeline, peer comparison, cases).
4. Production React frontend with detection queue, detail view, entity view, and
   case management.
5. Evidence bundling for export and compliance.
6. Guided demo flow that makes the system easy to present live.
