# M4: Query Layer & Investigation UX

## Goal

Build the investigation experience that makes detections actionable for SOC analysts.
A detection is only valuable if an analyst can quickly understand what happened, assess
severity, and decide on a response.

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

## Architecture

### Query Service (Rust)

```
API Request ──► Rust Query Service
                    │
                    ├── Hot path: DynamoDB/Postgres for recent detections and events
                    ├── Warm path: S3 Parquet scan for historical data
                    └── Cold path: S3 scan with partition pruning for deep investigation
```

- Stateless HTTP/gRPC service on EKS
- Query routing: recent data (< 24h) from DynamoDB, historical from S3 Parquet
- Partition pruning: use lake layout (tenant/date/hour) to minimize S3 scans
- Result caching: in-memory LRU cache per pod (not shared cache — keeps it stateless)

### Hot Event Index

- DynamoDB table with recent normalized events (retention: 7-14 days)
- Partitioned by tenant + principal_key, sorted by timestamp
- Supports: entity timeline queries, peer comparison lookups, detection evidence
  resolution
- Populated by projection workers (write-behind from S3)

### Investigation API

Extends the current FastAPI endpoints (may migrate to Rust later):

- `GET /detections` — paginated detection list with filters
- `GET /detections/{id}` — full detection with evidence, attributions, entity context
- `GET /entities/{principal_key}/timeline` — activity timeline across sources
- `GET /entities/{principal_key}/peers` — peer group comparison
- `POST /cases` — create investigation case from detections
- `PATCH /cases/{id}` — update case status, add notes
- `GET /cases/{id}/evidence` — bundled evidence for a case

### Frontend (React)

Evolve the current PoC frontend:

- **Detection queue**: prioritized by severity and score, filterable by source,
  scenario type, principal
- **Detection detail**: split-pane with detection summary, evidence timeline,
  feature attributions, peer comparison
- **Entity view**: cross-source activity timeline for a principal, baseline vs
  current behavior visualization
- **Case workspace**: group related detections, collaborative notes, status tracking
- **Dashboard**: detection volume trends, model health, source coverage

## Key Decisions

- **DynamoDB for hot index, not Postgres**: DynamoDB handles the access pattern
  (point lookups by principal + time range) more efficiently and scales without
  connection management overhead in BYOC environments.
- **Postgres for cases and structured queries**: case management, analyst workflows,
  and complex queries (joins, aggregations) stay in Postgres.
- **API layer stays Python initially**: FastAPI is productive for investigation
  workflows where latency requirements are human-scale (< 1s). Migrate to Rust
  only if Python becomes a bottleneck.

## Dependencies

- M3: detections in S3 and projected to hot stores
- M2: normalized events in S3 with entity keys for timeline queries

## Deliverables

1. Rust query service for event timeline and entity history.
2. DynamoDB hot event index design and projection workers.
3. Investigation API (detection detail, entity timeline, peer comparison, cases).
4. Production React frontend with detection queue, detail view, entity view, and
   case management.
5. Evidence bundling for export and compliance.
