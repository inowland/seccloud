# M5: Scale, Resilience & Benchmarking

## Goal

Prove that the demo is backed by production-shaped software. M5 is about load,
recovery, and hard evidence: the system should keep ingesting, scoring, projecting,
and serving investigations while operators can see what is happening in real time.

## Success Criteria

1. **Sustained load demo**: run representative enterprise-scale synthetic load and
   keep ingest/scoring/projection healthy for an extended session.
2. **Recovery story**: worker restart, projection interruption, dead-letter spikes,
   and backpressure are observable and recover without manual data surgery.
3. **Benchmark evidence**: generate repeatable benchmark reports for throughput,
   lag, detection latency, and investigation read-path responsiveness.
4. **Operator visibility**: queue depth, projection lag, source health, model mode,
   and detection volume are visible in one coherent operator surface.
5. **Demo reliability**: the team can run the live demo repeatedly without
   rebuilding state by hand between attempts.

## Architecture

### Scale Harness

```
Synthetic scale generator ──► Push gateway / collectors
                                  │
                                  ├── Rust worker-service
                                  ├── Feature + scoring runtime
                                  ├── Projection / hot index
                                  └── API + UI + benchmark reporters
```

### Failure And Recovery Drills

- restart worker-service during active ingest
- pause or break projection and verify it catches up
- inject dead-letter-producing records and observe containment
- run with oversized load to show queue growth, then recovery after pressure drops

### Benchmark Outputs

- ingest throughput
- normalization / feature / detection / projection lag
- detection volume and score distributions
- query latency for detection detail and entity timeline
- dead-letter rate and queue depth over time

### Observability Surfaces

- runtime status as the operator control panel
- benchmark artifacts checked into the workspace for repeatable comparison
- “demo health” summaries that explain whether the system is safe to present live
- comparison views that relate load conditions to investigation responsiveness

## Key Decisions

- **Benchmark before deploy**: production claims should come from repeatable local
  or controlled-environment evidence, not architecture diagrams.
- **Recovery matters as much as peak throughput**: a great demo should show that the
  system heals and catches up, not just that it can sprint.
- **Operator trust is part of the product**: queue depth, lag, and failure state
  must be first-class surfaces, not logs-only internals.

## Dependencies

- M1-M4: Rust ingestion/worker runtime, scoring, projection, and investigation UX
- Synthetic scale scenarios and benchmark harnesses

## Deliverables

1. Load-generation and benchmark harness for representative enterprise tenants.
2. Fault-injection and recovery drill suite for the local/shared runtime.
3. Operator-facing lag, throughput, and health reporting.
4. Repeatable benchmark reports suitable for demos and internal review.
5. Demo runbook for reset, warm-up, load, recovery, and live presentation.
