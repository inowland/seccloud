# M1: Production Ingestion Layer

## Goal

Replace the Python push gateway with a Rust ingestion tier, establish the lake layout
that the rest of the system will build on, and converge the local data plane onto a
Rust-first runtime. The implementation sequence is
local-first: shared-workspace filesystem adapters and cross-language compatibility
land before AWS-specific adapters.

## Success Criteria

1. **Bridge compatibility**: Rust-written raw Parquet batches are consumable through
   the shared workspace contract with no dual-write path.
2. **Durability**: no event loss once an HTTP 202 is returned to the client.
3. **Lake layout v1**: object-key structure and Parquet schema are versioned and documented.
4. **Backpressure**: graceful degradation under load (reject with 429, don't OOM).
5. **Dead-letter handling**: malformed events are quarantined, not dropped, and do not
   corrupt raw-batch manifests or idempotency state.
6. **Collector framework**: at least 4 pull-based collectors (Okta, Google Workspace,
   GitHub, Snowflake) with checkpoint-based resumption.
7. **Performance gate**: local load test demonstrates baseline throughput; 10K eps on a
   single pod remains the cloud-adapter follow-up target after batching/S3 tuning.

## Architecture

### Core Pipeline Crate (`seccloud-pipeline`)

All data plane services (ingestion, normalization, scoring) share a core crate that
defines the composable pipeline primitives. Every processing stage has the same shape:

```rust
/// Every stage — validation, normalization, routing, sinking — is a Transform.
trait Transform: Send + Sync {
    fn apply(&self, ctx: &Context, msg: Envelope) -> Result<Vec<Envelope>>;
}

/// Payload stays immutable after ingestion; metadata accumulates processing state.
struct Envelope {
    payload: Bytes,
    metadata: EventMetadata,         // batch_id, source, intake_ts, attempt_count, lineage
    control: Option<ControlSignal>,  // pipeline flow control
}

enum ControlSignal { Flush, BatchComplete, Shutdown }
```

Key design properties (inspired by Substation):

- **Fan-out**: a transform returns `Vec<Envelope>` — it can produce 0 (drop/filter),
  1 (map), or N (split) messages.
- **Data/metadata separation**: the raw event payload is immutable after ingestion.
  Processing state (dead-letter reason, retry count, lineage pointers) accumulates in
  metadata without polluting the event.
- **Control messages**: `Flush`, `BatchComplete`, and `Shutdown` signals flow through
  the same channel as data. This gives clean batch boundary semantics (for hitting the
  64-128 MB Parquet target) and graceful drain (for zero-downtime upgrades in M5).
- **Routing is transformation**: dead-letter handling, lake writes, and durable manifest/index updates
  are all transforms in the chain, not special-cased branches. This eliminates a class
  of bugs where the error path diverges from the happy path.

### Push Gateway (Rust)

The push gateway is a transform-driven service with a local-first persistence backend:

```
Client/Webhook ──► ALB / local dev client ──► Rust HTTP Service (axum)
                              │
                              Authenticate
                              │
                              Validate (shared pipeline)
                              │
                              ├──► valid batch write ──► lake/raw/*.parquet + intake manifest
                              │
                              └──► dead-letter write ──► lake/dead-letter/*.parquet
                              │
                              Queue entry / accepted-batch index
```

- Stateless compute; in local/dev mode the shared workspace is the persistence layer
- Horizontal scaling via EKS HPA on CPU/memory/request-rate remains the production target
- Authentication: bearer token per integration; local dev uses env-backed credentials,
  BYOC uses coordination-store backed credentials
- Schema validation: reject malformed payloads before buffering
- Dead-letter routing: validation failures produce an envelope with
  `metadata.dead_letter_reason` set, routed to a dead-letter sink transform
  (same pipeline, not a separate codepath)

### Pull Collectors (Rust)

- Run as EKS CronJobs or long-polling pods
- Each collector implements a trait: `poll() → Vec<RawEvent>`, `checkpoint() → CheckpointState`
- Checkpoints stored in durable manifest or Postgres state first; revisit a
  separate coordination store only if that becomes a bottleneck
- Rate limiting and backoff per source API
- Initial sources: Okta System Log, Google Workspace Activity, GitHub Audit Log,
  Snowflake Access History

### S3 Lake Layout v1

```
s3://{bucket}/{tenant}/v1/raw/{source}/{YYYY}/{MM}/{DD}/{HH}/{batch-id}.parquet
s3://{bucket}/{tenant}/v1/dead-letter/{source}/{YYYY}/{MM}/{DD}/{batch-id}.parquet
s3://{bucket}/{tenant}/v1/manifests/intake/{batch-id}.json
```

- Partition by tenant, source, and hour for efficient scan and lifecycle management
- Parquet files target 64-128 MB (batch multiple flushes if needed)
- Intake manifests reference their Parquet files and record row counts, byte sizes,
  and schema version

### Coordination State

- Local/dev mode: workspace-backed queue files plus manifest indexes for accepted batches
  and collector checkpoints
- BYOC mode: Postgres tables and/or durable manifest state for:
  - `intake-queue`: pending batches awaiting normalization
  - `collector-checkpoints`: per-source checkpoint state
  - `integration-config`: per-source configuration and credentials references

## Key Decisions

- **Parquet from ingestion**: raw events are written as Parquet, not JSON/JSONL. This
  means downstream processing never parses JSON again. Cost: slightly more complex
  ingestion code. Benefit: massive downstream performance win.
- **No separate message bus yet**: workers poll the queue abstraction directly. In
  local mode it is filesystem-backed; in BYOC mode the first production path should
  remain Postgres/manifests until that is demonstrably insufficient.
- **Tenant isolation at the S3 key level**: no shared prefixes between tenants. A
  tenant's data can be deleted by prefix. IAM policies can scope to tenant prefix.
- **Composable transform pipeline**: the `Transform` trait and `Envelope` type are
  defined in M1 and used through M2-M6. This is a one-time design cost (~1 week) that
  pays dividends: M2 normalization, M3 scoring, and M6 multi-cloud sinks all compose
  over the same abstraction. Control messages reuse the same channel for batch
  boundaries (M1), graceful drain (M5), and adaptive batching (M6).

## Dependencies

- M0 informs the raw event schema (what fields are needed for ML features), but M1
  can begin with the existing PoC schema and evolve.

## Deliverables

1. `seccloud-pipeline` core crate: `Transform` trait, `Envelope`, `ControlSignal`,
   `Chain` executor, and sink/dead-letter primitives.
2. Rust ingestion service (push gateway) deployable on EKS, built as a transform chain.
3. Rust collector framework with 4 source implementations.
4. S3 lake layout v1 specification and Parquet schemas.
5. Local coordination-store adapters for accepted-batch idempotency, intake queue, and
   collector checkpoints, all compatible with the existing Python workspace manifests.
6. Cloud adapter follow-up: Postgres-backed coordination schema and Terraform for
   ingestion tier (EKS, ALB, Postgres, S3).
7. Load test demonstrating baseline local throughput, with cloud 10K eps validation as
   the next optimization gate.

---

## Work Streams

### Context

M0 validated the ML approach (5/6 scenarios ROC AUC > 0.97). M1 replaces the Python
ingestion PoC with a Rust ingestion tier that can handle production enterprise log
volumes. The Substation-inspired design (Transform trait, Envelope, ControlSignal)
established in the M1 plan doc provides the architectural foundation.

**Constraints:**

- Solo developer — work streams are mostly sequential
- Gradual replacement — API, scoring, and UI surfaces must keep working while the
  Rust data plane lands
- Parquet-only output (update Python reader, no dual-write)
- Shared workspace filesystem for local object-store and coordination-state dev
- Local k8s (kind/minikube) instead of EKS — defer cloud infra to later
- Both push gateway and pull collectors needed equally for design partner

### Critical Compatibility Surface

The Rust gateway must produce output that downstream workspace readers can consume.
The bridge point is `Workspace.read_raw_batch_records()` at `storage.py:722-731`,
which historically read gzipped JSONL. We will update this to read Parquet instead.

Key files in the bridge:

- `src/seccloud/object_store.py` — already has `S3ObjectStore` (can read from LocalStack)
- `src/seccloud/storage.py:588-720` — `land_raw_intake_batch()` defines manifest format
- `src/seccloud/storage.py:722-731` — `read_raw_batch_records()` (update to pyarrow)
- `src/seccloud/workers.py` — local worker entrypoints that consume the same
  workspace contracts

---

### WS1: Rust Workspace + Pipeline Core Crate (1 week)

**Goal:** Establish the Rust cargo workspace, define the Transform/Envelope/ControlSignal
abstractions, set up local filesystem-backed dev flows and CI. This crate is the foundation
for every subsequent work stream and for M2-M6.

**What to build:**

1. Cargo workspace layout:

   ```
   rust/
     Cargo.toml                    # workspace root
     crates/seccloud-pipeline/     # Transform trait, Envelope, ControlSignal, Chain
     crates/seccloud-ingestion/    # push gateway (populated in WS3)
     crates/seccloud-collector/    # collector framework (populated in WS4)
   ```

2. `seccloud-pipeline` core types:

   ```rust
   trait Transform: Send + Sync {
       async fn apply(&self, ctx: &Context, msg: Envelope) -> Result<Vec<Envelope>>;
   }

   struct Envelope {
       payload: Bytes,                      // raw event bytes, immutable after ingestion
       metadata: EventMetadata,             // batch_id, source, tenant_id, intake_ts, etc.
       control: Option<ControlSignal>,
   }

   enum ControlSignal { Flush, BatchComplete, Shutdown }

   struct Context {
       tenant_id: String,
       config: Arc<PipelineConfig>,
   }
   ```

3. Chain executor — sequences transforms, handles fan-out (`Vec` return), routes
   dead-letter envelopes (where `metadata.dead_letter_reason.is_some()`).

4. In-memory test utilities:
   - `TestSink`: captures output envelopes for assertions
   - `FixtureSource`: emits configurable envelope sequences
   - Property tests: chain of [A → B → TestSink] where A fans out, B filters

5. Local dev adapters:
   - local object-store adapter rooted at the shared workspace
   - local intake-queue adapter rooted at `intake/pending/*.json`
   - local accepted-batch and collector-checkpoint indexes rooted at `manifests/*.json`

6. CI: add Rust build/test/clippy/fmt to existing pipeline. Python steps unchanged.

**Validation:**

- `cargo test` passes for Transform, Envelope, Chain fan-out, dead-letter routing
- local runtime writes compatible manifests and queue files under the workspace root
- CI runs Rust and Python tests

**Dependencies:** None.

### M1a / M1b Split

- **M1a (implemented first):** local shared-workspace bridge, Parquet contract, Rust
  gateway/collectors, persisted local idempotency/checkpoints, and the first
  Rust-first local worker path.
- **M1b (follow-up):** cloud adapters for S3/Postgres/EKS, larger cross-request batching,
  pod-level throughput tuning, and the 10K eps validation gate.

---

### WS2: Lake Layout + Storage Layer + Workspace Bridge (2 weeks)

**Goal:** Implement S3 storage, Parquet schema, durable coordination operations, and validate that
downstream workspace readers can consume Rust-written Parquet batches. This is the
highest-risk work stream — if the Parquet contract is wrong, everything downstream breaks.

**What to build:**

1. **Raw event Parquet schema** (arrow-rs):
   - Flat columns matching PoC `raw_envelope` structure: `raw_envelope_version`,
     `tenant_id`, `source`, `integration_id`, `intake_kind`, `batch_id`, `received_at`,
     `record_ordinal`, `metadata_json` (String), `record_json` (String)
   - The `record_json` column holds the full raw event as a JSON string (avoids
     nested struct complexity for raw events — full event schema normalization is M2)
   - Envelope metadata columns for pipeline tracing

2. **S3 sink transform**:
   - Serializes buffered envelopes to Parquet via arrow-rs
   - Uploads to S3: `lake/raw/layout=v1/tenant={id}/source={s}/integration={i}/dt={date}/hour={hh}/batch={bid}/part-00000.parquet`
   - Key layout compatible with existing PoC pattern (just `.parquet` instead of `.jsonl.gz`)
   - Returns confirmation with object_key, size_bytes, record_count, sha256

3. **Dead-letter sink transform**:
   - Writes to `lake/dead-letter/layout=v1/...` prefix
   - Extra column: `dead_letter_reason`

4. **Manifest format** (JSON, matching PoC `storage.py:654-689`):
   - Same fields: manifest_version, manifest_type, layout_version, manifest_id,
     batch_id, tenant_id, source, integration_id, produced_at, partition, time_bounds,
     record_count, idempotency_key, objects[]
   - `object_format` changes from `jsonl.gz` to `parquet`
   - Written to S3 at manifest key + durable intake queue

5. **Coordination operations**:
   - intake-queue: insert (new batch), query (pending by tenant), update (mark processed)
   - collector-checkpoints: load, conditional update (version check)
   - integration-config: read/write
   - local/dev mode uses workspace manifests; production-first follow-up should use Postgres

6. **Python bridge — update `read_raw_batch_records`** (small Python change):
   - Update `storage.py:722-731` to detect `.parquet` object keys and read via pyarrow
   - Deserialize `record_json` column back to dicts (same output as current JSONL reader)
   - Keep existing `.jsonl.gz` path for backward compatibility with old batches
   - Add a `PostgresIntakeQueue` option for `list_pending_intake_batches()` that polls
     Postgres instead of scanning `intake/pending/*.json` — toggled via env var
     `SECCLOUD_INTAKE_QUEUE_BACKEND=postgres|filesystem`

7. **Round-trip integration test** (the critical gate):
   - Rust writes a Parquet batch to LocalStack S3 + manifest to durable queue state
   - Python `read_raw_batch_records()` reads it via pyarrow + `S3ObjectStore`
   - Assert all raw event field values match, including timestamps and nested attributes
   - This test must pass before WS3 begins

**Validation:**

- Rust integration tests: write Parquet + manifest to LocalStack, read back, verify
- Python integration test: Rust writes, Python reads, all values match
- Coordination operations: unit tests with mocked adapters + integration tests vs local Postgres

**Dependencies:** WS1 (Transform trait, Docker Compose).

---

### WS3: Push Gateway (2.5 weeks)

**Goal:** Build the axum HTTP service that accepts push ingestion requests, buffers
events, flushes to S3 as Parquet, and writes manifests. Replaces the Python
`POST /api/intake/raw-events` endpoint (`api.py:116-174`).

**What to build:**

1. **axum HTTP service** (`crates/seccloud-ingestion/`):
   - `POST /intake/v1/raw-events` — main ingestion endpoint
   - `GET /health` — readiness probe
   - Graceful shutdown on SIGTERM (Shutdown signal through pipeline)

2. **Transform chain** (each is a separate Transform impl):
   - **AuthenticateTransform**: validate `Authorization: Bearer {token}` against
     durable integration-config (in-memory cache, 60s TTL). Extract tenant_id,
     source, integration_id. 401 on failure.
   - **DecompressTransform**: handle `Content-Encoding: gzip` via flate2. 400 on failure.
   - **ValidateTransform**: check required fields (source_event_id, observed_at),
     source allowlist. Dead-letter envelope on failure.
   - **AssignBatchTransform**: generate batch_id, compute idempotency_key, check
     accepted-batch state for duplicates. 409 on conflict.
   - **BufferTransform** (the hard part):
     - Accumulate envelopes in memory, bounded (default 10K events or 64 MB)
     - Flush on: size threshold, time interval (5s), Flush signal, Shutdown signal
     - Backpressure: 429 at 80% capacity
     - Implemented as tokio task with mpsc channel — build and test first
   - **FlushToS3Transform**: serialize to Parquet, upload to S3
   - **WriteManifestTransform**: write manifest JSON to S3 + update durable
     intake-queue/index state. This is the durability commit — 202 only after this succeeds.

3. **Request/response contract** (matching PoC):
   - Body limit: 5 MB compressed
   - Record limit: 5000 per batch
   - Response: `{ batch_id, tenant_id, source, integration_id, record_count,
manifest_key, object_key, duplicate }`
   - Idempotency: same key + same payload → return existing (duplicate=true);
     same key + different payload → 409

4. **Integration tests against LocalStack:**
   - Valid batch → S3 objects exist + durable queue/index entry
   - Invalid auth → 401
   - Oversized body → 413
   - Duplicate batch → idempotent 202
   - Dead-letter event → dead-letter S3 object
   - **Cross-language test**: POST via Rust gateway → workspace readers consume the
     batch correctly → downstream processing sees the expected records

**Validation:**

- All integration tests pass against LocalStack
- Cross-language test: Rust gateway → workspace reader bridge → expected output
- Backpressure: at capacity returns 429, does not OOM
- Graceful shutdown: SIGTERM flushes buffer before exit

**Risk:** BufferTransform is the hardest async Rust in M1. Build it as a standalone
tokio task with channel interface, write thorough unit tests, then integrate.

**Dependencies:** WS2 (S3 sink, coordination ops, manifest format, Parquet schema).

---

### WS4a: Collector Framework + Okta Collector (1.5 weeks)

**Goal:** Build the Rust collector framework (trait, checkpoints, scheduling) and
implement the first collector (Okta System Log). Okta is first because the PoC
already has `OktaSystemLogFixtureCollector` and fixture data to validate against.

**What to build:**

1. **Collector trait** (`crates/seccloud-collector/`):

   ```rust
   #[async_trait]
   trait Collector: Send + Sync {
       fn source(&self) -> &str;
       fn integration_id(&self) -> &str;
       async fn poll(&self, checkpoint: &Checkpoint) -> Result<CollectorPage>;
   }

   struct CollectorPage {
       records: Vec<serde_json::Value>,
       next_checkpoint: Checkpoint,
       has_more: bool,
   }

   struct Checkpoint { cursor: serde_json::Value, version: u64 }
   ```

2. **Durable checkpoint persistence:**
   - Load: GetItem from collector-checkpoints
   - Save: conditional PutItem on version (optimistic concurrency)
   - Checkpoint advances only after S3 upload + manifest write succeeds
   - Matches PoC semantics at `collectors.py:88-190`

3. **Scheduling loop:**
   - CronJob mode: poll once, exit
   - Long-poll mode: loop with configurable interval, backoff on empty
   - Rate limiting: per-source token bucket (default 10 req/s)
   - Exponential backoff on API errors: 1s → 2s → 4s → ... → max 60s

4. **Batch submission** (reuses WS2 S3 sink + manifest):
   - Idempotency key: `collector:{kind}:{source}:{integration_id}:{cursor_start}:{cursor_end}`

5. **Okta System Log collector:**
   - Fixture mode: read from JSONL file (for testing)
   - Live mode: poll `/api/v1/logs` with `since` param from checkpoint
   - Vendor mapping: Okta event fields → raw event schema
   - Pagination: follow `next` link header

6. **Integration tests:**
   - Fixture: collect events across 2 pages, verify checkpoint advances, verify S3 objects
   - Failure recovery: S3 upload fails → checkpoint not advanced → retry succeeds
   - Cross-language: Okta collector writes → Python normalizes

**Validation:**

- Fixture tests match PoC collector behavior (same checkpoint semantics, idempotency)
- Cross-language test passes
- Durable conditional update prevents checkpoint advance on failure

**Dependencies:** WS2 (coordination ops, S3 sink, manifest format).

---

### WS4b: Google Workspace + GitHub + Snowflake Collectors (1.5 weeks)

**Goal:** Implement the remaining three collectors using the framework from WS4a.

**What to build:**

1. **Google Workspace Activity** — Admin SDK Reports API, `startTime` checkpoint
2. **GitHub Audit Log** — REST API, `after` cursor pagination
3. **Snowflake Access History** — REST API query against `QUERY_HISTORY`,
   `END_TIME` checkpoint

Each collector: fixture mode + live mode + vendor field mapping + integration tests.

**Validation:**

- All 4 collectors pass fixture-based integration tests
- Cross-language tests pass for each
- Checkpoint resumption and failure recovery work correctly

**Dependencies:** WS4a (collector framework + trait).

**Risk:** Snowflake is the most different (SQL query vs REST poll). If it blocks,
defer it — 3 of 4 collectors is sufficient for initial design partner.

---

### WS5: Local Deployment + Docker (1.5 weeks)

**Goal:** Docker images, local k8s manifests (kind), and an end-to-end smoke test
running the full pipeline locally. Defer AWS Terraform/EKS to a follow-up.

**What to build:**

1. **Docker images:**
   - `Dockerfile.gateway`: multi-stage, Rust release binary (~20 MB)
   - `Dockerfile.collector`: same binary, different entrypoint
   - Health checks, structured JSON logging, graceful shutdown

2. **Local k8s manifests** (kind cluster):
   - Push gateway: Deployment + Service
   - Collectors: CronJob per source
   - LocalStack: StatefulSet (or rely on Docker Compose sidecar)
   - ConfigMap for per-source config, Secret refs for API credentials

3. **Docker Compose update** (for non-k8s local dev):
   - Add gateway container
   - Add collector container (fixture mode)
   - LocalStack, Postgres (for Python projection)

4. **End-to-end smoke test:**
   - POST to Rust gateway → collector runs → Rust local worker
     picks up batches (via queue polling) → detections projected → UI works
   - This is the "local Rust data plane works" gate

5. **Observability basics:**
   - Structured JSON logging with request_id, tenant_id, batch_id
   - Prometheus metrics: request_count, latency_p99, buffer_size, flush_count, errors
   - `/health` reports buffer fullness

**Validation:**

- Docker images build and run locally
- kind cluster deploys gateway + collector pods
- End-to-end smoke test: Rust ingestion → Rust local worker → UI shows events

**Dependencies:** WS3 (gateway binary), WS4a (collector binary).

---

### WS6: Load Test + Performance Validation (1 week)

**Goal:** Demonstrate 10K eps sustained on a single gateway instance and identify
bottlenecks.

**What to build:**

1. **Load test harness** (`crates/seccloud-loadtest/`):
   - Generate synthetic raw events matching 4 source schemas
   - Configurable: eps rate, duration, batch size, concurrent connections
   - Send gzipped batches to push gateway
   - Measure: e2e latency (p50/p95/p99), success rate, 429 rate

2. **Benchmarks:**
   - Baseline: 1K eps, 5 minutes
   - Target: 10K eps, 10 minutes sustained
   - Stress: 20K eps, find the breaking point

3. **Bottleneck analysis:**
   - Profile under load (tokio-console)
   - Identify: CPU (Parquet serialization)? Memory (buffer)? Network (S3 upload)?
     Postgres/local manifest state (manifest writes)?
   - Document tuning recommendations

**Validation:**

- 10K eps sustained for 10 minutes, <1% error rate, p99 < 500ms
- All 202'd events verifiable in S3 (durability guarantee)
- No OOM under stress (backpressure works)

**Dependencies:** WS5 (Docker images for realistic deployment).

---

### Timeline (Solo Dev, Sequential)

```
Week 1:       WS1  — Cargo workspace, Transform trait, Docker Compose, CI
Week 2-3:     WS2  — S3 layout, Parquet schema, durable coordination ops, Python bridge
Week 4-6:     WS3  — Push gateway (axum, transform chain, buffer/flush)
Week 6.5-8:   WS4a — Collector framework + Okta
Week 8-9.5:   WS4b — GWS + GitHub + Snowflake collectors
Week 9.5-11:  WS5  — Docker images, local k8s, e2e smoke test
Week 11-12:   WS6  — Load test, 10K eps benchmark
```

**Total: ~12 weeks.** WS2 is the critical gate — if the Parquet round-trip between
Rust and Python works, everything downstream flows smoothly. WS3's BufferTransform
is the riskiest async Rust work.

Overlap opportunities for context-switching relief:

- WS4a can start once WS3's transform chain is working (before WS3 integration tests are complete)
- WS4b is mechanical once WS4a's framework exists — good for lower-energy days

### Verification Plan

After each work stream, run:

1. `cargo test` — all Rust unit + integration tests
2. `python -m pytest` — all Python tests (including new bridge tests from WS2)
3. Cross-language test (WS2+): Rust writes → Python reads → values match
4. End-to-end test (WS5+): POST to gateway → Python normalizes → UI renders

Final gate (WS6): 10K eps sustained, all events durable in S3, backpressure works.
