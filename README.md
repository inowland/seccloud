# Seccloud

This repo is easiest to understand by running the local stack and using the synthetic stream controls.

If you are on macOS, have Homebrew, and have already cloned the repo, start with **Mac Quickstart**.

## What You Are Running

The local stack has seven pieces:

- **Rust push gateway** — HTTP ingestion endpoint that writes Parquet to the local workspace
- **Rust collectors** — pull-based ingestion from Okta, GitHub, Google Workspace, and Snowflake (fixture mode for local dev)
- **Local worker loop** — drains intake batches, runs Rust normalization and feature materialization, computes detection context and detections, refreshes projections
- **Local Postgres** — projection store used by the API for fast queries
- **FastAPI backend** on `http://localhost:8000`
- **React/Vite frontend** on `http://localhost:5173`
- **Python CLI** — orchestrates bootstrap, stream controls, model training, and operator commands

The Rust services write Parquet files and intake queue entries to the `.seccloud/` workspace directory. In local mode, the worker runtime itself is Rust: normalization, feature materialization, detection-context refresh, detections, native ONNX scoring, source stats, and projection sync all run through Rust binaries. Python handles model training/export, the API, and demo/runtime glue. No network services are needed between them, just a shared filesystem.

The detection pipeline uses a **two-tower contrastive model** (Facade architecture)
trained on benign enterprise activity. The model learns the relationship between
"who accesses a resource" (action tower) and "who the principal is" (context tower),
flagging actions where the two embeddings diverge. In the local stack, the
synthetic stream generates source events and org context; when an operator
trains and activates a model artifact, the Rust detector scores events in the
live runtime path as they are processed.

The UI is organized around:

- `Detections`: ML-scored detections with feature attribution breakdowns
- `Events`: normalized activity from four source types (Okta, Google Workspace, GitHub, Snowflake)
- `Integrations`: source health, contract coverage, and runtime substrate status per integration

In the current local demo, the durable data plane behind the UI includes:

- normalized lake Parquet/manifests
- feature lake tables
- an explicit feature-lake scoring contract in `manifests/feature_state.json`
- `manifests/event_index.json` for local detail/timeline lookups
- `manifests/detection_context.json` for prior-baseline detection context
- `manifests/identity_profiles.json` for org/profile context

Important:

- local Postgres is required for the API and web UI
- local Postgres is not required for unit tests or the core CLI flows
- the synthetic stream control endpoints live under `/api/stream/*`
- object-store partitions are tenant-aware and integration-aware

## Mac Quickstart

### 1. Install system dependencies

```bash
brew install uv node postgresql rust
```

You need:

- **uv** — Python package manager
- **node** — for the frontend and npm scripts
- **postgresql** — `initdb`, `pg_ctl`, `createdb` must be on your PATH
- **rust** — `cargo` and `rustc` for the Rust services (alternatively install via [rustup](https://rustup.rs/))

### 2. Install repo dependencies

```bash
uv sync
npm install
npm run precommit:install
```

### 3. Build the Rust services

```bash
cd rust && cargo build && cd ..
```

### 4. Bootstrap the runtime

This initializes the local runtime, starts Postgres, and sets up the persistent
continuous synthetic stream used by the live demo. The stream no longer
exhausts; it advances until you stop the generator or reset the runtime.

```bash
npm run dev:postgres:start
uv run seccloud bootstrap-local-runtime --workspace .seccloud --runtime-root . --reset-stream
```

### 5. Run the stack

Open four terminals in the repo root:

```bash
# Terminal 1 — Local worker loop
npm run dev:worker

# Terminal 2 — Python API server
npm run dev:api

# Terminal 3 — Rust push gateway
npm run dev:gateway

# Terminal 4 — Frontend
npm run web
```

Open `http://localhost:5173/` in your browser.

### 6. Ingest data via Rust collectors

In any terminal:

```bash
# Ingest fixture data from all 4 sources and process it
npm run dev:ingest
```

This runs the Rust collectors (fixture mode) to write Parquet to `.seccloud/`, then runs the local worker once to normalize, build features, materialize detection context, and score detections.

You can also ingest individual sources:

```bash
npm run dev:collector:okta
npm run dev:collector:github
npm run dev:collector:gworkspace
npm run dev:collector:snowflake
```

The running worker in Terminal 1 will pick up new batches automatically within a few seconds.

### 7. Run the ML demo

Drive the live demo from the CLI, not from the browser:

```bash
npm run dev:demo-stream
```

Keep that running in its own terminal while `npm run dev:worker`, `npm run dev:api`,
`npm run dev:gateway`, and `npm run web` are already up. Stop it with `Ctrl-C`
when you want the simulation to pause.

Then watch detections appear in the **Detections** tab as the stream progresses.
Each detection shows:

- `contrastive-facade-v1` model version
- anomaly score from the two-tower cosine distance
- feature attribution breakdown (which signals drove the score)
- linked attack events
- peer-context baseline fields showing what was known before the triggering event

Open the **Integrations** tab to inspect the runtime substrate per source. You should now see:

- contract coverage and dead-letter pressure
- whether scoring inputs are using the durable feature lake or the Python feature-pipeline fallback
- indexed event coverage
- feature-table materialization counts
- identity-profile availability
- detection-context readiness
- projection connectivity/state

If you want to compare the model-backed detector against the heuristic baseline
on the current workspace, run:

```bash
uv run seccloud compare-detection-modes --workspace .seccloud
```

This does not mutate the workspace. It reports how many detections each mode
produces, how much they overlap, and sample heuristic-only vs model-only alerts.

The live demo path now uses persistent continuous synthesis. Finite mode still
exists for deterministic tests and milestone acceptance coverage, but detections
now always come from the normal runtime detector path instead of a replayed
precomputed manifest.

To re-generate the demo data, stop the API server, rerun
`uv run seccloud bootstrap-local-runtime --workspace .seccloud --runtime-root . --reset-stream`, and start the API again.

To move from heuristic mode to model-backed scoring on the current workspace, run:

```bash
uv run seccloud train-model-artifact --workspace .seccloud --output-dir /tmp/seccloud-model --model-id demo-v1
uv run seccloud activate-model-artifact --workspace .seccloud demo-v1
```

## Dev Scripts Reference

All scripts are in `package.json` and run from the repo root.

### Rust services

| Script                             | What it does                                         |
| ---------------------------------- | ---------------------------------------------------- |
| `npm run dev:gateway`              | Rust push gateway on `:8080`, writes to `.seccloud/` |
| `npm run dev:collector:okta`       | Ingest Okta fixtures (one-shot)                      |
| `npm run dev:collector:github`     | Ingest GitHub fixtures (one-shot)                    |
| `npm run dev:collector:gworkspace` | Ingest Google Workspace fixtures (one-shot)          |
| `npm run dev:collector:snowflake`  | Ingest Snowflake fixtures (one-shot)                 |
| `npm run dev:collector:all`        | Ingest all 4 sources sequentially                    |

### Python services

| Script                    | What it does                                        |
| ------------------------- | --------------------------------------------------- |
| `npm run dev:worker`      | Rust local worker loop (polls every 2s)             |
| `npm run dev:worker:once` | Single Rust local worker pass                       |
| `npm run dev:api`         | API server on `:8000` with hot reload               |
| `npm run dev:ingest`      | Collect all fixtures + run worker once              |
| `npm run dev:demo-stream` | Continuously synthesize and advance the demo stream |

### Infrastructure

| Script                       | What it does                     |
| ---------------------------- | -------------------------------- |
| `npm run dev:postgres:start` | Start embedded Postgres          |
| `npm run dev:postgres:stop`  | Stop embedded Postgres           |
| `npm run web`                | Vite dev server for the React UI |

### Quality

| Script              | What it does                                           |
| ------------------- | ------------------------------------------------------ |
| `npm run lint`      | Full lint (Python + JS + Rust + TypeScript + Prettier) |
| `npm run test:rust` | Run all Rust tests                                     |
| `npm run fmt:rust`  | Format Rust code                                       |

## E2E Smoke Test

Verify the full Rust → Python pipeline works end-to-end:

```bash
bash scripts/e2e-smoke-test.sh
```

This clears the workspace, runs all 4 Rust collectors, then runs the Rust local worker and verifies the end-to-end path completes cleanly.

## Useful Non-UI Commands

If you want to exercise the core pipeline without the browser:

```bash
uv run python -m unittest discover -s tests -v
uv run seccloud run-runtime --workspace .seccloud
uv run seccloud show-source-capability-matrix --workspace .seccloud
```

If you want a single operator status view:

```bash
uv run seccloud show-runtime-status --workspace .seccloud --runtime-root .
```

If you want to inspect model-vs-heuristic divergence directly:

```bash
uv run seccloud compare-detection-modes --workspace .seccloud
```

If you want to evaluate model-vs-heuristic scoring against the synthetic truth
labels embedded in the workspace stream:

```bash
uv run seccloud evaluate-detection-modes --workspace .seccloud
```

This reports per-scenario event recall plus benign alert rate and score
quantiles by source for both scoring modes.

## Object Store Configuration

The runtime supports two object-store backends:

- `local` (default): stores objects under the workspace root
- `s3`: stores objects in an S3-compatible bucket using the same object-key layout

Relevant environment variables:

```bash
SECCLOUD_TENANT_ID=local
SECCLOUD_OBJECT_STORE_BACKEND=local
SECCLOUD_OBJECT_STORE_BUCKET=
SECCLOUD_OBJECT_STORE_PREFIX=
SECCLOUD_OBJECT_STORE_ENDPOINT_URL=
SECCLOUD_OBJECT_STORE_REGION=
```

The Rust services use `SECCLOUD_WORKSPACE` to select between filesystem mode (set to a path) and in-memory mode (unset). For local dev, both Rust and Python use the same `.seccloud/` workspace directory.

## Repo Map

- [src](src): Python package with pipeline, onboarding, mapping, API, synthetic stream controls, and CLI
- [rust](rust): Rust workspace with `seccloud-pipeline`, `seccloud-lake`, `seccloud-ingestion`, and `seccloud-collector` crates
- [tests](tests): Python tests + Parquet bridge tests
- [examples](examples): fixture bundles and reference inputs
- [web](web): React/Vite frontend
- [project/plan](project/plan): milestone plan (M0–M6) for production build-out

Runtime state is generated locally under `.seccloud/` by default and is gitignored.

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Rust (M1)                  │
│                                             │
│  Push Gateway (axum)     Collectors (4)     │
│  POST /intake/v1/...     Okta, GitHub,      │
│                          GWorkspace,        │
│                          Snowflake          │
│         │                      │            │
│         └──────┬───────────────┘            │
│                ▼                            │
│     .seccloud/lake/raw/*.parquet            │
│     .seccloud/intake/pending/*.json         │
└─────────────────────────────────────────────┘
                 │ (filesystem)
                 ▼
┌─────────────────────────────────────────────┐
│                Python (M0)                  │
│                                             │
│  Worker: normalize → detect → project       │
│  API: FastAPI on :8000                      │
│  Postgres: projection store                 │
│  ML: two-tower contrastive model            │
└─────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│           React/Vite UI on :5173            │
│  Detections │ Events │ Integrations         │
└─────────────────────────────────────────────┘
```

## ML Pipeline (M0)

The ML detection pipeline lives in four modules under `src/seccloud/`:

| Module                 | Purpose                                                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `synthetic_scale.py`   | WS1: Generates realistic enterprise activity at scale (demo: 200 principals, ~50K events) with 6 injected attack scenarios |
| `feature_pipeline.py`  | WS2: Transforms events into Facade model inputs — weighted accessor sets (action features) and principal context profiles  |
| `contrastive_model.py` | WS3: Two-tower contrastive model in PyTorch — action tower + context tower with pairwise ranking loss                      |
| `evaluation.py`        | WS4+5: Multi-scale detection (HAC clustering) and evaluation framework with per-scenario ROC AUC metrics                   |
| `onnx_export.py`       | WS6: ONNX export with numerical equivalence validation and latency benchmarks                                              |
| `ml_scoring.py`        | Deterministic finite-stream model precompute path and Python feature-pipeline fallback support                             |

M0 validation results are in `project/plan/m0-results.md`.

## Current Limits

This is a validated prototype:

- the data is synthetic (realistic distributions, not real enterprise data)
- the local stack is local and temporary
- model training and activation are still operator-driven, not automatic during bootstrap
- insider collaboration detection is weak (per-principal scoring can't detect coordination)
- the UI is meant to support product evaluation, not define the final production workflow

### 6. Operator Flow

The operator flow is explicit:

1. `bootstrap-local-runtime` creates the demo workspace, Postgres state, and synthetic stream data.
2. `train-model-artifact` exports a real ONNX model bundle from the current workspace.
3. `activate-model-artifact` switches the detector into `onnx_native` mode.
4. The Rust worker scores events with the active bundle as new events move through the runtime.

Bootstrapping alone does not train or activate a model artifact.
