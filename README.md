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
- `Pipeline`: intake, indexing, feature/materialization, model/runtime, and serving state in one view

Important:

- local Postgres is required for the API and web UI
- local Postgres is not required for unit tests or the core CLI flows
- the synthetic stream control endpoints live under `/api/stream/*`
- object-store partitions are tenant-aware and integration-aware

## Mac Quickstart

### 1. Install dependencies

```bash
brew install uv node postgresql rust
uv sync
npm install
npm run precommit:install
cd rust && cargo build && cd ..
```

### 2. Bootstrap the runtime

This starts local Postgres, ensures a pinned local Quickwit binary under
`.seccloud/bin/quickwit` when needed, and initializes the demo workspace and
continuous synthetic stream.

```bash
npm run demo:bootstrap -- --reset-stream
```

### 3. Start the app in separate terminals

Run the UI, API, and worker in separate terminals so you can see live logs while
you develop:

```bash
npm run app:web
```

```bash
npm run app:api
```

```bash
npm run app:worker
```

Open `http://localhost:5173/`. You should see `Detections`, `Events`, `Integrations`,
and `Pipeline` in the left nav.

### 4. Put data into it

To see activity in the product, you need to add demo data.

For the default model-backed demo, run the guided prep command:

```bash
npm run demo:prepare-model
```

`demo:prepare-model` bootstraps the runtime if needed, generates the full finite
synthetic corpus, processes it to idle, trains and activates the demo model,
and syncs Quickwit.

Before a walkthrough, get a single readiness verdict:

```bash
npm run demo:doctor
```

After the model is ready, start a continuous stream

```bash
npm run demo:stream
```

### 5. What to look at

- `Pipeline` shows intake, indexing, feature/materialization, model/runtime, and serving state.
- `Detections` shows alerts and the investigation pivots.
- `Events` shows the normalized event surface.
- `Integrations` shows source health and contract coverage.

### Resetting the demo

To regenerate the local demo data:

```bash
npm run demo:bootstrap -- --reset-stream
```

### Model-backed scoring

`npm run demo:train-model` trains a model from the current workspace feature
lake and installs it as the active scoring model for the demo.

If you want to compare model-backed detections against the heuristic baseline:

```bash
uv run seccloud compare-detection-modes --workspace .seccloud
```

## Command Surface

Use `uv run seccloud ...` for stable Seccloud runtime and operator commands. Use
`npm run ...` for repo-level workflows: frontend tasks, lint/typecheck/format,
and convenience aliases that wrap one or more Seccloud commands. Keep direct
`scripts/` execution as an internal implementation detail unless a doc section is
explicitly about a low-level helper.

## NPM Commands

All scripts are in `package.json` and run from the repo root.

### App processes

| Script                     | What it does                                          |
| -------------------------- | ----------------------------------------------------- |
| `npm run app:web`          | React UI on `:5173`                                   |
| `npm run app:web:dev`      | React UI on `:5173` (explicit dev alias)              |
| `npm run app:api`          | Stable API server on `:8000`                          |
| `npm run app:api:dev`      | API server on `:8000` with hot reload                 |
| `npm run app:worker`       | Background processor for events, features, and alerts |
| `npm run app:push-gateway` | Local ingestion endpoint on `:8080`                   |

### Demo flows

| Script                       | What it does                                                    |
| ---------------------------- | --------------------------------------------------------------- |
| `npm run demo:bootstrap`     | Bootstrap the full local demo runtime                           |
| `npm run demo:doctor`        | Report whether the demo is ready and what to do next            |
| `npm run demo:prepare-model` | Generate data, process it, train/install the model, sync search |
| `npm run demo:generate-data` | Generate the full synthetic demo corpus and wait for processing |
| `npm run demo:train-model`   | Train and activate the default demo model                       |
| `npm run demo:stream`        | Continuously generate synthetic activity                        |

### Demo data loaders

| Script                                       | What it does                                               |
| -------------------------------------------- | ---------------------------------------------------------- |
| `npm run demo:load-example-batch`            | Queue the tiny fixed example batch and wait for processing |
| `npm run demo:load-example-batch:okta`       | Load the fixed Okta example batch                          |
| `npm run demo:load-example-batch:github`     | Load the fixed GitHub example batch                        |
| `npm run demo:load-example-batch:gworkspace` | Load the fixed Google Workspace example batch              |
| `npm run demo:load-example-batch:snowflake`  | Load the fixed Snowflake example batch                     |
| `npm run demo:queue-example-batch`           | Queue the full fixed example batch without waiting         |
| `npm run demo:load-example-batch:once`       | Load the full fixed example batch and process it once      |
| `npm run demo:process:once`                  | Run one processing pass without the background loop        |

### Local runtime support

| Script                          | What it does                                |
| ------------------------------- | ------------------------------------------- |
| `npm run local:postgres:start`  | Start embedded Postgres                     |
| `npm run local:postgres:stop`   | Stop embedded Postgres                      |
| `npm run local:quickwit:ensure` | Install or resolve local Quickwit           |
| `npm run local:quickwit:start`  | Start embedded Quickwit                     |
| `npm run local:quickwit:stop`   | Stop embedded Quickwit                      |
| `npm run local:quickwit:sync`   | Sync the local Quickwit event index         |
| `npm run local:quickwit:smoke`  | Run the Quickwit event-plane smoke test     |
| `npm run smoke:e2e`             | Run the full collector -> worker smoke test |

### Quality

| Script              | What it does                                           |
| ------------------- | ------------------------------------------------------ |
| `npm run lint`      | Full lint (Python + JS + Rust + TypeScript + Prettier) |
| `npm run test:rust` | Run all Rust tests                                     |
| `npm run fmt:rust`  | Format Rust code                                       |

## E2E Smoke Test

Verify the full Rust → Python pipeline works end-to-end:

```bash
npm run smoke:e2e
```

This clears the workspace, runs all 4 Rust collectors, then runs the Rust local worker and verifies the end-to-end path completes cleanly.
