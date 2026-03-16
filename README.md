# Seccloud

This repo is easiest to understand by running the local stack and using the synthetic stream controls.

If you are on macOS, have Homebrew, and have already cloned the repo, start with **Mac Quickstart**.

## What You Are Running

The local stack has five pieces:

- a Python pipeline and CLI
- a separate Python worker loop that drains intake and refreshes projections
- a local Postgres projection used by the API
- a FastAPI backend on `http://localhost:8000`
- a React/Vite frontend on `http://localhost:5173`

The detection pipeline uses a **two-tower contrastive model** (Facade architecture)
trained on benign enterprise activity. The model learns the relationship between
"who accesses a resource" (action tower) and "who the principal is" (context tower),
flagging actions where the two embeddings diverge. At stream reset the model is
trained on synthetic data and detections are pre-computed; as events stream into
the UI the detections appear at the point in the timeline where the attack
pattern has manifested.

The UI is organized around:

- `Detections`: ML-scored detections with feature attribution breakdowns
- `Events`: normalized activity from four source types (Okta, Google Workspace, GitHub, Snowflake)
- `Integrations`: source health and contract coverage per integration

Important:

- local Postgres is required for the API and web UI
- local Postgres is not required for unit tests or the core CLI flows
- the synthetic stream control endpoints live under `/api/stream/*`
- object-store partitions are tenant-aware and integration-aware

## Mac Quickstart

### 1. Install system dependencies with Homebrew

```bash
brew install uv node postgresql
```

You need Postgres command-line tools such as `initdb`, `pg_ctl`, and `createdb`.

If you already installed a versioned Homebrew Postgres formula and those commands are not on your `PATH`, add its `bin` directory before continuing.

### 2. Install repo dependencies

From the repo root:

```bash
uv python install 3.12
uv sync
source .venv/bin/activate
npm install
npm run precommit:install
```

Notes:

- If `uv sync` already finds Python 3.12, you can skip `uv python install 3.12`.

### 3. Bootstrap the local runtime and start the API

Open Terminal 1 in the repo root and run:

```bash
source .venv/bin/activate
seccloud bootstrap-local-runtime --reset-stream
seccloud run-api --reload
```

Leave that terminal running.

### 4. Start the worker loop

Open Terminal 2 in the repo root and run:

```bash
source .venv/bin/activate
seccloud run-worker-service --poll-interval-seconds 1
```

Leave that terminal running.

### 5. Start the frontend

Open Terminal 3 in the repo root and run:

```bash
npm run web
```

Then open the Vite URL shown in the terminal, usually:

```text
http://localhost:5173/
```

### 6. Run the ML demo

The `bootstrap-local-runtime --reset-stream` step (step 3) generates ~50K
synthetic events from 200 principals, trains the contrastive model, and
pre-computes ML detections. This takes ~90 seconds.

In the browser, use the **Demo Control** overlay at the bottom of the screen:

1. Click **Advance 5K** a few times — events stream into the system and get
   normalized + projected. The worker loop in Terminal 2 processes each batch.

2. Watch detections appear in the **Detections** tab as the stream cursor
   passes each attack scenario's trigger point. Each detection shows:
   - `contrastive-facade-v1` model version
   - anomaly score from the two-tower cosine distance
   - feature attribution breakdown (which signals drove the score)
   - linked attack events

The six injected attack scenarios are: slow exfiltration, credential compromise,
privilege escalation, departing employee, account takeover, and insider
collaboration.

To re-generate the data and retrain the model, stop the API server, rerun
`seccloud bootstrap-local-runtime --reset-stream`, and start the API again.

## Useful Non-UI Commands

If you want to exercise the core pipeline without the browser:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
seccloud run-runtime
seccloud show-source-capability-matrix
```

If you want a single operator status view:

```bash
source .venv/bin/activate
seccloud show-runtime-status
```

If you want a one-shot recovery drain without leaving a loop running:

```bash
source .venv/bin/activate
seccloud run-worker-service-once
```

## Object Store Configuration

The runtime now supports two object-store backends:

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

Notes:

- `SECCLOUD_TENANT_ID` is used in raw, normalized, and dead-letter object partitions.
- `SECCLOUD_OBJECT_STORE_BUCKET` is required when `SECCLOUD_OBJECT_STORE_BACKEND=s3`.
- the S3 backend currently expects `boto3` to be available in the Python environment

## Operator-Only Commands

These are useful for local operations and debugging, not for the normal product flow:

```bash
source .venv/bin/activate
seccloud bootstrap-local-runtime
seccloud run-api
seccloud run-worker-service-once
seccloud run-worker-service --exit-when-idle --poll-interval-seconds 0
seccloud show-worker-state
seccloud show-runtime-status
seccloud start-postgres
seccloud stop-postgres
```

Notes:

- `seccloud run-api` wires `SECCLOUD_WORKSPACE` and `SECCLOUD_PROJECTION_DSN` for you.
- `seccloud show-runtime-status` aggregates stream state, worker state, queue depth, and projection availability.
- `seccloud run-worker-service --exit-when-idle` is useful for operator-driven recovery and backlog draining.
- `seccloud run-worker-service-once` is the narrowest recovery command when you want to drain one pass and inspect the result before starting the continuous loop.

## Repo Map

- [src](src): Python package with pipeline, onboarding, mapping, API, synthetic stream controls, and CLI
- [tests](tests): tests
- [examples](examples): fixture bundles and reference inputs
- [web](web): React/Vite frontend
- [project/plan](project/plan): milestone plan (M0–M6) for production build-out

Runtime state is generated locally under `.seccloud/runtime` by default and is gitignored.

## ML Pipeline (M0)

The ML detection pipeline lives in four modules under `src/seccloud/`:

| Module                 | Purpose                                                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `synthetic_scale.py`   | WS1: Generates realistic enterprise activity at scale (demo: 200 principals, ~50K events) with 6 injected attack scenarios |
| `feature_pipeline.py`  | WS2: Transforms events into Facade model inputs — weighted accessor sets (action features) and principal context profiles  |
| `contrastive_model.py` | WS3: Two-tower contrastive model in PyTorch — action tower + context tower with pairwise ranking loss                      |
| `evaluation.py`        | WS4+5: Multi-scale detection (HAC clustering) and evaluation framework with per-scenario ROC AUC metrics                   |
| `onnx_export.py`       | WS6: ONNX export with numerical equivalence validation and latency benchmarks                                              |
| `ml_scoring.py`        | M0.5: Bridge between the ML pipeline and the Detection contract for UI integration                                         |

M0 validation results are in `project/plan/m0-results.md`.

## Current Limits

This is a validated prototype:

- the data is synthetic (realistic distributions, not real enterprise data)
- the local stack is local and temporary
- the ML model is pre-computed at stream reset, not scoring in real-time
- insider collaboration detection is weak (per-principal scoring can't detect coordination)
- the UI is meant to support product evaluation, not define the final production workflow
