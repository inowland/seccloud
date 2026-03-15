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

The current UI is organized around:

- `Detections`: evidence-backed detections that need review
- `Events`: normalized activity, separate from the queue
- `Integrations`: a source list with configuration, state, and recovery guidance per integration

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
export UV_CACHE_DIR=.uv-cache
uv python install 3.12
uv sync
source .venv/bin/activate
npm install
npm run precommit:install
```

Notes:

- If `uv sync` already finds Python 3.12, you can skip `uv python install 3.12`.

### 3. Start the API stack

Open Terminal 1 in the repo root and run:

```bash
source .venv/bin/activate
seccloud start-postgres
seccloud init-stream
uvicorn seccloud.api:app --host 127.0.0.1 --port 8000
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

## Useful Non-UI Commands

If you want to exercise the core pipeline without the browser:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
seccloud run-runtime
seccloud show-source-capability-matrix
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
seccloud run-worker-service-once
seccloud show-worker-state
```

## Repo Map

- [src](src): Python package with pipeline, onboarding, mapping, API, synthetic stream controls, and CLI
- [tests](tests): tests
- [examples](examples): fixture bundles and reference inputs
- [web](web): React/Vite frontend
- [project](project): internal specs, plans, and engineering notes

Runtime state is generated locally under `.seccloud/runtime` by default and is gitignored.

## Current Limits

This is still a PoC:

- the data is synthetic or fixture-driven
- the local stack is local and temporary
- the detector path is still heuristic-first
- the current UI is meant to support product evaluation, not define the final production workflow
