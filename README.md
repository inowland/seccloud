# Seccloud PoC

This repo is a synthetic-first PoC for a BYOC insider-risk and behavioral detection product.

It currently has three useful surfaces:

- a Python ingest, scoring, and investigation engine
- onboarding and mapping flows for fixed source packs and vendor-shaped exports
- a temporary hybrid demo stack: streamed demo state, Postgres projections, FastAPI, and a React UI

If you only read one section, read **First Run**.

## What This Repo Is For

Use this repo to answer four questions:

- Can the current source pack be ingested, scored, and investigated end to end?
- Can we validate and import candidate customer data shapes without bespoke parser work?
- Can vendor-shaped exports be mapped into the internal raw-event contract cleanly?
- Can the current product concepts be shown through a live demo surface instead of only through CLI output?

This is still a PoC. The demo UI and local Postgres stack are temporary demo architecture, not the final supported product shape.

## Repo Map

- [src](src): Python package with ingest, scoring, onboarding, mapping, demo API, and CLI
- [tests](tests): unit and workflow tests
- [examples](examples): example runtimes, fixed fixture bundles, and vendor-shaped fixture bundles
- [web](web): React/Vite demo frontend
- [.project](.project): internal specs, plans, ADRs, and engineering guidance

## Prerequisites

You need:

- Python 3.12+
- `uv`
- Node.js and `npm`
- local PostgreSQL binaries from Homebrew or equivalent: `initdb`, `pg_ctl`, `createdb`

This repo manages Python dependencies with `uv` and frontend dependencies with `npm`.

## First Run

If you want to confirm the repo works end to end without touching the web demo yet:

```bash
uv sync
uv run python -m unittest discover -s tests -v
uv run seccloud run-demo --workspace examples/poc/runtime
uv run seccloud show-evaluation-report --workspace examples/poc/runtime
uv run seccloud show-source-capability-matrix --workspace examples/poc/runtime
```

That gives you:

- a generated PoC runtime under [examples/poc/runtime](examples/poc/runtime)
- seeded detections and cases
- founder/reporting artifacts
- a sanity check that the pipeline, scoring, and investigation workflow are behaving

## Common Paths

### 1. Run The Core PoC

Use this if you want the fastest path into the current product logic.

```bash
uv run seccloud run-demo --workspace examples/poc/runtime
uv run seccloud list-detections --workspace examples/poc/runtime
uv run seccloud show-timeline --workspace examples/poc/runtime --principal alice@example.com
uv run seccloud show-conversation-pack --workspace examples/poc/runtime
```

Useful outputs:

- [evaluation-summary.md](examples/poc/runtime/founder_artifacts/evaluation-summary.md)
- [conversation-pack.md](examples/poc/runtime/founder_artifacts/conversation-pack.md)
- [source-capability-matrix.md](examples/poc/runtime/founder_artifacts/source-capability-matrix.md)

### 2. Validate Fixed Fixture Bundles

Use this if you want to exercise onboarding against the repo’s internal raw-event contract.

```bash
uv run seccloud validate-source-fixtures --fixtures-dir examples/poc/fixtures/fixed-source-pack
uv run seccloud show-onboarding-report --fixtures-dir examples/poc/fixtures/fixed-source-pack
uv run seccloud import-source-fixtures --workspace /tmp/seccloud-fixtures --fixtures-dir examples/poc/fixtures/fixed-source-pack
```

The fixed fixture bundle lives under [examples/poc/fixtures/fixed-source-pack](examples/poc/fixtures/fixed-source-pack).

### 3. Validate Vendor-Shaped Exports

Use this if you want to test the more realistic mapper layer.

```bash
uv run seccloud validate-vendor-fixtures --fixtures-dir examples/poc/vendor-fixtures/fixed-source-pack
uv run seccloud show-vendor-mapping-report --fixtures-dir examples/poc/vendor-fixtures/fixed-source-pack
uv run seccloud import-vendor-fixtures --workspace /tmp/seccloud-vendor-fixtures --fixtures-dir examples/poc/vendor-fixtures/fixed-source-pack
```

The vendor-shaped examples live under [examples/poc/vendor-fixtures/fixed-source-pack](examples/poc/vendor-fixtures/fixed-source-pack).

### 4. Run The Demo UI

Use this if you want the current hybrid demo stack: streamed data, Postgres projections, FastAPI, and React.

Terminal 1:

```bash
uv run seccloud demo-init-postgres
uv run seccloud demo-start-postgres
uv run seccloud demo-init-stream --workspace examples/poc/demo-runtime
uv run seccloud demo-advance-stream --workspace examples/poc/demo-runtime --batch-size 8
uv run seccloud demo-sync-postgres --workspace examples/poc/demo-runtime
uv run uvicorn seccloud.demo_api:app --host 127.0.0.1 --port 8000
```

Terminal 2:

```bash
npm install
npm run demo:web
```

Then open the Vite URL, typically `http://127.0.0.1:5173`.

When you are done:

```bash
uv run seccloud demo-stop-postgres
```

Notes:

- the API serves projected state from the workspace-local Postgres demo database
- the React app proxies `/api` to `http://127.0.0.1:8000`
- the stream only advances when you call `demo-advance-stream` or use the UI controls against a running API

## Important CLI Commands

Core:

- `uv run seccloud run-demo --workspace <path>`
- `uv run seccloud run-pipeline --workspace <path>`
- `uv run seccloud list-detections --workspace <path>`
- `uv run seccloud show-timeline --workspace <path> --principal <email>`
- `uv run seccloud show-case-summary --workspace <path> --case-id <id>`

Onboarding:

- `uv run seccloud validate-source-fixtures --fixtures-dir <dir>`
- `uv run seccloud validate-vendor-fixtures --fixtures-dir <dir>`
- `uv run seccloud export-source-manifest --workspace <path>`
- `uv run seccloud export-vendor-source-manifest --workspace <path>`

Demo:

- `uv run seccloud demo-init-stream --workspace examples/poc/demo-runtime`
- `uv run seccloud demo-advance-stream --workspace examples/poc/demo-runtime --batch-size 5`
- `uv run seccloud demo-sync-postgres --workspace examples/poc/demo-runtime`
- `uv run uvicorn seccloud.demo_api:app --host 127.0.0.1 --port 8000`
- `npm run demo:web`

## Where To Look In Code

If you want to understand the repo by reading code:

- [cli.py](src/seccloud/cli.py): all supported entrypoints
- [pipeline.py](src/seccloud/pipeline.py): ingest, normalization, derived state, evaluation
- [scoring.py](src/seccloud/scoring.py): detector modules and fused scoring
- [investigation.py](src/seccloud/investigation.py): evidence, timelines, cases, peer comparison
- [onboarding.py](src/seccloud/onboarding.py): fixed source bundle validation/import
- [vendor_exports.py](src/seccloud/vendor_exports.py): vendor-shaped export mapping
- [demo_stream.py](src/seccloud/demo_stream.py): incremental demo stream
- [demo_projection.py](src/seccloud/demo_projection.py): Postgres projection layer
- [demo_api.py](src/seccloud/demo_api.py): FastAPI demo backend
- [App.jsx](web/src/App.jsx): React demo UI

## Internal Project Docs

If you want the product truth and implementation history rather than just the runtime:

- [.project/README.md](.project/README.md)

The most important internal docs are:

- [product-contract.md](.project/spec/product-contract.md)
- [roadmap.md](.project/spec/roadmap.md)
- [implementation-status.md](.project/planning/implementation-status.md)

## Current Limits

This repo still has important limits:

- the main detection path is still heuristic-first
- the demo stack is temporary architecture
- the data is synthetic or fixture-driven, not live customer telemetry
- the onboarding layer proves compatibility with supported shapes, not full customer variability

That is intentional. The repo is meant to answer productization and architecture questions quickly, not to masquerade as a production system.
