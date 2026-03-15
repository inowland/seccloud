# WS07 Operator Runtime Notes

This note documents the current local operator flow for the separated API and worker runtime. It does not change the frozen lake, hot-index, or identity contracts.

## Local Runtime Flow

- `seccloud bootstrap-local-runtime` starts the local Postgres dependency and initializes the synthetic stream manifest if needed.
- `seccloud run-api` runs the FastAPI service with `SECCLOUD_WORKSPACE` and `SECCLOUD_PROJECTION_DSN` wired from the operator runtime.
- `seccloud run-worker-service` runs the separate worker loop that drains intake and refreshes projections independently of the API process.

## Operator Health Surfaces

- `seccloud show-worker-state` returns the persisted worker service counters and last-run status.
- `seccloud show-runtime-status` aggregates worker state, runtime stream state, queue depth, Postgres paths, and projection availability in one operator-only view.

## Recovery Paths

- `seccloud run-worker-service-once` drains pending intake work once and exits.
- `seccloud run-worker-service --exit-when-idle --poll-interval-seconds 0` is useful for backlog draining and local recovery after restarting the worker process.
- The worker loop persists state in the workspace, so restarting the loop does not redefine queue or storage semantics; it resumes against the existing intake and projection state.
