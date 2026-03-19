# Demo Reliability Plan

## Summary

The current demo experience is blocked less by missing product surface area and
more by fragile local operations. The product can already tell a strong story
once it is running, but it is still too easy for the demo to fall into a bad
state:

- stale local processes hold ports
- the API and worker are started by hand in multiple terminals
- long-running worker activity is hard to distinguish from a hang
- Quickwit/index readiness is still too easy to infer incorrectly
- the "default demo flow" is still too dependent on operator judgment

This plan focuses on making the existing demo path reliable, observable, and
repeatable before adding more feature depth.

## Problem Statement

Right now the operator has to manually coordinate:

- local dependencies
- API and worker processes
- Quickwit readiness
- dataset generation
- model training
- pipeline health interpretation

That creates three product risks:

1. **Demo fragility**
   A presentation can fail because local process state is stale or ambiguous.
2. **Operator uncertainty**
   The system can be "working" without making that obvious in either the CLI or
   the UI.
3. **False negatives in product confidence**
   It becomes hard to tell whether the product is actually broken, merely
   catching up, or just poorly packaged for local use.

## Goals

- Make the default demo setup a single guided path.
- Make long-running processes visibly alive while they work.
- Make "ready", "catching up", and "broken" unambiguous in both CLI and UI.
- Reduce the number of stale-port and half-started-runtime failure modes.
- Give the operator one command to verify demo readiness before a walkthrough.

## Non-Goals

- New investigation surfaces
- New detection logic
- New customer deployment primitives
- Replacing Quickwit immediately
- Re-architecting the canonical lake / event plane split

## Workstreams

### 1. Demo Process Supervision

Replace the current multi-terminal bootstrap pattern with a managed demo runtime
entry point.

Deliverables:

- `demo:start`
  - starts Postgres
  - ensures and starts Quickwit
  - starts API in demo mode
  - starts worker in demo mode
  - optionally starts web
- `demo:stop`
  - stops managed processes cleanly
  - clears stale pid metadata
- runtime pid/state manifest for managed demo processes
- stale-port detection with direct, actionable errors

Acceptance criteria:

- A fresh operator can start the demo from one command.
- Restarting after a bad local state is predictable.
- The system can explain which managed process failed and why.

### 2. Separate Demo Mode From Dev Mode

The current commands still mix development ergonomics and demo ergonomics.

Deliverables:

- `app:api:dev` with reload
- `app:api` without reload
- equivalent split anywhere else watch-mode behavior is enabled
- README quickstart that defaults to stable demo-mode commands

Acceptance criteria:

- The recommended demo path avoids reload/watch-mode process churn.
- Port conflicts and stale reload children become less common.

### 3. Worker Observability And Liveness

The worker must be obviously alive when it is healthy and obviously broken when
it is not.

Deliverables:

- live Rust worker logs surfaced in `app:worker`
- periodic heartbeat lines with:
  - pending batches
  - processed batches
  - service iteration count
  - current service status
- explicit stage context on failure
  - normalization
  - source stats
  - feature build
  - detection
  - projection
- detection of contradictory states such as:
  - large backlog + repeated `idle`
  - growing queue + no recent processing timestamp

Acceptance criteria:

- An operator can tell within 10 seconds whether the worker is making progress.
- A worker crash points at a stage, not just an exit code.

### 4. Guided Demo Data Preparation

The "default demo" should not require the operator to infer which data path is
big enough to support model training.

Deliverables:

- one guided command, e.g. `demo:prepare-model`, that:
  - generates a large finite synthetic corpus
  - waits for processing to catch up
  - verifies scoring input readiness
  - trains and installs the demo model
  - optionally syncs Quickwit
- clearer separation between:
  - fixed tiny example batch
  - finite synthetic demo corpus
  - continuous live stream

Acceptance criteria:

- The recommended model-backed demo flow is one command sequence, not an
  operator checklist.
- Tiny example data is no longer confused with the model-training corpus.

### 5. Quickwit Readiness And Event Plane Clarity

Quickwit should be explicit and predictable in the demo path.

Deliverables:

- auto-create or auto-sync Quickwit during demo preparation
- explicit event-plane readiness states:
  - index missing
  - catching up
  - healthy
  - failed
- no silent backend switching in the main events UI path
- clearer CLI and UI messages when the proxy cannot reach the API or when the
  API cannot reach Quickwit

Acceptance criteria:

- `/api/events` never crashes the UI due to a missing Quickwit index.
- The operator can tell whether the problem is:
  - API not reachable
  - Quickwit not running
  - Quickwit index missing
  - Quickwit lagging

### 6. Demo Doctor

Add one readiness check command for pre-demo confidence.

Deliverables:

- `demo:doctor`
  - checks API reachability
  - checks worker recency and backlog movement
  - checks Quickwit process and index readiness
  - checks model scoring activation
  - checks pipeline catch-up state
- concise output:
  - overall ready / not ready
  - blocking issues
  - recommended next action

Acceptance criteria:

- An operator can run one command before a walkthrough and know whether the demo
  is trustworthy.

### 7. Pipeline Page Simplification

The pipeline page should reflect the same operational language as the CLI.

Deliverables:

- keep the top-level badge vocabulary small:
  - `Ready`
  - `Catching up`
  - `Not ready`
  - `Error`
- align CLI and UI terminology for:
  - worker progress
  - Quickwit/index status
  - model readiness
  - demo readiness
- keep advanced detail collapsed by default

Acceptance criteria:

- A novice operator can answer:
  - Is it working?
  - Is it current?
  - What is the backlog?
  - What should I do next?
    from the first screen of the pipeline page.

## Priority Order

Immediate priority:

1. Demo process supervision
2. Worker observability and liveness
3. Guided demo data preparation
4. Quickwit readiness and event plane clarity
5. Demo doctor

Secondary priority:

6. Separate demo mode from dev mode
7. Pipeline page simplification follow-through

## Recommended Execution Sequence

### Slice A: Stabilize Long-Lived Processes

- add `demo:start` / `demo:stop`
- make `app:api` non-reload by default for demo use
- improve stale-port diagnostics

### Slice B: Make The Worker Legible

- keep live Rust output
- add periodic heartbeats
- add clearer stage-specific failure messages
- detect stuck/contradictory worker states

### Slice C: Make Model Prep One Path

- implement `demo:prepare-model`
- make Quickwit sync part of the happy path
- make tiny example data explicitly non-default

### Slice D: Add Demo Readiness Checks

- implement `demo:doctor`
- align pipeline page wording with the doctor output

## Success Criteria

This plan succeeds when:

- a fresh local demo can be started reliably without manual process juggling
- the operator can tell the difference between healthy progress and failure
- model-backed scoring can be prepared from one obvious default flow
- the UI no longer hides operational state behind ambiguous badges or backend
  fallbacks
- a pre-demo check can say, with high confidence, whether the system is ready

## Immediate Next Step

Build **Slice A** first:

- add managed `demo:start`
- add managed `demo:stop`
- make `app:api` stable for demo mode
- make stale-port failures explicit

That will remove the largest source of demo fragility before we spend more time
on higher-level product polish.
