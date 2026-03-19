# M4 Gap Audit

## Purpose

This audit reframes M4 around the actual product problem: turning the current
runtime into an investigation experience that is legible, fast, and compelling in
a live demo. The question is not whether the repo has "case management." The
question is whether an analyst can move from alert to understanding without a
backend engineer narrating the system.

## Current Surface

### What already exists

- FastAPI exposes projected list/detail reads for detections and events, plus
  runtime and source-capability state. See `GET /api/detections`,
  `GET /api/detections/{id}`, `GET /api/events`, and `GET /api/events/{id}` in
  [src/seccloud/api.py](/Users/inowland/Development/seccloud/src/seccloud/api.py).
- The investigation layer already computes entity timelines, peer comparison,
  evidence bundles, and workspace-local case artifacts. See
  [src/seccloud/investigation.py](/Users/inowland/Development/seccloud/src/seccloud/investigation.py).
- The React app already has a strong shell, split-pane layout, detail pages, and a
  reasonably polished integrations view. See
  [web/src/App.tsx](/Users/inowland/Development/seccloud/web/src/App.tsx) and
  [web/src/detail-content.tsx](/Users/inowland/Development/seccloud/web/src/detail-content.tsx).

### What the UI currently optimizes for

- `Detections`: queue plus detection detail
- `Events`: event browser plus event detail
- `Integrations`: contract-health and operator workflow

This is visible in [web/src/routes.ts](/Users/inowland/Development/seccloud/web/src/routes.ts), where the only first-class pages are `detections`, `events`, and `integrations`.

## Audit Against M4 Success Criteria

### 1. Detection-to-investigation time

Partially met.

- The queue and split-pane detail are already fast-shaped UX.
- Clicking a detection loads a focused detail view with reasons, model rationale,
  feature attributions, peer context, related events, and evidence object keys.

Gap:

- The detail view explains fragments, not the investigation. It does not compress
  the answer to "what changed, why this matters, and what to look at next."
- There is no guided analyst path after opening a detection.
- The full experience still feels like a record inspector, not an investigation
  workflow.

### 2. Entity timeline

Not met in product surface.

- The backend already has `get_entity_timeline(...)` in
  [src/seccloud/investigation.py](/Users/inowland/Development/seccloud/src/seccloud/investigation.py).

Gap:

- FastAPI does not expose an entity timeline endpoint today.
- The UI has no entity page, no entity route, and no principal-centered view.
- A detection can open linked events, but cannot pivot into "show me this
  principal across sources over time."

### 3. Peer comparison

Partially met.

- Detection detail already includes peer-context metrics.

Gap:

- The peer comparison is a flat metric list, not an interpretable comparison.
- There is no visual baseline-vs-current view.
- There is no way to compare behavior outside the current detection context or
  pivot into peer context as its own exploration mode.

### 4. Evidence bundling

Partially met.

- Detection detail includes feature attributions, linked events, and evidence
  object keys.
- Event detail links a normalized event to provider event ID and raw object key.

Gap:

- The evidence is not assembled into a coherent chain of proof.
- "Evidence objects" stops at storage keys and retention status; it does not show
  the raw payload inline or summarize what in that payload matters.
- Feature attributions are present, but the UI does not connect them back to the
  event behavior in plain language.

### 5. Case management

Technically present in workspace logic, but not meaningful for M4 yet.

- Workspace helpers can create/update cases and summarize grouped detections.

Gap:

- These flows are not surfaced in the current product.
- More importantly, this is not the highest-leverage problem right now. A weak
  investigation surface wrapped in case CRUD is still a weak demo.

### 6. Demo narrative

Not met.

- The integrations page already has a strong "status -> implications -> next
  actions" narrative.

Gap:

- Detections do not have the equivalent narrative treatment.
- There is no presenter flow across queue -> detection -> entity -> related
  activity.
- The app does not currently answer the full M4 narrative:
  `what changed`, `why it fired`, `what else is related`, `what should I do next`.

## Key Product Gaps

### Gap 1: No principal-centered investigation surface

The biggest hole is that the product has no entity view. The system can talk
about a detection or an event, but not about the person or principal whose
behavior changed. That makes the experience forensic only in fragments.

Evidence in code:

- Backend helper exists: `get_entity_timeline(...)` in
  [src/seccloud/investigation.py](/Users/inowland/Development/seccloud/src/seccloud/investigation.py)
- No API route for entities in
  [src/seccloud/api.py](/Users/inowland/Development/seccloud/src/seccloud/api.py)
- No entity route in
  [web/src/routes.ts](/Users/inowland/Development/seccloud/web/src/routes.ts)

### Gap 2: Detection detail is informative but not narrative

The detection detail page contains good ingredients, but they are presented as
independent sections. An analyst still has to synthesize the story manually.

Symptoms:

- reasons list
- model rationale list
- feature attribution bars
- peer metrics list
- related events list
- evidence object key list

Those are useful components, but the page does not produce a ranked, plain-language
investigation summary.

### Gap 3: Queue triage is shallow

The detection queue is mostly a searchable list. It does not support the triage
questions M4 calls for.

Current limitations:

- search only; no first-class filters for source, scenario, severity, principal,
  or status
- no grouping by principal or scenario
- no "what changed most recently" or "highest-confidence story" prioritization in
  the UI
- no quick analyst actions except acknowledge

### Gap 4: The event browser is disconnected from the investigation flow

The events page is a decent raw browser, but it is not integrated into a
principal-centered investigation path.

Symptoms:

- rich event table controls exist
- event detail exists
- but there is no "show me all related activity around this detection" lens
- there is no pivot from event back to principal timeline or peer context

### Gap 5: Detections lack "next action" guidance

The integrations view is the strongest page in the app because it tells the user
what to do next. Detection detail does not.

Current detection actions:

- acknowledge
- open related event

Missing:

- recommended next checks
- why this severity matters operationally
- related-scope pivots such as "view this principal", "view peer baseline", or
  "inspect raw evidence"

### Gap 6: M4 has no explicit presenter mode or narrative path

The current shell supports pages and detail routes, but not a guided story for
live demo use.

Missing:

- curated path from queue -> alert -> principal -> related activity
- persistence of investigation context while pivoting
- presenter-friendly summary state that keeps model/runtime health visible without
  overtaking the investigation

### Gap 7: The API surface does not match the M4 plan

The plan states that timelines, peer comparison, and case flows are already
exposed. That is only partially true.

Reality:

- the workspace/investigation layer has timeline and case helpers
- the API currently exposes only detections, events, runtime status, overview, and
  source capability

This mismatch matters because frontend progress on M4 depends on whether the
principal-centered surfaces are actually reachable over HTTP.

## What Is Strong Today

The audit is not "everything is missing." The repo already has a useful base:

- The split-pane shell is good and should be preserved.
- Detection detail already has the right underlying ingredients.
- The integrations page proves the team can build opinionated operational UX, not
  just inspectors.
- Runtime status and source-capability data already make the system feel alive.

The problem is not lack of substrate. The problem is lack of investigation
compression and narrative focus.

## Recommended M4 Reframe

The first objective for M4 should be:

`Make detections understandable in one screen and make principals investigable in one click.`

That implies the following priority order.

### Priority 1: Principal-centered investigation

Build an entity view that answers:

- what did this principal do across sources?
- what changed relative to baseline?
- what is unusual relative to peers?

This is the missing center of gravity for M4.

### Priority 2: Detection narrative layer

Transform detection detail from a component dump into a concise investigation
story:

- what changed
- why the model or policy fired
- what evidence supports it
- what else the analyst should inspect next

### Priority 3: Triage controls in the queue

Add queue filters and grouping that help the analyst find important or coherent
stories quickly:

- severity
- source
- scenario
- principal
- status

### Priority 4: Better evidence legibility

Keep the current evidence bundle, but make it interpretable:

- inline raw payload preview or summarized raw evidence
- stronger mapping from feature attribution to concrete behavior
- clearer chain from detection -> event -> raw proof

### Priority 5: Presenter flow

After the above exists, add a lightweight guided demo flow. This should come
after the detection and entity surfaces are strong, not before.

## Suggested Build Slices

### Slice A: Entity route and entity page

Goal:

- add a first-class principal view and route

Includes:

- entity route in the frontend
- entity timeline API surface
- principal summary and cross-source activity timeline
- pivots from detection detail into the entity page

### Slice B: Detection narrative rewrite

Goal:

- make the detection page read like an analyst briefing

Includes:

- top-level "why this fired" summary
- "what changed" summary card
- "next actions" section for detections
- tighter connection between reasons, features, and linked events

### Slice C: Queue triage controls

Goal:

- make the queue useful for triage, not just browsing

Includes:

- structured filters
- grouping or sorting presets
- principal and source-focused views

## Explicit De-prioritization

For this stage of M4, case CRUD should not drive the work. Case surfaces can be
useful later, but they are downstream of a stronger detection story and a real
entity investigation flow.

## Working Thesis

If M4 succeeds, the most impressive thing in the demo should not be that a user
can acknowledge an alert. It should be that a user can open a detection and, in
seconds, understand the suspicious behavior, inspect the principal behind it, and
see enough evidence to make the alert feel credible.
