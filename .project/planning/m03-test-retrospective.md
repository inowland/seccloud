# M03 Test Retrospective

Status: In Progress

## Failure Or Gap
- The first scoring pass initially failed to detect the compromised privileged identity scenario.
- The first evaluation harness was too weak to catch a baseline false positive once the scorer changed.
- Re-running the demo in an existing workspace preserved stale detections from the prior scorer output.
- The first report commands were not robust against invalid entries while reading an existing runtime's raw-event store.
- The first clean-path fixtures did not test whether ingest could safely handle malformed, unsupported, duplicated, or delayed source events.

## Root Cause
- The initial heuristics did not weight the combination of privileged activity and unfamiliar geography strongly enough.
- The evaluation only checked whether a scenario appeared at least once, not whether detections attached to the correct raw event scenarios.
- The demo pipeline appended to existing generated state instead of rebuilding from a clean runtime.
- The reporting path trusted every raw JSON file to deserialize into a valid event payload.
- The first synthetic source pack was too clean to exercise realistic ingest failure modes.

## Fix Applied Or Planned
- Added explicit scoring weights for privileged identity activity and for the combined unfamiliar-geo plus privileged-login pattern.
- Required existing geo history before treating a login geography as unfamiliar.
- Extended scenario evaluation to count detections by raw event scenario and to fail on unexpected detections tied to baseline or benign-control events.
- Reset generated runtime state at the start of `run-demo` so regenerated founder artifacts always reflect the current scorer.
- Hardened the raw-event loader so reporting skips invalid entries instead of crashing.
- Added initial benign-drift scenarios and explicit suppressions so the scorer is tested against more realistic approved-travel, incident-response, and finance-close behavior.
- Added source-noise fixtures plus dead-letter handling, semantic duplicate suppression, and late-arrival accounting so ingest errors do not silently contaminate normalized analytics.
- Kept the scoring interface unchanged so later learned models can replace the heuristic implementation cleanly.
- Split the scorer into explicit detector modules so later learned or customer-specific detectors can be added without rewriting investigation or evidence workflows.

## Durable Lesson
- Synthetic seeded scenarios are useful only if they fail loudly enough to force better scenario-specific detection logic before customer exposure.
- Evaluation harnesses for security detections must test for false positives and rerun cleanliness, not just positive-case coverage.
- Real product confidence comes from surviving realistic benign context, not just detecting synthetic attacks.
- Source onboarding confidence also depends on surviving realistic delivery failures and schema defects, not just parsing happy-path examples.
- Scoring logic should be modular before it becomes sophisticated; otherwise every later model iteration forces avoidable product-surface churn.

## Spec Or Plan Updates Triggered
- Patched the product contract to lock the rules-plus-heuristics PoC scoring default and the fixed scenario pack.
