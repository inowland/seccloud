# Founder And Design-Partner Conversation Pack

## What The Current PoC Proves
- The current BYOC-friendly contracts support a fixed four-source pack: Okta, Google Workspace, GitHub, and Snowflake.
- The current substrate can ingest synthetic telemetry, retain derived state, rebuild after raw-data deletion, and produce evidence-backed detections.
- The current scorer detects the fixed privileged behavior pack with no leakage on the current benign drift controls, including approved travel, incident response access, and finance-close access.
- The current ingest path can also classify malformed or unsupported source events into dead letters without contaminating normalized analytics.
- The current onboarding layer can validate a candidate fixture bundle against the fixed source-pack contract before import.
- The current real-source productization layer can validate vendor-shaped export bundles and map them into the raw-event contract.

## What The Current PoC Does Not Yet Prove
- It does not yet prove performance on real customer telemetry.
- It does not yet prove robustness under a much broader benign drift set such as larger travel patterns, sustained incident response, quarter-end finance surges, or major re-orgs.
- It does not yet prove that the same contracts can onboard new customers without bespoke adapter or investigation work.
- It does not yet prove resilience against larger volumes of duplicate, delayed, reordered, or schema-drifting telemetry.

## Current Discovery Questions
- Will design partners allow the derived-state retention contract in their cloud account?
- Do the four chosen sources map to repeated, high-value investigations in their environment?
- Do they want a technical telemetry product first, or do they expect HR, sentiment, or other whole-person workflows immediately?
- Which customer asks are reusable product surface, and which would drag the company toward services work?

## Current PoC Metrics
- Overall evaluation status: `pass`
- Total detections: `5`
- Unexpected detections: `0`
- Missed expected events: `0`
- Dead letters: `2`

## Next Build Priorities
- Expand beyond the current benign-drift controls into much richer drift and source-realism fixtures.
- Add stronger investigation summaries and peer-comparison views to make evidence review more reusable.
- Split the scorer into multiple detector types with a fused explanation layer.
- Extend the current source manifest and fixture validation flow into a stronger onboarding package with more semi-real and customer-provided fixtures.
