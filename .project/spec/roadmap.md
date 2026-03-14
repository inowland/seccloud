# Roadmap

## Purpose
This document defines milestone truth. It names the official milestone sequence, the goal of each milestone, key dependencies, and the exit criteria required to move forward.

## Milestone Sequence

| Milestone | Title | Goal | Depends On |
| --- | --- | --- | --- |
| `m00` | Customer truthing and go/no-go | Validate the commercial and retention assumptions required for the product thesis. | None |
| `m01` | Security data substrate | Build the normalized ingest, retention, replay, and derived-state foundation. | `m00` |
| `m02` | Investigation product | Deliver the first analyst workflow and evidence-backed investigation loop. | `m01` |
| `m03` | Behavioral scoring alpha | Add customer-specific behavioral scoring on top of the investigation workflow. | `m01`, `m02` |
| `m04` | Response controls beta | Add narrow, high-confidence response actions. | `m02`, `m03` |
| `m05` | Agent expansion | Extend identities, evidence, and investigation workflow to agents and service accounts. | `m01`, `m02`, `m03` |

## Milestone Summaries

### `m00` Customer Truthing And Go/No-Go
Validate that target design partners accept vendor-run BYOC, short-lived raw telemetry, and long-lived derived state inside their own cloud account. Confirm that the proven single-company approach can be turned into a repeatable product. Lock the initial source pack, the first five detection scenarios, and the analyst workflow expectations.

Exit criteria:
- at least five design partners confirm vendor-run BYOC is acceptable
- at least five design partners accept long-lived derived state in their cloud account
- the top detection scenarios materially overlap across customers
- the initial source pack is stable enough to support productized implementation work
- founders have evidence that the solution can be sold and deployed as a reusable product, not only as a custom internal build

### `m01` Security Data Substrate
Build the minimum reusable security data plane needed for collection, normalization, replay, retention, and derived-state materialization.

Exit criteria:
- end-to-end ingest from at least four v1 sources exists
- replay is idempotent
- raw retention can be enforced without invalidating allowed derived state
- derived state can be rebuilt from retained artifacts and manifests

### `m02` Investigation Product
Ship the first analyst-facing investigation workflow with evidence retrieval, case assembly, peer comparisons, suppression, and feedback.

Exit criteria:
- the top five scenarios can be investigated in-product
- analysts can explain alerts without raw-log spelunking
- the workflow is reusable enough that most investigations do not require engineering assistance

### `m03` Behavioral Scoring Alpha
Add customer-specific behavioral scoring for the narrow v1 event classes, using retained derived state and stable feature interfaces.

Exit criteria:
- scoring improves prioritization over baseline rules
- alerts remain evidence-backed and understandable
- seeded scenario precision is stable enough for design partner evaluation

### `m04` Response Controls Beta
Add narrow response actions for high-confidence detections without broad inline blocking.

Exit criteria:
- at least two response actions are usable in a controlled beta
- response workflows integrate with the case lifecycle
- false-positive handling remains low-friction

### `m05` Agent Expansion
Extend the product surface to include service accounts, internal agents, and external agents while preserving the same evidence and case contract.

Exit criteria:
- non-human principals appear as first-class identities
- mixed human and agent cases are supported
- the same evidence and derived-state substrate supports agent use cases

## Post-M05 Direction
After the initial technical telemetry product proves repeatable value, future roadmap candidates may include:
- richer intent inference from identity and organizational context
- optional whole-person risk enrichment where customers explicitly want it
- deeper agent and non-human identity governance

These are future directions, not part of the current committed milestone sequence.

## Dependency Rules
- A milestone may not claim exit criteria completion in planning docs unless the required evidence is linked from execution artifacts.
- Scope changes that affect milestone goal, dependencies, or exit criteria require a patch to this roadmap.
- Architecture changes that alter how a milestone is achieved require both a plan update and an ADR if the change is material.
