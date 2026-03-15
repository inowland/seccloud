# Coding Sequence

## Purpose

This document recommends the implementation order across milestones so early work de-risks the most important assumptions and minimizes rework.

## Recommended Sequence

1. Repo conventions and templates
2. Canonical schemas and product contracts
3. Source adapter scaffolds for the v1 source pack
4. Object-store-native ingestion, manifests, and replay
5. Derived-state materialization and retention enforcement
6. Evidence retrieval and case workflow
7. Baseline rule evaluation and seeded-scenario harness
8. Scoring interface and customer-local model artifact flow
9. Richer behavioral scoring
10. Narrow response controls
11. Agent identity expansion

## Sequence Rationale

- The first two steps lock truth and repo discipline.
- Adapter scaffolds and replay de-risk the data path before analyst workflow work starts.
- Derived-state and retention must exist before a meaningful insider-risk product conversation can scale.
- Investigation workflow should precede sophisticated scoring because evidence and analyst trust are higher-risk than model novelty.
- Response controls and agent expansion should follow only after the human-centered workflow is stable.
