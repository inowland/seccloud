# Agent Notes

## Build & Lint

- After any code changes, run `npm run lint` from the repo root before handoff.
- If this workspace is in a git clone, install hooks with `npm run precommit:install`.
- This repo uses `uv` for Python dependency management. No special cache configuration is needed — uv uses its default global cache.
- If you change FastAPI routes, request/response models, or the OpenAPI contract, run `npm run generate:api-types` before `npm run lint`.
- If linting fails, either fix the issue or call out the blocker explicitly.
- Use `npm run lint:fix` and `npm run format` when automatic fixes are appropriate.

## Project Plan

- The build-out plan lives in `project/plan/`. Read `project/plan/README.md` for the
  milestone overview (M0–M6) and dependency graph.
- Each milestone has its own file (e.g., `m0-ml-validation.md`) with goals, success
  criteria, architecture, and deliverables.
- The current focus is M0 (ML Validation). M1 (Rust ingestion) can begin in parallel.
- When working on a milestone, read its plan file first to understand scope and
  constraints before writing code.
