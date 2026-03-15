# Agent Notes

- After any code changes, run `npm run lint` from the repo root before handoff.
- If this workspace is in a git clone, install hooks with `npm run precommit:install`.
- This repo uses a workspace-local `uv` cache at `.uv-cache` in npm scripts. If you run `uv` commands directly, prefer prefixing them with `UV_CACHE_DIR=.uv-cache`.
- If you change FastAPI routes, request/response models, or the OpenAPI contract, run `npm run generate:api-types` before `npm run lint`.
- If linting fails, either fix the issue or call out the blocker explicitly.
- Use `npm run lint:fix` and `npm run format` when automatic fixes are appropriate.
