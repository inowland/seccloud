# Contributing

## Working Agreement

- Keep `main` stable and demoable.
- Prefer short-lived branches for any non-trivial change.
- Open a pull request for changes that affect product behavior, repo structure, or shared workflows.
- Keep commits focused. Avoid mixing feature work, refactors, and generated output unless they are tightly coupled.

## Local Setup

```bash
uv sync
npm install
npm run precommit:install
```

## Before Opening A Pull Request

Run:

```bash
npm run lint
uv run python -m unittest discover -s tests -v
```

If you change tracked fixtures or example inputs intentionally, mention that clearly in the PR description.

## Branching

- Branch from `main`.
- Use descriptive branch names such as `feature/demo-api-timeline` or `chore/lint-cleanup`.
- Rebase or merge from `main` before merging if the branch has drifted.

## Pull Requests

Include:

- what changed
- why it changed
- how you verified it
- any follow-up work or known limitations

## Repository Hygiene

- Do not commit local caches, virtualenvs, or generated runtime directories.
- Keep README and contributor docs updated when commands or workflows change.
- Treat `project/` as product and implementation context, not scratch space.
