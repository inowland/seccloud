# Coding Principles

## Purpose

This guide defines the working rules for implementation, documentation, and architecture changes in this repo.

## Core Rules

- Write or patch the relevant spec before implementing behavior that changes product truth.
- Treat specs as authoritative product truth and planning docs as execution intent.
- Never describe planned functionality as already implemented.
- Every implementation plan must map requirements to executable tests and concrete evidence.
- Every temporary architecture must name its retirement gate before implementation begins.
- If a plan changes scope or architecture, patch the relevant spec or add an ADR immediately in the same change.
- Execution logs record facts, deviations, blockers, and evidence only. They do not record aspiration.

## Naming And Structure

- Milestones use zero-padded names from `m00` through `m05` unless the roadmap is revised.
- Product truth lives in `project/spec/`.
- Execution blueprints, logs, and retrospectives live in `project/planning/`.
- Architectural decisions live in `project/adr/` using numbered ADR files.
- Reusable recurring document shapes live in `project/templates/`.

## Python Workflow

- Use `uv` as the default Python workflow for local development, testing, and CLI execution.
- Prefer `uv sync` and `uv run ...` over ad hoc `python` or `PYTHONPATH` command patterns in repo docs.

## Temporary Architecture Rule

Any temporary design or bridge implementation must state:

- why it exists
- what will replace it
- what evidence will prove replacement is ready
- the retirement gate milestone or condition

Temporary architecture without a retirement gate is not allowed.

## Scope Change Rule

Any of the following requires a spec patch before or alongside implementation:

- a change to supported source pack
- a change to the retention model
- a new stable public contract
- a change to milestone exit criteria
- a new operating model

Material architecture changes also require an ADR.

## Truthfulness Rule

- `project/spec/` may define `planned` product surfaces.
- `project/planning/implementation-status.md` must reflect actual current state.
- `project/planning/*-execution-log.md` must state what happened, even if the result was failure or deferral.
- `project/planning/*-test-retrospective.md` must record failures, gaps, root causes, fixes, and durable lessons after testing occurs.
