# Testing Guide

## Purpose

This guide defines how requirements map to executable tests and evidence in this repo.

## Test Taxonomy

All milestone plans must map requirements to one or more of these test classes.

- Schema and adapter correctness
- Replay and idempotency
- Retention deletion and derived-state rebuildability
- Seeded insider scenarios
- Benign-change scenarios
- Deployment and bootstrap validation
- Evidence-backed alert review
- Case workflow and analyst feedback behavior
- Response control safety, when applicable

## Requirement-To-Test-To-Evidence Rule

Every implementation plan must contain a mapping table or equivalent section that states:

- the requirement being satisfied
- the exact test or validation method
- the evidence artifact that proves the result

Valid evidence examples:

- test output
- replay report
- retention deletion report
- seeded scenario evaluation report
- screenshots or API traces for evidence-backed investigation flows
- deployment bootstrap logs

## Retention Validation

Retention work is not complete unless testing proves:

- raw events are deleted on schedule
- allowed derived state is preserved
- retained derived state remains usable for scoring and investigations
- rebuild from retained artifacts and manifests works as specified

## Seeded Scenario Expectations

Scenario testing must include both malicious and benign behavior.

Required early scenarios:

- compromised privileged identity using unfamiliar resources
- unusual repo export or clone by an engineer
- unusual document or dataset access
- unusual external sharing
- benign role change, team change, or project onboarding

## Evidence-Backed Alert Review

An alert passes review only if an analyst can answer:

- why the alert fired
- what evidence supports it
- what related context is attached
- what action the analyst can take next

## Test Output Rules

- Record tests and validation results in milestone execution logs.
- Record failures and durable lessons in milestone retrospectives.
- Update implementation status only after evidence exists.

## Python Tooling

- Use `uv` as the default Python workflow for this repo.
- Create or refresh the local environment with `uv sync`.
- Run tests with `uv run python -m unittest discover -s tests -v`.
- Run the PoC CLI with `uv run seccloud ...`.
