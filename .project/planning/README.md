# Planning README

## Purpose
This directory holds execution intent and execution history. It is not product truth.

## Document Authority
- `.project/spec/` defines product truth.
- `.project/planning/*-implementation-plan.md` defines how work is expected to be executed.
- `.project/planning/*-execution-log.md` records what actually happened.
- `.project/planning/*-test-retrospective.md` records failures, root causes, fixes, and durable lessons.
- `.project/planning/implementation-status.md` records the actual present state of the project.

## Operating Rules
- Do not update specs to imply implementation without execution evidence.
- Do not update implementation status from roadmap intent.
- If implementation changes scope or architecture, patch the spec and add an ADR when required.
- Every plan must contain a requirement-to-test-to-evidence mapping.
- Every temporary architecture must declare its retirement gate.

## Expected Workflow
1. Patch the relevant spec if product truth changes.
2. Author or update the milestone implementation plan.
3. Execute work and record facts in the execution log.
4. Run tests and record failures and lessons in the retrospective.
5. Update implementation status based on evidence.
