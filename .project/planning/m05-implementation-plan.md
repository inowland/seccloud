# M05 Implementation Plan

Status: Planned

## Objective And Linked Requirements
- Implement agent expansion as defined in [m05.md](/Users/inowland/Development/seccloud/.project/spec/m05.md).
- Preserve the core contracts in [product-contract.md](/Users/inowland/Development/seccloud/.project/spec/product-contract.md).

## Exact Implementation Steps
1. Extend the `Principal`, `Action`, and `Event` contracts to represent service accounts and agents.
2. Update evidence retrieval and case timeline assembly for mixed principal types.
3. Extend policy packs and scoring interfaces to supported non-human actions.
4. Validate mixed human and agent investigations using seeded scenarios.
5. Document the supported non-human principal model and its limits.

## Dependency Order
1. Principal and action contract updates before case workflow changes.
2. Case and evidence updates before mixed-principal scenario validation.
3. Policy and scoring extensions after the core data model is stable.

## Requirement To Test To Evidence

| Requirement | Test Or Validation | Evidence |
| --- | --- | --- |
| Non-human principals are first-class identities | Schema and workflow validation for mixed principal cases | Mixed-principal validation report |
| Mixed human and agent cases work | Seeded mixed-principal scenario tests | Mixed-scenario evaluation report |
| No bespoke pipeline is required | Integration tests using the existing substrate and case flow | Reuse validation report |
| Evidence remains understandable | Analyst review of mixed-principal investigations | Analyst review notes |

## Required Tests
- schema and adapter correctness for extended principal types
- seeded mixed-principal scenarios
- evidence-backed alert review for mixed-principal cases
- reuse validation across the existing substrate

## Required Evidence Artifacts
- mixed-principal validation report
- mixed-scenario evaluation report
- reuse validation report
- analyst review notes

## Open Risks
- agent identity semantics may vary significantly by source
- current evidence contracts may not capture enough tool lineage
- mixed-principal workflows may force earlier surface redesign than planned

## Temporary Architecture And Retirement Gate
- Temporary element: limited agent model covering only the approved principal and action classes
- Replacement: broader agent support only after repeated use cases justify expansion
- Retirement gate: revisit after mixed-principal investigations are proven in supported scenarios
