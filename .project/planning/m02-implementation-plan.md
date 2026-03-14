# M02 Implementation Plan

Status: Planned

## Objective And Linked Requirements
- Implement the investigation workflow defined in [m02.md](/Users/inowland/Development/seccloud/.project/spec/m02.md).
- Preserve the alert and case contract defined in [product-contract.md](/Users/inowland/Development/seccloud/.project/spec/product-contract.md).
- Lock the privileged behavior scenario pack and expose it through an internal API or CLI suitable for the PoC.

## Exact Implementation Steps
1. Implement evidence retrieval and related-context assembly from the substrate.
2. Implement entity timeline assembly for principal, resource, and case views.
3. Implement case creation, disposition, suppression, and analyst feedback capture.
4. Implement productized policy packs for the fixed privileged behavior scenario pack.
5. Build the minimum analyst-facing internal API or CLI needed to execute investigations.
6. Document case lifecycle and analyst operating expectations.

## Dependency Order
1. Evidence retrieval before timeline assembly.
2. Timeline assembly before case presentation.
3. Case model before suppression and feedback.
4. Policy packs before end-to-end scenario validation.

## Requirement To Test To Evidence

| Requirement | Test Or Validation | Evidence |
| --- | --- | --- |
| Analysts can explain alerts | Evidence-backed alert review using seeded cases | Investigation review notes or traces |
| Top scenarios can be investigated in-product | Seeded scenario tests across the first policy pack | Scenario evaluation report |
| Case workflow records analyst actions | Case workflow and disposition tests | Workflow test output |
| Suppression and feedback are reusable | Feedback persistence and re-evaluation tests | Feedback behavior report |

## Required Tests
- evidence-backed alert review
- seeded insider scenarios
- case workflow and analyst feedback behavior
- benign-change scenarios where supported policy packs should not flood analysts

## Required Evidence Artifacts
- investigation walkthrough traces
- seeded scenario evaluation report
- case workflow test output
- suppression and feedback behavior report
- CLI or API walkthrough output

## Open Risks
- evidence assembly may reveal missing fields in the event schema
- policy packs may rely on unsupported context not available from the substrate
- analyst workflow may be broader than the initial interface supports

## Temporary Architecture And Retirement Gate
- Temporary element: thin analyst interface optimized for internal and design-partner use
- Replacement: fuller product surface once repeated workflows are validated
- Retirement gate: revisit after design-partner evidence shows repeated case patterns and usability gaps
