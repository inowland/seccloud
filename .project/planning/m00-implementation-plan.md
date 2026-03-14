# M00 Implementation Plan

Status: Planned

## Objective And Linked Requirements
- Validate the assumptions defined in [m00.md](/Users/inowland/Development/seccloud/.project/spec/m00.md) and the deployment and retention commitments in [product-contract.md](/Users/inowland/Development/seccloud/.project/spec/product-contract.md).
- Translate prior single-company product evidence into a repeatability test for the first commercial wedge.

## Exact Implementation Steps
1. Write the founder conversation pack covering deployment model, retention model, source pack, and top scenarios.
2. Include explicit questions that separate reusable product needs from bespoke services expectations and that distinguish technical telemetry demand from whole-person IRM demand.
3. Create a standardized design-partner interview template and decision log.
4. Run founder-led conversations with target design partners and capture responses consistently.
5. Summarize common requirements, objections, productization risks, and allowed retention boundaries.
6. Produce a go/no-go memo that states whether the assumptions remain valid.

## Dependency Order
1. Product contract and roadmap must be current.
2. Conversation pack and capture template must exist before interviews start.
3. Interview outputs must be consolidated before the go/no-go memo is written.

## Requirement To Test To Evidence

| Requirement | Test Or Validation | Evidence |
| --- | --- | --- |
| Vendor-run BYOC is acceptable | Interview confirmation from at least five target customers | Interview notes and summary memo |
| Long-lived derived state is acceptable | Written retention answers from design partners | Retention matrix and go/no-go memo |
| Initial source pack is viable | Source priority ranking from customer conversations | Source pack summary and decision log |
| Top scenarios overlap | Compare scenario rankings across conversations | Scenario overlap analysis |
| Wedge is productizable | Review interview data for recurring versus bespoke asks | Productization risk memo |
| Current phase should stay technical-telemetry-first | Capture whether buyers want HR, sentiment, or OSINT workflows now | Scope-boundary summary |

## Required Tests
- completeness check that each design-partner record captures deployment, retention, sources, and top scenarios
- consistency check that evidence is sufficient to support a go/no-go decision

## Required Evidence Artifacts
- founder conversation pack
- interview template
- design-partner decision log
- retention matrix
- source priority summary
- productization risk memo
- scope-boundary summary
- go/no-go memo

## Open Risks
- customers may reject long-lived derived state even inside their own cloud account
- customers may require broader source coverage than the initial wedge can support
- design-partner enthusiasm may not translate into repeated workflows
- customers may want whole-person IRM capabilities earlier than the team can responsibly support
- prior internal success may not map cleanly to a multi-customer product boundary

## Temporary Architecture And Retirement Gate
- Temporary element: founder-maintained interview and decision capture documents
- Replacement: structured product and sales qualification workflows
- Retirement gate: retire after `m00` closes and the design-partner truth is codified in roadmap and planning updates
