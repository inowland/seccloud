# Not Building

## Purpose
This document names the deliberate non-goals and deferred surfaces for the current company phase. It exists to prevent accidental scope drift and to force explicit revisit triggers.

## Deferred Or Rejected For The Current Phase

| Surface | Decision | Rationale | Revisit Trigger |
| --- | --- | --- | --- |
| Custom Rust data plane | Not building now | The first risk is product and workflow validation, not infrastructure novelty. | Revisit after `m02` proves workflow value and `m03` proves scoring value. |
| Arbitrary stream processing platform | Not building now | The initial product needs a narrow security data substrate, not a generic data engine. | Revisit only if product requirements exceed the object-store-native substrate. |
| Generic observability platform | Not building now | The wedge is insider-risk detection and investigation, not a general platform sale. | Revisit only if a future product line requires separate observability positioning. |
| Generalized RAG and query engine | Not building now | Freeform retrieval is not the bottleneck for the first product; evidence-backed workflows are. | Revisit after reusable case workflows exist and demand is proven. |
| Broad blocking or inline enforcement | Not building now | High-confidence detection and investigation must come before broad prevention. | Revisit after `m04` response controls beta validates narrow actions. |
| Cross-customer learning on raw data | Not building now | It conflicts with the BYOC and retention posture expected by target customers. | Revisit only if product posture and customer approvals change materially. |
| Customer-operated day-2 runtime | Not building now | Vendor-run BYOC reduces operational fragmentation early. | Revisit if procurement or scale forces a different operating model. |
| Bespoke per-customer pipelines | Not building now | This creates an FDE-heavy business and blocks scalable productization. | Revisit only for temporary bridge work with an explicit retirement gate. |
| HR-system-native workflows | Not building now | The current phase is a technical security product, not a cross-functional insider-risk-management suite. | Revisit only after the technical telemetry wedge proves repeatable and customers ask for deeper workflow integration. |
| Employee sentiment analysis | Not building now | This expands privacy, legal, and product scope far beyond the current wedge. | Revisit only if customers explicitly want whole-person monitoring and the governance model is clear. |
| OSINT-driven employee risk scoring | Not building now | Public-source intelligence introduces major privacy, ethics, and operational complexity. | Revisit only after explicit customer demand and policy review. |
| Federated learning and differential privacy infrastructure | Not building now | The current product does not yet need multi-party training infrastructure to validate value. | Revisit only when real customer model-training constraints require it. |
| GNN-first or LLM-first detection stack | Not building now | The first step is proving product value and repeatability with simpler, inspectable scoring interfaces. | Revisit after the baseline scoring path and data contracts are validated on real customer fixtures. |

## Rules
- Any item removed from this list requires a spec patch and likely an ADR.
- Temporary exceptions must name an owner, retirement gate, and evidence showing why the exception is necessary.
