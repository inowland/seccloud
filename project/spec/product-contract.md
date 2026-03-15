# Product Contract

## Purpose

This document defines the authoritative product truth for the first company wedge. It describes what the product is, what it must do, and the operating assumptions that cannot drift without a spec update.

## Product Thesis

The product is a BYOC behavioral detection and investigation system for privileged human actions in high-value enterprise systems. It is designed to operate inside the customer's cloud account, retain only short-lived raw telemetry, and preserve long-lived derived state required for customer-specific behavioral analysis.

The initial commercial wedge is not a general insider-risk suite, a SIEM, or a general observability platform. The initial wedge is high-confidence detection and investigation for privileged human activity in a narrow source pack.

The company-level direction is intent-aware security for privileged humans and agents. In the current phase, "intent-aware" means inferring risk from technical telemetry, behavioral history, peer context, privilege context, and evidence-backed investigations. It does not yet include whole-person monitoring, HR workflows, sentiment analysis, or OSINT.

## Prior Product Evidence

- A cofounder has previously built this class of system successfully inside a single enterprise environment.
- The primary early-company question is not whether the underlying detection and investigation approach can solve a real problem.
- The primary early-company question is whether that success can be productized into a repeatable, scalable, BYOC offering across many enterprises without becoming a bespoke services business.

## Buyer And Users

- Primary buyer: enterprise security leadership, including CISO and insider-risk or SOC leadership.
- Primary user: insider-risk analysts, SOC analysts, and security engineering teams responsible for investigations.
- Secondary user: technical account teams and forward deployed engineering teams, limited to configuration, onboarding, and policy-pack tuning.

## Deployment Model

- Single-tenant BYOC deployment in the customer cloud account.
- Vendor-run operating model for the initial product.
- AWS is the first deployment target.
- Storage, collector, and execution interfaces must remain portable enough to support GCP and Azure later without rewriting the product contract.

## Data Plane Architecture

- API-serving processes are stateless and separate from collector, ingestion, normalization, and detection workers.
- The product must support both scheduled pull-based collectors against customer APIs and authenticated push-based ingestion for high-volume telemetry.
- The first product-target substrate uses customer-local object storage for raw and normalized event artifacts, an asynchronous queue for processing coordination, and `Postgres` for detections, cases, checkpoints, manifests, hot event indexes, and application-facing read models.
- The lake layout, manifest contract, and object-pointer semantics for customer-local object storage are versioned product surfaces and must not drift outside the contract defined in `/Users/inowland/Development/seccloud/project/spec/lake-contract.md`.
- The hot event index schema and detection-to-event linkage rules for `Postgres` are versioned product surfaces and must not drift outside the contract defined in `/Users/inowland/Development/seccloud/project/spec/hot-event-index-contract.md`.
- The identity rules for `tenant_id`, `integration_id`, `event_id`, `event_key`, `entity_id`, and alias bindings are versioned product surfaces and must not drift outside the contract defined in `/Users/inowland/Development/seccloud/project/spec/identity-contract.md`.
- The API tier may read from projections and event indexes, but it must not depend on running ingestion or detection work inline with request handling.

## Data Retention Contract

- Raw source payloads are short-lived and retained only for a bounded hot window, initially targeted at about 7 days.
- Derived state is long-lived and retained inside the customer account for 90 to 365 days.
- Normalized analytical event records and event indexes may be retained inside the customer account for longer windows when required for replay, detection explainability, and investigation timelines, subject to customer-configured retention rules.
- Derived state may include:
  - per-principal profiles
  - peer groups
  - resource access histories
  - aggregates
  - embeddings
  - normalized analytical event batches
  - event indexes and event-to-detection linkage metadata
  - case artifacts
  - suppression state
  - analyst feedback labels
- The product contract requires the ability to delete raw telemetry on schedule while preserving valid derived state and case evidence.

## Initial Supported Source Pack

The active PoC source pack is fixed and must stay narrow and high-signal.

- Identity provider: Okta.
- Collaboration suite: Google Workspace.
- Code system: GitHub.
- High-value data system: Snowflake.

Additional sources are not part of the initial contract unless added by roadmap milestone or later spec revision.

## Initial Scenario Pack

The active PoC scenario pack is fixed to the privileged behavior pack.

- compromised privileged identity using unfamiliar resources
- unusual repo clone or export by engineer
- unusual document or dataset access
- unusual external sharing
- benign role, project, or team change as a false-positive control

## Core Data Contracts

The following concepts are part of the stable product surface and must remain versioned from day one.

- `Principal`: a human identity in v1, with reserved fields for service accounts and agents.
- `Resource`: repo, document, dataset, table, API, app, or model.
- `Action`: read, write, share, clone, query, upload, prompt, execute, or admin-change.
- `Event`: principal, action, resource, timestamp, environment, provenance, and evidence pointer.
- `Detection`: score, reason codes, confidence, related events, related entities, and model version.
- `Case`: timeline, evidence bundle, analyst actions, disposition, and feedback.
- `DerivedState`: customer-local long-lived behavioral context needed for scoring and investigation.
- `ObjectPointer`: a versioned reference to an immutable retained lake object and, when applicable, a specific record inside that object.
- `BatchManifest`: a versioned description of one immutable raw or normalized lake batch, including replay and retention metadata.
- `HotEventIndexRecord`: a versioned Postgres read-model row for one retained normalized event, including query fields and lake pointers.
- `DetectionEventEdge`: a versioned Postgres linkage between a detection and an indexed event, preserving event order within the detection.
- `TenantIdentity`: the stable deployment-scoped identifier that bounds deterministic keys and retained data.
- `IntegrationIdentity`: the stable configured source-instance identifier within one tenant.
- `AliasBinding`: the tenant-scoped mapping from one observed principal or resource alias to one canonical `entity_id`.

These contracts must remain extensible enough for later agent identities, privilege-state context, session lineage, and graph-oriented relationship modeling without breaking the initial technical telemetry wedge.

## Detection And Evidence Contract

Every alert must be explainable and evidence-backed.

Each detection must provide:

- a reason for why the activity is unusual
- relevant feature or comparison context
- evidence pointers back to immutable source material
- enough related context to support an investigation timeline

The detection path must not depend on opaque LLM output. LLMs may assist explanation or summarization, but the alert itself must remain attributable to structured evidence and stable detection logic.

## Control-Plane Boundary

- No raw telemetry leaves the customer account boundary.
- The control plane may receive deployment health, inventory, and optional redacted operational metrics.
- Detection logic, retained state, and raw evidence remain inside the customer environment.

## PoC Defaults

- Synthetic-first data generation is allowed for the PoC as long as the stable contracts are written so real customer fixtures can replace synthetic generators without redesign.
- The analyst-facing surface for the PoC may include an internal API, CLI, and temporary demo web UI, but none of these should be mistaken for the final supported product surface.
- The first scoring path is rules plus heuristics behind a stable scoring interface.
- The PoC is optimized to answer productization and architecture questions for founder and design-partner conversations, not to prove that insider risk exists as a category.
- The current local demo may continue to simulate the product data plane with a filesystem-backed runtime, but that simulation is not the product-target deployment architecture.
- A temporary Postgres-backed read model may be used for demo projections while the repo transitions toward the separate-worker, object-storage-backed substrate defined for the product target.

## Case Contract

The investigation workflow must support:

- creating a case from one or more detections
- attaching evidence and related events
- producing an entity timeline
- recording analyst decisions and dispositions
- capturing feedback for future prioritization or suppression

## Non-Negotiable Operating Assumptions

- Vendor-run BYOC is the default operational model for the first product phase.
- Cross-customer training on raw customer data is out of scope.
- The hot path is primarily structured features, rules, peer-history context, and customer-specific scoring.
- Data and model execution stay inside the customer environment.
- TAMs and FDEs may configure supported packs and policies, but they must not author bespoke core pipelines or per-customer scoring systems.
- The current product phase is a technical security product, not a full insider-risk-management program that depends on HR, legal, sentiment, or public-source intelligence workflows.

## Revision Rules

This contract must be patched before or alongside any change to:

- deployment model
- retention model
- v1 source pack
- stable data contracts
- evidence or case expectations
