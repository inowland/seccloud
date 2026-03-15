# M01 Execution Log

Status: In Progress

## Date

- 2026-03-14

## Work Actually Completed

- Patched the active PoC source pack into the product contract and milestone specs.
- Implemented canonical contracts for `Principal`, `Resource`, `Action`, `Event`, `Detection`, `Case`, and `DerivedState`.
- Implemented a filesystem-backed object-store simulation with raw evidence storage, normalized segments, ingest manifests, derived-state persistence, detections, and cases.
- Implemented synthetic source adapters and a fixed source pack for `Okta`, `Google Workspace`, `GitHub`, and `Snowflake`.
- Implemented replay-safe ingest, derived-state materialization, raw-retention enforcement, and rebuild from normalized segments.
- Extended ingest to classify malformed and unsupported source events into dead letters, track semantic duplicates, and count late arrivals.
- Added a runtime-derived source-pack capability matrix so onboarding assumptions are generated from the same source contracts used by the PoC.
- Added a source-manifest contract, fixture-bundle validator, and import path so candidate source packs can be checked before they enter the ingest pipeline.
- Added vendor-shaped export fixtures plus explicit source mappers so realistic source exports can be translated into the raw-event contract before ingest.

## Deviations From Plan

- Used a local filesystem-backed runtime instead of real cloud services for the PoC. This is recorded in ADR 0001 and keeps the interfaces portable while avoiding premature cloud provisioning.

## Evidence Produced

- `/Users/inowland/Development/seccloud/src/seccloud/contracts.py`
- `/Users/inowland/Development/seccloud/src/seccloud/storage.py`
- `/Users/inowland/Development/seccloud/src/seccloud/pipeline.py`
- `/Users/inowland/Development/seccloud/src/seccloud/source_pack.py`
- `/Users/inowland/Development/seccloud/src/seccloud/onboarding.py`
- `/Users/inowland/Development/seccloud/src/seccloud/vendor_exports.py`
- `/Users/inowland/Development/seccloud/tests/test_poc.py`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/manifests/ingest_manifest.json`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/dead_letters`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/derived/derived_state.json`
- `/Users/inowland/Development/seccloud/examples/poc/runtime/founder_artifacts/source-capability-matrix.md`
- `/Users/inowland/Development/seccloud/examples/poc/fixtures/fixed-source-pack`
- `/Users/inowland/Development/seccloud/examples/poc/vendor-fixtures/fixed-source-pack`

## Blockers And Follow-Ups

- Real design-partner fixtures are still missing, so source drift and retention posture are only validated against synthetic data.
- The fixture validator now supports repeatable dry runs, but it still only proves compatibility with the current raw-event contract, not raw vendor exports.
- The vendor mapping layer now proves compatibility with a small set of realistic export shapes, but not yet the full variability of actual customer exports or live APIs.
- A real object-store adapter and deployment substrate are still future work.
