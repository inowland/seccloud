from __future__ import annotations

import json

from seccloud.onboarding import build_source_manifest
from seccloud.pipeline import collect_ops_metadata
from seccloud.runtime_status import build_runtime_status
from seccloud.source_pack import build_source_capability_markdown, build_source_capability_matrix
from seccloud.storage import Workspace
from seccloud.vendor_exports import build_vendor_source_manifest


def _format_detection_bullets(detections: list[dict[str, object]]) -> str:
    lines: list[str] = []
    for detection in detections:
        lines.append(
            "- "
            + f"{detection['title']} "
            + f"(`{detection['scenario']}`, score={detection['score']}, severity={detection['severity']})"
        )
    return "\n".join(lines)


def _build_report_metrics(workspace: Workspace) -> dict[str, object]:
    detections = workspace.list_detections()
    dead_letters = workspace.list_dead_letters()
    if not workspace.load_ops_metadata():
        collect_ops_metadata(workspace)
    runtime_status = build_runtime_status(workspace, dsn=None)
    capability = build_source_capability_matrix(workspace)
    sources = capability["sources"]
    seeded_sources = [source for source, details in sources.items() if details["normalized_event_count"] > 0]
    contract_ready_sources = [
        source
        for source, details in sources.items()
        if details["normalized_event_count"] > 0
        and not details["missing_required_event_types"]
        and not details["missing_required_fields"]
        and details["dead_letter_count"] == 0
    ]
    detection_scenarios = sorted({item["scenario"] for item in detections})
    feature_tables = runtime_status["feature_tables"]
    materialized_feature_tables = sum(1 for value in feature_tables.values() if int(value) > 0)
    projection = runtime_status["projection"]
    projection_stream = projection.get("overview", {}).get("stream_state", {})
    return {
        "detections": detections,
        "dead_letters": dead_letters,
        "source_count": len(sources),
        "seeded_source_count": len(seeded_sources),
        "contract_ready_source_count": len(contract_ready_sources),
        "detection_scenarios": detection_scenarios,
        "feature_principal_count": runtime_status["feature_vocab"]["principal_count"],
        "feature_resource_count": runtime_status["feature_vocab"]["resource_count"],
        "materialized_feature_tables": materialized_feature_tables,
        "scoring_input_mode": runtime_status["scoring_input"]["mode"],
        "scoring_input_ready": runtime_status["scoring_input"]["ready"],
        "scoring_input_reason": runtime_status["scoring_input"]["reason"],
        "model_runtime_available": runtime_status["model_runtime"]["available"],
        "model_runtime_requested_mode": runtime_status["model_runtime"]["requested_mode"],
        "model_runtime_effective_mode": runtime_status["model_runtime"]["effective_mode"],
        "model_runtime_reason": runtime_status["model_runtime"]["reason"],
        "model_runtime_version": runtime_status["model_runtime"]["model_version"] or "none",
        "model_activation_gate_eligible": runtime_status["model_runtime"]["activation_gate"]["eligible"],
        "model_activation_gate_reason": runtime_status["model_runtime"]["activation_gate"]["reason"],
        "model_activation_history_count": len(runtime_status["model_runtime"]["recent_activation_history"]),
        "indexed_event_count": runtime_status["event_index"]["event_count"],
        "indexed_principal_count": runtime_status["event_index"]["principal_key_count"],
        "indexed_resource_count": runtime_status["event_index"]["resource_key_count"],
        "indexed_department_count": runtime_status["event_index"]["department_count"],
        "detection_context_available": runtime_status["detection_context"]["available"],
        "detection_context_event_count": runtime_status["detection_context"]["event_count"],
        "identity_profile_principal_count": runtime_status["identity_profiles"]["principal_count"],
        "identity_profile_team_count": runtime_status["identity_profiles"]["team_count"],
        "identity_profile_source": runtime_status["identity_profiles"]["source"] or "unknown",
        "projected_normalized_event_count": projection_stream.get("normalized_event_count", 0),
        "projected_detection_count": projection_stream.get("detection_count", 0),
        "projection_available": bool(projection.get("available")),
    }


def build_conversation_pack_markdown(workspace: Workspace) -> str:
    metrics = _build_report_metrics(workspace)
    detection_scenarios = metrics["detection_scenarios"]
    detection_labels = ", ".join(f"`{scenario}`" for scenario in detection_scenarios) or "`none`"
    return f"""# Founder And Design-Partner Conversation Pack

## What The Current PoC Proves
- The current BYOC-friendly contracts support a fixed four-source pack: \
Okta, Google Workspace, GitHub, and Snowflake.
- The current substrate can ingest synthetic telemetry, retain derived state, \
rebuild after raw-data deletion, and produce evidence-backed detections.
- The current scorer surfaces a small set of concrete high-risk detections across \
identity, code, collaboration, and data workflows without requiring a bespoke \
analyst workflow.
- The current ingest path can also classify malformed or unsupported source events \
into dead letters without contaminating normalized analytics.
- The current onboarding layer can validate a candidate fixture bundle against the \
fixed source-pack contract before import.
- The current real-source productization layer can validate vendor-shaped export \
bundles and map them into the raw-event contract.

## What The Current PoC Does Not Yet Prove
- It does not yet prove performance on real customer telemetry.
- It does not yet prove robustness under a much broader benign drift set such as \
larger travel patterns, sustained incident response, quarter-end finance surges, \
or major re-orgs.
- It does not yet prove that the same contracts can onboard new customers without \
bespoke adapter or investigation work.
- It does not yet prove resilience against larger volumes of duplicate, delayed, \
reordered, or schema-drifting telemetry.

## Current Discovery Questions
- Will design partners allow the derived-state retention contract in their \
cloud account?
- Do the four chosen sources map to repeated, high-value investigations in their \
environment?
- Do they want a technical telemetry product first, or do they expect HR, \
sentiment, or other whole-person workflows immediately?
- Which customer asks are reusable product surface, and which would drag the \
company toward services work?

## Current PoC Metrics
- Total detections: `{len(metrics["detections"])}`
- Detection types observed: {detection_labels}
- Dead letters: `{len(metrics["dead_letters"])}`
- Sources with normalized telemetry: \
`{metrics["seeded_source_count"]}/{metrics["source_count"]}`
- Source contracts fully satisfied: \
`{metrics["contract_ready_source_count"]}/{metrics["source_count"]}`
- Durable feature tables materialized: `{metrics["materialized_feature_tables"]}/5`
- Scoring input mode: `{metrics["scoring_input_mode"]}` \
(`ready={metrics["scoring_input_ready"]}`, `{metrics["scoring_input_reason"]}`)
- Model runtime: requested=`{metrics["model_runtime_requested_mode"]}`, \
effective=`{metrics["model_runtime_effective_mode"]}`, \
available=`{metrics["model_runtime_available"]}`, \
version=`{metrics["model_runtime_version"]}` \
(`{metrics["model_runtime_reason"]}`)
- Model activation gate: eligible=`{metrics["model_activation_gate_eligible"]}` \
(`{metrics["model_activation_gate_reason"]}`), history entries=`{metrics["model_activation_history_count"]}`
- Principals in feature vocab: `{metrics["feature_principal_count"]}`
- Resources in feature vocab: `{metrics["feature_resource_count"]}`
- Indexed normalized events: `{metrics["indexed_event_count"]}`
- Indexed principals/resources/departments: \
`{metrics["indexed_principal_count"]}/{metrics["indexed_resource_count"]}/{metrics["indexed_department_count"]}`
- Detection context materialized: `{metrics["detection_context_available"]}` \
for `{metrics["detection_context_event_count"]}` events
- Identity profiles loaded: `{metrics["identity_profile_principal_count"]}` principals / \
`{metrics["identity_profile_team_count"]}` teams \
(`{metrics["identity_profile_source"]}`)
- Projection available: `{metrics["projection_available"]}`
- Projected normalized events/detections: \
`{metrics["projected_normalized_event_count"]}/{metrics["projected_detection_count"]}`

## Next Build Priorities
- Expand beyond the current benign-drift controls into much richer drift and \
source-realism fixtures.
- Add stronger investigation summaries and peer-comparison views to make evidence \
review more reusable.
- Split the scorer into multiple detector types with a fused explanation layer.
- Extend the current source manifest and fixture validation flow into a stronger \
onboarding package with more semi-real and customer-provided fixtures.

## Active Detections
{_format_detection_bullets(metrics["detections"])}
"""


def export_founder_artifacts(workspace: Workspace) -> dict[str, str]:
    source_inventory = """# Source Inventory

## Fixed PoC Source Pack
- Okta
- Google Workspace
- GitHub
- Snowflake

## Contract Notes
- Raw telemetry stays inside the customer environment.
- The source contracts are stable enough that synthetic fixtures can later be \
replaced with customer fixtures without redesign.
"""

    retention_matrix = """# Retention Matrix

| Class | Retention | Notes |
| --- | --- | --- |
| Raw events | 7 days hot | Deleted by retention enforcement. |
| Profiles | 90-365 days | Durable identity and feature manifests. |
| Peer groups | 90-365 days | Durable feature manifests. |
| Access histories | 90-365 days | Durable feature manifests. |
| Aggregates | 90-365 days | Retained derived state. |
| Embeddings | Ephemeral | Rebuilt from current features when needed. |
| Case artifacts | 90-365 days | Preserved to survive raw-data deletion. |
| Feedback labels | 90-365 days | Retained for future prioritization. |
"""

    deployment_boundary = """# Deployment Boundary

```mermaid
flowchart LR
    subgraph Customer["Customer Account Boundary"]
        Raw["Raw Evidence Objects"]
        Norm["Normalized Segments"]
        State["Derived State"]
        Detect["Detection and Case CLI/API"]
    end
    subgraph Vendor["Vendor Control Plane"]
        Ops["Health, Inventory, Redacted Ops Metrics"]
    end
    Raw --> Norm --> State --> Detect
    State --> Ops
```

- No raw telemetry leaves the customer account boundary.
- Only deployment health, inventory, and optional redacted operational metrics may leave.
"""

    artifacts = {
        "source-manifest.json": json.dumps(build_source_manifest(), indent=2, sort_keys=True) + "\n",
        "vendor-source-manifest.json": json.dumps(build_vendor_source_manifest(), indent=2, sort_keys=True) + "\n",
        "source-inventory.md": source_inventory,
        "source-capability-matrix.md": build_source_capability_markdown(workspace),
        "retention-matrix.md": retention_matrix,
        "deployment-boundary.md": deployment_boundary,
        "conversation-pack.md": build_conversation_pack_markdown(workspace),
    }
    for name, content in artifacts.items():
        workspace.save_founder_artifact(name, content)
    return {name: str(workspace.founder_dir / name) for name in artifacts}
