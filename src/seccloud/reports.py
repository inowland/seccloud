from __future__ import annotations

import json

from seccloud.onboarding import build_source_manifest
from seccloud.pipeline import evaluate_scenarios
from seccloud.source_pack import build_source_capability_markdown
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


def build_evaluation_summary_markdown(workspace: Workspace) -> str:
    evaluation = evaluate_scenarios(workspace)
    detections = workspace.list_detections()
    ingest_diagnostics = evaluation["ingest_diagnostics"]
    lines = [
        "# Evaluation Summary",
        "",
        "## Headline",
        f"- Overall evaluation status: `{'pass' if evaluation['summary']['passes'] else 'fail'}`",
        f"- Total detections: `{evaluation['summary']['total_detection_count']}`",
        f"- Unexpected detections: `{evaluation['summary']['unexpected_detection_count']}`",
        f"- Missed expected events: `{evaluation['summary']['missed_expected_event_count']}`",
        "",
        "## Scenario Coverage",
    ]
    for scenario, result in evaluation["results"].items():
        lines.append(
            "- "
            + f"`{scenario}`: status=`{result['status']}`, "
            + f"expected_events=`{result['expected_event_count']}`, "
            + f"matching_detections=`{result['matching_detection_count']}`"
        )
    lines.extend(
        [
            "",
            "## Source Metrics",
        ]
    )
    for source, metrics in evaluation["source_metrics"].items():
        lines.append(
            "- "
            + f"`{source}`: raw_events=`{metrics['raw_event_count']}`, "
            + f"detections=`{metrics['detection_count']}`, "
            + f"unexpected_detections=`{metrics['unexpected_detection_count']}`, "
            + f"dead_letters=`{metrics['dead_letter_count']}`"
        )
    lines.extend(
        [
            "",
            "## Ingest Diagnostics",
            f"- Dead letters: `{ingest_diagnostics['dead_letter_count']}`",
            f"- Dead-letter reasons: `{ingest_diagnostics['dead_letter_reason_counts']}`",
        ]
    )
    lines.extend(
        [
            "",
            "## Current Decision Signals",
            "- The PoC currently shows zero unexpected detections on baseline, benign-control, and initial benign-drift events."
            if not evaluation["unexpected_detections"]
            else "- The PoC still leaks detections onto baseline or benign-context events.",
            "- The fixed source pack produces evidence-backed detections across identity, code, collaboration, and data sources.",
            "- The ingest path now distinguishes valid telemetry from malformed or unsupported events instead of pretending every source delivery is trustworthy.",
            "- The next product question is whether the same contracts survive a much broader benign-drift set and more realistic source variability.",
            "",
            "## Active Detections",
            _format_detection_bullets(detections),
        ]
    )
    if evaluation["missed_expected_events"]:
        lines.extend(["", "## Missed Expected Events"])
        for item in evaluation["missed_expected_events"]:
            lines.append(
                "- "
                + f"`{item['source_event_id']}` from `{item['source']}` "
                + f"for scenario `{item['scenario']}` on resource `{item['resource_id']}`"
            )
    return "\n".join(lines) + "\n"


def build_conversation_pack_markdown(workspace: Workspace) -> str:
    evaluation = evaluate_scenarios(workspace)
    return f"""# Founder And Design-Partner Conversation Pack

## What The Current PoC Proves
- The current BYOC-friendly contracts support a fixed four-source pack: Okta, Google Workspace, GitHub, and Snowflake.
- The current substrate can ingest synthetic telemetry, retain derived state, rebuild after raw-data deletion, and produce evidence-backed detections.
- The current scorer detects the fixed privileged behavior pack with no leakage on the current benign drift controls, including approved travel, incident response access, and finance-close access.
- The current ingest path can also classify malformed or unsupported source events into dead letters without contaminating normalized analytics.
- The current onboarding layer can validate a candidate fixture bundle against the fixed source-pack contract before import.
- The current real-source productization layer can validate vendor-shaped export bundles and map them into the raw-event contract.

## What The Current PoC Does Not Yet Prove
- It does not yet prove performance on real customer telemetry.
- It does not yet prove robustness under a much broader benign drift set such as larger travel patterns, sustained incident response, quarter-end finance surges, or major re-orgs.
- It does not yet prove that the same contracts can onboard new customers without bespoke adapter or investigation work.
- It does not yet prove resilience against larger volumes of duplicate, delayed, reordered, or schema-drifting telemetry.

## Current Discovery Questions
- Will design partners allow the derived-state retention contract in their cloud account?
- Do the four chosen sources map to repeated, high-value investigations in their environment?
- Do they want a technical telemetry product first, or do they expect HR, sentiment, or other whole-person workflows immediately?
- Which customer asks are reusable product surface, and which would drag the company toward services work?

## Current PoC Metrics
- Overall evaluation status: `{"pass" if evaluation["summary"]["passes"] else "fail"}`
- Total detections: `{evaluation["summary"]["total_detection_count"]}`
- Unexpected detections: `{evaluation["summary"]["unexpected_detection_count"]}`
- Missed expected events: `{evaluation["summary"]["missed_expected_event_count"]}`
- Dead letters: `{evaluation["ingest_diagnostics"]["dead_letter_count"]}`

## Next Build Priorities
- Expand beyond the current benign-drift controls into much richer drift and source-realism fixtures.
- Add stronger investigation summaries and peer-comparison views to make evidence review more reusable.
- Split the scorer into multiple detector types with a fused explanation layer.
- Extend the current source manifest and fixture validation flow into a stronger onboarding package with more semi-real and customer-provided fixtures.
"""


def export_founder_artifacts(workspace: Workspace) -> dict[str, str]:
    detections = workspace.list_detections()
    evaluation = evaluate_scenarios(workspace)

    source_inventory = """# Source Inventory

## Fixed PoC Source Pack
- Okta
- Google Workspace
- GitHub
- Snowflake

## Contract Notes
- Raw telemetry stays inside the customer environment.
- The source contracts are stable enough that synthetic fixtures can later be replaced with customer fixtures without redesign.
"""

    retention_matrix = """# Retention Matrix

| Class | Retention | Notes |
| --- | --- | --- |
| Raw events | 7 days hot | Deleted by retention enforcement. |
| Profiles | 90-365 days | Retained derived state. |
| Peer groups | 90-365 days | Retained derived state. |
| Access histories | 90-365 days | Retained derived state. |
| Aggregates | 90-365 days | Retained derived state. |
| Embeddings | 90-365 days | Simple PoC vectors now, model-ready contract later. |
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

    scenario_outputs = "# Evidence-Backed Scenario Outputs\n\n"
    scenario_outputs += "## Scenario Evaluation\n"
    scenario_outputs += "```json\n"
    scenario_outputs += json.dumps(evaluation, indent=2, sort_keys=True)
    scenario_outputs += "\n```\n\n## Detections\n```json\n"
    scenario_outputs += json.dumps(detections, indent=2, sort_keys=True)
    scenario_outputs += "\n```\n"

    artifacts = {
        "source-manifest.json": json.dumps(build_source_manifest(), indent=2, sort_keys=True) + "\n",
        "vendor-source-manifest.json": json.dumps(build_vendor_source_manifest(), indent=2, sort_keys=True) + "\n",
        "source-inventory.md": source_inventory,
        "source-capability-matrix.md": build_source_capability_markdown(workspace),
        "retention-matrix.md": retention_matrix,
        "deployment-boundary.md": deployment_boundary,
        "scenario-outputs.md": scenario_outputs,
        "evaluation-summary.md": build_evaluation_summary_markdown(workspace),
        "conversation-pack.md": build_conversation_pack_markdown(workspace),
    }
    for name, content in artifacts.items():
        workspace.save_founder_artifact(name, content)
    return {name: str(workspace.founder_dir / name) for name in artifacts}
