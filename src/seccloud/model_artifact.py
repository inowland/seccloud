from __future__ import annotations

import hashlib
import shutil
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from seccloud.onnx_export import ExportedModel
from seccloud.storage import Workspace, read_json, write_json

MODEL_ARTIFACT_MANIFEST_VERSION = 2
MODEL_METADATA_VERSION = 1
VALID_REQUESTED_MODES = {"heuristic", "onnx"}
MODEL_PROMOTION_POLICY_VERSION = 1
DEFAULT_REQUIRED_SOURCE_COUNT = 2
DEFAULT_IDENTITY_SOURCES = ("okta",)
DEFAULT_RESOURCE_SOURCES = ("github", "gworkspace", "snowflake")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _manifest_history_entry(
    *,
    model_id: str | None,
    metadata_path: str | None,
    requested_mode: str,
    activation_source: str,
    action: str,
    activated_at: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "model_id": model_id,
        "metadata_path": metadata_path,
        "requested_mode": requested_mode,
        "activation_source": activation_source,
        "activated_at": activated_at,
    }


def _default_model_artifact_manifest() -> dict[str, Any]:
    return {
        "manifest_version": MODEL_ARTIFACT_MANIFEST_VERSION,
        "requested_mode": "heuristic",
        "active_model_id": None,
        "active_metadata_path": None,
        "activated_at": None,
        "activation_source": None,
        "activation_history": [],
    }


def _default_model_promotion_policy() -> dict[str, Any]:
    return {
        "manifest_version": MODEL_PROMOTION_POLICY_VERSION,
        "required_source_count": DEFAULT_REQUIRED_SOURCE_COUNT,
        "require_identity_coverage": True,
        "require_resource_coverage": True,
        "identity_sources": list(DEFAULT_IDENTITY_SOURCES),
        "resource_sources": list(DEFAULT_RESOURCE_SOURCES),
    }


def _coerce_model_promotion_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload_dict = payload if isinstance(payload, dict) else {}
    policy = dict(_default_model_promotion_policy())
    policy.update(payload_dict)
    required_source_count = policy.get("required_source_count", DEFAULT_REQUIRED_SOURCE_COUNT) or 0
    policy["required_source_count"] = max(0, int(required_source_count))
    policy["require_identity_coverage"] = bool(policy.get("require_identity_coverage", True))
    policy["require_resource_coverage"] = bool(policy.get("require_resource_coverage", True))
    for key, default in (
        ("identity_sources", DEFAULT_IDENTITY_SOURCES),
        ("resource_sources", DEFAULT_RESOURCE_SOURCES),
    ):
        values = policy.get(key)
        if isinstance(values, list):
            policy[key] = sorted({str(value) for value in values if isinstance(value, str) and value})
        else:
            policy[key] = list(default)
    return policy


def load_model_promotion_policy(workspace: Workspace) -> dict[str, Any]:
    return _coerce_model_promotion_policy(workspace.load_model_promotion_policy())


def save_model_promotion_policy(
    workspace: Workspace,
    *,
    required_source_count: int,
    require_identity_coverage: bool,
    require_resource_coverage: bool,
    identity_sources: list[str],
    resource_sources: list[str],
) -> dict[str, Any]:
    policy = _coerce_model_promotion_policy(
        {
            "manifest_version": MODEL_PROMOTION_POLICY_VERSION,
            "required_source_count": required_source_count,
            "require_identity_coverage": require_identity_coverage,
            "require_resource_coverage": require_resource_coverage,
            "identity_sources": identity_sources,
            "resource_sources": resource_sources,
        }
    )
    workspace.save_model_promotion_policy(policy)
    return policy


def _coerce_model_artifact_manifest(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload_dict = payload if isinstance(payload, dict) else {}
    manifest = dict(_default_model_artifact_manifest())
    manifest.update(payload_dict)
    if "requested_mode" not in payload_dict or manifest.get("requested_mode") not in VALID_REQUESTED_MODES:
        manifest["requested_mode"] = "onnx" if manifest.get("active_metadata_path") else "heuristic"
    if not isinstance(manifest.get("activation_history"), list):
        manifest["activation_history"] = []
    return manifest


def _load_eval_report(bundle_root: Path, metadata: dict[str, Any]) -> dict[str, Any] | None:
    eval_report_path = metadata.get("eval_report_path")
    if not isinstance(eval_report_path, str) or not eval_report_path:
        return None
    payload = read_json(bundle_root / eval_report_path, None)
    return payload if isinstance(payload, dict) else None


def _coverage_gate_from_policy(
    source_gates: dict[str, Any],
    policy: dict[str, Any],
    *,
    default_reason: str,
) -> dict[str, Any]:
    required_source_count = max(0, int(policy.get("required_source_count", DEFAULT_REQUIRED_SOURCE_COUNT) or 0))
    require_identity_coverage = bool(policy.get("require_identity_coverage", True))
    require_resource_coverage = bool(policy.get("require_resource_coverage", True))
    identity_sources = {
        str(source)
        for source in policy.get("identity_sources", list(DEFAULT_IDENTITY_SOURCES))
        if isinstance(source, str) and source
    }
    resource_sources = {
        str(source)
        for source in policy.get("resource_sources", list(DEFAULT_RESOURCE_SOURCES))
        if isinstance(source, str) and source
    }
    covered_sources = sorted(
        source for source, gate in source_gates.items() if isinstance(source, str) and isinstance(gate, dict)
    )
    covered_identity_sources = [source for source in covered_sources if source in identity_sources]
    covered_resource_sources = [source for source in covered_sources if source in resource_sources]
    eligible = True
    reason = "cross_domain_coverage_gate_pass"
    if len(covered_sources) < required_source_count:
        eligible = False
        reason = "insufficient_covered_sources"
    elif require_identity_coverage and not covered_identity_sources:
        eligible = False
        reason = "identity_source_coverage_missing"
    elif require_resource_coverage and not covered_resource_sources:
        eligible = False
        reason = "resource_source_coverage_missing"
    if not covered_sources and default_reason:
        reason = default_reason
    return {
        "eligible": eligible,
        "reason": reason,
        "covered_sources": covered_sources,
        "covered_identity_sources": covered_identity_sources,
        "covered_resource_sources": covered_resource_sources,
        "required_source_count": required_source_count,
        "require_identity_coverage": require_identity_coverage,
        "require_resource_coverage": require_resource_coverage,
        "identity_sources": sorted(identity_sources),
        "resource_sources": sorted(resource_sources),
    }


def evaluate_model_activation_gate(
    bundle_root: Path,
    metadata: dict[str, Any],
    *,
    promotion_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = _coerce_model_promotion_policy(promotion_policy)
    report = _load_eval_report(bundle_root, metadata)
    if report is None:
        return {
            "eligible": False,
            "reason": "eval_report_missing",
            "status": None,
            "training_pair_count": 0,
            "final_loss": None,
            "evaluation_scope": None,
            "sampled_top1_accuracy": None,
            "pairwise_win_rate": None,
            "mean_margin": None,
            "source_gates": {},
            "evaluated_source_count": 0,
            "failing_sources": [],
            "coverage_gate": _coverage_gate_from_policy({}, policy, default_reason="eval_report_missing"),
        }

    activation_gate = report.get("activation_gate")
    if isinstance(activation_gate, dict):
        source_gates = activation_gate.get("source_gates", {})
        source_gates = source_gates if isinstance(source_gates, dict) else {}
        coverage_gate = _coverage_gate_from_policy(source_gates, policy, default_reason="coverage_gate_unavailable")
        eligible = bool(activation_gate.get("eligible", activation_gate.get("pass", False)))
        reason = str(activation_gate.get("reason", "activation_gate_declared"))
        if reason.startswith("coverage_gate_failed:"):
            eligible = coverage_gate["eligible"]
            reason = (
                "activation_gate_declared"
                if coverage_gate["eligible"]
                else f"coverage_gate_failed:{coverage_gate['reason']}"
            )
        elif eligible and not coverage_gate["eligible"]:
            eligible = False
            reason = f"coverage_gate_failed:{coverage_gate['reason']}"
        return {
            "eligible": eligible,
            "reason": reason,
            "status": report.get("status"),
            "training_pair_count": int(report.get("training_pair_count", 0) or 0),
            "final_loss": activation_gate.get("final_loss", report.get("final_loss")),
            "evaluation_scope": activation_gate.get("evaluation_scope", report.get("sampled_retrieval_scope")),
            "sampled_top1_accuracy": activation_gate.get("sampled_top1_accuracy"),
            "pairwise_win_rate": activation_gate.get("pairwise_win_rate"),
            "mean_margin": activation_gate.get("mean_margin"),
            "source_gates": source_gates,
            "evaluated_source_count": int(activation_gate.get("evaluated_source_count", 0) or 0),
            "failing_sources": list(activation_gate.get("failing_sources", [])),
            "coverage_gate": coverage_gate,
        }

    losses = report.get("losses")
    training_pair_count = int(report.get("training_pair_count", 0) or 0)
    final_loss = None
    if isinstance(losses, list) and losses:
        candidate = losses[-1]
        if isinstance(candidate, int | float):
            final_loss = float(candidate)
    eligible = report.get("status") == "trained_local_workspace" and training_pair_count > 0 and final_loss is not None
    return {
        "eligible": eligible,
        "reason": ("trained_local_workspace_eval_pass" if eligible else "trained_local_workspace_eval_missing_signal"),
        "status": report.get("status"),
        "training_pair_count": training_pair_count,
        "final_loss": final_loss,
        "evaluation_scope": report.get("sampled_retrieval_scope"),
        "sampled_top1_accuracy": report.get("sampled_retrieval", {}).get("sampled_top1_accuracy")
        if isinstance(report.get("sampled_retrieval"), dict)
        else None,
        "pairwise_win_rate": report.get("sampled_retrieval", {}).get("pairwise_win_rate")
        if isinstance(report.get("sampled_retrieval"), dict)
        else None,
        "mean_margin": report.get("sampled_retrieval", {}).get("mean_margin")
        if isinstance(report.get("sampled_retrieval"), dict)
        else None,
        "source_gates": {},
        "evaluated_source_count": 0,
        "failing_sources": [],
        "coverage_gate": _coverage_gate_from_policy(
            {},
            policy,
            default_reason="trained_local_workspace_eval_missing_signal",
        ),
    }


def build_model_artifact_metadata(
    exported: ExportedModel,
    *,
    tenant_id: str,
    model_id: str,
    model_version: str | None = None,
    model_family: str = "contrastive-facade",
    feature_schema_version: str = "feature.v1",
    exported_at: str | None = None,
    eval_report_path: str = "eval-report.json",
    principal_entity_keys_path: str | None = None,
    resource_entity_keys_path: str | None = None,
    categorical_vocabs_path: str | None = None,
    principal_vocab_count: int = 0,
    resource_vocab_count: int = 0,
    categorical_vocab_counts: dict[str, int] | None = None,
    score_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    action_towers = {
        source: {
            "path": path.name,
            "sha256": _sha256_file(path),
            "input_names": ["indices", "weights", "mask"],
            "output_name": "embedding",
        }
        for source, path in sorted(exported.action_tower_paths.items())
    }
    return {
        "artifact_version": MODEL_METADATA_VERSION,
        "tenant_id": tenant_id,
        "model_id": model_id,
        "model_version": model_version or model_id,
        "model_family": model_family,
        "scoring_mode": "onnx",
        "exported_at": exported_at or _utc_now(),
        "feature_schema_version": feature_schema_version,
        "required_feature_tables": ["action", "history", "static", "peer_group"],
        "padding": {
            "max_tokens": exported.max_tokens,
            "max_windows": exported.max_windows,
            "max_res_per_window": exported.max_res_per_window,
            "max_peers": exported.max_peers,
        },
        "action_towers": action_towers,
        "context_tower": {
            "path": exported.context_tower_path.name,
            "sha256": _sha256_file(exported.context_tower_path),
            "input_names": [
                "hist_window_indices",
                "hist_window_mask",
                "hist_num_windows",
                "peer_indices",
                "peer_weights",
                "peer_mask",
                "role",
                "location",
                "duration",
                "privilege",
            ],
            "output_name": "embedding",
        },
        "model_config": asdict(exported.config),
        "eval_report_path": eval_report_path,
        "input_vocabs": {
            "principal_entity_keys_path": principal_entity_keys_path,
            "resource_entity_keys_path": resource_entity_keys_path,
            "categorical_vocabs_path": categorical_vocabs_path,
            "principal_vocab_count": principal_vocab_count,
            "resource_vocab_count": resource_vocab_count,
            "categorical_vocab_counts": categorical_vocab_counts or {},
        },
        "score_policy": score_policy
        or {
            "detection_threshold": 0.6,
            "high_severity_threshold": 0.8,
            "calibration_source": "default_fallback",
            "calibration_reason": "legacy_default_thresholds",
            "source_policies": {},
        },
    }


def write_model_artifact_bundle(
    exported: ExportedModel,
    output_dir: Path,
    *,
    tenant_id: str,
    model_id: str,
    model_version: str | None = None,
    model_family: str = "contrastive-facade",
    feature_schema_version: str = "feature.v1",
    eval_report: dict[str, Any] | None = None,
    exported_at: str | None = None,
    principal_entity_keys: list[str] | None = None,
    resource_entity_keys: list[str] | None = None,
    categorical_vocabs: dict[str, dict[str, int]] | None = None,
    score_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    principal_entity_keys_path = None
    resource_entity_keys_path = None
    categorical_vocabs_path = None
    if principal_entity_keys is not None:
        principal_entity_keys_path = "principal-vocab.json"
        write_json(output_dir / principal_entity_keys_path, principal_entity_keys)
    if resource_entity_keys is not None:
        resource_entity_keys_path = "resource-vocab.json"
        write_json(output_dir / resource_entity_keys_path, resource_entity_keys)
    if categorical_vocabs is not None:
        categorical_vocabs_path = "categorical-vocabs.json"
        write_json(output_dir / categorical_vocabs_path, categorical_vocabs)
    metadata = build_model_artifact_metadata(
        exported,
        tenant_id=tenant_id,
        model_id=model_id,
        model_version=model_version,
        model_family=model_family,
        feature_schema_version=feature_schema_version,
        exported_at=exported_at,
        principal_entity_keys_path=principal_entity_keys_path,
        resource_entity_keys_path=resource_entity_keys_path,
        categorical_vocabs_path=categorical_vocabs_path,
        principal_vocab_count=len(principal_entity_keys or []),
        resource_vocab_count=len(resource_entity_keys or []),
        categorical_vocab_counts={key: len(values) for key, values in (categorical_vocabs or {}).items()},
        score_policy=score_policy,
    )
    write_json(output_dir / "metadata.json", metadata)
    write_json(
        output_dir / "eval-report.json",
        eval_report or {"status": "not_evaluated", "model_id": model_id},
    )
    return metadata


def install_model_artifact_bundle(
    workspace: Workspace,
    bundle_dir: Path,
    *,
    activation_source: str = "local_install",
    force: bool = False,
) -> dict[str, Any]:
    metadata = read_json(bundle_dir / "metadata.json", {})
    if not isinstance(metadata, dict) or not metadata.get("model_id"):
        raise ValueError(f"Model bundle missing metadata.json model_id: {bundle_dir}")

    model_id = str(metadata["model_id"])
    destination = workspace.models_dir / model_id
    source_root = bundle_dir.resolve()
    destination_root = destination.resolve()
    if source_root != destination_root:
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(bundle_dir, destination)
    return activate_model_artifact(
        workspace,
        model_id,
        activation_source=activation_source,
        force=force,
    )


def list_installed_model_artifacts(workspace: Workspace) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for metadata_path in sorted(workspace.models_dir.glob("*/metadata.json")):
        metadata = read_json(metadata_path, None)
        if not isinstance(metadata, dict):
            continue
        artifacts.append(
            {
                "model_id": metadata.get("model_id") or metadata_path.parent.name,
                "model_version": metadata.get("model_version"),
                "model_family": metadata.get("model_family"),
                "exported_at": metadata.get("exported_at"),
                "metadata_path": str(metadata_path.relative_to(workspace.root)),
            }
        )
    return artifacts


def activate_model_artifact(
    workspace: Workspace,
    model_id: str,
    *,
    activation_source: str = "manual_activate",
    action: str = "activate",
    force: bool = False,
) -> dict[str, Any]:
    metadata_path = workspace.models_dir / model_id / "metadata.json"
    metadata = read_json(metadata_path, None)
    if not isinstance(metadata, dict) or not metadata.get("model_id"):
        raise ValueError(f"installed model artifact is missing metadata.json: {model_id}")
    activation_gate = evaluate_model_activation_gate(
        metadata_path.parent,
        metadata,
        promotion_policy=load_model_promotion_policy(workspace),
    )
    if not force and not activation_gate["eligible"]:
        raise ValueError(f"model artifact failed activation gate: {activation_gate['reason']}")
    manifest = _coerce_model_artifact_manifest(workspace.load_model_artifact_manifest())
    activated_at = _utc_now()
    relative_metadata_path = str(metadata_path.relative_to(workspace.root))
    manifest.update(
        {
            "manifest_version": MODEL_ARTIFACT_MANIFEST_VERSION,
            "requested_mode": "onnx",
            "active_model_id": str(metadata.get("model_id") or model_id),
            "active_metadata_path": relative_metadata_path,
            "activated_at": activated_at,
            "activation_source": activation_source,
        }
    )
    manifest["activation_history"] = [
        *manifest["activation_history"],
        _manifest_history_entry(
            model_id=manifest["active_model_id"],
            metadata_path=relative_metadata_path,
            requested_mode="onnx",
            activation_source=activation_source,
            action=action,
            activated_at=activated_at,
        ),
    ]
    workspace.save_model_artifact_manifest(manifest)
    return manifest


def deactivate_model_artifact(
    workspace: Workspace,
    *,
    activation_source: str = "manual_deactivate",
) -> dict[str, Any]:
    manifest = _coerce_model_artifact_manifest(workspace.load_model_artifact_manifest())
    activated_at = _utc_now()
    manifest.update(
        {
            "manifest_version": MODEL_ARTIFACT_MANIFEST_VERSION,
            "requested_mode": "heuristic",
            "active_model_id": None,
            "active_metadata_path": None,
            "activated_at": activated_at,
            "activation_source": activation_source,
        }
    )
    manifest["activation_history"] = [
        *manifest["activation_history"],
        _manifest_history_entry(
            model_id=None,
            metadata_path=None,
            requested_mode="heuristic",
            activation_source=activation_source,
            action="deactivate",
            activated_at=activated_at,
        ),
    ]
    workspace.save_model_artifact_manifest(manifest)
    return manifest


def rollback_model_artifact(
    workspace: Workspace,
    *,
    steps: int = 1,
    activation_source: str = "manual_rollback",
    force: bool = False,
) -> dict[str, Any]:
    if steps < 1:
        raise ValueError("rollback steps must be at least 1")
    manifest = _coerce_model_artifact_manifest(workspace.load_model_artifact_manifest())
    current_model_id = manifest.get("active_model_id")
    candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in reversed(manifest["activation_history"]):
        if not isinstance(entry, dict):
            continue
        model_id = entry.get("model_id")
        metadata_path = entry.get("metadata_path")
        if not isinstance(model_id, str) or not model_id:
            continue
        if not isinstance(metadata_path, str) or not metadata_path:
            continue
        candidate = (model_id, metadata_path)
        if candidate in seen:
            continue
        seen.add(candidate)
        if model_id == current_model_id:
            continue
        candidates.append(candidate)
    if len(candidates) < steps:
        raise ValueError("no previous installed model artifact available for rollback")
    model_id, _metadata_path = candidates[steps - 1]
    return activate_model_artifact(
        workspace,
        model_id,
        activation_source=activation_source,
        action="rollback",
        force=force,
    )


def load_active_model_metadata(workspace: Workspace) -> dict[str, Any] | None:
    manifest = _coerce_model_artifact_manifest(workspace.load_model_artifact_manifest())
    metadata_path = manifest.get("active_metadata_path")
    if not isinstance(metadata_path, str) or not metadata_path:
        return None
    path = workspace.root / metadata_path
    payload = read_json(path, None)
    return payload if isinstance(payload, dict) else None


def build_model_runtime_status(workspace: Workspace) -> dict[str, Any]:
    manifest = _coerce_model_artifact_manifest(workspace.load_model_artifact_manifest())
    promotion_policy = load_model_promotion_policy(workspace)
    metadata_path = manifest.get("active_metadata_path")
    requested_mode = str(manifest.get("requested_mode", "heuristic"))
    metadata = load_active_model_metadata(workspace)
    installed_model_ids = [str(item["model_id"]) for item in list_installed_model_artifacts(workspace)]
    no_active_reason = (
        "requested_heuristic_mode"
        if manifest.get("activation_source") or manifest.get("activation_history")
        else "no_active_model"
    )
    if not isinstance(metadata_path, str) or not metadata_path:
        return {
            "available": False,
            "requested_mode": requested_mode,
            "effective_mode": "heuristic",
            "reason": no_active_reason if requested_mode == "heuristic" else "no_active_model",
            "model_id": None,
            "model_version": None,
            "model_family": None,
            "exported_at": None,
            "metadata_path": None,
            "activated_at": manifest.get("activated_at"),
            "activation_source": manifest.get("activation_source"),
            "action_source_count": 0,
            "principal_vocab_count": 0,
            "resource_vocab_count": 0,
            "activation_gate": {
                "eligible": False,
                "reason": "no_active_model",
                "status": None,
                "training_pair_count": 0,
                "final_loss": None,
            },
            "recent_activation_history": list(manifest.get("activation_history", [])[-5:]),
            "installed_model_count": len(installed_model_ids),
            "installed_model_ids": installed_model_ids,
            "score_policy": None,
            "promotion_policy": promotion_policy,
        }
    full_metadata_path = workspace.root / metadata_path
    if metadata is None:
        return {
            "available": False,
            "requested_mode": requested_mode,
            "effective_mode": "heuristic",
            "reason": "active_metadata_missing",
            "model_id": manifest.get("active_model_id"),
            "model_version": None,
            "model_family": None,
            "exported_at": None,
            "metadata_path": metadata_path,
            "activated_at": manifest.get("activated_at"),
            "activation_source": manifest.get("activation_source"),
            "action_source_count": 0,
            "principal_vocab_count": 0,
            "resource_vocab_count": 0,
            "activation_gate": {
                "eligible": False,
                "reason": "active_metadata_missing",
                "status": None,
                "training_pair_count": 0,
                "final_loss": None,
            },
            "recent_activation_history": list(manifest.get("activation_history", [])[-5:]),
            "installed_model_count": len(installed_model_ids),
            "installed_model_ids": installed_model_ids,
            "score_policy": None,
            "promotion_policy": promotion_policy,
        }
    action_towers = metadata.get("action_towers", {})
    context_tower = metadata.get("context_tower", {})
    input_vocabs = metadata.get("input_vocabs", {})
    required_paths = []
    if isinstance(action_towers, dict):
        required_paths.extend(
            full_metadata_path.parent / str(payload.get("path", ""))
            for payload in action_towers.values()
            if isinstance(payload, dict)
        )
    if isinstance(context_tower, dict):
        required_paths.append(full_metadata_path.parent / str(context_tower.get("path", "")))
    if isinstance(input_vocabs, dict):
        for key in (
            "principal_entity_keys_path",
            "resource_entity_keys_path",
            "categorical_vocabs_path",
        ):
            value = input_vocabs.get(key)
            if isinstance(value, str) and value:
                required_paths.append(full_metadata_path.parent / value)
    has_complete_vocab_contract = isinstance(input_vocabs, dict) and all(
        isinstance(input_vocabs.get(key), str) and input_vocabs.get(key)
        for key in (
            "principal_entity_keys_path",
            "resource_entity_keys_path",
            "categorical_vocabs_path",
        )
    )
    complete = (
        bool(required_paths)
        and has_complete_vocab_contract
        and all(path.exists() for path in required_paths if path.name)
    )
    effective_mode = "onnx_native" if complete and requested_mode == "onnx" else "heuristic"
    if requested_mode == "heuristic":
        reason = "requested_heuristic_mode"
    elif complete:
        reason = "model_artifact_loaded_onnx_native"
    else:
        reason = "model_artifact_incomplete"
    activation_gate = evaluate_model_activation_gate(
        full_metadata_path.parent,
        metadata,
        promotion_policy=promotion_policy,
    )
    score_policy = metadata.get("score_policy")
    return {
        "available": complete,
        "requested_mode": requested_mode,
        "effective_mode": effective_mode,
        "reason": reason,
        "model_id": metadata.get("model_id"),
        "model_version": metadata.get("model_version"),
        "model_family": metadata.get("model_family"),
        "exported_at": metadata.get("exported_at"),
        "metadata_path": metadata_path,
        "activated_at": manifest.get("activated_at"),
        "activation_source": manifest.get("activation_source"),
        "action_source_count": len(action_towers) if isinstance(action_towers, dict) else 0,
        "principal_vocab_count": int(input_vocabs.get("principal_vocab_count", 0))
        if isinstance(input_vocabs, dict)
        else 0,
        "resource_vocab_count": int(input_vocabs.get("resource_vocab_count", 0))
        if isinstance(input_vocabs, dict)
        else 0,
        "activation_gate": activation_gate,
        "recent_activation_history": list(manifest.get("activation_history", [])[-5:]),
        "installed_model_count": len(installed_model_ids),
        "installed_model_ids": installed_model_ids,
        "score_policy": score_policy if isinstance(score_policy, dict) else None,
        "promotion_policy": promotion_policy,
    }
