from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

import torch

from seccloud.contrastive_model import (
    FacadeDataset,
    FacadeModel,
    build_categorical_vocabs,
    build_training_pairs,
    collate_facade,
    config_from_features,
    evaluate_sampled_retrieval,
    train_epoch,
)
from seccloud.feature_contract import feature_scoring_contract
from seccloud.feature_lake import load_feature_lake_feature_set
from seccloud.model_artifact import install_model_artifact_bundle
from seccloud.onnx_export import export_model_artifact_bundle
from seccloud.storage import Workspace

DEVICE = torch.device("cpu")
MIN_ACTIVATION_TOP1_ACCURACY = 0.15
MIN_ACTIVATION_PAIRWISE_WIN_RATE = 0.55
MIN_ACTIVATION_MARGIN = 0.0
MIN_HELDOUT_PAIR_COUNT = 5
MIN_SOURCE_POLICY_PAIR_COUNT = 3
MIN_SOURCE_ACTIVATION_PAIR_COUNT = 5
DEFAULT_MODEL_DETECTION_THRESHOLD = 0.60
DEFAULT_MODEL_HIGH_SEVERITY_THRESHOLD = 0.80
IDENTITY_SOURCES = {"okta"}
RESOURCE_SOURCES = {"github", "gworkspace", "snowflake"}
MIN_AUTO_PROMOTION_SOURCE_COUNT = 2


@dataclass(slots=True)
class ModelTrainingResult:
    model_id: str
    output_dir: str
    scoring_input_mode: str
    training_pair_count: int
    principal_vocab_count: int
    resource_vocab_count: int
    installed: bool
    manifest: dict[str, object]


def _thresholds() -> dict[str, float]:
    return {
        "min_top1_accuracy": MIN_ACTIVATION_TOP1_ACCURACY,
        "min_pairwise_win_rate": MIN_ACTIVATION_PAIRWISE_WIN_RATE,
        "min_mean_margin": MIN_ACTIVATION_MARGIN,
    }


def _gate_result_from_evaluation(
    evaluation: dict[str, float | int],
    *,
    evaluation_scope: str,
    final_loss: float | None,
    eligible_prefix: str = "sampled_retrieval_gate_pass",
) -> dict[str, object]:
    top1 = float(evaluation.get("sampled_top1_accuracy", 0.0))
    pairwise_win_rate = float(evaluation.get("pairwise_win_rate", 0.0))
    mean_margin = float(evaluation.get("mean_margin", 0.0))
    eligible = (
        top1 >= MIN_ACTIVATION_TOP1_ACCURACY
        and pairwise_win_rate >= MIN_ACTIVATION_PAIRWISE_WIN_RATE
        and mean_margin > MIN_ACTIVATION_MARGIN
    )
    if eligible:
        reason = eligible_prefix
    elif top1 < MIN_ACTIVATION_TOP1_ACCURACY:
        reason = "sampled_top1_accuracy_below_threshold"
    elif pairwise_win_rate < MIN_ACTIVATION_PAIRWISE_WIN_RATE:
        reason = "pairwise_win_rate_below_threshold"
    else:
        reason = "mean_margin_below_threshold"
    return {
        "eligible": eligible,
        "reason": reason,
        "final_loss": final_loss,
        "evaluation_scope": evaluation_scope,
        "sampled_top1_accuracy": top1,
        "pairwise_win_rate": pairwise_win_rate,
        "mean_margin": mean_margin,
        "thresholds": _thresholds(),
    }


def _activation_gate_from_metrics(
    *,
    training_pair_count: int,
    heldout_pair_count: int,
    final_loss: float | None,
    evaluation: dict[str, float | int],
    evaluation_scope: str,
    evaluation_by_source: dict[str, dict[str, object]],
) -> dict[str, object]:
    if training_pair_count <= 0:
        return {
            "eligible": False,
            "reason": "no_training_pairs",
            "final_loss": final_loss,
            "evaluation_scope": evaluation_scope,
            "thresholds": _thresholds(),
            "source_gates": {},
            "evaluated_source_count": 0,
            "failing_sources": [],
            "coverage_gate": {
                "eligible": False,
                "reason": "no_training_pairs",
                "covered_sources": [],
                "covered_identity_sources": [],
                "covered_resource_sources": [],
                "required_source_count": MIN_AUTO_PROMOTION_SOURCE_COUNT,
            },
        }
    if final_loss is None or not torch.isfinite(torch.tensor(final_loss)):
        return {
            "eligible": False,
            "reason": "final_loss_invalid",
            "final_loss": final_loss,
            "evaluation_scope": evaluation_scope,
            "thresholds": _thresholds(),
            "source_gates": {},
            "evaluated_source_count": 0,
            "failing_sources": [],
            "coverage_gate": {
                "eligible": False,
                "reason": "final_loss_invalid",
                "covered_sources": [],
                "covered_identity_sources": [],
                "covered_resource_sources": [],
                "required_source_count": MIN_AUTO_PROMOTION_SOURCE_COUNT,
            },
        }
    if heldout_pair_count <= 0 and evaluation_scope == "heldout":
        return {
            "eligible": False,
            "reason": "heldout_eval_missing",
            "final_loss": final_loss,
            "evaluation_scope": evaluation_scope,
            "thresholds": _thresholds(),
            "source_gates": {},
            "evaluated_source_count": 0,
            "failing_sources": [],
            "coverage_gate": {
                "eligible": False,
                "reason": "heldout_eval_missing",
                "covered_sources": [],
                "covered_identity_sources": [],
                "covered_resource_sources": [],
                "required_source_count": MIN_AUTO_PROMOTION_SOURCE_COUNT,
            },
        }

    gate = _gate_result_from_evaluation(
        evaluation,
        evaluation_scope=evaluation_scope,
        final_loss=final_loss,
    )
    source_gates: dict[str, dict[str, object]] = {}
    failing_sources: list[str] = []
    for source, source_evaluation in sorted(evaluation_by_source.items()):
        pair_count = source_evaluation.get("pair_count")
        metrics = source_evaluation.get("metrics")
        if not isinstance(pair_count, int) or pair_count < MIN_SOURCE_ACTIVATION_PAIR_COUNT:
            continue
        if not isinstance(metrics, dict):
            continue
        source_gate = _gate_result_from_evaluation(
            metrics,
            evaluation_scope=f"{evaluation_scope}:{source}",
            final_loss=final_loss,
            eligible_prefix="source_sampled_retrieval_gate_pass",
        )
        source_gate["pair_count"] = pair_count
        source_gates[source] = source_gate
        if not source_gate["eligible"]:
            failing_sources.append(source)

    if failing_sources:
        gate["eligible"] = False
        gate["reason"] = f"source_gate_failed:{','.join(failing_sources)}"
    covered_sources = sorted(source_gates)
    covered_identity_sources = [source for source in covered_sources if source in IDENTITY_SOURCES]
    covered_resource_sources = [source for source in covered_sources if source in RESOURCE_SOURCES]
    coverage_reason = "cross_domain_coverage_gate_pass"
    coverage_eligible = (
        len(covered_sources) >= MIN_AUTO_PROMOTION_SOURCE_COUNT
        and bool(covered_identity_sources)
        and bool(covered_resource_sources)
    )
    if len(covered_sources) < MIN_AUTO_PROMOTION_SOURCE_COUNT:
        coverage_reason = "insufficient_covered_sources"
    elif not covered_identity_sources:
        coverage_reason = "identity_source_coverage_missing"
    elif not covered_resource_sources:
        coverage_reason = "resource_source_coverage_missing"
    if gate["eligible"] and not coverage_eligible:
        gate["eligible"] = False
        gate["reason"] = f"coverage_gate_failed:{coverage_reason}"
    gate["source_gates"] = source_gates
    gate["evaluated_source_count"] = len(source_gates)
    gate["failing_sources"] = failing_sources
    gate["coverage_gate"] = {
        "eligible": coverage_eligible,
        "reason": coverage_reason,
        "covered_sources": covered_sources,
        "covered_identity_sources": covered_identity_sources,
        "covered_resource_sources": covered_resource_sources,
        "required_source_count": MIN_AUTO_PROMOTION_SOURCE_COUNT,
    }
    return gate


def _split_training_pairs(
    pairs: list,
    *,
    seed: int,
) -> tuple[list, list]:
    if len(pairs) < MIN_HELDOUT_PAIR_COUNT:
        return list(pairs), []
    shuffled = list(pairs)
    random.Random(seed).shuffle(shuffled)
    heldout_count = max(1, int(len(shuffled) * 0.2))
    if len(shuffled) - heldout_count < 1:
        heldout_count = max(0, len(shuffled) - 1)
    train_pairs = shuffled[:-heldout_count] if heldout_count else shuffled
    heldout_pairs = shuffled[-heldout_count:] if heldout_count else []
    return train_pairs, heldout_pairs


def _source_evaluations(
    model: FacadeModel,
    feature_set: object,
    cat_vocabs: dict[str, dict[str, int]],
    config: object,
    *,
    pairs: list,
    device: torch.device,
    seed: int,
) -> dict[str, dict[str, object]]:
    pairs_by_source: dict[str, list] = {}
    for pair in pairs:
        pairs_by_source.setdefault(pair.source, []).append(pair)

    evaluations: dict[str, dict[str, object]] = {}
    for source, source_pairs in sorted(pairs_by_source.items()):
        if len(source_pairs) < MIN_SOURCE_POLICY_PAIR_COUNT:
            continue
        evaluations[source] = {
            "pair_count": len(source_pairs),
            "metrics": evaluate_sampled_retrieval(
                model,
                feature_set,
                cat_vocabs,
                config,
                pairs=source_pairs,
                device=device,
                seed=seed,
            ),
        }
    return evaluations


def _single_score_policy(
    evaluation: dict[str, float | int],
    *,
    evaluation_scope: str,
    pair_count: int | None = None,
) -> dict[str, object]:
    positive_p95 = float(evaluation.get("positive_distance_p95", evaluation.get("mean_positive_distance", 0.0)) or 0.0)
    negative_p50 = float(evaluation.get("negative_distance_p50", evaluation.get("mean_negative_distance", 0.0)) or 0.0)
    negative_p90 = float(evaluation.get("negative_distance_p90", evaluation.get("mean_negative_distance", 0.0)) or 0.0)

    if negative_p50 > positive_p95:
        detection_threshold = (positive_p95 + negative_p50) / 2.0
        calibration_reason = "heldout_midpoint_between_positive_p95_and_negative_p50"
    else:
        detection_threshold = DEFAULT_MODEL_DETECTION_THRESHOLD
        calibration_reason = "fallback_default_threshold_due_to_overlap"

    detection_threshold = max(0.05, min(detection_threshold, 0.99))
    high_severity_threshold = max(
        detection_threshold + 0.1,
        min(max(negative_p90, detection_threshold + 0.05), 0.99),
    )
    high_severity_threshold = max(detection_threshold, min(high_severity_threshold, 0.99))

    return {
        "detection_threshold": round(detection_threshold, 4),
        "high_severity_threshold": round(high_severity_threshold, 4),
        "calibration_source": evaluation_scope,
        "calibration_reason": calibration_reason,
        "positive_distance_p95": round(positive_p95, 4),
        "negative_distance_p50": round(negative_p50, 4),
        "negative_distance_p90": round(negative_p90, 4),
        "evaluation_pair_count": pair_count,
    }


def _score_policy_from_metrics(
    evaluation: dict[str, float | int],
    *,
    evaluation_scope: str,
    evaluation_by_source: dict[str, dict[str, object]],
) -> dict[str, object]:
    policy = _single_score_policy(
        evaluation,
        evaluation_scope=evaluation_scope,
    )
    source_policies: dict[str, dict[str, object]] = {}
    for source, source_evaluation in sorted(evaluation_by_source.items()):
        metrics = source_evaluation.get("metrics")
        pair_count = source_evaluation.get("pair_count")
        if not isinstance(metrics, dict):
            continue
        source_policies[source] = _single_score_policy(
            metrics,
            evaluation_scope=f"{evaluation_scope}:{source}",
            pair_count=int(pair_count) if isinstance(pair_count, int) else None,
        )
    policy["source_policies"] = source_policies
    return policy


def export_workspace_model_artifact(
    workspace: Workspace,
    output_dir: str | Path,
    *,
    model_id: str,
    epochs: int = 5,
    seed: int = 42,
    install: bool = False,
) -> ModelTrainingResult:
    feature_state = feature_scoring_contract(workspace.load_feature_state())
    if feature_state.get("scoring_input_mode") != "feature_lake":
        raise ValueError("workspace feature lake is not ready for model export; run the Rust feature builder first")

    try:
        feature_set = load_feature_lake_feature_set(workspace)
    except Exception as exc:  # pragma: no cover - protective wrapper
        raise RuntimeError("failed to load feature lake feature set for model export") from exc

    cat_vocabs = build_categorical_vocabs(feature_set)
    config = config_from_features(
        feature_set,
        embed_dim=64,
        token_dim=32,
        static_embed_dim=8,
        action_hidden=[256, 128],
        context_hidden=[256, 128],
        n_positive=10,
        batch_size=1024,
        learning_rate=3e-4,
        epochs=epochs,
    )
    model = FacadeModel(config).to(DEVICE)
    pairs = build_training_pairs(feature_set)
    if not pairs:
        raise ValueError("workspace feature lake does not contain any trainable action/context pairs")
    train_pairs, heldout_pairs = _split_training_pairs(pairs, seed=seed)
    dataset = FacadeDataset(feature_set, train_pairs, cat_vocabs, config, rng_seed=seed)
    loader = torch.utils.data.DataLoader(
        dataset,
        batch_size=config.batch_size,
        shuffle=True,
        collate_fn=collate_facade,
        num_workers=0,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)
    losses: list[float] = []
    for _ in range(epochs):
        losses.append(train_epoch(model, loader, optimizer, config, DEVICE))
    final_loss = losses[-1] if losses else None
    evaluation_scope = "heldout" if heldout_pairs else "train_fallback"
    evaluation = evaluate_sampled_retrieval(
        model,
        feature_set,
        cat_vocabs,
        config,
        pairs=heldout_pairs or train_pairs,
        device=DEVICE,
        seed=seed,
    )
    evaluation_by_source = _source_evaluations(
        model,
        feature_set,
        cat_vocabs,
        config,
        pairs=heldout_pairs or train_pairs,
        device=DEVICE,
        seed=seed,
    )
    activation_gate = _activation_gate_from_metrics(
        training_pair_count=len(train_pairs),
        heldout_pair_count=len(heldout_pairs),
        final_loss=final_loss,
        evaluation=evaluation,
        evaluation_scope=evaluation_scope,
        evaluation_by_source=evaluation_by_source,
    )
    score_policy = _score_policy_from_metrics(
        evaluation,
        evaluation_scope=evaluation_scope,
        evaluation_by_source=evaluation_by_source,
    )

    feature_vocab = workspace.load_feature_vocab()
    principal_entity_keys = feature_vocab.get("principal_entity_keys", [])
    resource_entity_keys = feature_vocab.get("resource_entity_keys", [])
    output_path = Path(output_dir)
    try:
        metadata = export_model_artifact_bundle(
            model,
            output_path,
            tenant_id=workspace.tenant_id,
            model_id=model_id,
            model_version=model_id,
            feature_schema_version="feature.v1",
            eval_report={
                "status": "trained_local_workspace",
                "model_id": model_id,
                "training_pair_count": len(train_pairs),
                "heldout_pair_count": len(heldout_pairs),
                "epochs": epochs,
                "losses": losses,
                "final_loss": final_loss,
                "sampled_retrieval": evaluation,
                "sampled_retrieval_scope": evaluation_scope,
                "sampled_retrieval_by_source": evaluation_by_source,
                "activation_gate": activation_gate,
                "score_policy": score_policy,
            },
            principal_entity_keys=principal_entity_keys if isinstance(principal_entity_keys, list) else None,
            resource_entity_keys=resource_entity_keys if isinstance(resource_entity_keys, list) else None,
            categorical_vocabs=cat_vocabs,
            score_policy=score_policy,
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "ONNX export dependencies are missing; run `uv sync` to install the demo model export stack "
            "before exporting a model"
        ) from exc

    manifest = (
        install_model_artifact_bundle(workspace, output_path, activation_source="workspace_train_export")
        if install
        else {}
    )
    return ModelTrainingResult(
        model_id=model_id,
        output_dir=str(output_path),
        scoring_input_mode=str(feature_state.get("scoring_input_mode", "unknown")),
        training_pair_count=len(train_pairs),
        principal_vocab_count=len(principal_entity_keys) if isinstance(principal_entity_keys, list) else 0,
        resource_vocab_count=len(resource_entity_keys) if isinstance(resource_entity_keys, list) else 0,
        installed=install,
        manifest=manifest or metadata,
    )
