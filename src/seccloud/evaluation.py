"""Multi-scale detection and evaluation for M0 ML validation (WS4 + WS5).

WS4 — Clustering-based multi-scale detection:
  Aggregates per-action anomaly scores into per-principal scores using
  hierarchical agglomerative clustering on action embeddings.

WS5 — Evaluation framework:
  Temporal-spatial data splitting, per-scenario detection metrics (ROC AUC,
  PR AUC, TPR at fixed FPR thresholds), and an end-to-end evaluation pipeline.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import torch
from scipy.cluster.hierarchy import fcluster, linkage
from sklearn.metrics import average_precision_score, roc_auc_score

from seccloud.contrastive_model import (
    FacadeDataset,
    FacadeModel,
    ModelConfig,
    build_categorical_vocabs,
    build_training_pairs,
    collate_facade,
    config_from_features,
    tensorize_action,
    tensorize_context,
)
from seccloud.feature_pipeline import FeatureSet, build_features
from seccloud.synthetic_scale import (
    SCENARIO_NAMES,
    OrgPrincipal,
)

# ---------------------------------------------------------------------------
# WS4: Multi-scale detection
# ---------------------------------------------------------------------------


def multi_scale_score(
    action_embeddings: np.ndarray,
    action_scores: np.ndarray,
    delta: float = 0.3,
) -> float:
    """Cluster-aggregated anomaly score for a principal's action set.

    Groups similar actions via hierarchical agglomerative clustering (cosine
    similarity, threshold *delta*).  Takes the max score per cluster, then
    sums across clusters.

    Parameters
    ----------
    action_embeddings : ndarray, shape [n_actions, d]
        L2-normalized action-tower outputs.
    action_scores : ndarray, shape [n_actions]
        Per-action cosine distances f(a, c).
    delta : float
        Clustering merge threshold (cosine distance).

    Returns
    -------
    float
        Aggregated principal score g(A, c).
    """
    n = len(action_scores)
    if n == 0:
        return 0.0
    if n == 1:
        return float(action_scores[0])

    Z = linkage(action_embeddings, method="average", metric="cosine")
    clusters = fcluster(Z, t=delta, criterion="distance")

    total = 0.0
    for cid in set(clusters):
        mask = clusters == cid
        total += float(action_scores[mask].max())
    return total


@torch.no_grad()
def score_principal(
    model: FacadeModel,
    principal_idx: int,
    actions: list[tuple[str, str]],
    feature_set: FeatureSet,
    cat_vocabs: dict[str, dict[str, int]],
    max_act: int,
    max_win: int,
    max_res: int,
    max_peers: int,
    delta: float = 0.3,
    device: torch.device | None = None,
) -> float:
    """Score a principal for a set of actions using multi-scale detection.

    Parameters
    ----------
    actions : list of (resource_id, source) tuples
        Distinct resources accessed by this principal in the scoring window.
    """
    if not actions or principal_idx not in feature_set.contexts:
        return 0.0
    if device is None:
        device = next(model.parameters()).device

    ctx = feature_set.contexts[principal_idx]
    ctx_t = tensorize_context(
        ctx,
        feature_set.resource_vocab,
        cat_vocabs,
        max_win,
        max_res,
        max_peers,
    )
    ctx_batch = {k: v.unsqueeze(0).to(device) for k, v in ctx_t.items()}
    ctx_emb = model.encode_context(ctx_batch)  # [1, d]

    # Group actions by source type
    by_source: dict[str, list[str]] = defaultdict(list)
    for rid, source in actions:
        if rid in feature_set.actions:
            by_source[source].append(rid)

    all_embs: list[torch.Tensor] = []
    all_scores: list[float] = []

    for source, rids in by_source.items():
        if source not in model.action_towers:
            continue
        for rid in rids:
            af = feature_set.actions[rid]
            act_t = tensorize_action(af, max_act)
            act_batch = {k: v.unsqueeze(0).to(device) for k, v in act_t.items()}
            act_emb = model.encode_action(
                source,
                act_batch["action_indices"],
                act_batch["action_weights"],
                act_batch["action_mask"],
            )  # [1, d]
            all_embs.append(act_emb)
            dist = 1.0 - (act_emb * ctx_emb).sum(dim=-1)
            all_scores.append(dist.item())

    if not all_scores:
        return 0.0

    emb_np = torch.cat(all_embs, dim=0).cpu().numpy()
    scores_np = np.array(all_scores)
    return multi_scale_score(emb_np, scores_np, delta)


# ---------------------------------------------------------------------------
# WS5: Data splitting
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DataSplit:
    """Temporal-spatial train/test split."""

    train_events: list[dict[str, Any]]
    test_events: list[dict[str, Any]]
    train_principal_indices: set[int]
    test_principal_indices: set[int]
    split_date: datetime


def temporal_spatial_split(
    events: list[dict[str, Any]],
    principals: list[OrgPrincipal],
    *,
    train_days: int = 20,
    start_date: datetime | None = None,
    spatial_holdout: float = 0.5,
    seed: int = 42,
) -> DataSplit:
    """Split events temporally and spatially.

    Temporal: events before *split_date* (start + train_days) are train,
    the rest are test.

    Spatial: *spatial_holdout* fraction of principals are held out from
    training pairs (their events still contribute to action features, but
    they never appear as training-pair contexts).
    """
    if start_date is None:
        timestamps = [e.get("observed_at", "") for e in events if e.get("observed_at")]
        start_date = (
            datetime.fromisoformat(min(timestamps).replace("Z", "+00:00"))
            if timestamps
            else datetime(2026, 1, 1, tzinfo=UTC)
        )

    split_date = start_date + timedelta(days=train_days)
    split_str = split_date.isoformat()

    train_events = [e for e in events if e.get("observed_at", "") < split_str]
    test_events = [e for e in events if e.get("observed_at", "") >= split_str]

    rng = random.Random(seed)
    all_indices = [p.idx for p in principals]
    rng.shuffle(all_indices)
    n_holdout = int(len(all_indices) * spatial_holdout)
    test_pidxs = set(all_indices[:n_holdout])
    train_pidxs = set(all_indices[n_holdout:])

    return DataSplit(
        train_events=train_events,
        test_events=test_events,
        train_principal_indices=train_pidxs,
        test_principal_indices=test_pidxs,
        split_date=split_date,
    )


# ---------------------------------------------------------------------------
# WS5: Metrics
# ---------------------------------------------------------------------------


def tpr_at_fpr(
    y_true: np.ndarray,
    scores: np.ndarray,
    target_fpr: float,
) -> float:
    """True positive rate at a given false positive rate threshold.

    Returns 0.0 if there are no positives or no negatives.
    """
    pos = y_true == 1
    neg = ~pos
    if pos.sum() == 0 or neg.sum() == 0:
        return 0.0

    # Sort by score descending
    order = np.argsort(-scores)
    sorted_labels = y_true[order]

    n_neg = neg.sum()
    fp = 0
    tp = 0
    best_tpr = 0.0
    for label in sorted_labels:
        if label == 1:
            tp += 1
        else:
            fp += 1
        fpr = fp / n_neg
        if fpr > target_fpr:
            break
        best_tpr = tp / pos.sum()

    return float(best_tpr)


@dataclass(slots=True)
class ScenarioMetrics:
    """Detection metrics for one scenario type."""

    scenario: str
    roc_auc: float
    pr_auc: float
    tpr_at_fpr_01: float  # TPR at FPR = 0.1%
    tpr_at_fpr_001: float  # TPR at FPR = 0.01%
    num_attack_principals: int
    num_benign_principals: int


@dataclass(slots=True)
class EvaluationResult:
    """Complete evaluation output."""

    per_scenario: dict[str, ScenarioMetrics]
    aggregate_roc_auc: float
    aggregate_pr_auc: float
    aggregate_tpr_at_fpr_01: float
    principal_scores: dict[int, float]
    attack_principals: dict[str, set[int]]


# ---------------------------------------------------------------------------
# WS5: Scoring all principals
# ---------------------------------------------------------------------------


def _compute_padding_limits(feature_set: FeatureSet, config: ModelConfig) -> dict[str, int]:
    """Compute effective padding limits from data, matching FacadeDataset logic."""
    max_act = min(
        max((len(af.accessor_weights) for af in feature_set.actions.values()), default=1),
        config.max_action_tokens,
    )
    max_peers = min(
        max(
            (
                max(
                    len(c.peers.department_peers),
                    len(c.peers.manager_peers),
                    len(c.peers.group_peers),
                    len(c.collaboration.co_access),
                    1,
                )
                for c in feature_set.contexts.values()
            ),
            default=1,
        ),
        config.max_peers,
    )
    hist_lens = [len(c.history) for c in feature_set.contexts.values()]
    max_win = max(1, min(max(hist_lens, default=1), config.max_history_windows))
    res_lens = [len(hw.resource_ids) for c in feature_set.contexts.values() for hw in c.history]
    max_res = max(1, min(max(res_lens, default=1), config.max_res_per_window))
    return {"max_act": max_act, "max_win": max_win, "max_res": max_res, "max_peers": max_peers}


@torch.no_grad()
def score_all_principals(
    model: FacadeModel,
    test_events: list[dict[str, Any]],
    feature_set: FeatureSet,
    cat_vocabs: dict[str, dict[str, int]],
    config: ModelConfig,
    email_to_idx: dict[str, int],
    *,
    delta: float = 0.3,
    device: torch.device | None = None,
) -> dict[int, float]:
    """Score every principal active in test_events.

    For each principal, collects their distinct (resource_id, source) pairs
    from test events and computes a multi-scale anomaly score.
    """
    model.eval()
    if device is None:
        device = next(model.parameters()).device

    # Collect per-principal distinct actions from test events
    principal_actions: dict[int, set[tuple[str, str]]] = defaultdict(set)
    for e in test_events:
        email = e.get("actor_email")
        rid = e.get("resource_id")
        source = e.get("source")
        if not email or not rid or not source:
            continue
        pidx = email_to_idx.get(email)
        if pidx is not None:
            principal_actions[pidx].add((rid, source))

    limits = _compute_padding_limits(feature_set, config)

    scores: dict[int, float] = {}
    for pidx, action_set in principal_actions.items():
        scores[pidx] = score_principal(
            model,
            pidx,
            list(action_set),
            feature_set,
            cat_vocabs,
            limits["max_act"],
            limits["max_win"],
            limits["max_res"],
            limits["max_peers"],
            delta=delta,
            device=device,
        )

    return scores


# ---------------------------------------------------------------------------
# WS5: Evaluation
# ---------------------------------------------------------------------------


def _identify_attack_principals(
    events: list[dict[str, Any]],
    email_to_idx: dict[str, int],
) -> dict[str, set[int]]:
    """Map each scenario name to the set of attacking principal indices."""
    attack: dict[str, set[int]] = defaultdict(set)
    for e in events:
        scenario = e.get("scenario", "baseline")
        if scenario == "baseline":
            continue
        email = e.get("actor_email")
        if email:
            pidx = email_to_idx.get(email)
            if pidx is not None:
                attack[scenario].add(pidx)
    return dict(attack)


def evaluate(
    principal_scores: dict[int, float],
    attack_principals: dict[str, set[int]],
    all_scored_principals: set[int] | None = None,
) -> EvaluationResult:
    """Compute detection metrics from principal scores and ground truth.

    Parameters
    ----------
    principal_scores : dict mapping principal_idx -> anomaly score
    attack_principals : dict mapping scenario name -> set of attacker indices
    all_scored_principals : set of all principals that were scored (if None,
        uses the keys of principal_scores)
    """
    if all_scored_principals is None:
        all_scored_principals = set(principal_scores.keys())

    all_attackers: set[int] = set()
    for pidxs in attack_principals.values():
        all_attackers |= pidxs

    scored_list = sorted(all_scored_principals)
    scores_arr = np.array([principal_scores.get(p, 0.0) for p in scored_list])

    per_scenario: dict[str, ScenarioMetrics] = {}
    for scenario in SCENARIO_NAMES:
        attackers = attack_principals.get(scenario, set())
        if not attackers:
            continue

        y_true = np.array([1 if p in attackers else 0 for p in scored_list])
        n_pos = int(y_true.sum())
        n_neg = len(y_true) - n_pos

        if n_pos == 0 or n_neg == 0:
            per_scenario[scenario] = ScenarioMetrics(
                scenario=scenario,
                roc_auc=0.0,
                pr_auc=0.0,
                tpr_at_fpr_01=0.0,
                tpr_at_fpr_001=0.0,
                num_attack_principals=n_pos,
                num_benign_principals=n_neg,
            )
            continue

        per_scenario[scenario] = ScenarioMetrics(
            scenario=scenario,
            roc_auc=float(roc_auc_score(y_true, scores_arr)),
            pr_auc=float(average_precision_score(y_true, scores_arr)),
            tpr_at_fpr_01=tpr_at_fpr(y_true, scores_arr, 0.001),
            tpr_at_fpr_001=tpr_at_fpr(y_true, scores_arr, 0.0001),
            num_attack_principals=n_pos,
            num_benign_principals=n_neg,
        )

    # Aggregate: any attacker vs all benign
    y_agg = np.array([1 if p in all_attackers else 0 for p in scored_list])
    if y_agg.sum() > 0 and (y_agg == 0).sum() > 0:
        agg_roc = float(roc_auc_score(y_agg, scores_arr))
        agg_pr = float(average_precision_score(y_agg, scores_arr))
        agg_tpr = tpr_at_fpr(y_agg, scores_arr, 0.001)
    else:
        agg_roc = agg_pr = agg_tpr = 0.0

    return EvaluationResult(
        per_scenario=per_scenario,
        aggregate_roc_auc=agg_roc,
        aggregate_pr_auc=agg_pr,
        aggregate_tpr_at_fpr_01=agg_tpr,
        principal_scores=principal_scores,
        attack_principals=attack_principals,
    )


# ---------------------------------------------------------------------------
# WS5: End-to-end pipeline
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class M0EvalConfig:
    """Configuration for the end-to-end M0 evaluation."""

    # Data generation
    num_principals: int = 200
    num_days: int = 30
    seed: int = 42

    # Data splitting
    train_days: int = 20
    spatial_holdout: float = 0.5

    # Model
    embed_dim: int = 64
    token_dim: int = 32
    static_embed_dim: int = 8
    action_hidden: list[int] = field(default_factory=lambda: [256, 128])
    context_hidden: list[int] = field(default_factory=lambda: [256, 128])

    # Training
    n_positive: int = 10
    batch_size: int = 512
    learning_rate: float = 3e-4
    epochs: int = 20

    # Detection
    delta: float = 0.3


def run_m0_evaluation(
    cfg: M0EvalConfig | None = None,
    *,
    device: torch.device | None = None,
    verbose: bool = True,
) -> EvaluationResult:
    """End-to-end M0 evaluation pipeline.

    1. Generate synthetic data with attack scenarios
    2. Temporal-spatial split
    3. Feature engineering on training data
    4. Train contrastive model
    5. Score test principals
    6. Compute detection metrics

    Returns the EvaluationResult with per-scenario and aggregate metrics.
    """
    from seccloud.synthetic_scale import ScaleConfig, generate_scaled_dataset

    if cfg is None:
        cfg = M0EvalConfig()
    if device is None:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # 1. Generate data
    if verbose:
        print(f"Generating synthetic data: {cfg.num_principals} principals, {cfg.num_days} days...")
    scale_cfg = ScaleConfig(
        num_principals=cfg.num_principals,
        num_days=cfg.num_days,
        seed=cfg.seed,
        inject_scenarios=True,
    )
    dataset = generate_scaled_dataset(scale_cfg)
    events = dataset.raw_events
    if verbose:
        print(f"  {len(events)} events generated")

    # Reconstruct org for feature engineering
    import random as stdlib_random

    from seccloud.synthetic_scale import generate_org

    rng = stdlib_random.Random(scale_cfg.seed)
    principals, teams = generate_org(scale_cfg, rng)
    email_to_idx = {p.email: p.idx for p in principals}

    # 2. Split
    if verbose:
        print(f"Splitting: {cfg.train_days} train days, {cfg.spatial_holdout:.0%} spatial holdout...")
    split = temporal_spatial_split(
        events,
        principals,
        train_days=cfg.train_days,
        start_date=scale_cfg.start_date,
        spatial_holdout=cfg.spatial_holdout,
        seed=cfg.seed,
    )
    if verbose:
        print(f"  Train: {len(split.train_events)} events, {len(split.train_principal_indices)} principals")
        print(f"  Test:  {len(split.test_events)} events, {len(split.test_principal_indices)} principals")

    # 3. Feature engineering on ALL events (action features need full history,
    #    but training pairs are filtered to train principals + train period)
    if verbose:
        print("Building features...")
    fs = build_features(events, principals, teams)
    cat_vocabs = build_categorical_vocabs(fs)

    # 4. Train — filter training pairs to train principals in train period
    if verbose:
        print("Building training pairs...")
    all_pairs = build_training_pairs(fs)
    train_pairs = [p for p in all_pairs if p.principal_idx in split.train_principal_indices]
    if verbose:
        print(f"  {len(train_pairs)} training pairs (from {len(all_pairs)} total)")

    model_cfg = config_from_features(
        fs,
        embed_dim=cfg.embed_dim,
        token_dim=cfg.token_dim,
        static_embed_dim=cfg.static_embed_dim,
        action_hidden=cfg.action_hidden,
        context_hidden=cfg.context_hidden,
        n_positive=cfg.n_positive,
        batch_size=cfg.batch_size,
        learning_rate=cfg.learning_rate,
        epochs=cfg.epochs,
    )

    model = FacadeModel(model_cfg).to(device)

    if verbose:
        print(f"Training for {cfg.epochs} epochs...")
    ds = FacadeDataset(fs, train_pairs, cat_vocabs, model_cfg, rng_seed=cfg.seed)
    from torch.utils.data import DataLoader

    loader = DataLoader(
        ds,
        batch_size=model_cfg.batch_size,
        shuffle=True,
        collate_fn=collate_facade,
        num_workers=0,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=model_cfg.learning_rate)

    from seccloud.contrastive_model import train_epoch

    for epoch in range(cfg.epochs):
        loss = train_epoch(model, loader, optimizer, model_cfg, device)
        if verbose:
            print(f"  Epoch {epoch + 1}/{cfg.epochs}: loss={loss:.6f}")

    # 5. Score test principals
    if verbose:
        print("Scoring test principals...")
    principal_scores = score_all_principals(
        model,
        split.test_events,
        fs,
        cat_vocabs,
        model_cfg,
        email_to_idx,
        delta=cfg.delta,
        device=device,
    )
    if verbose:
        print(f"  Scored {len(principal_scores)} principals")

    # 6. Evaluate
    attack_principals = _identify_attack_principals(
        split.test_events,
        email_to_idx,
    )
    if verbose:
        for scenario, pidxs in attack_principals.items():
            print(f"  {scenario}: {len(pidxs)} attacker(s) in test set")

    result = evaluate(principal_scores, attack_principals)

    if verbose:
        print("\n--- Results ---")
        print(f"Aggregate ROC AUC: {result.aggregate_roc_auc:.4f}")
        print(f"Aggregate PR AUC:  {result.aggregate_pr_auc:.4f}")
        print(f"Aggregate TPR@0.1% FPR: {result.aggregate_tpr_at_fpr_01:.4f}")
        for name, m in result.per_scenario.items():
            print(f"\n  {name}:")
            print(f"    ROC AUC: {m.roc_auc:.4f}")
            print(f"    PR AUC:  {m.pr_auc:.4f}")
            print(f"    TPR@0.1% FPR: {m.tpr_at_fpr_01:.4f}")
            print(f"    TPR@0.01% FPR: {m.tpr_at_fpr_001:.4f}")
            print(f"    Attackers: {m.num_attack_principals}, Benign: {m.num_benign_principals}")

    return result
