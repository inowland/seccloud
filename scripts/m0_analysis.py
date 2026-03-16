"""M0 Analysis: ablation studies and cold-start evaluation.

Runs all experiments needed for the M0 decision point write-up.
"""

from __future__ import annotations

import copy
import json
import random
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import torch

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from seccloud.contrastive_model import (
    FacadeDataset,
    FacadeModel,
    build_categorical_vocabs,
    build_training_pairs,
    collate_facade,
    config_from_features,
    train_epoch,
)
from seccloud.evaluation import (
    EvaluationResult,
    _identify_attack_principals,
    evaluate,
    score_all_principals,
    temporal_spatial_split,
)
from seccloud.feature_pipeline import (
    CollaborationFeatures,
    FeatureSet,
    PeerFeatures,
    StaticFeatures,
    build_features,
)
from seccloud.synthetic_scale import (
    SCENARIO_NAMES,
    ScaleConfig,
    generate_org,
    generate_scaled_dataset,
)

DEVICE = torch.device("cpu")
SEED = 42


def _train_and_evaluate(
    fs: FeatureSet,
    events: list,
    principals: list,
    train_days: int = 20,
    start_date: datetime | None = None,
    epochs: int = 20,
    embed_dim: int = 64,
    label: str = "",
) -> tuple[EvaluationResult, list[float]]:
    """Train a model on features and evaluate. Returns (result, losses)."""
    cat_vocabs = build_categorical_vocabs(fs)
    split = temporal_spatial_split(
        events, principals,
        train_days=train_days,
        start_date=start_date,
        spatial_holdout=0.5,
        seed=SEED,
    )

    all_pairs = build_training_pairs(fs)
    train_pairs = [p for p in all_pairs if p.principal_idx in split.train_principal_indices]

    cfg = config_from_features(
        fs,
        embed_dim=embed_dim, token_dim=32, static_embed_dim=8,
        action_hidden=[256, 128], context_hidden=[256, 128],
        n_positive=10, batch_size=512, learning_rate=3e-4, epochs=epochs,
    )

    model = FacadeModel(cfg).to(DEVICE)
    ds = FacadeDataset(fs, train_pairs, cat_vocabs, cfg, rng_seed=SEED)
    loader = torch.utils.data.DataLoader(
        ds, batch_size=cfg.batch_size, shuffle=True,
        collate_fn=collate_facade, num_workers=0,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)

    losses = []
    for epoch in range(epochs):
        loss = train_epoch(model, loader, optimizer, cfg, DEVICE)
        losses.append(loss)

    email_to_idx = {p.email: p.idx for p in principals}
    scores = score_all_principals(
        model, split.test_events, fs, cat_vocabs, cfg,
        email_to_idx, delta=0.3, device=DEVICE,
    )
    attack = _identify_attack_principals(split.test_events, email_to_idx)
    result = evaluate(scores, attack)
    return result, losses


def ablate(fs: FeatureSet, ablation: str) -> FeatureSet:
    """Return a copy of fs with specific features zeroed out."""
    fs = copy.deepcopy(fs)
    for ctx in fs.contexts.values():
        if ablation == "no_history":
            ctx.history = []
        elif ablation == "no_peers":
            ctx.peers = PeerFeatures({}, {}, {})
        elif ablation == "no_collaboration":
            ctx.collaboration = CollaborationFeatures({})
        elif ablation == "no_static":
            ctx.static = StaticFeatures("_ablated", "1-3yr", "_ablated", "regular")
        elif ablation == "history_only":
            ctx.peers = PeerFeatures({}, {}, {})
            ctx.collaboration = CollaborationFeatures({})
            ctx.static = StaticFeatures("_ablated", "1-3yr", "_ablated", "regular")
        elif ablation == "peers_only":
            ctx.history = []
            ctx.collaboration = CollaborationFeatures({})
            ctx.static = StaticFeatures("_ablated", "1-3yr", "_ablated", "regular")
    return fs


def run_ablation_study(
    events: list, principals: list, teams: list,
    start_date: datetime,
) -> dict[str, dict]:
    """Run full model + 6 ablation variants."""
    print("\n" + "=" * 70)
    print("ABLATION STUDIES (200 principals)")
    print("=" * 70)

    fs_full = build_features(events, principals, teams)

    ablations = [
        ("full_model", fs_full),
        ("no_history", ablate(fs_full, "no_history")),
        ("no_peers", ablate(fs_full, "no_peers")),
        ("no_collaboration", ablate(fs_full, "no_collaboration")),
        ("no_static", ablate(fs_full, "no_static")),
        ("history_only", ablate(fs_full, "history_only")),
        ("peers_only", ablate(fs_full, "peers_only")),
    ]

    results = {}
    for name, fs in ablations:
        t0 = time.time()
        result, losses = _train_and_evaluate(
            fs, events, principals,
            start_date=start_date, epochs=20, label=name,
        )
        elapsed = time.time() - t0

        scenario_aucs = {
            s: m.roc_auc for s, m in result.per_scenario.items()
        }
        results[name] = {
            "aggregate_roc_auc": result.aggregate_roc_auc,
            "aggregate_pr_auc": result.aggregate_pr_auc,
            "scenario_aucs": scenario_aucs,
            "final_loss": losses[-1],
            "elapsed_s": elapsed,
        }

        print(f"\n  {name}: agg_roc={result.aggregate_roc_auc:.4f} "
              f"({elapsed:.1f}s)")
        for s, auc in sorted(scenario_aucs.items()):
            print(f"    {s}: {auc:.4f}")

    return results


def run_cold_start_study(
    all_events: list, principals: list, teams: list,
    start_date: datetime, total_days: int = 30,
) -> dict[int, dict]:
    """Evaluate detection quality with varying training history lengths."""
    print("\n" + "=" * 70)
    print("COLD-START STUDY (200 principals)")
    print("=" * 70)

    train_day_options = [3, 7, 14, 20, 25]
    results = {}

    for train_days in train_day_options:
        # Use events up to train_days + 5 test days
        test_days = min(5, total_days - train_days)
        if test_days < 1:
            continue

        cutoff = start_date + timedelta(days=train_days + test_days)
        cutoff_str = cutoff.isoformat()
        events = [e for e in all_events if e.get("observed_at", "") < cutoff_str]

        fs = build_features(events, principals, teams)

        t0 = time.time()
        result, losses = _train_and_evaluate(
            fs, events, principals,
            start_date=start_date,
            train_days=train_days,
            epochs=10,
            label=f"cold_start_{train_days}d",
        )
        elapsed = time.time() - t0

        scenario_aucs = {s: m.roc_auc for s, m in result.per_scenario.items()}
        results[train_days] = {
            "aggregate_roc_auc": result.aggregate_roc_auc,
            "scenario_aucs": scenario_aucs,
            "elapsed_s": elapsed,
        }

        print(f"\n  {train_days} train days: agg_roc={result.aggregate_roc_auc:.4f} "
              f"({elapsed:.1f}s)")
        for s, auc in sorted(scenario_aucs.items()):
            print(f"    {s}: {auc:.4f}")

    return results


def run_embedding_dim_study(
    events: list, principals: list, teams: list,
    start_date: datetime,
) -> dict[int, dict]:
    """Compare embedding dimensions."""
    print("\n" + "=" * 70)
    print("EMBEDDING DIMENSION STUDY (200 principals)")
    print("=" * 70)

    fs = build_features(events, principals, teams)
    dims = [32, 64, 128]
    results = {}

    for d in dims:
        t0 = time.time()
        result, losses = _train_and_evaluate(
            fs, events, principals,
            start_date=start_date, epochs=20, embed_dim=d,
            label=f"dim_{d}",
        )
        elapsed = time.time() - t0

        scenario_aucs = {s: m.roc_auc for s, m in result.per_scenario.items()}
        results[d] = {
            "aggregate_roc_auc": result.aggregate_roc_auc,
            "scenario_aucs": scenario_aucs,
            "final_loss": losses[-1],
            "elapsed_s": elapsed,
        }

        print(f"\n  d={d}: agg_roc={result.aggregate_roc_auc:.4f} "
              f"loss={losses[-1]:.6f} ({elapsed:.1f}s)")
        for s, auc in sorted(scenario_aucs.items()):
            print(f"    {s}: {auc:.4f}")

    return results


def main():
    torch.manual_seed(SEED)
    np.random.seed(SEED)

    # Generate data at 100 principals for ablation speed
    # (ablations measure relative impact, not absolute quality)
    print("Generating synthetic data (100 principals, 30 days)...")
    scale_cfg = ScaleConfig(
        num_principals=100, num_days=30, seed=SEED, inject_scenarios=True,
    )
    dataset = generate_scaled_dataset(scale_cfg)
    events = dataset.raw_events
    print(f"  {len(events)} events")

    rng = random.Random(SEED)
    principals, teams = generate_org(scale_cfg, rng)

    start_date = scale_cfg.start_date

    # Run studies
    ablation_results = run_ablation_study(events, principals, teams, start_date)
    cold_start_results = run_cold_start_study(events, principals, teams, start_date)
    dim_results = run_embedding_dim_study(events, principals, teams, start_date)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print("\n--- Ablation: Aggregate ROC AUC ---")
    baseline = ablation_results["full_model"]["aggregate_roc_auc"]
    for name, data in sorted(ablation_results.items(), key=lambda x: -x[1]["aggregate_roc_auc"]):
        delta = data["aggregate_roc_auc"] - baseline
        sign = "+" if delta >= 0 else ""
        print(f"  {name:25s}: {data['aggregate_roc_auc']:.4f} ({sign}{delta:.4f})")

    print("\n--- Cold Start: Aggregate ROC AUC ---")
    for days in sorted(cold_start_results):
        print(f"  {days:3d} days: {cold_start_results[days]['aggregate_roc_auc']:.4f}")

    print("\n--- Embedding Dim: Aggregate ROC AUC ---")
    for d in sorted(dim_results):
        print(f"  d={d:3d}: {dim_results[d]['aggregate_roc_auc']:.4f}")

    # Write JSON for the write-up
    output = {
        "ablation": {k: {kk: vv for kk, vv in v.items()} for k, v in ablation_results.items()},
        "cold_start": {str(k): v for k, v in cold_start_results.items()},
        "embedding_dim": {str(k): v for k, v in dim_results.items()},
    }
    out_path = Path(__file__).parent.parent / "project" / "m0_analysis_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    main()
