from __future__ import annotations

import random
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import torch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.contrastive_model import (
    FacadeModel,
    ModelConfig,
    build_categorical_vocabs,
    build_training_pairs,
    config_from_features,
    tensorize_action,
    tensorize_context,
)
from seccloud.evaluation import (
    DataSplit,
    EvaluationResult,
    M0EvalConfig,
    ScenarioMetrics,
    _compute_padding_limits,
    _identify_attack_principals,
    evaluate,
    multi_scale_score,
    run_m0_evaluation,
    score_all_principals,
    score_principal,
    temporal_spatial_split,
    tpr_at_fpr,
)
from seccloud.feature_pipeline import (
    ActionFeatures,
    CollaborationFeatures,
    ContextFeatures,
    FeatureSet,
    HistoryWindow,
    PeerFeatures,
    StaticFeatures,
    build_features,
)
from seccloud.synthetic_scale import (
    OrgPrincipal,
    OrgTeam,
    ScaleConfig,
    generate_org,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_principal(idx: int, dept: str = "eng", role: str = "engineer") -> OrgPrincipal:
    return OrgPrincipal(
        idx=idx, email=f"p{idx:05d}@example.com", name=f"P{idx}",
        department=dept, team=f"{dept}-team-1", role=role,
        location="US-NY", is_manager=False, manager_idx=None, tz_offset=-5.0,
    )


def _small_feature_set() -> FeatureSet:
    actions = {
        "doc-a": ActionFeatures("doc-a", "gworkspace", {0: 0.6, 1: 0.4}),
        "doc-b": ActionFeatures("doc-b", "gworkspace", {1: 0.5, 2: 0.5}),
        "repo-x": ActionFeatures("repo-x", "github", {0: 1.0}),
    }

    def _ctx(idx: int, role: str = "engineer") -> ContextFeatures:
        return ContextFeatures(
            principal_idx=idx,
            history=[HistoryWindow(
                window_start=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                resource_ids=frozenset({"doc-a"}),
            )],
            peers=PeerFeatures(
                department_peers={j: 1.0 for j in range(5) if j != idx},
                manager_peers={}, group_peers={},
            ),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures(role, "1-3yr", "US-NY", "regular"),
        )

    contexts = {i: _ctx(i) for i in range(5)}
    return FeatureSet(
        actions=actions, contexts=contexts,
        principal_vocab_size=5, resource_vocab={"doc-a": 0, "doc-b": 1, "repo-x": 2},
    )


# ---------------------------------------------------------------------------
# WS4: Multi-scale detection tests
# ---------------------------------------------------------------------------


class TestMultiScaleScore(unittest.TestCase):
    def test_single_action(self):
        emb = np.array([[1.0, 0.0, 0.0]])
        scores = np.array([0.5])
        self.assertAlmostEqual(multi_scale_score(emb, scores), 0.5)

    def test_empty_actions(self):
        self.assertAlmostEqual(multi_scale_score(np.array([]), np.array([]), 0.3), 0.0)

    def test_identical_actions_cluster_to_one(self):
        """100 identical action embeddings -> 1 cluster -> max score once."""
        emb = np.tile([1.0, 0.0, 0.0], (100, 1))
        scores = np.random.default_rng(42).uniform(0.1, 0.9, 100)
        result = multi_scale_score(emb, scores, delta=0.3)
        self.assertAlmostEqual(result, float(scores.max()), places=5)

    def test_distinct_actions_sum(self):
        """Two orthogonal action embeddings -> 2 clusters -> sum of maxes."""
        emb = np.array([[1.0, 0.0], [0.0, 1.0]])
        scores = np.array([0.3, 0.7])
        result = multi_scale_score(emb, scores, delta=0.3)
        self.assertAlmostEqual(result, 1.0, places=5)

    def test_idempotence(self):
        """Repeated access to same resource -> same score as single access."""
        emb_single = np.array([[1.0, 0.0, 0.0]])
        scores_single = np.array([0.8])
        emb_repeated = np.tile([1.0, 0.0, 0.0], (50, 1))
        scores_repeated = np.full(50, 0.8)
        s1 = multi_scale_score(emb_single, scores_single)
        s2 = multi_scale_score(emb_repeated, scores_repeated, delta=0.3)
        self.assertAlmostEqual(s1, s2, places=5)

    def test_monotonicity(self):
        """More distinct unusual resources -> higher score."""
        emb_1 = np.array([[1.0, 0.0, 0.0]])
        scores_1 = np.array([0.5])
        s1 = multi_scale_score(emb_1, scores_1)

        # Add a second distinct action
        emb_2 = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
        scores_2 = np.array([0.5, 0.5])
        s2 = multi_scale_score(emb_2, scores_2, delta=0.3)

        self.assertGreater(s2, s1)


class TestScorePrincipal(unittest.TestCase):
    def test_returns_float(self):
        fs = _small_feature_set()
        cfg = config_from_features(
            fs, embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            sources=["gworkspace", "github"],
        )
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        model.eval()
        limits = _compute_padding_limits(fs, cfg)
        score = score_principal(
            model, 0, [("doc-a", "gworkspace"), ("repo-x", "github")],
            fs, vocabs, limits["max_act"], limits["max_win"],
            limits["max_res"], limits["max_peers"],
            device=torch.device("cpu"),
        )
        self.assertIsInstance(score, float)
        self.assertGreaterEqual(score, 0.0)

    def test_no_actions_returns_zero(self):
        fs = _small_feature_set()
        cfg = config_from_features(
            fs, embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            sources=["gworkspace"],
        )
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        model.eval()
        limits = _compute_padding_limits(fs, cfg)
        score = score_principal(
            model, 0, [], fs, vocabs,
            limits["max_act"], limits["max_win"],
            limits["max_res"], limits["max_peers"],
        )
        self.assertEqual(score, 0.0)

    def test_missing_principal_returns_zero(self):
        fs = _small_feature_set()
        cfg = config_from_features(
            fs, embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            sources=["gworkspace"],
        )
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        limits = _compute_padding_limits(fs, cfg)
        score = score_principal(
            model, 999, [("doc-a", "gworkspace")], fs, vocabs,
            limits["max_act"], limits["max_win"],
            limits["max_res"], limits["max_peers"],
        )
        self.assertEqual(score, 0.0)


# ---------------------------------------------------------------------------
# WS5: Data splitting tests
# ---------------------------------------------------------------------------


class TestTemporalSpatialSplit(unittest.TestCase):
    def setUp(self):
        self.principals = [_make_principal(i) for i in range(20)]
        base = datetime(2026, 1, 1, tzinfo=UTC)
        self.events = []
        for d in range(30):
            ts = (base + timedelta(days=d)).isoformat()
            for i in range(5):
                self.events.append({
                    "observed_at": ts,
                    "actor_email": f"p{i:05d}@example.com",
                    "resource_id": f"doc-{d}",
                    "source": "gworkspace",
                    "scenario": "baseline",
                })

    def test_temporal_split_correct(self):
        split = temporal_spatial_split(
            self.events, self.principals,
            train_days=20, start_date=datetime(2026, 1, 1, tzinfo=UTC),
        )
        # Events in first 20 days -> train, rest -> test
        self.assertGreater(len(split.train_events), 0)
        self.assertGreater(len(split.test_events), 0)
        # All train events before split date
        for e in split.train_events:
            self.assertLess(e["observed_at"], split.split_date.isoformat())
        # All test events at or after split date
        for e in split.test_events:
            self.assertGreaterEqual(e["observed_at"], split.split_date.isoformat())

    def test_spatial_holdout(self):
        split = temporal_spatial_split(
            self.events, self.principals,
            train_days=20, spatial_holdout=0.5,
        )
        self.assertEqual(
            len(split.train_principal_indices) + len(split.test_principal_indices),
            len(self.principals),
        )
        self.assertEqual(
            split.train_principal_indices & split.test_principal_indices, set(),
        )
        # Approximately 50% holdout
        holdout_frac = len(split.test_principal_indices) / len(self.principals)
        self.assertAlmostEqual(holdout_frac, 0.5, delta=0.1)

    def test_deterministic(self):
        s1 = temporal_spatial_split(self.events, self.principals, seed=42)
        s2 = temporal_spatial_split(self.events, self.principals, seed=42)
        self.assertEqual(s1.train_principal_indices, s2.train_principal_indices)
        self.assertEqual(s1.test_principal_indices, s2.test_principal_indices)


# ---------------------------------------------------------------------------
# WS5: Metrics tests
# ---------------------------------------------------------------------------


class TestTprAtFpr(unittest.TestCase):
    def test_perfect_separation(self):
        y = np.array([1, 1, 0, 0, 0])
        scores = np.array([1.0, 0.9, 0.1, 0.05, 0.01])
        # At FPR=0.1% (very tight), we can still detect both positives
        # because all negatives score far below all positives
        result = tpr_at_fpr(y, scores, target_fpr=1.0)
        self.assertAlmostEqual(result, 1.0)

    def test_no_positives(self):
        y = np.array([0, 0, 0])
        scores = np.array([0.5, 0.3, 0.1])
        self.assertEqual(tpr_at_fpr(y, scores, 0.1), 0.0)

    def test_no_negatives(self):
        y = np.array([1, 1, 1])
        scores = np.array([0.5, 0.3, 0.1])
        self.assertEqual(tpr_at_fpr(y, scores, 0.1), 0.0)

    def test_random_scores(self):
        rng = np.random.default_rng(42)
        y = np.zeros(1000)
        y[:10] = 1
        scores = rng.uniform(0, 1, 1000)
        result = tpr_at_fpr(y, scores, 0.1)
        # With random scores, TPR at 0.1% FPR should be very low
        self.assertLessEqual(result, 0.5)


class TestEvaluate(unittest.TestCase):
    def test_perfect_detection(self):
        # Attackers score high, benign scores low
        scores = {i: 0.1 for i in range(100)}
        scores[50] = 5.0  # attacker
        scores[51] = 4.0  # attacker
        attack_principals = {"slow_exfiltration": {50, 51}}
        result = evaluate(scores, attack_principals)
        self.assertIn("slow_exfiltration", result.per_scenario)
        m = result.per_scenario["slow_exfiltration"]
        self.assertGreater(m.roc_auc, 0.99)
        self.assertEqual(m.num_attack_principals, 2)

    def test_random_detection(self):
        rng = np.random.default_rng(42)
        scores = {i: float(rng.uniform(0, 1)) for i in range(200)}
        attack_principals = {"credential_compromise": {0, 1}}
        result = evaluate(scores, attack_principals)
        m = result.per_scenario["credential_compromise"]
        # Random scores -> AUC should be near 0.5
        self.assertAlmostEqual(m.roc_auc, 0.5, delta=0.15)

    def test_no_attackers_in_test(self):
        scores = {i: 0.1 for i in range(50)}
        result = evaluate(scores, {})
        self.assertEqual(len(result.per_scenario), 0)
        self.assertEqual(result.aggregate_roc_auc, 0.0)

    def test_aggregate_combines_scenarios(self):
        scores = {i: 0.1 for i in range(100)}
        scores[10] = 5.0
        scores[20] = 5.0
        attack_principals = {
            "slow_exfiltration": {10},
            "credential_compromise": {20},
        }
        result = evaluate(scores, attack_principals)
        self.assertGreater(result.aggregate_roc_auc, 0.9)
        self.assertEqual(len(result.per_scenario), 2)


class TestIdentifyAttackPrincipals(unittest.TestCase):
    def test_extracts_attackers(self):
        events = [
            {"scenario": "baseline", "actor_email": "p00000@example.com"},
            {"scenario": "slow_exfiltration", "actor_email": "p00005@example.com"},
            {"scenario": "slow_exfiltration", "actor_email": "p00005@example.com"},
            {"scenario": "credential_compromise", "actor_email": "p00010@example.com"},
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(20)}
        result = _identify_attack_principals(events, email_to_idx)
        self.assertEqual(result["slow_exfiltration"], {5})
        self.assertEqual(result["credential_compromise"], {10})
        self.assertNotIn("baseline", result)

    def test_insider_collaboration_two_principals(self):
        events = [
            {"scenario": "insider_collaboration", "actor_email": "p00001@example.com"},
            {"scenario": "insider_collaboration", "actor_email": "p00002@example.com"},
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(5)}
        result = _identify_attack_principals(events, email_to_idx)
        self.assertEqual(result["insider_collaboration"], {1, 2})


# ---------------------------------------------------------------------------
# WS5: Score all principals tests
# ---------------------------------------------------------------------------


class TestScoreAllPrincipals(unittest.TestCase):
    def test_scores_active_principals(self):
        fs = _small_feature_set()
        cfg = config_from_features(
            fs, embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            sources=["gworkspace", "github"],
        )
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        model.eval()

        test_events = [
            {"actor_email": "p00000@example.com", "resource_id": "doc-a",
             "source": "gworkspace", "observed_at": "2026-01-20T10:00:00Z"},
            {"actor_email": "p00001@example.com", "resource_id": "doc-b",
             "source": "gworkspace", "observed_at": "2026-01-20T11:00:00Z"},
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(5)}

        scores = score_all_principals(
            model, test_events, fs, vocabs, cfg, email_to_idx,
            device=torch.device("cpu"),
        )
        self.assertIn(0, scores)
        self.assertIn(1, scores)
        self.assertNotIn(2, scores)  # not active in test events


# ---------------------------------------------------------------------------
# WS5: End-to-end pipeline test
# ---------------------------------------------------------------------------


class TestRunM0Evaluation(unittest.TestCase):
    def test_small_evaluation(self):
        """Run full pipeline at tiny scale to verify it works end-to-end."""
        cfg = M0EvalConfig(
            num_principals=30,
            num_days=10,
            seed=42,
            train_days=7,
            spatial_holdout=0.3,
            embed_dim=8,
            token_dim=4,
            static_embed_dim=4,
            action_hidden=[16],
            context_hidden=[16],
            n_positive=2,
            batch_size=32,
            epochs=2,
        )
        result = run_m0_evaluation(cfg, device=torch.device("cpu"), verbose=False)

        # Should have evaluation result
        self.assertIsInstance(result, EvaluationResult)

        # Should have scored some principals
        self.assertGreater(len(result.principal_scores), 0)

        # All scores should be finite non-negative
        for pidx, score in result.principal_scores.items():
            self.assertGreaterEqual(score, 0.0, f"Negative score for {pidx}")
            self.assertTrue(np.isfinite(score), f"Non-finite score for {pidx}")

        # Aggregate metrics should be in valid ranges
        self.assertGreaterEqual(result.aggregate_roc_auc, 0.0)
        self.assertLessEqual(result.aggregate_roc_auc, 1.0)


if __name__ == "__main__":
    unittest.main()
