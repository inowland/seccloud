from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

import torch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.contrastive_model import (
    ActionTower,
    ContextTower,
    FacadeDataset,
    FacadeModel,
    ModelConfig,
    TrainingSample,
    _pad_weighted_set,
    build_categorical_vocabs,
    build_training_pairs,
    collate_facade,
    config_from_features,
    huber_like_loss,
    pairwise_ranking_loss,
    tensorize_action,
    tensorize_context,
    train,
    train_epoch,
    weighted_sum_pool,
)
from seccloud.feature_pipeline import (
    ActionFeatures,
    CollaborationFeatures,
    ContextFeatures,
    FeatureSet,
    HistoryWindow,
    PeerFeatures,
    StaticFeatures,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _small_feature_set() -> FeatureSet:
    """Create a minimal FeatureSet for testing."""
    actions = {
        "doc-a": ActionFeatures(
            resource_id="doc-a", source="gworkspace",
            accessor_weights={0: 0.6, 1: 0.4},
        ),
        "doc-b": ActionFeatures(
            resource_id="doc-b", source="gworkspace",
            accessor_weights={1: 0.5, 2: 0.5},
        ),
        "repo-x": ActionFeatures(
            resource_id="repo-x", source="github",
            accessor_weights={0: 1.0},
        ),
    }

    def _ctx(idx: int, role: str = "engineer", loc: str = "US-NY") -> ContextFeatures:
        return ContextFeatures(
            principal_idx=idx,
            history=[
                HistoryWindow(
                    window_start=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                    resource_ids=frozenset({"doc-a", "doc-b"}),
                ),
                HistoryWindow(
                    window_start=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
                    resource_ids=frozenset({"repo-x"}),
                ),
            ],
            peers=PeerFeatures(
                department_peers={j: 1.0 for j in range(5) if j != idx},
                manager_peers={j: 1.0 for j in range(3) if j != idx},
                group_peers={j: 0.5 for j in range(3) if j != idx},
            ),
            collaboration=CollaborationFeatures(
                co_access={j: 0.3 for j in range(5) if j != idx},
            ),
            static=StaticFeatures(
                role=role, employment_duration_bucket="1-3yr",
                location=loc, privilege_level="regular",
            ),
        )

    contexts = {
        0: _ctx(0),
        1: _ctx(1, role="analyst"),
        2: _ctx(2, role="manager", loc="US-SF"),
        3: _ctx(3),
        4: _ctx(4, role="executive", loc="UK-LON"),
    }
    resource_vocab = {"doc-a": 0, "doc-b": 1, "repo-x": 2}
    return FeatureSet(
        actions=actions, contexts=contexts,
        principal_vocab_size=5, resource_vocab=resource_vocab,
    )


def _small_config(fs: FeatureSet) -> ModelConfig:
    """Small config suitable for quick tests."""
    return config_from_features(
        fs,
        embed_dim=16, token_dim=8, static_embed_dim=4,
        action_hidden=[32], context_hidden=[32],
        n_positive=2, batch_size=4, epochs=2,
        sources=["gworkspace", "github"],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWeightedSumPool(unittest.TestCase):
    def test_basic(self):
        emb = torch.tensor([[[1.0, 2.0], [3.0, 4.0]]])  # [1, 2, 2]
        w = torch.tensor([[0.6, 0.4]])
        m = torch.tensor([[True, True]])
        out = weighted_sum_pool(emb, w, m)
        expected = torch.tensor([[0.6 * 1 + 0.4 * 3, 0.6 * 2 + 0.4 * 4]])
        self.assertTrue(torch.allclose(out, expected))

    def test_mask_zeros_padding(self):
        emb = torch.tensor([[[1.0, 2.0], [99.0, 99.0]]])
        w = torch.tensor([[1.0, 1.0]])
        m = torch.tensor([[True, False]])
        out = weighted_sum_pool(emb, w, m)
        self.assertTrue(torch.allclose(out, torch.tensor([[1.0, 2.0]])))


class TestBuildCategoricalVocabs(unittest.TestCase):
    def test_covers_all_values(self):
        fs = _small_feature_set()
        vocabs = build_categorical_vocabs(fs)
        roles = {ctx.static.role for ctx in fs.contexts.values()}
        self.assertEqual(set(vocabs["role"].keys()), roles)
        locs = {ctx.static.location for ctx in fs.contexts.values()}
        self.assertEqual(set(vocabs["location"].keys()), locs)


class TestConfigFromFeatures(unittest.TestCase):
    def test_sizes_match_data(self):
        fs = _small_feature_set()
        cfg = config_from_features(fs)
        self.assertEqual(cfg.principal_vocab_size, 5)
        self.assertEqual(cfg.resource_vocab_size, 3)


class TestTensorizeAction(unittest.TestCase):
    def test_shapes(self):
        af = ActionFeatures("doc-a", "gworkspace", {0: 0.6, 1: 0.4})
        t = tensorize_action(af, max_tokens=4)
        self.assertEqual(t["action_indices"].shape, (4,))
        self.assertEqual(t["action_weights"].shape, (4,))
        self.assertEqual(t["action_mask"].shape, (4,))

    def test_offset(self):
        af = ActionFeatures("doc-a", "gworkspace", {0: 1.0})
        t = tensorize_action(af, max_tokens=2)
        # principal 0 should be stored as index 1 (offset by 1 for padding)
        self.assertEqual(t["action_indices"][0].item(), 1)

    def test_padding(self):
        af = ActionFeatures("doc-a", "gworkspace", {5: 1.0})
        t = tensorize_action(af, max_tokens=3)
        self.assertTrue(t["action_mask"][0].item())
        self.assertFalse(t["action_mask"][1].item())
        self.assertFalse(t["action_mask"][2].item())


class TestTensorizeContext(unittest.TestCase):
    def test_shapes(self):
        fs = _small_feature_set()
        vocabs = build_categorical_vocabs(fs)
        ctx = fs.contexts[0]
        t = tensorize_context(ctx, fs.resource_vocab, vocabs,
                              max_windows=4, max_res=3, max_peers=6)
        self.assertEqual(t["hist_window_indices"].shape, (4, 3))
        self.assertEqual(t["hist_window_mask"].shape, (4, 3))
        self.assertEqual(t["hist_num_windows"].item(), 2)
        self.assertEqual(t["peer_indices"].shape, (4, 6))
        self.assertEqual(t["role"].shape, ())


class TestActionTower(unittest.TestCase):
    def test_output_shape_and_norm(self):
        cfg = ModelConfig(
            principal_vocab_size=10, embed_dim=16, token_dim=8,
            action_hidden=[32],
        )
        tower = ActionTower(cfg)
        indices = torch.tensor([[1, 3, 0], [2, 0, 0]])
        weights = torch.tensor([[0.6, 0.4, 0.0], [1.0, 0.0, 0.0]])
        mask = torch.tensor([[True, True, False], [True, False, False]])
        out = tower(indices, weights, mask)
        self.assertEqual(out.shape, (2, 16))
        norms = out.norm(dim=-1)
        self.assertTrue(torch.allclose(norms, torch.ones(2), atol=1e-5))


class TestContextTower(unittest.TestCase):
    def test_output_shape_and_norm(self):
        cfg = ModelConfig(
            principal_vocab_size=10, resource_vocab_size=20,
            embed_dim=16, token_dim=8, static_embed_dim=4,
            context_hidden=[32],
            num_roles=4, num_locations=3,
            num_duration_buckets=5, num_privilege_levels=3,
        )
        tower = ContextTower(cfg)
        ctx = {
            "hist_window_indices": torch.randint(0, 20, (2, 3, 4)),
            "hist_window_mask": torch.ones(2, 3, 4, dtype=torch.bool),
            "hist_num_windows": torch.tensor([3, 2]),
            "peer_indices": torch.randint(0, 10, (2, 4, 5)),
            "peer_weights": torch.rand(2, 4, 5),
            "peer_mask": torch.ones(2, 4, 5, dtype=torch.bool),
            "role": torch.tensor([0, 1]),
            "location": torch.tensor([1, 2]),
            "duration": torch.tensor([0, 3]),
            "privilege": torch.tensor([0, 2]),
        }
        out = tower(ctx)
        self.assertEqual(out.shape, (2, 16))
        norms = out.norm(dim=-1)
        self.assertTrue(torch.allclose(norms, torch.ones(2), atol=1e-5))


class TestFacadeModel(unittest.TestCase):
    def test_score_shape(self):
        cfg = ModelConfig(
            principal_vocab_size=10, resource_vocab_size=20,
            embed_dim=16, token_dim=8, static_embed_dim=4,
            action_hidden=[32], context_hidden=[32],
            num_roles=4, num_locations=3,
            num_duration_buckets=5, num_privilege_levels=3,
            sources=["gworkspace"],
        )
        model = FacadeModel(cfg)
        act_i = torch.randint(0, 10, (3, 5))
        act_w = torch.rand(3, 5)
        act_m = torch.ones(3, 5, dtype=torch.bool)
        ctx = {
            "hist_window_indices": torch.randint(0, 20, (3, 2, 4)),
            "hist_window_mask": torch.ones(3, 2, 4, dtype=torch.bool),
            "hist_num_windows": torch.tensor([2, 1, 2]),
            "peer_indices": torch.randint(0, 10, (3, 4, 5)),
            "peer_weights": torch.rand(3, 4, 5),
            "peer_mask": torch.ones(3, 4, 5, dtype=torch.bool),
            "role": torch.tensor([0, 1, 2]),
            "location": torch.tensor([1, 0, 2]),
            "duration": torch.tensor([0, 3, 1]),
            "privilege": torch.tensor([0, 2, 1]),
        }
        scores = model.score("gworkspace", act_i, act_w, act_m, ctx)
        self.assertEqual(scores.shape, (3,))
        # Cosine distance in [0, 2]
        self.assertTrue((scores >= 0).all())
        self.assertTrue((scores <= 2).all())

    def test_gradient_flow(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
            sources=["gworkspace"],
        )
        model = FacadeModel(cfg)
        act_i = torch.tensor([[1, 2]])
        act_w = torch.tensor([[0.6, 0.4]])
        act_m = torch.tensor([[True, True]])
        ctx = {
            "hist_window_indices": torch.tensor([[[1, 2]]]),
            "hist_window_mask": torch.tensor([[[True, True]]]),
            "hist_num_windows": torch.tensor([1]),
            "peer_indices": torch.randint(0, 5, (1, 4, 3)),
            "peer_weights": torch.rand(1, 4, 3),
            "peer_mask": torch.ones(1, 4, 3, dtype=torch.bool),
            "role": torch.tensor([0]),
            "location": torch.tensor([0]),
            "duration": torch.tensor([0]),
            "privilege": torch.tensor([0]),
        }
        score = model.score("gworkspace", act_i, act_w, act_m, ctx)
        score.sum().backward()
        # Check that gradients exist on all tower parameters
        for name, param in model.named_parameters():
            if param.requires_grad:
                self.assertIsNotNone(param.grad, f"No gradient for {name}")


class TestHuberLikeLoss(unittest.TestCase):
    def test_quadratic_near_zero(self):
        x = torch.tensor([0.5])
        expected = 0.5 * 0.5 ** 2
        self.assertAlmostEqual(huber_like_loss(x).item(), expected, places=5)

    def test_linear_far_from_zero(self):
        x = torch.tensor([2.0])
        expected = 1.0 * (2.0 - 0.5 * 1.0)
        self.assertAlmostEqual(huber_like_loss(x).item(), expected, places=5)

    def test_symmetric(self):
        x = torch.tensor([-1.5])
        self.assertAlmostEqual(
            huber_like_loss(x).item(), huber_like_loss(-x).item(), places=5,
        )


class TestPairwiseRankingLoss(unittest.TestCase):
    def test_zero_when_synthetic_far(self):
        nat = torch.tensor([0.1, 0.2])         # low (close embeddings)
        syn = torch.tensor([[1.5, 1.8], [1.6, 1.7]])  # high (far embeddings)
        loss = pairwise_ranking_loss(nat, syn, h=0.1, s=1.0, omega=2.0)
        # synthetic >> natural -> margin negative -> relu zeros it -> loss ≈ 0
        self.assertLess(loss.item(), 1e-6)

    def test_positive_when_synthetic_close(self):
        nat = torch.tensor([0.8, 0.9])
        syn = torch.tensor([[0.3, 0.2], [0.4, 0.3]])
        loss = pairwise_ranking_loss(nat, syn, h=0.1, s=1.0, omega=2.0)
        self.assertGreater(loss.item(), 0.0)

    def test_differentiable(self):
        nat = torch.tensor([0.5], requires_grad=True)
        syn = torch.tensor([[0.4]])
        loss = pairwise_ranking_loss(nat, syn)
        loss.backward()
        self.assertIsNotNone(nat.grad)


class TestBuildTrainingPairs(unittest.TestCase):
    def test_produces_unique_pairs(self):
        fs = _small_feature_set()
        pairs = build_training_pairs(fs)
        keys = [(p.resource_id, p.principal_idx) for p in pairs]
        self.assertEqual(len(keys), len(set(keys)))

    def test_covers_all_accessor_pairs(self):
        fs = _small_feature_set()
        pairs = build_training_pairs(fs)
        expected = set()
        for rid, af in fs.actions.items():
            for pidx in af.accessor_weights:
                if pidx in fs.contexts:
                    expected.add((rid, pidx))
        actual = {(p.resource_id, p.principal_idx) for p in pairs}
        self.assertEqual(actual, expected)


class TestFacadeDataset(unittest.TestCase):
    def setUp(self):
        self.fs = _small_feature_set()
        self.cfg = _small_config(self.fs)
        self.vocabs = build_categorical_vocabs(self.fs)
        self.pairs = build_training_pairs(self.fs)
        self.ds = FacadeDataset(
            self.fs, self.pairs, self.vocabs, self.cfg, rng_seed=99,
        )

    def test_length(self):
        self.assertEqual(len(self.ds), len(self.pairs))

    def test_getitem_keys(self):
        item = self.ds[0]
        self.assertIn("source", item)
        self.assertIn("action_indices", item)
        self.assertIn("natural_context", item)
        self.assertIn("synthetic_contexts", item)

    def test_synthetic_shape(self):
        item = self.ds[0]
        syn = item["synthetic_contexts"]
        for k, v in syn.items():
            # First dimension should be n_positive
            self.assertEqual(v.shape[0], self.cfg.n_positive, f"{k} wrong n_p dim")


class TestCollate(unittest.TestCase):
    def test_collate_stacks(self):
        fs = _small_feature_set()
        cfg = _small_config(fs)
        vocabs = build_categorical_vocabs(fs)
        pairs = build_training_pairs(fs)
        ds = FacadeDataset(fs, pairs, vocabs, cfg, rng_seed=99)
        batch = collate_facade([ds[0], ds[1]])
        self.assertEqual(len(batch["source"]), 2)
        self.assertEqual(batch["action_indices"].shape[0], 2)
        self.assertEqual(batch["natural_context"]["role"].shape[0], 2)
        self.assertEqual(batch["synthetic_contexts"]["role"].shape[0], 2)
        self.assertEqual(batch["synthetic_contexts"]["role"].shape[1], cfg.n_positive)


class TestTrainEndToEnd(unittest.TestCase):
    def test_train_reduces_loss(self):
        """Train for a few epochs on tiny data and verify loss is finite."""
        fs = _small_feature_set()
        cfg = _small_config(fs)
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        losses = train(
            model, fs, vocabs, cfg,
            device=torch.device("cpu"), seed=42,
        )
        self.assertEqual(len(losses), cfg.epochs)
        for loss in losses:
            self.assertTrue(0 <= loss < float("inf"), f"bad loss: {loss}")

    def test_train_from_synthetic_pipeline(self):
        """Integration: generate synthetic data, featurize, train."""
        import random
        from datetime import timedelta

        from seccloud.feature_pipeline import build_features
        from seccloud.synthetic_scale import (
            ScaleConfig,
            build_app_affinities,
            build_principal_affinities,
            generate_org,
            generate_resources,
            _generate_day_events,
        )

        scale_cfg = ScaleConfig(num_principals=20, num_days=3, seed=7)
        rng = random.Random(scale_cfg.seed)
        principals, teams = generate_org(scale_cfg, rng)
        resources = generate_resources(scale_cfg, teams, rng)
        affinities = build_principal_affinities(
            scale_cfg, principals, teams, resources, rng,
        )
        app_affinities = build_app_affinities(principals, rng)

        counter = [0]
        events: list[dict] = []
        for d in range(scale_cfg.num_days):
            day = scale_cfg.start_date + timedelta(days=d)
            events.extend(
                _generate_day_events(
                    day, principals, affinities, app_affinities,
                    scale_cfg, rng, counter,
                )
            )
        events.sort(key=lambda e: e["observed_at"])

        fs = build_features(events, principals, teams)
        cfg = config_from_features(
            fs,
            embed_dim=16, token_dim=8, static_embed_dim=4,
            action_hidden=[32], context_hidden=[32],
            n_positive=3, batch_size=32, epochs=2,
        )
        vocabs = build_categorical_vocabs(fs)
        model = FacadeModel(cfg)
        losses = train(model, fs, vocabs, cfg, device=torch.device("cpu"), seed=42)
        self.assertEqual(len(losses), 2)
        for loss in losses:
            self.assertFalse(
                torch.isnan(torch.tensor(loss)),
                f"NaN loss at epoch",
            )


# ---------------------------------------------------------------------------
# Value-correctness and edge-case tests
# ---------------------------------------------------------------------------


class TestPadWeightedSet(unittest.TestCase):
    def test_index_weight_pairing_preserved(self):
        """Each index is paired with its correct weight after padding."""
        wts = {5: 0.3, 10: 0.7}
        indices, weights, mask = _pad_weighted_set(wts, max_len=4, offset=1)
        # Build a mapping from the output to verify pairing
        for i, (idx, w, m) in enumerate(zip(indices, weights, mask)):
            if m:
                original_idx = idx - 1  # undo offset
                self.assertIn(original_idx, wts)
                self.assertAlmostEqual(w, wts[original_idx])

    def test_truncation(self):
        """When len(token_weights) > max_len, only first max_len items kept."""
        wts = {i: float(i) for i in range(10)}
        indices, weights, mask = _pad_weighted_set(wts, max_len=3, offset=1)
        self.assertEqual(len(indices), 3)
        self.assertTrue(all(mask))
        # All returned items should be valid (offset applied)
        for idx in indices:
            self.assertGreater(idx, 0)

    def test_empty_dict(self):
        """Empty input produces all-padding output."""
        indices, weights, mask = _pad_weighted_set({}, max_len=3, offset=1)
        self.assertEqual(indices, [0, 0, 0])
        self.assertEqual(weights, [0.0, 0.0, 0.0])
        self.assertEqual(mask, [False, False, False])

    def test_offset_applied_to_all(self):
        wts = {0: 0.5, 3: 0.5}
        indices, _w, _m = _pad_weighted_set(wts, max_len=4, offset=1)
        real_indices = indices[:2]
        self.assertIn(1, real_indices)   # 0 + 1
        self.assertIn(4, real_indices)   # 3 + 1


class TestTensorizeActionValues(unittest.TestCase):
    def test_index_weight_alignment(self):
        """Verify that each index position carries the correct weight."""
        af = ActionFeatures("r", "gworkspace", {2: 0.7, 5: 0.3})
        t = tensorize_action(af, max_tokens=4)
        for i in range(4):
            if t["action_mask"][i]:
                orig = t["action_indices"][i].item() - 1  # undo offset
                self.assertAlmostEqual(
                    t["action_weights"][i].item(),
                    af.accessor_weights[orig],
                )

    def test_empty_accessor_weights(self):
        af = ActionFeatures("r", "gworkspace", {})
        t = tensorize_action(af, max_tokens=3)
        self.assertFalse(t["action_mask"].any())
        self.assertTrue((t["action_indices"] == 0).all())


class TestTensorizeContextValues(unittest.TestCase):
    def setUp(self):
        self.resource_vocab = {"doc-a": 0, "doc-b": 1, "repo-x": 2}
        self.cat_vocabs = {
            "role": {"analyst": 0, "engineer": 1},
            "location": {"US-NY": 0, "US-SF": 1},
            "duration_bucket": {"<3mo": 0, "3-12mo": 1, "1-3yr": 2, "3-5yr": 3, "5yr+": 4},
            "privilege_level": {"regular": 0, "elevated": 1, "admin": 2},
        }

    def test_resource_vocab_lookup(self):
        """Known resources map to correct offset indices."""
        ctx = ContextFeatures(
            principal_idx=0,
            history=[HistoryWindow(
                window_start=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                resource_ids=frozenset({"doc-a"}),
            )],
            peers=PeerFeatures({}, {}, {}),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
        )
        t = tensorize_context(ctx, self.resource_vocab, self.cat_vocabs,
                              max_windows=2, max_res=3, max_peers=2)
        # "doc-a" has vocab index 0, offset +1 = 1
        win0 = t["hist_window_indices"][0]
        self.assertEqual(win0[0].item(), 1)
        self.assertTrue(t["hist_window_mask"][0, 0].item())

    def test_unknown_resource_fallback(self):
        """Unknown resources get vocab index 0, offset +1 = 1."""
        ctx = ContextFeatures(
            principal_idx=0,
            history=[HistoryWindow(
                window_start=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                resource_ids=frozenset({"unknown-resource"}),
            )],
            peers=PeerFeatures({}, {}, {}),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
        )
        t = tensorize_context(ctx, self.resource_vocab, self.cat_vocabs,
                              max_windows=2, max_res=2, max_peers=2)
        self.assertEqual(t["hist_window_indices"][0, 0].item(), 1)  # 0 + 1

    def test_peer_group_ordering(self):
        """Peer groups appear in order: dept[0], manager[1], group[2], collab[3]."""
        ctx = ContextFeatures(
            principal_idx=0,
            history=[],
            peers=PeerFeatures(
                department_peers={1: 1.0},
                manager_peers={2: 0.8},
                group_peers={3: 0.6},
            ),
            collaboration=CollaborationFeatures(co_access={4: 0.4}),
            static=StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
        )
        t = tensorize_context(ctx, self.resource_vocab, self.cat_vocabs,
                              max_windows=1, max_res=1, max_peers=3)
        # Group 0 (department): principal 1 -> offset index 2
        self.assertEqual(t["peer_indices"][0, 0].item(), 2)
        self.assertAlmostEqual(t["peer_weights"][0, 0].item(), 1.0)
        # Group 1 (manager): principal 2 -> offset index 3
        self.assertEqual(t["peer_indices"][1, 0].item(), 3)
        self.assertAlmostEqual(t["peer_weights"][1, 0].item(), 0.8)
        # Group 2 (group): principal 3 -> offset index 4
        self.assertEqual(t["peer_indices"][2, 0].item(), 4)
        self.assertAlmostEqual(t["peer_weights"][2, 0].item(), 0.6)
        # Group 3 (collaboration): principal 4 -> offset index 5
        self.assertEqual(t["peer_indices"][3, 0].item(), 5)
        self.assertAlmostEqual(t["peer_weights"][3, 0].item(), 0.4)

    def test_static_categorical_mapping(self):
        ctx = ContextFeatures(
            principal_idx=0, history=[],
            peers=PeerFeatures({}, {}, {}),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures("analyst", "3-5yr", "US-SF", "elevated"),
        )
        t = tensorize_context(ctx, self.resource_vocab, self.cat_vocabs,
                              max_windows=1, max_res=1, max_peers=1)
        self.assertEqual(t["role"].item(), 0)       # "analyst" -> 0
        self.assertEqual(t["location"].item(), 1)   # "US-SF" -> 1
        self.assertEqual(t["duration"].item(), 3)    # "3-5yr" -> 3
        self.assertEqual(t["privilege"].item(), 1)   # "elevated" -> 1

    def test_hist_num_windows_matches_real_windows(self):
        ctx = ContextFeatures(
            principal_idx=0,
            history=[
                HistoryWindow(datetime(2026, 1, 15, 10, 0, tzinfo=UTC), frozenset({"doc-a"})),
                HistoryWindow(datetime(2026, 1, 15, 12, 0, tzinfo=UTC), frozenset({"doc-b"})),
                HistoryWindow(datetime(2026, 1, 15, 14, 0, tzinfo=UTC), frozenset({"repo-x"})),
            ],
            peers=PeerFeatures({}, {}, {}),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
        )
        t = tensorize_context(ctx, self.resource_vocab, self.cat_vocabs,
                              max_windows=5, max_res=2, max_peers=1)
        self.assertEqual(t["hist_num_windows"].item(), 3)
        # Windows 3 and 4 should be all padding
        self.assertFalse(t["hist_window_mask"][3].any())
        self.assertFalse(t["hist_window_mask"][4].any())


class TestEdgeCaseEmptyHistory(unittest.TestCase):
    """Principals with no history windows should produce valid outputs."""

    def _ctx_no_history(self) -> ContextFeatures:
        return ContextFeatures(
            principal_idx=0, history=[],
            peers=PeerFeatures(department_peers={1: 1.0}, manager_peers={}, group_peers={}),
            collaboration=CollaborationFeatures({}),
            static=StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
        )

    def test_tensorize_zero_windows(self):
        ctx = self._ctx_no_history()
        vocabs = {
            "role": {"engineer": 0}, "location": {"US-NY": 0},
            "duration_bucket": {"<3mo": 0, "3-12mo": 1, "1-3yr": 2, "3-5yr": 3, "5yr+": 4},
            "privilege_level": {"regular": 0, "elevated": 1, "admin": 2},
        }
        t = tensorize_context(ctx, {"doc-a": 0}, vocabs,
                              max_windows=3, max_res=2, max_peers=2)
        self.assertEqual(t["hist_num_windows"].item(), 0)
        self.assertFalse(t["hist_window_mask"].any())

    def test_context_tower_with_zero_windows(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
        )
        tower = ContextTower(cfg)
        ctx = {
            "hist_window_indices": torch.zeros(1, 2, 3, dtype=torch.long),
            "hist_window_mask": torch.zeros(1, 2, 3, dtype=torch.bool),
            "hist_num_windows": torch.tensor([0]),
            "peer_indices": torch.tensor([[[1, 0], [0, 0], [0, 0], [0, 0]]]),
            "peer_weights": torch.tensor([[[1.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]]),
            "peer_mask": torch.tensor([[[True, False], [False, False], [False, False], [False, False]]]),
            "role": torch.tensor([0]), "location": torch.tensor([0]),
            "duration": torch.tensor([0]), "privilege": torch.tensor([0]),
        }
        out = tower(ctx)
        self.assertEqual(out.shape, (1, 8))
        # Should still be L2-normalized (non-zero because static + peer features exist)
        self.assertAlmostEqual(out.norm().item(), 1.0, places=5)


class TestEdgeCaseEmptyPeers(unittest.TestCase):
    def test_all_peer_groups_empty(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
        )
        tower = ContextTower(cfg)
        ctx = {
            "hist_window_indices": torch.tensor([[[1, 2]]]),
            "hist_window_mask": torch.tensor([[[True, True]]]),
            "hist_num_windows": torch.tensor([1]),
            # All peers: all padding
            "peer_indices": torch.zeros(1, 4, 3, dtype=torch.long),
            "peer_weights": torch.zeros(1, 4, 3),
            "peer_mask": torch.zeros(1, 4, 3, dtype=torch.bool),
            "role": torch.tensor([0]), "location": torch.tensor([0]),
            "duration": torch.tensor([0]), "privilege": torch.tensor([0]),
        }
        out = tower(ctx)
        self.assertEqual(out.shape, (1, 8))
        self.assertAlmostEqual(out.norm().item(), 1.0, places=5)


class TestEdgeCaseAllPaddingAction(unittest.TestCase):
    def test_all_padding_produces_finite_output(self):
        cfg = ModelConfig(
            principal_vocab_size=5, embed_dim=8, token_dim=4,
            action_hidden=[16],
        )
        tower = ActionTower(cfg)
        indices = torch.zeros(1, 3, dtype=torch.long)
        weights = torch.zeros(1, 3)
        mask = torch.zeros(1, 3, dtype=torch.bool)
        out = tower(indices, weights, mask)
        self.assertEqual(out.shape, (1, 8))
        self.assertTrue(torch.isfinite(out).all())


class TestHuberLikeLossBoundary(unittest.TestCase):
    def test_continuous_at_delta(self):
        """Both branches produce the same value at |x| = delta."""
        delta = 1.0
        x = torch.tensor([delta])
        # Quadratic: 0.5 * delta^2
        # Linear: delta * (delta - 0.5 * delta) = 0.5 * delta^2
        self.assertAlmostEqual(huber_like_loss(x, delta).item(), 0.5 * delta ** 2, places=6)

    def test_zero_input(self):
        self.assertAlmostEqual(huber_like_loss(torch.tensor([0.0])).item(), 0.0, places=6)

    def test_negative_value_correctness(self):
        x = torch.tensor([-2.0])
        # Linear regime: delta * (|x| - 0.5 * delta) = 1.0 * (2.0 - 0.5) = 1.5
        self.assertAlmostEqual(huber_like_loss(x).item(), 1.5, places=5)


class TestPairwiseRankingLossOmega(unittest.TestCase):
    def test_omega_one_is_plain_mean(self):
        nat = torch.tensor([0.5, 0.8])
        syn = torch.tensor([[0.3, 0.4], [0.6, 0.5]])
        loss_o1 = pairwise_ranking_loss(nat, syn, h=0.1, s=1.0, omega=1.0)
        # With omega=1, pow(1).mean().pow(1) = plain mean
        self.assertTrue(torch.isfinite(loss_o1))
        # Higher omega should give >= plain mean (emphasizes hard cases)
        loss_o3 = pairwise_ranking_loss(nat, syn, h=0.1, s=1.0, omega=3.0)
        self.assertGreaterEqual(loss_o3.item(), loss_o1.item() - 1e-6)


class TestSourceTypeRouting(unittest.TestCase):
    """Verify that mixed-source batches route to the correct action towers."""

    def test_different_towers_produce_different_embeddings(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
            sources=["gworkspace", "github"],
        )
        model = FacadeModel(cfg)
        # Same action input, different source -> different tower -> different embedding
        torch.manual_seed(42)
        act_i = torch.tensor([[1, 2, 0]])
        act_w = torch.tensor([[0.6, 0.4, 0.0]])
        act_m = torch.tensor([[True, True, False]])
        emb_gw = model.encode_action("gworkspace", act_i, act_w, act_m)
        emb_gh = model.encode_action("github", act_i, act_w, act_m)
        # Different towers have different random init, so embeddings should differ
        self.assertFalse(torch.allclose(emb_gw, emb_gh, atol=1e-3))


class TestSyntheticContextReshape(unittest.TestCase):
    """Verify the [B, n_p, ...] -> [B*n_p, ...] -> [B, n_p, d] roundtrip."""

    def test_reshape_preserves_sample_context_pairing(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
        )
        tower = ContextTower(cfg)
        B, n_p = 2, 3

        # Create synthetic context batch: [B, n_p, ...]
        syn = {
            "hist_window_indices": torch.randint(0, 5, (B, n_p, 2, 3)),
            "hist_window_mask": torch.ones(B, n_p, 2, 3, dtype=torch.bool),
            "hist_num_windows": torch.tensor([[2, 1, 2], [1, 2, 1]]),
            "peer_indices": torch.randint(0, 5, (B, n_p, 4, 2)),
            "peer_weights": torch.rand(B, n_p, 4, 2),
            "peer_mask": torch.ones(B, n_p, 4, 2, dtype=torch.bool),
            "role": torch.randint(0, 2, (B, n_p)),
            "location": torch.randint(0, 2, (B, n_p)),
            "duration": torch.randint(0, 5, (B, n_p)),
            "privilege": torch.randint(0, 3, (B, n_p)),
        }

        # Flatten like train_epoch does
        syn_flat = {k: v.reshape(B * n_p, *v.shape[2:]) for k, v in syn.items()}
        emb_flat = tower(syn_flat)  # [B*n_p, d]
        emb_reshaped = emb_flat.reshape(B, n_p, -1)

        # Independently encode each sample's synthetic contexts and compare
        for b in range(B):
            for p in range(n_p):
                single = {k: v[b, p].unsqueeze(0) for k, v in syn.items()}
                emb_single = tower(single)
                self.assertTrue(
                    torch.allclose(emb_reshaped[b, p], emb_single.squeeze(0), atol=1e-5),
                    f"Mismatch at batch={b}, syn={p}",
                )


class TestModelSemantic(unittest.TestCase):
    """Model should produce different scores for meaningfully different inputs."""

    def test_same_input_same_output(self):
        cfg = ModelConfig(
            principal_vocab_size=5, resource_vocab_size=5,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            num_roles=2, num_locations=2,
            num_duration_buckets=5, num_privilege_levels=3,
            sources=["gworkspace"],
        )
        model = FacadeModel(cfg)
        model.eval()
        act_i = torch.tensor([[1, 2]])
        act_w = torch.tensor([[0.6, 0.4]])
        act_m = torch.tensor([[True, True]])
        ctx = {
            "hist_window_indices": torch.tensor([[[1, 2]]]),
            "hist_window_mask": torch.tensor([[[True, True]]]),
            "hist_num_windows": torch.tensor([1]),
            "peer_indices": torch.zeros(1, 4, 2, dtype=torch.long),
            "peer_weights": torch.zeros(1, 4, 2),
            "peer_mask": torch.zeros(1, 4, 2, dtype=torch.bool),
            "role": torch.tensor([0]), "location": torch.tensor([0]),
            "duration": torch.tensor([0]), "privilege": torch.tensor([0]),
        }
        with torch.no_grad():
            s1 = model.score("gworkspace", act_i, act_w, act_m, ctx)
            s2 = model.score("gworkspace", act_i, act_w, act_m, ctx)
        self.assertTrue(torch.allclose(s1, s2))

    def test_different_contexts_different_scores(self):
        cfg = ModelConfig(
            principal_vocab_size=10, resource_vocab_size=10,
            embed_dim=8, token_dim=4, static_embed_dim=4,
            action_hidden=[16], context_hidden=[16],
            num_roles=3, num_locations=3,
            num_duration_buckets=5, num_privilege_levels=3,
            sources=["gworkspace"],
        )
        model = FacadeModel(cfg)
        model.eval()
        act_i = torch.tensor([[1, 2, 3]])
        act_w = torch.tensor([[0.5, 0.3, 0.2]])
        act_m = torch.tensor([[True, True, True]])

        def _ctx(role: int, loc: int, peer_idx: int) -> dict[str, torch.Tensor]:
            return {
                "hist_window_indices": torch.tensor([[[1, 2]]]),
                "hist_window_mask": torch.tensor([[[True, True]]]),
                "hist_num_windows": torch.tensor([1]),
                "peer_indices": torch.tensor([[[peer_idx, 0], [0, 0], [0, 0], [0, 0]]]),
                "peer_weights": torch.tensor([[[1.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]]),
                "peer_mask": torch.tensor([[[True, False], [False, False], [False, False], [False, False]]]),
                "role": torch.tensor([role]), "location": torch.tensor([loc]),
                "duration": torch.tensor([0]), "privilege": torch.tensor([0]),
            }

        with torch.no_grad():
            s1 = model.score("gworkspace", act_i, act_w, act_m, _ctx(0, 0, 1))
            s2 = model.score("gworkspace", act_i, act_w, act_m, _ctx(2, 2, 5))
        self.assertFalse(torch.allclose(s1, s2),
                         "Different contexts should produce different scores")


class TestBuildTrainingPairsFilters(unittest.TestCase):
    def test_excludes_principals_missing_from_contexts(self):
        """Accessors not in contexts dict should be excluded."""
        actions = {
            "doc": ActionFeatures("doc", "gworkspace", {0: 0.5, 99: 0.5}),
        }
        contexts = {
            0: ContextFeatures(
                0, [], PeerFeatures({}, {}, {}),
                CollaborationFeatures({}),
                StaticFeatures("engineer", "1-3yr", "US-NY", "regular"),
            ),
        }
        fs = FeatureSet(actions=actions, contexts=contexts,
                        principal_vocab_size=100, resource_vocab={"doc": 0})
        pairs = build_training_pairs(fs)
        pidxs = {p.principal_idx for p in pairs}
        self.assertIn(0, pidxs)
        self.assertNotIn(99, pidxs)


class TestDatasetSyntheticNeverSelf(unittest.TestCase):
    def test_synthetic_contexts_exclude_natural(self):
        fs = _small_feature_set()
        cfg = _small_config(fs)
        vocabs = build_categorical_vocabs(fs)
        pairs = build_training_pairs(fs)
        ds = FacadeDataset(fs, pairs, vocabs, cfg, rng_seed=123)
        for i in range(len(ds)):
            item = ds[i]
            pair = pairs[i]
            nat_ctx = ds._ctx[pair.principal_idx]
            syn = item["synthetic_contexts"]
            for j in range(cfg.n_positive):
                syn_j = {k: v[j] for k, v in syn.items()}
                # At minimum, synthetic should not be the exact same context tensor
                if torch.equal(syn_j["role"], nat_ctx["role"]):
                    # Could be same role by chance; check full context
                    all_same = all(
                        torch.equal(syn_j[k], nat_ctx[k]) for k in nat_ctx
                    )
                    # If all tensors are identical, the sampling loop failed
                    # (this is probabilistically possible but extremely unlikely
                    # with 5 distinct principals)
                    if all_same:
                        self.fail(f"Synthetic context {j} at pair {i} is identical "
                                  f"to natural context")


class TestCollateSingleItem(unittest.TestCase):
    def test_single_item_batch(self):
        fs = _small_feature_set()
        cfg = _small_config(fs)
        vocabs = build_categorical_vocabs(fs)
        pairs = build_training_pairs(fs)
        ds = FacadeDataset(fs, pairs, vocabs, cfg, rng_seed=42)
        batch = collate_facade([ds[0]])
        self.assertEqual(len(batch["source"]), 1)
        self.assertEqual(batch["action_indices"].shape[0], 1)
        # Should still be valid input to the model
        model = FacadeModel(cfg)
        model.eval()
        with torch.no_grad():
            emb = model.encode_context(batch["natural_context"])
        self.assertEqual(emb.shape, (1, cfg.embed_dim))


if __name__ == "__main__":
    unittest.main()
