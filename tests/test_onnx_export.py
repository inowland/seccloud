from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
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
)
from seccloud.onnx_export import (
    ONNXActionTower,
    ONNXContextTower,
    benchmark_latency,
    export_model,
    validate_equivalence,
)


def _small_config() -> ModelConfig:
    return ModelConfig(
        principal_vocab_size=20,
        resource_vocab_size=30,
        embed_dim=16,
        token_dim=8,
        static_embed_dim=4,
        action_hidden=[32],
        context_hidden=[32],
        num_roles=4,
        num_locations=3,
        num_duration_buckets=5,
        num_privilege_levels=3,
        sources=["gworkspace", "github"],
    )


class TestExportModel(unittest.TestCase):
    def setUp(self):
        self.cfg = _small_config()
        self.model = FacadeModel(self.cfg)
        self.model.eval()
        self.tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-onnx-"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_export_creates_files(self):
        exported = export_model(self.model, self.tmpdir)
        # One action tower per source
        self.assertEqual(set(exported.action_tower_paths.keys()), {"gworkspace", "github"})
        for path in exported.action_tower_paths.values():
            self.assertTrue(path.exists())
            self.assertGreater(path.stat().st_size, 0)
        # Context tower
        self.assertTrue(exported.context_tower_path.exists())
        self.assertGreater(exported.context_tower_path.stat().st_size, 0)

    def test_exported_dimensions_stored(self):
        exported = export_model(
            self.model, self.tmpdir,
            max_tokens=32, max_windows=16, max_res=8, max_peers=24,
        )
        self.assertEqual(exported.max_tokens, 32)
        self.assertEqual(exported.max_windows, 16)
        self.assertEqual(exported.max_res_per_window, 8)
        self.assertEqual(exported.max_peers, 24)


class TestONNXActionTower(unittest.TestCase):
    def setUp(self):
        self.cfg = _small_config()
        self.model = FacadeModel(self.cfg)
        self.model.eval()
        self.tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-onnx-"))
        self.exported = export_model(self.model, self.tmpdir, max_tokens=8)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_output_shape(self):
        ort = ONNXActionTower(self.exported.action_tower_paths["gworkspace"])
        indices = np.array([[1, 3, 0, 0, 0, 0, 0, 0]], dtype=np.int64)
        weights = np.array([[0.6, 0.4, 0, 0, 0, 0, 0, 0]], dtype=np.float32)
        mask = np.array([[True, True, False, False, False, False, False, False]])
        out = ort(indices, weights, mask)
        self.assertEqual(out.shape, (1, 16))

    def test_output_l2_normalized(self):
        ort = ONNXActionTower(self.exported.action_tower_paths["gworkspace"])
        indices = np.array([[1, 5, 10, 0, 0, 0, 0, 0]], dtype=np.int64)
        weights = np.array([[0.5, 0.3, 0.2, 0, 0, 0, 0, 0]], dtype=np.float32)
        mask = np.array([[True, True, True, False, False, False, False, False]])
        out = ort(indices, weights, mask)
        norm = float(np.linalg.norm(out[0]))
        self.assertAlmostEqual(norm, 1.0, places=4)

    def test_batch_inference(self):
        ort = ONNXActionTower(self.exported.action_tower_paths["github"])
        indices = np.random.randint(0, 20, (4, 8), dtype=np.int64)
        weights = np.random.rand(4, 8).astype(np.float32)
        mask = np.ones((4, 8), dtype=bool)
        out = ort(indices, weights, mask)
        self.assertEqual(out.shape, (4, 16))


class TestONNXContextTower(unittest.TestCase):
    def setUp(self):
        self.cfg = _small_config()
        self.model = FacadeModel(self.cfg)
        self.model.eval()
        self.tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-onnx-"))
        self.exported = export_model(
            self.model, self.tmpdir,
            max_windows=4, max_res=4, max_peers=6,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_output_shape(self):
        ort = ONNXContextTower(self.exported.context_tower_path)
        out = ort(
            hist_window_indices=np.zeros((1, 4, 4), dtype=np.int64),
            hist_window_mask=np.ones((1, 4, 4), dtype=bool),
            hist_num_windows=np.array([2], dtype=np.int64),
            peer_indices=np.zeros((1, 4, 6), dtype=np.int64),
            peer_weights=np.zeros((1, 4, 6), dtype=np.float32),
            peer_mask=np.zeros((1, 4, 6), dtype=bool),
            role=np.array([0], dtype=np.int64),
            location=np.array([0], dtype=np.int64),
            duration=np.array([0], dtype=np.int64),
            privilege=np.array([0], dtype=np.int64),
        )
        self.assertEqual(out.shape, (1, 16))

    def test_output_l2_normalized(self):
        ort = ONNXContextTower(self.exported.context_tower_path)
        out = ort(
            hist_window_indices=np.random.randint(0, 30, (1, 4, 4), dtype=np.int64),
            hist_window_mask=np.ones((1, 4, 4), dtype=bool),
            hist_num_windows=np.array([4], dtype=np.int64),
            peer_indices=np.random.randint(0, 20, (1, 4, 6), dtype=np.int64),
            peer_weights=np.random.rand(1, 4, 6).astype(np.float32),
            peer_mask=np.ones((1, 4, 6), dtype=bool),
            role=np.array([1], dtype=np.int64),
            location=np.array([2], dtype=np.int64),
            duration=np.array([3], dtype=np.int64),
            privilege=np.array([0], dtype=np.int64),
        )
        norm = float(np.linalg.norm(out[0]))
        self.assertAlmostEqual(norm, 1.0, places=4)


class TestValidateEquivalence(unittest.TestCase):
    def test_pytorch_matches_onnx(self):
        cfg = _small_config()
        model = FacadeModel(cfg)
        model.eval()
        tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-onnx-"))
        try:
            exported = export_model(model, tmpdir)
            result = validate_equivalence(
                model, exported, num_samples=20, tolerance=1e-5,
            )
            self.assertTrue(
                result.all_passed,
                f"Equivalence failed: action={result.action_max_diff}, "
                f"context={result.context_max_diff}",
            )
            for source, diff in result.action_max_diff.items():
                self.assertLess(diff, 1e-5, f"{source} diff too large: {diff}")
            self.assertLess(result.context_max_diff, 1e-5)
        finally:
            shutil.rmtree(tmpdir)


class TestBenchmarkLatency(unittest.TestCase):
    def test_returns_positive_latencies(self):
        cfg = _small_config()
        model = FacadeModel(cfg)
        model.eval()
        tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-onnx-"))
        try:
            exported = export_model(
                model, tmpdir,
                max_tokens=16, max_windows=8, max_res=4, max_peers=8,
            )
            result = benchmark_latency(
                exported, num_warmup=3, num_iterations=10,
            )
            for source, latency in result.action_latency_ms.items():
                self.assertGreater(latency, 0, f"{source} latency not positive")
            self.assertGreater(result.context_latency_ms, 0)
        finally:
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    unittest.main()
