"""ONNX export and inference validation for M0 ML validation (WS6).

Exports the trained action and context towers to ONNX, validates numerical
equivalence against PyTorch inference, and benchmarks latency.

The exported ONNX models define the contract between Python training and
future Rust inference:

  Action tower (one per source type):
    Inputs:  indices  int64   [batch, max_tokens]
             weights  float32 [batch, max_tokens]
             mask     bool    [batch, max_tokens]
    Output:  embedding float32 [batch, embed_dim]  (L2-normalized)

  Context tower:
    Inputs:  hist_window_indices  int64   [batch, max_win, max_res]
             hist_window_mask     bool    [batch, max_win, max_res]
             hist_num_windows     int64   [batch]
             peer_indices         int64   [batch, 4, max_peers]
             peer_weights         float32 [batch, 4, max_peers]
             peer_mask            bool    [batch, 4, max_peers]
             role                 int64   [batch]
             location             int64   [batch]
             duration             int64   [batch]
             privilege            int64   [batch]
    Output:  embedding float32 [batch, embed_dim]  (L2-normalized)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn

from seccloud.contrastive_model import (
    ActionTower,
    ContextTower,
    FacadeModel,
    ModelConfig,
)

# ---------------------------------------------------------------------------
# ONNX-compatible wrappers
# ---------------------------------------------------------------------------


class ActionTowerONNX(nn.Module):
    """Thin wrapper around ActionTower with positional args for ONNX export."""

    def __init__(self, tower: ActionTower) -> None:
        super().__init__()
        self.tower = tower

    def forward(
        self,
        indices: torch.Tensor,
        weights: torch.Tensor,
        mask: torch.Tensor,
    ) -> torch.Tensor:
        return self.tower(indices, weights, mask)


class ContextTowerONNX(nn.Module):
    """Thin wrapper around ContextTower with positional args for ONNX export.

    Converts positional arguments to the dict format the ContextTower expects.
    """

    def __init__(self, tower: ContextTower) -> None:
        super().__init__()
        self.tower = tower

    def forward(
        self,
        hist_window_indices: torch.Tensor,
        hist_window_mask: torch.Tensor,
        hist_num_windows: torch.Tensor,
        peer_indices: torch.Tensor,
        peer_weights: torch.Tensor,
        peer_mask: torch.Tensor,
        role: torch.Tensor,
        location: torch.Tensor,
        duration: torch.Tensor,
        privilege: torch.Tensor,
    ) -> torch.Tensor:
        ctx = {
            "hist_window_indices": hist_window_indices,
            "hist_window_mask": hist_window_mask,
            "hist_num_windows": hist_num_windows,
            "peer_indices": peer_indices,
            "peer_weights": peer_weights,
            "peer_mask": peer_mask,
            "role": role,
            "location": location,
            "duration": duration,
            "privilege": privilege,
        }
        return self.tower(ctx)


# ---------------------------------------------------------------------------
# Example inputs for tracing
# ---------------------------------------------------------------------------


def _action_example(config: ModelConfig, batch: int = 1, max_tokens: int = 8) -> tuple:
    return (
        torch.randint(0, config.principal_vocab_size + 1, (batch, max_tokens)),
        torch.rand(batch, max_tokens),
        torch.ones(batch, max_tokens, dtype=torch.bool),
    )


def _context_example(
    config: ModelConfig,
    batch: int = 1,
    max_win: int = 4,
    max_res: int = 4,
    max_peers: int = 8,
) -> tuple:
    return (
        torch.randint(0, config.resource_vocab_size + 1, (batch, max_win, max_res)),
        torch.ones(batch, max_win, max_res, dtype=torch.bool),
        torch.tensor([max_win] * batch, dtype=torch.long),
        torch.randint(0, config.principal_vocab_size + 1, (batch, 4, max_peers)),
        torch.rand(batch, 4, max_peers),
        torch.ones(batch, 4, max_peers, dtype=torch.bool),
        torch.randint(0, config.num_roles, (batch,)),
        torch.randint(0, config.num_locations, (batch,)),
        torch.randint(0, config.num_duration_buckets, (batch,)),
        torch.randint(0, config.num_privilege_levels, (batch,)),
    )


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ExportedModel:
    """Paths and metadata for an exported ONNX model set."""

    action_tower_paths: dict[str, Path]
    context_tower_path: Path
    config: ModelConfig
    max_tokens: int
    max_windows: int
    max_res_per_window: int
    max_peers: int


def export_model(
    model: FacadeModel,
    output_dir: Path,
    *,
    max_tokens: int = 64,
    max_windows: int = 64,
    max_res: int = 16,
    max_peers: int = 64,
) -> ExportedModel:
    """Export action and context towers to ONNX files.

    Creates one ONNX file per action tower (one per source type) and one
    for the context tower under *output_dir*.

    Parameters
    ----------
    max_tokens, max_windows, max_res, max_peers :
        Fixed padding dimensions baked into the ONNX models.  Inputs must
        be padded to these sizes at inference time.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    model.eval()
    config = model.config

    action_paths: dict[str, Path] = {}
    for source, tower in model.action_towers.items():
        wrapper = ActionTowerONNX(tower)
        wrapper.eval()
        example = _action_example(config, batch=1, max_tokens=max_tokens)
        path = output_dir / f"action_tower_{source}.onnx"
        batch_dim = torch.export.Dim("batch", min=1)
        torch.onnx.export(
            wrapper,
            example,
            str(path),
            input_names=["indices", "weights", "mask"],
            output_names=["embedding"],
            dynamic_shapes={
                "indices": {0: batch_dim},
                "weights": {0: batch_dim},
                "mask": {0: batch_dim},
            },
            opset_version=18,
        )
        action_paths[source] = path

    ctx_wrapper = ContextTowerONNX(model.context_tower)
    ctx_wrapper.eval()
    ctx_example = _context_example(
        config,
        batch=1,
        max_win=max_windows,
        max_res=max_res,
        max_peers=max_peers,
    )
    ctx_path = output_dir / "context_tower.onnx"
    batch_dim = torch.export.Dim("batch", min=1)
    torch.onnx.export(
        ctx_wrapper,
        ctx_example,
        str(ctx_path),
        input_names=[
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
        output_names=["embedding"],
        dynamic_shapes={
            "hist_window_indices": {0: batch_dim},
            "hist_window_mask": {0: batch_dim},
            "hist_num_windows": {0: batch_dim},
            "peer_indices": {0: batch_dim},
            "peer_weights": {0: batch_dim},
            "peer_mask": {0: batch_dim},
            "role": {0: batch_dim},
            "location": {0: batch_dim},
            "duration": {0: batch_dim},
            "privilege": {0: batch_dim},
        },
        opset_version=18,
    )

    return ExportedModel(
        action_tower_paths=action_paths,
        context_tower_path=ctx_path,
        config=config,
        max_tokens=max_tokens,
        max_windows=max_windows,
        max_res_per_window=max_res,
        max_peers=max_peers,
    )


# ---------------------------------------------------------------------------
# ONNX Runtime inference
# ---------------------------------------------------------------------------


class ONNXActionTower:
    """ONNX Runtime wrapper for an exported action tower."""

    def __init__(self, path: Path) -> None:
        import onnxruntime as ort

        self.session = ort.InferenceSession(
            str(path),
            providers=["CPUExecutionProvider"],
        )

    def __call__(
        self,
        indices: np.ndarray,
        weights: np.ndarray,
        mask: np.ndarray,
    ) -> np.ndarray:
        return self.session.run(
            ["embedding"],
            {
                "indices": indices.astype(np.int64),
                "weights": weights.astype(np.float32),
                "mask": mask.astype(bool),
            },
        )[0]


class ONNXContextTower:
    """ONNX Runtime wrapper for the exported context tower."""

    def __init__(self, path: Path) -> None:
        import onnxruntime as ort

        self.session = ort.InferenceSession(
            str(path),
            providers=["CPUExecutionProvider"],
        )

    def __call__(self, **kwargs: np.ndarray) -> np.ndarray:
        feed = {}
        int_keys = {
            "hist_window_indices",
            "hist_num_windows",
            "peer_indices",
            "role",
            "location",
            "duration",
            "privilege",
        }
        bool_keys = {"hist_window_mask", "peer_mask"}
        for k, v in kwargs.items():
            if k in int_keys:
                feed[k] = v.astype(np.int64)
            elif k in bool_keys:
                feed[k] = v.astype(bool)
            else:
                feed[k] = v.astype(np.float32)
        return self.session.run(["embedding"], feed)[0]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ValidationResult:
    """Results from numerical equivalence validation."""

    action_max_diff: dict[str, float]
    context_max_diff: float
    all_passed: bool
    tolerance: float


@torch.no_grad()
def validate_equivalence(
    model: FacadeModel,
    exported: ExportedModel,
    *,
    num_samples: int = 50,
    tolerance: float = 1e-5,
    seed: int = 42,
) -> ValidationResult:
    """Validate ONNX outputs match PyTorch within *tolerance*.

    Generates *num_samples* random inputs and compares PyTorch vs ONNX
    Runtime outputs.
    """
    model.eval()
    config = model.config
    torch.manual_seed(seed)

    action_max_diffs: dict[str, float] = {}

    for source, onnx_path in exported.action_tower_paths.items():
        ort_tower = ONNXActionTower(onnx_path)
        max_diff = 0.0
        for _ in range(num_samples):
            example = _action_example(config, batch=1, max_tokens=exported.max_tokens)
            pt_out = model.action_towers[source](*example).numpy()
            ort_out = ort_tower(
                example[0].numpy(),
                example[1].numpy(),
                example[2].numpy(),
            )
            diff = float(np.abs(pt_out - ort_out).max())
            max_diff = max(max_diff, diff)
        action_max_diffs[source] = max_diff

    ort_ctx = ONNXContextTower(exported.context_tower_path)
    ctx_max_diff = 0.0
    for _ in range(num_samples):
        example = _context_example(
            config,
            batch=1,
            max_win=exported.max_windows,
            max_res=exported.max_res_per_window,
            max_peers=exported.max_peers,
        )
        ctx_dict = {
            "hist_window_indices": example[0],
            "hist_window_mask": example[1],
            "hist_num_windows": example[2],
            "peer_indices": example[3],
            "peer_weights": example[4],
            "peer_mask": example[5],
            "role": example[6],
            "location": example[7],
            "duration": example[8],
            "privilege": example[9],
        }
        pt_out = model.context_tower(ctx_dict).numpy()
        ort_out = ort_ctx(**{k: v.numpy() for k, v in ctx_dict.items()})
        diff = float(np.abs(pt_out - ort_out).max())
        ctx_max_diff = max(ctx_max_diff, diff)

    all_passed = all(d <= tolerance for d in action_max_diffs.values())
    all_passed = all_passed and ctx_max_diff <= tolerance

    return ValidationResult(
        action_max_diff=action_max_diffs,
        context_max_diff=ctx_max_diff,
        all_passed=all_passed,
        tolerance=tolerance,
    )


# ---------------------------------------------------------------------------
# Benchmarking
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BenchmarkResult:
    """Inference latency benchmarks."""

    action_latency_ms: dict[str, float]
    context_latency_ms: float
    action_target_met: bool
    target_ms: float


def benchmark_latency(
    exported: ExportedModel,
    *,
    num_warmup: int = 10,
    num_iterations: int = 100,
    target_ms: float = 1.0,
    seed: int = 42,
) -> BenchmarkResult:
    """Benchmark single-sample inference latency on CPU.

    Runs *num_warmup* warm-up iterations then *num_iterations* timed
    iterations.  Reports median latency per tower.
    """
    np.random.seed(seed)
    config = exported.config

    action_latencies: dict[str, float] = {}
    for source, path in exported.action_tower_paths.items():
        ort_tower = ONNXActionTower(path)
        indices = np.random.randint(
            0,
            config.principal_vocab_size + 1,
            (1, exported.max_tokens),
            dtype=np.int64,
        )
        weights = np.random.rand(1, exported.max_tokens).astype(np.float32)
        mask = np.ones((1, exported.max_tokens), dtype=bool)

        for _ in range(num_warmup):
            ort_tower(indices, weights, mask)

        times = []
        for _ in range(num_iterations):
            t0 = time.perf_counter()
            ort_tower(indices, weights, mask)
            times.append((time.perf_counter() - t0) * 1000)
        action_latencies[source] = float(np.median(times))

    ort_ctx = ONNXContextTower(exported.context_tower_path)
    ctx_kwargs = {
        "hist_window_indices": np.random.randint(
            0,
            config.resource_vocab_size + 1,
            (1, exported.max_windows, exported.max_res_per_window),
            dtype=np.int64,
        ),
        "hist_window_mask": np.ones(
            (1, exported.max_windows, exported.max_res_per_window),
            dtype=bool,
        ),
        "hist_num_windows": np.array([exported.max_windows], dtype=np.int64),
        "peer_indices": np.random.randint(
            0,
            config.principal_vocab_size + 1,
            (1, 4, exported.max_peers),
            dtype=np.int64,
        ),
        "peer_weights": np.random.rand(1, 4, exported.max_peers).astype(np.float32),
        "peer_mask": np.ones((1, 4, exported.max_peers), dtype=bool),
        "role": np.array([0], dtype=np.int64),
        "location": np.array([0], dtype=np.int64),
        "duration": np.array([0], dtype=np.int64),
        "privilege": np.array([0], dtype=np.int64),
    }

    for _ in range(num_warmup):
        ort_ctx(**ctx_kwargs)

    times = []
    for _ in range(num_iterations):
        t0 = time.perf_counter()
        ort_ctx(**ctx_kwargs)
        times.append((time.perf_counter() - t0) * 1000)
    ctx_latency = float(np.median(times))

    all_action_met = all(t <= target_ms for t in action_latencies.values())

    return BenchmarkResult(
        action_latency_ms=action_latencies,
        context_latency_ms=ctx_latency,
        action_target_met=all_action_met,
        target_ms=target_ms,
    )


def export_model_artifact_bundle(
    model: FacadeModel,
    output_dir: Path,
    *,
    tenant_id: str,
    model_id: str,
    model_version: str | None = None,
    model_family: str = "contrastive-facade",
    feature_schema_version: str = "feature.v1",
    eval_report: dict[str, object] | None = None,
    principal_entity_keys: list[str] | None = None,
    resource_entity_keys: list[str] | None = None,
    categorical_vocabs: dict[str, dict[str, int]] | None = None,
    score_policy: dict[str, object] | None = None,
    max_tokens: int = 64,
    max_windows: int = 64,
    max_res: int = 16,
    max_peers: int = 64,
) -> dict[str, object]:
    """Export ONNX files plus the M3 model artifact bundle metadata."""
    from seccloud.model_artifact import write_model_artifact_bundle

    exported = export_model(
        model,
        output_dir,
        max_tokens=max_tokens,
        max_windows=max_windows,
        max_res=max_res,
        max_peers=max_peers,
    )
    return write_model_artifact_bundle(
        exported,
        output_dir,
        tenant_id=tenant_id,
        model_id=model_id,
        model_version=model_version,
        model_family=model_family,
        feature_schema_version=feature_schema_version,
        eval_report=eval_report,
        principal_entity_keys=principal_entity_keys,
        resource_entity_keys=resource_entity_keys,
        categorical_vocabs=categorical_vocabs,
        score_policy=score_policy,
    )
