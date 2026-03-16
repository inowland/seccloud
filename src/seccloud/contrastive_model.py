"""Two-tower contrastive model for M0 ML validation (WS3).

Implements the Facade-style contrastive learning model:

- Action tower (per source type): weighted accessor set over principal vocab
  → token embedding → weighted sum pooling → MLP → L2-normalized unit vector.
- Context tower: history windows + peer groups + static categoricals
  → encoders → concatenate → MLP → L2-normalized unit vector.
- Pairwise ranking loss with Huber-like pointwise term and power-mean emphasis.

Consumes FeatureSet from feature_pipeline.py (WS2).
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset

from seccloud.feature_pipeline import (
    ActionFeatures,
    ContextFeatures,
    FeatureSet,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_TYPES: list[str] = ["okta", "gworkspace", "github", "snowflake"]
DURATION_BUCKETS: list[str] = ["<3mo", "3-12mo", "1-3yr", "3-5yr", "5yr+"]
PRIVILEGE_LEVELS: list[str] = ["regular", "elevated", "admin"]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ModelConfig:
    """Hyperparameters for the Facade contrastive model."""

    # Embedding dimensions
    embed_dim: int = 64
    token_dim: int = 32
    static_embed_dim: int = 8

    # Tower MLP hidden dimensions
    action_hidden: list[int] = field(default_factory=lambda: [256, 128])
    context_hidden: list[int] = field(default_factory=lambda: [256, 128])

    # Vocabulary sizes (set from data via config_from_features)
    principal_vocab_size: int = 1000
    resource_vocab_size: int = 5000

    # Categorical vocabulary sizes
    num_roles: int = 10
    num_locations: int = 10
    num_duration_buckets: int = len(DURATION_BUCKETS)
    num_privilege_levels: int = len(PRIVILEGE_LEVELS)

    # Source types
    sources: list[str] = field(default_factory=lambda: list(SOURCE_TYPES))

    # Padding limits (clipped from data)
    max_history_windows: int = 360
    max_action_tokens: int = 512
    max_peers: int = 256
    max_res_per_window: int = 32

    # Training
    n_positive: int = 10
    batch_size: int = 1024
    learning_rate: float = 3e-4
    epochs: int = 30

    # Loss parameters
    hard_margin: float = 0.1
    soft_margin: float = 1.0
    omega: float = 2.0


# ---------------------------------------------------------------------------
# Categorical vocabulary builder
# ---------------------------------------------------------------------------


def build_categorical_vocabs(fs: FeatureSet) -> dict[str, dict[str, int]]:
    """Build string-to-index mappings for categorical context features."""
    roles: set[str] = set()
    locations: set[str] = set()
    for ctx in fs.contexts.values():
        roles.add(ctx.static.role)
        locations.add(ctx.static.location)
    return {
        "role": {v: i for i, v in enumerate(sorted(roles))},
        "location": {v: i for i, v in enumerate(sorted(locations))},
        "duration_bucket": {v: i for i, v in enumerate(DURATION_BUCKETS)},
        "privilege_level": {v: i for i, v in enumerate(PRIVILEGE_LEVELS)},
    }


def config_from_features(fs: FeatureSet, **overrides: Any) -> ModelConfig:
    """Create a ModelConfig sized to a FeatureSet."""
    cat_vocabs = build_categorical_vocabs(fs)
    kwargs: dict[str, Any] = {
        "principal_vocab_size": fs.principal_vocab_size,
        "resource_vocab_size": len(fs.resource_vocab),
        "num_roles": len(cat_vocabs["role"]),
        "num_locations": len(cat_vocabs["location"]),
        "num_duration_buckets": len(cat_vocabs["duration_bucket"]),
        "num_privilege_levels": len(cat_vocabs["privilege_level"]),
    }
    kwargs.update(overrides)
    return ModelConfig(**kwargs)


# ---------------------------------------------------------------------------
# Weighted-set pooling
# ---------------------------------------------------------------------------


def weighted_sum_pool(
    embeddings: torch.Tensor,  # [batch, max_tokens, dim]
    weights: torch.Tensor,  # [batch, max_tokens]
    mask: torch.Tensor,  # [batch, max_tokens] bool
) -> torch.Tensor:
    """Weighted sum pooling with mask.  Returns zero vector for empty sets."""
    w = weights * mask.float()
    return (embeddings * w.unsqueeze(-1)).sum(dim=1)


# ---------------------------------------------------------------------------
# Action Tower
# ---------------------------------------------------------------------------


class ActionTower(nn.Module):
    """Maps a weighted accessor set to a d-dimensional unit vector.

    Input: sparse weighted token set over the principal vocabulary.
    Principal indices are expected to be offset by +1 (0 = padding).
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.token_embed = nn.Embedding(
            config.principal_vocab_size + 1,
            config.token_dim,
            padding_idx=0,
        )
        layers: list[nn.Module] = []
        in_dim = config.token_dim
        for h in config.action_hidden:
            layers.extend([nn.Linear(in_dim, h), nn.ReLU()])
            in_dim = h
        layers.append(nn.Linear(in_dim, config.embed_dim))
        self.mlp = nn.Sequential(*layers)

    def forward(
        self,
        indices: torch.Tensor,  # [batch, max_tokens]
        weights: torch.Tensor,  # [batch, max_tokens]
        mask: torch.Tensor,  # [batch, max_tokens] bool
    ) -> torch.Tensor:
        emb = self.token_embed(indices)
        pooled = weighted_sum_pool(emb, weights, mask)
        return F.normalize(self.mlp(pooled), p=2, dim=-1)


# ---------------------------------------------------------------------------
# Context Tower components
# ---------------------------------------------------------------------------


class HistoryEncoder(nn.Module):
    """Encode per-principal activity history.

    Per window: resource token embeddings -> mean pool -> window vector.
    Across windows: mean aggregation -> history vector.
    Resource indices are expected offset by +1 (0 = padding).
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.resource_embed = nn.Embedding(
            config.resource_vocab_size + 1,
            config.token_dim,
            padding_idx=0,
        )
        self.output_dim = config.token_dim

    def forward(
        self,
        window_indices: torch.Tensor,  # [batch, max_win, max_res]
        window_mask: torch.Tensor,  # [batch, max_win, max_res] bool
        num_windows: torch.Tensor,  # [batch]
    ) -> torch.Tensor:
        _batch, max_w, _max_r = window_indices.shape
        emb = self.resource_embed(window_indices)  # [B, W, R, d]
        wm = window_mask.float().unsqueeze(-1)  # [B, W, R, 1]
        w_sum = (emb * wm).sum(dim=2)  # [B, W, d]
        w_cnt = window_mask.float().sum(dim=2).clamp(min=1).unsqueeze(-1)
        w_repr = w_sum / w_cnt  # [B, W, d]
        # Mask padding windows, mean pool across time
        device = num_windows.device
        has_w = (torch.arange(max_w, device=device).unsqueeze(0) < num_windows.unsqueeze(1)).float()
        masked = w_repr * has_w.unsqueeze(-1)
        return masked.sum(dim=1) / num_windows.float().clamp(min=1).unsqueeze(-1)


class PeerEncoder(nn.Module):
    """Encode peer relationships (department, manager, group, collaboration).

    Each peer group is a weighted token set over the principal vocabulary.
    Principal indices are expected offset by +1 (0 = padding).
    """

    NUM_GROUPS = 4

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.peer_embed = nn.Embedding(
            config.principal_vocab_size + 1,
            config.token_dim,
            padding_idx=0,
        )
        self.output_dim = self.NUM_GROUPS * config.token_dim

    def forward(
        self,
        indices: torch.Tensor,  # [batch, 4, max_peers]
        weights: torch.Tensor,  # [batch, 4, max_peers]
        mask: torch.Tensor,  # [batch, 4, max_peers] bool
    ) -> torch.Tensor:
        B, G, P = indices.shape
        emb = self.peer_embed(indices.reshape(B * G, P))
        pooled = weighted_sum_pool(
            emb,
            weights.reshape(B * G, P),
            mask.reshape(B * G, P),
        )
        return pooled.reshape(B, G * self.peer_embed.embedding_dim)


class StaticEncoder(nn.Module):
    """Embed categorical static features (role, location, duration, privilege)."""

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        d = config.static_embed_dim
        self.role_embed = nn.Embedding(config.num_roles, d)
        self.location_embed = nn.Embedding(config.num_locations, d)
        self.duration_embed = nn.Embedding(config.num_duration_buckets, d)
        self.privilege_embed = nn.Embedding(config.num_privilege_levels, d)
        self.output_dim = 4 * d

    def forward(
        self,
        role: torch.Tensor,
        location: torch.Tensor,
        duration: torch.Tensor,
        privilege: torch.Tensor,
    ) -> torch.Tensor:
        return torch.cat(
            [
                self.role_embed(role),
                self.location_embed(location),
                self.duration_embed(duration),
                self.privilege_embed(privilege),
            ],
            dim=-1,
        )


class ContextTower(nn.Module):
    """Maps full principal context to a d-dimensional unit vector."""

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.history_enc = HistoryEncoder(config)
        self.peer_enc = PeerEncoder(config)
        self.static_enc = StaticEncoder(config)
        in_dim = self.history_enc.output_dim + self.peer_enc.output_dim + self.static_enc.output_dim
        layers: list[nn.Module] = []
        for h in config.context_hidden:
            layers.extend([nn.Linear(in_dim, h), nn.ReLU()])
            in_dim = h
        layers.append(nn.Linear(in_dim, config.embed_dim))
        self.mlp = nn.Sequential(*layers)

    def forward(self, ctx: dict[str, torch.Tensor]) -> torch.Tensor:
        h = self.history_enc(
            ctx["hist_window_indices"],
            ctx["hist_window_mask"],
            ctx["hist_num_windows"],
        )
        p = self.peer_enc(
            ctx["peer_indices"],
            ctx["peer_weights"],
            ctx["peer_mask"],
        )
        s = self.static_enc(
            ctx["role"],
            ctx["location"],
            ctx["duration"],
            ctx["privilege"],
        )
        return F.normalize(self.mlp(torch.cat([h, p, s], dim=-1)), p=2, dim=-1)


# ---------------------------------------------------------------------------
# Full model
# ---------------------------------------------------------------------------


class FacadeModel(nn.Module):
    """Two-tower contrastive model for insider-threat detection."""

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.config = config
        self.action_towers = nn.ModuleDict({s: ActionTower(config) for s in config.sources})
        self.context_tower = ContextTower(config)

    def encode_action(
        self,
        source: str,
        indices: torch.Tensor,
        weights: torch.Tensor,
        mask: torch.Tensor,
    ) -> torch.Tensor:
        return self.action_towers[source](indices, weights, mask)

    def encode_context(self, ctx: dict[str, torch.Tensor]) -> torch.Tensor:
        return self.context_tower(ctx)

    def score(
        self,
        source: str,
        act_i: torch.Tensor,
        act_w: torch.Tensor,
        act_m: torch.Tensor,
        ctx: dict[str, torch.Tensor],
    ) -> torch.Tensor:
        """Cosine distance between action and context embeddings."""
        e_a = self.encode_action(source, act_i, act_w, act_m)
        e_c = self.encode_context(ctx)
        return 1.0 - (e_a * e_c).sum(dim=-1)


# ---------------------------------------------------------------------------
# Loss
# ---------------------------------------------------------------------------


def huber_like_loss(x: torch.Tensor, delta: float = 1.0) -> torch.Tensor:
    """Huber-like pointwise loss: quadratic near zero, linear far from zero."""
    return torch.where(
        x.abs() <= delta,
        0.5 * x**2,
        delta * (x.abs() - 0.5 * delta),
    )


def pairwise_ranking_loss(
    natural_scores: torch.Tensor,  # [batch]
    synthetic_scores: torch.Tensor,  # [batch, n_p]
    h: float = 0.1,
    s: float = 1.0,
    omega: float = 2.0,
) -> torch.Tensor:
    """Pairwise ranking loss with Huber pointwise loss and power-mean emphasis.

    Natural scores (real pairs) should be LOW (close embeddings).
    Synthetic scores (random pairs) should be HIGH (far embeddings).
    Loss penalises when synthetic scores are not sufficiently larger.
    """
    # margin positive when natural >= synthetic (bad — want synthetic > natural)
    margins = h + (natural_scores.unsqueeze(1) - synthetic_scores) / s
    pw = huber_like_loss(F.relu(margins))
    per_sample = pw.mean(dim=1)
    return per_sample.pow(omega).mean().pow(1.0 / omega)


# ---------------------------------------------------------------------------
# Tensorization helpers
# ---------------------------------------------------------------------------


def _pad_weighted_set(
    token_weights: dict[int, float],
    max_len: int,
    offset: int = 1,
) -> tuple[list[int], list[float], list[bool]]:
    """Pad a sparse weighted token set to *max_len*.

    Indices are shifted by *offset* so that 0 can serve as padding_idx.
    """
    items = list(token_weights.items())[:max_len]
    indices = [idx + offset for idx, _ in items]
    weights = [w for _, w in items]
    mask = [True] * len(items)
    pad = max_len - len(items)
    indices.extend([0] * pad)
    weights.extend([0.0] * pad)
    mask.extend([False] * pad)
    return indices, weights, mask


def tensorize_action(af: ActionFeatures, max_tokens: int) -> dict[str, torch.Tensor]:
    """Convert an ActionFeatures to padded tensors."""
    indices, weights, mask = _pad_weighted_set(af.accessor_weights, max_tokens)
    return {
        "action_indices": torch.tensor(indices, dtype=torch.long),
        "action_weights": torch.tensor(weights, dtype=torch.float32),
        "action_mask": torch.tensor(mask, dtype=torch.bool),
    }


def tensorize_context(
    ctx: ContextFeatures,
    resource_vocab: dict[str, int],
    cat_vocabs: dict[str, dict[str, int]],
    max_windows: int,
    max_res: int,
    max_peers: int,
) -> dict[str, torch.Tensor]:
    """Convert a ContextFeatures to padded tensors."""
    # --- History windows ---
    win_idx: list[list[int]] = []
    win_mask: list[list[bool]] = []
    for hw in ctx.history[:max_windows]:
        rids = list(hw.resource_ids)[:max_res]
        ri = [resource_vocab.get(r, 0) + 1 for r in rids]
        rm = [True] * len(ri)
        ri.extend([0] * (max_res - len(ri)))
        rm.extend([False] * (max_res - len(rm)))
        win_idx.append(ri)
        win_mask.append(rm)
    num_w = len(win_idx)
    empty_r = [0] * max_res
    empty_m = [False] * max_res
    for _ in range(max_windows - num_w):
        win_idx.append(list(empty_r))
        win_mask.append(list(empty_m))

    # --- Peers: [4, max_peers] ---
    groups = [
        ctx.peers.department_peers,
        ctx.peers.manager_peers,
        ctx.peers.group_peers,
        ctx.collaboration.co_access,
    ]
    p_indices: list[list[int]] = []
    p_weights: list[list[float]] = []
    p_mask: list[list[bool]] = []
    for g in groups:
        gi, gw, gm = _pad_weighted_set(g, max_peers)
        p_indices.append(gi)
        p_weights.append(gw)
        p_mask.append(gm)

    # --- Static categoricals ---
    cv = cat_vocabs
    return {
        "hist_window_indices": torch.tensor(win_idx, dtype=torch.long),
        "hist_window_mask": torch.tensor(win_mask, dtype=torch.bool),
        "hist_num_windows": torch.tensor(num_w, dtype=torch.long),
        "peer_indices": torch.tensor(p_indices, dtype=torch.long),
        "peer_weights": torch.tensor(p_weights, dtype=torch.float32),
        "peer_mask": torch.tensor(p_mask, dtype=torch.bool),
        "role": torch.tensor(cv["role"].get(ctx.static.role, 0), dtype=torch.long),
        "location": torch.tensor(cv["location"].get(ctx.static.location, 0), dtype=torch.long),
        "duration": torch.tensor(
            cv["duration_bucket"].get(ctx.static.employment_duration_bucket, 0),
            dtype=torch.long,
        ),
        "privilege": torch.tensor(
            cv["privilege_level"].get(ctx.static.privilege_level, 0),
            dtype=torch.long,
        ),
    }


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TrainingSample:
    """A unique (action, context, source) training pair."""

    resource_id: str
    principal_idx: int
    source: str


def build_training_pairs(fs: FeatureSet) -> list[TrainingSample]:
    """Extract unique (resource, principal, source) triples from action features."""
    seen: set[tuple[str, int]] = set()
    pairs: list[TrainingSample] = []
    for rid, af in fs.actions.items():
        for pidx in af.accessor_weights:
            if pidx in fs.contexts and (rid, pidx) not in seen:
                seen.add((rid, pidx))
                pairs.append(
                    TrainingSample(
                        resource_id=rid,
                        principal_idx=pidx,
                        source=af.source,
                    )
                )
    return pairs


class FacadeDataset(Dataset):
    """PyTorch dataset for Facade contrastive training.

    Pre-tensorizes all actions and contexts at construction (feasible at M0
    scale ~1K principals).  Each ``__getitem__`` returns the natural pair plus
    *n_positive* randomly sampled synthetic contexts.
    """

    def __init__(
        self,
        feature_set: FeatureSet,
        pairs: list[TrainingSample],
        cat_vocabs: dict[str, dict[str, int]],
        config: ModelConfig,
        rng_seed: int = 42,
    ) -> None:
        self.pairs = pairs
        self.config = config
        self.rng = random.Random(rng_seed)
        self.all_pidx = list(feature_set.contexts.keys())

        # Compute effective padding limits from data
        self.max_act = min(
            max((len(af.accessor_weights) for af in feature_set.actions.values()), default=1),
            config.max_action_tokens,
        )
        self.max_peers = min(
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
        self.max_win = max(1, min(max(hist_lens, default=1), config.max_history_windows))
        res_lens = [len(hw.resource_ids) for c in feature_set.contexts.values() for hw in c.history]
        self.max_res = max(1, min(max(res_lens, default=1), config.max_res_per_window))

        # Pre-tensorize all contexts
        self._ctx: dict[int, dict[str, torch.Tensor]] = {}
        for pidx, ctx in feature_set.contexts.items():
            self._ctx[pidx] = tensorize_context(
                ctx,
                feature_set.resource_vocab,
                cat_vocabs,
                self.max_win,
                self.max_res,
                self.max_peers,
            )

        # Pre-tensorize all actions
        self._act: dict[str, dict[str, torch.Tensor]] = {}
        for rid, af in feature_set.actions.items():
            self._act[rid] = tensorize_action(af, self.max_act)

    def __len__(self) -> int:
        return len(self.pairs)

    def __getitem__(self, idx: int) -> dict[str, Any]:
        pair = self.pairs[idx]
        act = self._act[pair.resource_id]
        nat = self._ctx[pair.principal_idx]

        # Sample n_p synthetic contexts (random principal != natural)
        syn_list: list[dict[str, torch.Tensor]] = []
        for _ in range(self.config.n_positive):
            j = self.rng.choice(self.all_pidx)
            while j == pair.principal_idx:
                j = self.rng.choice(self.all_pidx)
            syn_list.append(self._ctx[j])

        syn = {k: torch.stack([s[k] for s in syn_list]) for k in nat}

        return {
            "source": pair.source,
            "action_indices": act["action_indices"],
            "action_weights": act["action_weights"],
            "action_mask": act["action_mask"],
            "natural_context": nat,
            "synthetic_contexts": syn,
        }


# ---------------------------------------------------------------------------
# Collation
# ---------------------------------------------------------------------------


def collate_facade(batch: list[dict[str, Any]]) -> dict[str, Any]:
    """Custom collate for FacadeDataset output."""
    return {
        "source": [b["source"] for b in batch],
        "action_indices": torch.stack([b["action_indices"] for b in batch]),
        "action_weights": torch.stack([b["action_weights"] for b in batch]),
        "action_mask": torch.stack([b["action_mask"] for b in batch]),
        "natural_context": {
            k: torch.stack([b["natural_context"][k] for b in batch]) for k in batch[0]["natural_context"]
        },
        "synthetic_contexts": {
            k: torch.stack([b["synthetic_contexts"][k] for b in batch]) for k in batch[0]["synthetic_contexts"]
        },
    }


# ---------------------------------------------------------------------------
# Training loop
# ---------------------------------------------------------------------------


def train_epoch(
    model: FacadeModel,
    dataloader: DataLoader,
    optimizer: torch.optim.Optimizer,
    config: ModelConfig,
    device: torch.device,
) -> float:
    """Run one training epoch.  Returns mean loss."""
    model.train()
    total_loss = 0.0
    count = 0

    for batch in dataloader:
        optimizer.zero_grad()

        sources = batch["source"]
        act_i = batch["action_indices"].to(device)
        act_w = batch["action_weights"].to(device)
        act_m = batch["action_mask"].to(device)
        nat_ctx = {k: v.to(device) for k, v in batch["natural_context"].items()}
        syn_ctx = {k: v.to(device) for k, v in batch["synthetic_contexts"].items()}

        B = act_i.shape[0]
        n_p = config.n_positive

        # Encode natural contexts
        nat_emb = model.encode_context(nat_ctx)  # [B, d]

        # Encode synthetic contexts: [B, n_p, ...] -> [B*n_p, ...] -> [B, n_p, d]
        syn_flat = {k: v.reshape(B * n_p, *v.shape[2:]) for k, v in syn_ctx.items()}
        syn_emb = model.encode_context(syn_flat).reshape(B, n_p, -1)

        # Encode actions grouped by source type
        source_groups: dict[str, list[int]] = defaultdict(list)
        for i, s in enumerate(sources):
            source_groups[s].append(i)

        action_emb = torch.zeros(B, config.embed_dim, device=device)
        for src, idxs in source_groups.items():
            idx_t = torch.tensor(idxs, dtype=torch.long, device=device)
            a_emb = model.encode_action(
                src,
                act_i[idx_t],
                act_w[idx_t],
                act_m[idx_t],
            )
            action_emb.index_copy_(0, idx_t, a_emb)

        # Cosine distance scores
        nat_scores = 1.0 - (action_emb * nat_emb).sum(dim=-1)  # [B]
        syn_scores = 1.0 - (action_emb.unsqueeze(1) * syn_emb).sum(dim=-1)  # [B, n_p]

        loss = pairwise_ranking_loss(
            nat_scores,
            syn_scores,
            config.hard_margin,
            config.soft_margin,
            config.omega,
        )
        loss.backward()
        optimizer.step()

        total_loss += loss.item() * B
        count += B

    return total_loss / max(count, 1)


def train(
    model: FacadeModel,
    feature_set: FeatureSet,
    cat_vocabs: dict[str, dict[str, int]],
    config: ModelConfig,
    *,
    device: torch.device | None = None,
    seed: int = 42,
) -> list[float]:
    """Train the model on a FeatureSet.  Returns per-epoch loss list."""
    if device is None:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    pairs = build_training_pairs(feature_set)
    dataset = FacadeDataset(feature_set, pairs, cat_vocabs, config, rng_seed=seed)
    dataloader = DataLoader(
        dataset,
        batch_size=config.batch_size,
        shuffle=True,
        collate_fn=collate_facade,
        num_workers=0,
    )

    optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)

    losses: list[float] = []
    for _epoch in range(config.epochs):
        epoch_loss = train_epoch(model, dataloader, optimizer, config, device)
        losses.append(epoch_loss)

    return losses
