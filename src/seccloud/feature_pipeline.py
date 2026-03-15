"""Feature engineering pipeline for M0 ML validation (WS2).

Transforms raw events from the synthetic data generator into Facade model
inputs:

- Action features: per-resource weighted accessor sets (sparse vectors over
  the principal vocabulary).
- Context features: per-principal history profiles, peer features,
  collaboration features, and static features.

These are the direct inputs to the two-tower contrastive model (WS3).

Following Facade, timestamps and access modality are intentionally excluded
from action representations — temporal features enter only through the context
tower's history profile.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from seccloud.synthetic_scale import OrgPrincipal, OrgTeam

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HISTORY_WINDOW_HOURS: int = 2
HISTORY_TRAILING_DAYS: int = 30

PRIVILEGE_ROLES: dict[str, str] = {
    "security-admin": "admin",
    "executive": "elevated",
    "manager": "elevated",
    "staff-engineer": "elevated",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ActionFeatures:
    """Per-resource weighted accessor set — the action tower input.

    accessor_weights maps principal_idx -> weight where weight is
    access_count / total_accesses for this resource.  Weights sum to 1.
    """

    resource_id: str
    source: str
    accessor_weights: dict[int, float]


@dataclass(slots=True)
class HistoryWindow:
    """A 2-hour non-overlapping window in a principal's activity history.

    Captures the set of distinct resource IDs accessed during this window.
    Input to the context tower's history encoder.
    """

    window_start: datetime
    resource_ids: frozenset[str]


@dataclass(slots=True)
class PeerFeatures:
    """Structural peer relationships for a principal.

    Each dict maps peer principal_idx -> weight.
    """

    department_peers: dict[int, float]
    manager_peers: dict[int, float]
    group_peers: dict[int, float]


@dataclass(slots=True)
class CollaborationFeatures:
    """Inferred collaboration relationships from shared resource access.

    co_access maps principal_idx -> weight.  Weight is the sum over shared
    resources *r* of ``min(count_i[r], count_j[r]) / popularity(r)`` where
    popularity is the number of unique accessors.
    """

    co_access: dict[int, float]


@dataclass(slots=True)
class StaticFeatures:
    """Static categorical features for a principal."""

    role: str
    employment_duration_bucket: str
    location: str
    privilege_level: str


@dataclass(slots=True)
class ContextFeatures:
    """Full context feature set for a single principal — context tower input."""

    principal_idx: int
    history: list[HistoryWindow]
    peers: PeerFeatures
    collaboration: CollaborationFeatures
    static: StaticFeatures


@dataclass(slots=True)
class FeatureSet:
    """Complete feature engineering output for the Facade model.

    actions:              resource_id -> ActionFeatures
    contexts:             principal_idx -> ContextFeatures
    principal_vocab_size: token vocabulary size for the action tower
    resource_vocab:       resource_id -> integer index (token vocabulary
                          for the context tower history encoder)
    """

    actions: dict[str, ActionFeatures]
    contexts: dict[int, ContextFeatures]
    principal_vocab_size: int
    resource_vocab: dict[str, int]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_timestamp(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def _window_key(ts: datetime) -> datetime:
    """Return the start of the 2-hour window containing *ts*."""
    wh = (ts.hour // HISTORY_WINDOW_HOURS) * HISTORY_WINDOW_HOURS
    return ts.replace(hour=wh, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Action featurization
# ---------------------------------------------------------------------------


def build_action_features(
    events: list[dict[str, Any]],
    email_to_idx: dict[str, int],
) -> dict[str, ActionFeatures]:
    """Build weighted accessor sets for every observed resource.

    Counts per-principal accesses per resource, then normalises to a
    probability distribution (weights sum to 1).
    """
    access_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    resource_source: dict[str, str] = {}

    for event in events:
        resource_id = event.get("resource_id")
        actor_email = event.get("actor_email")
        if resource_id is None or actor_email is None:
            continue
        pidx = email_to_idx.get(actor_email)
        if pidx is None:
            continue
        access_counts[resource_id][pidx] += 1
        if resource_id not in resource_source:
            resource_source[resource_id] = event.get("source", "unknown")

    actions: dict[str, ActionFeatures] = {}
    for rid, counts in access_counts.items():
        total = sum(counts.values())
        if total == 0:
            continue
        inv = 1.0 / total
        actions[rid] = ActionFeatures(
            resource_id=rid,
            source=resource_source.get(rid, "unknown"),
            accessor_weights={idx: cnt * inv for idx, cnt in counts.items()},
        )
    return actions


# ---------------------------------------------------------------------------
# History profiles
# ---------------------------------------------------------------------------


def _build_history_profiles(
    events: list[dict[str, Any]],
    email_to_idx: dict[str, int],
    reference_date: datetime,
    trailing_days: int = HISTORY_TRAILING_DAYS,
) -> dict[int, list[HistoryWindow]]:
    """Build per-principal history profiles.

    For each principal, creates non-overlapping 2-hour windows over the
    trailing *trailing_days* ending at *reference_date*.  Each window records
    the set of distinct resources accessed.  Empty windows are omitted.
    """
    cutoff = reference_date - timedelta(days=trailing_days)

    # principal_idx -> {window_start -> set of resource_ids}
    windows: dict[int, dict[datetime, set[str]]] = defaultdict(lambda: defaultdict(set))

    for event in events:
        ts_str = event.get("observed_at")
        resource_id = event.get("resource_id")
        actor_email = event.get("actor_email")
        if not ts_str or not resource_id or not actor_email:
            continue
        ts = _parse_timestamp(ts_str)
        if ts < cutoff:
            continue
        pidx = email_to_idx.get(actor_email)
        if pidx is None:
            continue
        windows[pidx][_window_key(ts)].add(resource_id)

    profiles: dict[int, list[HistoryWindow]] = {}
    for idx, wmap in windows.items():
        profiles[idx] = [
            HistoryWindow(window_start=ws, resource_ids=frozenset(rids))
            for ws, rids in sorted(wmap.items())
        ]
    return profiles


# ---------------------------------------------------------------------------
# Peer features
# ---------------------------------------------------------------------------


def _build_peer_features(
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
) -> dict[int, PeerFeatures]:
    """Compute structural peer features from the org chart.

    - Department peers: weight 1.0 for every principal in the same department.
    - Manager peers: weight 1.0 for same-manager siblings, 0.5 for
      grand-manager cousins.
    - Group (team) peers: weight = 1 / team_size, summed across shared teams.
    """
    dept_members: dict[str, set[int]] = defaultdict(set)
    for p in principals:
        dept_members[p.department].add(p.idx)

    manager_reports: dict[int, set[int]] = defaultdict(set)
    for p in principals:
        if p.manager_idx is not None:
            manager_reports[p.manager_idx].add(p.idx)

    team_members: dict[str, list[int]] = {t.name: t.member_indices for t in teams}

    principal_teams: dict[int, list[str]] = defaultdict(list)
    for t in teams:
        for idx in t.member_indices:
            principal_teams[idx].append(t.name)

    by_idx: dict[int, OrgPrincipal] = {p.idx: p for p in principals}

    peers: dict[int, PeerFeatures] = {}
    for p in principals:
        # Department peers (exclude self)
        dp = {o: 1.0 for o in dept_members[p.department] if o != p.idx}

        # Manager peers
        mp: dict[int, float] = {}
        if p.manager_idx is not None:
            for sib in manager_reports[p.manager_idx]:
                if sib != p.idx:
                    mp[sib] = 1.0
            mgr = by_idx.get(p.manager_idx)
            if mgr and mgr.manager_idx is not None:
                for uncle in manager_reports[mgr.manager_idx]:
                    if uncle == p.manager_idx:
                        continue
                    for cousin in manager_reports[uncle]:
                        if cousin != p.idx and cousin not in mp:
                            mp[cousin] = 0.5

        # Group (team) co-membership
        gp: dict[int, float] = {}
        for tname in principal_teams[p.idx]:
            members = team_members.get(tname, [])
            if len(members) <= 1:
                continue
            w = 1.0 / len(members)
            for o in members:
                if o != p.idx:
                    gp[o] = gp.get(o, 0.0) + w

        peers[p.idx] = PeerFeatures(department_peers=dp, manager_peers=mp, group_peers=gp)

    return peers


# ---------------------------------------------------------------------------
# Collaboration features
# ---------------------------------------------------------------------------


def _build_collaboration_features(
    events: list[dict[str, Any]],
    email_to_idx: dict[str, int],
    num_principals: int,
) -> dict[int, CollaborationFeatures]:
    """Infer collaboration from shared resource access patterns.

    For each pair of principals sharing a resource *r*, the contribution is
    ``min(count_i, count_j) / num_unique_accessors(r)``.
    """
    resource_accessors: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for event in events:
        rid = event.get("resource_id")
        email = event.get("actor_email")
        if not rid or not email:
            continue
        pidx = email_to_idx.get(email)
        if pidx is None:
            continue
        resource_accessors[rid][pidx] += 1

    collab: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))

    for _rid, accessors in resource_accessors.items():
        pop = len(accessors)
        if pop <= 1:
            continue
        inv_pop = 1.0 / pop
        items = list(accessors.items())
        for i in range(len(items)):
            idx_i, cnt_i = items[i]
            for j in range(i + 1, len(items)):
                idx_j, cnt_j = items[j]
                w = min(cnt_i, cnt_j) * inv_pop
                collab[idx_i][idx_j] += w
                collab[idx_j][idx_i] += w

    return {
        idx: CollaborationFeatures(co_access=dict(collab.get(idx, {})))
        for idx in range(num_principals)
    }


# ---------------------------------------------------------------------------
# Static features
# ---------------------------------------------------------------------------


def _privilege_level(role: str) -> str:
    return PRIVILEGE_ROLES.get(role, "regular")


def _employment_duration_bucket(principal: OrgPrincipal) -> str:
    """Stable synthetic tenure bucket derived from principal index.

    Real deployments will use the actual account-creation date from Okta.
    """
    tenure_years = (principal.idx * 7.3) % 6.0
    if tenure_years < 0.25:
        return "<3mo"
    if tenure_years < 1.0:
        return "3-12mo"
    if tenure_years < 3.0:
        return "1-3yr"
    if tenure_years < 5.0:
        return "3-5yr"
    return "5yr+"


def _build_static_features(principals: list[OrgPrincipal]) -> dict[int, StaticFeatures]:
    return {
        p.idx: StaticFeatures(
            role=p.role,
            employment_duration_bucket=_employment_duration_bucket(p),
            location=p.location,
            privilege_level=_privilege_level(p.role),
        )
        for p in principals
    }


# ---------------------------------------------------------------------------
# Resource vocabulary
# ---------------------------------------------------------------------------


def _build_resource_vocab(events: list[dict[str, Any]]) -> dict[str, int]:
    rids: set[str] = set()
    for e in events:
        rid = e.get("resource_id")
        if rid:
            rids.add(rid)
    return {rid: i for i, rid in enumerate(sorted(rids))}


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------


def build_features(
    events: list[dict[str, Any]],
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    *,
    reference_date: datetime | None = None,
) -> FeatureSet:
    """Run the complete WS2 feature engineering pipeline.

    Transforms raw events + org structure into Facade model inputs:

    - **Action features** — per-resource weighted accessor sets
    - **Context features** — per-principal history, peers, collaboration,
      static attributes

    Parameters
    ----------
    events:
        Raw event dicts from ``synthetic_scale.generate_scaled_dataset()``.
    principals:
        Org principals from ``generate_org()``.
    teams:
        Org teams from ``generate_org()``.
    reference_date:
        End of observation window.  Defaults to the latest event timestamp.

    Returns
    -------
    FeatureSet
        Action and context features ready for the WS3 model.
    """
    email_to_idx = {p.email: p.idx for p in principals}
    n = len(principals)

    if reference_date is None:
        max_ts = max((e.get("observed_at", "") for e in events), default="")
        reference_date = _parse_timestamp(max_ts) if max_ts else datetime.now(UTC)

    actions = build_action_features(events, email_to_idx)
    history = _build_history_profiles(events, email_to_idx, reference_date)
    peers = _build_peer_features(principals, teams)
    collabs = _build_collaboration_features(events, email_to_idx, n)
    statics = _build_static_features(principals)
    resource_vocab = _build_resource_vocab(events)

    contexts: dict[int, ContextFeatures] = {}
    for p in principals:
        contexts[p.idx] = ContextFeatures(
            principal_idx=p.idx,
            history=history.get(p.idx, []),
            peers=peers[p.idx],
            collaboration=collabs[p.idx],
            static=statics[p.idx],
        )

    return FeatureSet(
        actions=actions,
        contexts=contexts,
        principal_vocab_size=n,
        resource_vocab=resource_vocab,
    )


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------


def write_parquet(features: FeatureSet, output_dir: Path) -> dict[str, Path]:
    """Write feature set to Parquet files for caching / checkpointing.

    Creates five files under *output_dir*:

    - ``actions.parquet``   — (resource_id, source, principal_idx, weight)
    - ``history.parquet``   — (principal_idx, window_start, resource_id)
    - ``peers.parquet``     — (principal_idx, peer_type, peer_idx, weight)
    - ``collaboration.parquet`` — (principal_idx, peer_idx, weight)
    - ``static.parquet``    — (principal_idx, role, duration_bucket, location, privilege)

    Returns a dict mapping table name to file path.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    # --- actions ---
    rows: list[dict[str, Any]] = []
    for af in features.actions.values():
        for pidx, w in af.accessor_weights.items():
            rows.append({"resource_id": af.resource_id, "source": af.source, "principal_idx": pidx, "weight": w})
    p = output_dir / "actions.parquet"
    pq.write_table(pa.Table.from_pylist(rows), p)
    paths["actions"] = p

    # --- history ---
    rows = []
    for ctx in features.contexts.values():
        for hw in ctx.history:
            for rid in hw.resource_ids:
                rows.append({
                    "principal_idx": ctx.principal_idx,
                    "window_start": hw.window_start.isoformat(),
                    "resource_id": rid,
                })
    p = output_dir / "history.parquet"
    empty_hist = pa.table({
        "principal_idx": pa.array([], pa.int64()),
        "window_start": pa.array([], pa.utf8()),
        "resource_id": pa.array([], pa.utf8()),
    })
    pq.write_table(pa.Table.from_pylist(rows) if rows else empty_hist, p)
    paths["history"] = p

    # --- peers ---
    rows = []
    for ctx in features.contexts.values():
        pidx = ctx.principal_idx
        for peer_idx, w in ctx.peers.department_peers.items():
            rows.append({"principal_idx": pidx, "peer_type": "department", "peer_idx": peer_idx, "weight": w})
        for peer_idx, w in ctx.peers.manager_peers.items():
            rows.append({"principal_idx": pidx, "peer_type": "manager", "peer_idx": peer_idx, "weight": w})
        for peer_idx, w in ctx.peers.group_peers.items():
            rows.append({"principal_idx": pidx, "peer_type": "group", "peer_idx": peer_idx, "weight": w})
    p = output_dir / "peers.parquet"
    pq.write_table(pa.Table.from_pylist(rows), p)
    paths["peers"] = p

    # --- collaboration ---
    rows = []
    for ctx in features.contexts.values():
        for peer_idx, w in ctx.collaboration.co_access.items():
            rows.append({"principal_idx": ctx.principal_idx, "peer_idx": peer_idx, "weight": w})
    p = output_dir / "collaboration.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows) if rows else pa.table({"principal_idx": [], "peer_idx": [], "weight": []}), p
    )
    paths["collaboration"] = p

    # --- static ---
    rows = []
    for ctx in features.contexts.values():
        s = ctx.static
        rows.append({
            "principal_idx": ctx.principal_idx,
            "role": s.role,
            "duration_bucket": s.employment_duration_bucket,
            "location": s.location,
            "privilege": s.privilege_level,
        })
    p = output_dir / "static.parquet"
    pq.write_table(pa.Table.from_pylist(rows), p)
    paths["static"] = p

    return paths


def read_parquet(input_dir: Path) -> FeatureSet:
    """Reconstruct a FeatureSet from Parquet files written by :func:`write_parquet`.

    Reads the five tables from *input_dir* and reassembles the in-memory
    data structures that the WS3 model consumes.
    """
    import pyarrow.parquet as pq

    # --- actions ---
    at = pq.read_table(input_dir / "actions.parquet")
    actions: dict[str, ActionFeatures] = {}
    max_pidx = 0
    for rid, src, pidx, w in zip(
        at.column("resource_id").to_pylist(),
        at.column("source").to_pylist(),
        at.column("principal_idx").to_pylist(),
        at.column("weight").to_pylist(),
    ):
        if rid not in actions:
            actions[rid] = ActionFeatures(resource_id=rid, source=src, accessor_weights={})
        actions[rid].accessor_weights[pidx] = w
        if pidx > max_pidx:
            max_pidx = pidx

    # --- static (read first to discover all principal indices) ---
    st = pq.read_table(input_dir / "static.parquet")
    static_by_idx: dict[int, StaticFeatures] = {}
    all_pidxs: list[int] = []
    for pidx, role, dur, loc, priv in zip(
        st.column("principal_idx").to_pylist(),
        st.column("role").to_pylist(),
        st.column("duration_bucket").to_pylist(),
        st.column("location").to_pylist(),
        st.column("privilege").to_pylist(),
    ):
        static_by_idx[pidx] = StaticFeatures(
            role=role,
            employment_duration_bucket=dur,
            location=loc,
            privilege_level=priv,
        )
        all_pidxs.append(pidx)
        if pidx > max_pidx:
            max_pidx = pidx

    # --- history ---
    ht = pq.read_table(input_dir / "history.parquet")
    # principal_idx -> {window_start_str -> set of resource_ids}
    hist_raw: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for pidx, ws, rid in zip(
        ht.column("principal_idx").to_pylist(),
        ht.column("window_start").to_pylist(),
        ht.column("resource_id").to_pylist(),
    ):
        hist_raw[pidx][ws].add(rid)

    history_by_idx: dict[int, list[HistoryWindow]] = {}
    for pidx, wmap in hist_raw.items():
        history_by_idx[pidx] = [
            HistoryWindow(window_start=_parse_timestamp(ws), resource_ids=frozenset(rids))
            for ws, rids in sorted(wmap.items())
        ]

    # --- peers ---
    pt = pq.read_table(input_dir / "peers.parquet")
    peers_raw: dict[int, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))
    for pidx, ptype, peer_idx, w in zip(
        pt.column("principal_idx").to_pylist(),
        pt.column("peer_type").to_pylist(),
        pt.column("peer_idx").to_pylist(),
        pt.column("weight").to_pylist(),
    ):
        peers_raw[pidx][ptype][peer_idx] = w

    peers_by_idx: dict[int, PeerFeatures] = {}
    for pidx in all_pidxs:
        pdata = peers_raw.get(pidx, {})
        peers_by_idx[pidx] = PeerFeatures(
            department_peers=pdata.get("department", {}),
            manager_peers=pdata.get("manager", {}),
            group_peers=pdata.get("group", {}),
        )

    # --- collaboration ---
    ct = pq.read_table(input_dir / "collaboration.parquet")
    collab_raw: dict[int, dict[int, float]] = defaultdict(dict)
    for pidx, peer_idx, w in zip(
        ct.column("principal_idx").to_pylist(),
        ct.column("peer_idx").to_pylist(),
        ct.column("weight").to_pylist(),
    ):
        collab_raw[pidx][peer_idx] = w

    # --- assemble contexts ---
    contexts: dict[int, ContextFeatures] = {}
    for pidx in all_pidxs:
        contexts[pidx] = ContextFeatures(
            principal_idx=pidx,
            history=history_by_idx.get(pidx, []),
            peers=peers_by_idx.get(pidx, PeerFeatures({}, {}, {})),
            collaboration=CollaborationFeatures(co_access=dict(collab_raw.get(pidx, {}))),
            static=static_by_idx[pidx],
        )

    # --- resource vocab (reconstruct from actions + history) ---
    all_rids: set[str] = set(actions.keys())
    for hw_list in history_by_idx.values():
        for hw in hw_list:
            all_rids |= hw.resource_ids
    resource_vocab = {rid: i for i, rid in enumerate(sorted(all_rids))}

    return FeatureSet(
        actions=actions,
        contexts=contexts,
        principal_vocab_size=max_pidx + 1 if all_pidxs else 0,
        resource_vocab=resource_vocab,
    )


# ---------------------------------------------------------------------------
# Feature store — compute-or-load caching
# ---------------------------------------------------------------------------


class FeatureStore:
    """Lightweight feature cache backed by Parquet on the local filesystem.

    Computes features from raw events + org structure on first call, then
    persists to Parquet.  Subsequent loads skip the compute step.

    For M0 this is a local directory; in production this would front S3 or
    a similar object store.

    Usage::

        store = FeatureStore(Path("data/features/run-001"))

        # First time: computes and writes
        fs = store.get_or_build(events, principals, teams)

        # Second time (or new process): reads from Parquet
        fs = store.load()
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @property
    def exists(self) -> bool:
        return (self.directory / "static.parquet").exists()

    def get_or_build(
        self,
        events: list[dict[str, Any]],
        principals: list[OrgPrincipal],
        teams: list[OrgTeam],
        *,
        reference_date: datetime | None = None,
    ) -> FeatureSet:
        """Return cached features if available, otherwise compute and persist."""
        if self.exists:
            return self.load()
        fs = build_features(events, principals, teams, reference_date=reference_date)
        self.save(fs)
        return fs

    def save(self, features: FeatureSet) -> dict[str, Path]:
        return write_parquet(features, self.directory)

    def load(self) -> FeatureSet:
        return read_parquet(self.directory)
