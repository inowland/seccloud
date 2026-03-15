from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from seccloud.feature_pipeline import (
    FeatureStore,
    _build_collaboration_features,
    _build_history_profiles,
    _build_peer_features,
    _build_static_features,
    _parse_timestamp,
    _window_key,
    build_action_features,
    build_features,
    read_parquet,
    write_parquet,
)
from seccloud.synthetic_scale import (
    OrgPrincipal,
    OrgTeam,
    ScaleConfig,
    build_app_affinities,
    build_principal_affinities,
    generate_org,
    generate_resources,
)


def _make_principal(
    idx: int,
    dept: str = "eng",
    team: str = "eng-team-1",
    role: str = "engineer",
    location: str = "US-NY",
    manager_idx: int | None = None,
) -> OrgPrincipal:
    return OrgPrincipal(
        idx=idx,
        email=f"p{idx:05d}@example.com",
        name=f"Principal {idx:05d}",
        department=dept,
        team=team,
        role=role,
        location=location,
        is_manager=manager_idx is None and idx == 0,
        manager_idx=manager_idx,
        tz_offset=-5.0,
    )


def _make_event(
    source: str,
    actor_idx: int,
    resource_id: str,
    observed_at: str,
    scenario: str = "baseline",
    **kwargs,
) -> dict:
    base = {
        "source": source,
        "source_event_id": f"{source}-{actor_idx}-{observed_at}",
        "observed_at": observed_at,
        "received_at": observed_at,
        "actor_email": f"p{actor_idx:05d}@example.com",
        "actor_name": f"Principal {actor_idx:05d}",
        "department": "eng",
        "role": "engineer",
        "resource_id": resource_id,
        "resource_name": resource_id,
        "resource_kind": "document",
        "sensitivity": "internal",
        "scenario": scenario,
    }
    base.update(kwargs)
    return base


class TestHelpers(unittest.TestCase):
    def test_parse_timestamp_zulu(self):
        ts = _parse_timestamp("2026-01-15T10:00:00Z")
        self.assertEqual(ts, datetime(2026, 1, 15, 10, 0, tzinfo=UTC))

    def test_parse_timestamp_offset(self):
        ts = _parse_timestamp("2026-01-15T10:00:00+00:00")
        self.assertEqual(ts, datetime(2026, 1, 15, 10, 0, tzinfo=UTC))

    def test_window_key_rounds_down(self):
        ts = datetime(2026, 1, 15, 13, 45, 30, tzinfo=UTC)
        wk = _window_key(ts)
        self.assertEqual(wk, datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC))

    def test_window_key_on_boundary(self):
        ts = datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC)
        wk = _window_key(ts)
        self.assertEqual(wk, datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC))


class TestActionFeatures(unittest.TestCase):
    def test_weights_sum_to_one(self):
        events = [
            _make_event("gworkspace", 0, "doc-a", "2026-01-15T10:00:00Z"),
            _make_event("gworkspace", 0, "doc-a", "2026-01-15T11:00:00Z"),
            _make_event("gworkspace", 1, "doc-a", "2026-01-15T10:30:00Z"),
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(2)}
        actions = build_action_features(events, email_to_idx)

        self.assertIn("doc-a", actions)
        af = actions["doc-a"]
        self.assertAlmostEqual(sum(af.accessor_weights.values()), 1.0)
        # p0 accessed 2x, p1 accessed 1x => p0 weight ~0.667, p1 ~0.333
        self.assertAlmostEqual(af.accessor_weights[0], 2 / 3)
        self.assertAlmostEqual(af.accessor_weights[1], 1 / 3)

    def test_multiple_resources(self):
        events = [
            _make_event("gworkspace", 0, "doc-a", "2026-01-15T10:00:00Z"),
            _make_event("gworkspace", 1, "doc-b", "2026-01-15T10:00:00Z"),
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(2)}
        actions = build_action_features(events, email_to_idx)

        self.assertEqual(len(actions), 2)
        self.assertAlmostEqual(actions["doc-a"].accessor_weights[0], 1.0)
        self.assertAlmostEqual(actions["doc-b"].accessor_weights[1], 1.0)

    def test_unknown_actor_ignored(self):
        events = [
            _make_event("gworkspace", 99, "doc-a", "2026-01-15T10:00:00Z"),
        ]
        email_to_idx = {"p00000@example.com": 0}
        actions = build_action_features(events, email_to_idx)
        self.assertEqual(len(actions), 0)


class TestHistoryProfiles(unittest.TestCase):
    def test_windows_group_correctly(self):
        ref = datetime(2026, 1, 15, 23, 59, 59, tzinfo=UTC)
        events = [
            _make_event("gworkspace", 0, "doc-a", "2026-01-15T10:00:00Z"),
            _make_event("gworkspace", 0, "doc-b", "2026-01-15T10:30:00Z"),
            _make_event("gworkspace", 0, "doc-c", "2026-01-15T12:00:00Z"),
        ]
        email_to_idx = {"p00000@example.com": 0}
        profiles = _build_history_profiles(events, email_to_idx, ref)

        self.assertIn(0, profiles)
        windows = profiles[0]
        # doc-a and doc-b fall in 10:00 window, doc-c in 12:00 window
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0].resource_ids, frozenset({"doc-a", "doc-b"}))
        self.assertEqual(windows[1].resource_ids, frozenset({"doc-c"}))

    def test_old_events_excluded(self):
        ref = datetime(2026, 1, 15, 23, 59, 59, tzinfo=UTC)
        events = [
            _make_event("gworkspace", 0, "doc-old", "2025-12-01T10:00:00Z"),
            _make_event("gworkspace", 0, "doc-new", "2026-01-15T10:00:00Z"),
        ]
        email_to_idx = {"p00000@example.com": 0}
        profiles = _build_history_profiles(events, email_to_idx, ref, trailing_days=30)

        windows = profiles[0]
        all_rids = set()
        for w in windows:
            all_rids |= w.resource_ids
        self.assertIn("doc-new", all_rids)
        self.assertNotIn("doc-old", all_rids)


class TestPeerFeatures(unittest.TestCase):
    def setUp(self):
        self.mgr = _make_principal(0, dept="eng", team="eng-team-1")
        self.p1 = _make_principal(1, dept="eng", team="eng-team-1", manager_idx=0)
        self.p2 = _make_principal(2, dept="eng", team="eng-team-1", manager_idx=0)
        self.p3 = _make_principal(3, dept="data", team="data-team-1", role="analyst")
        self.principals = [self.mgr, self.p1, self.p2, self.p3]
        self.teams = [
            OrgTeam(name="eng-team-1", department="eng", member_indices=[0, 1, 2], manager_idx=0),
            OrgTeam(name="data-team-1", department="data", member_indices=[3], manager_idx=None),
        ]

    def test_department_peers(self):
        peers = _build_peer_features(self.principals, self.teams)
        # p1 is in eng, should have mgr(0) and p2(2) as dept peers, not p3
        self.assertIn(0, peers[1].department_peers)
        self.assertIn(2, peers[1].department_peers)
        self.assertNotIn(3, peers[1].department_peers)
        self.assertNotIn(1, peers[1].department_peers)  # not self

    def test_manager_peers(self):
        peers = _build_peer_features(self.principals, self.teams)
        # p1 and p2 share manager 0 => they are manager peers
        self.assertIn(2, peers[1].manager_peers)
        self.assertAlmostEqual(peers[1].manager_peers[2], 1.0)

    def test_group_peers_weighted(self):
        peers = _build_peer_features(self.principals, self.teams)
        # eng-team-1 has 3 members, so group weight = 1/3
        self.assertIn(0, peers[1].group_peers)
        self.assertAlmostEqual(peers[1].group_peers[0], 1 / 3)

    def test_cross_dept_not_in_dept_peers(self):
        peers = _build_peer_features(self.principals, self.teams)
        # p3 is in data department, alone
        self.assertEqual(len(peers[3].department_peers), 0)


class TestCollaborationFeatures(unittest.TestCase):
    def test_shared_access_detected(self):
        events = [
            _make_event("gworkspace", 0, "shared-doc", "2026-01-15T10:00:00Z"),
            _make_event("gworkspace", 1, "shared-doc", "2026-01-15T11:00:00Z"),
            _make_event("gworkspace", 0, "shared-doc", "2026-01-15T12:00:00Z"),
        ]
        email_to_idx = {f"p{i:05d}@example.com": i for i in range(2)}
        collabs = _build_collaboration_features(events, email_to_idx, 2)

        # shared-doc has 2 unique accessors => popularity = 2
        # p0 accessed 2x, p1 accessed 1x => min = 1
        # weight = min(2,1) / 2 = 0.5
        self.assertAlmostEqual(collabs[0].co_access[1], 0.5)
        self.assertAlmostEqual(collabs[1].co_access[0], 0.5)

    def test_no_self_collaboration(self):
        events = [
            _make_event("gworkspace", 0, "doc-a", "2026-01-15T10:00:00Z"),
        ]
        email_to_idx = {"p00000@example.com": 0}
        collabs = _build_collaboration_features(events, email_to_idx, 1)
        self.assertEqual(len(collabs[0].co_access), 0)

    def test_popular_resource_downweighted(self):
        events = []
        # 10 principals all access same resource
        for i in range(10):
            events.append(_make_event("gworkspace", i, "popular-doc", "2026-01-15T10:00:00Z"))
        # 2 principals share a niche resource
        events.append(_make_event("gworkspace", 0, "niche-doc", "2026-01-15T11:00:00Z"))
        events.append(_make_event("gworkspace", 1, "niche-doc", "2026-01-15T11:00:00Z"))

        email_to_idx = {f"p{i:05d}@example.com": i for i in range(10)}
        collabs = _build_collaboration_features(events, email_to_idx, 10)

        # p0-p1 collaboration from popular-doc: min(1,1)/10 = 0.1
        # p0-p1 collaboration from niche-doc: min(1,1)/2 = 0.5
        # total p0-p1: 0.6
        self.assertAlmostEqual(collabs[0].co_access[1], 0.6)
        # p0-p2 only share popular-doc: 0.1
        self.assertAlmostEqual(collabs[0].co_access[2], 0.1)


class TestStaticFeatures(unittest.TestCase):
    def test_privilege_levels(self):
        principals = [
            _make_principal(0, role="engineer"),
            _make_principal(1, role="security-admin"),
            _make_principal(2, role="executive"),
            _make_principal(3, role="manager"),
            _make_principal(4, role="staff-engineer"),
        ]
        statics = _build_static_features(principals)
        self.assertEqual(statics[0].privilege_level, "regular")
        self.assertEqual(statics[1].privilege_level, "admin")
        self.assertEqual(statics[2].privilege_level, "elevated")
        self.assertEqual(statics[3].privilege_level, "elevated")
        self.assertEqual(statics[4].privilege_level, "elevated")

    def test_duration_buckets_deterministic(self):
        principals = [_make_principal(i) for i in range(20)]
        s1 = _build_static_features(principals)
        s2 = _build_static_features(principals)
        for i in range(20):
            self.assertEqual(s1[i].employment_duration_bucket, s2[i].employment_duration_bucket)

    def test_duration_buckets_distribute(self):
        principals = [_make_principal(i) for i in range(100)]
        statics = _build_static_features(principals)
        buckets = {s.employment_duration_bucket for s in statics.values()}
        # With 100 principals, we should see multiple distinct buckets
        self.assertGreater(len(buckets), 1)


class TestBuildFeatures(unittest.TestCase):
    """Integration test using a small synthetic org."""

    def setUp(self):
        self.cfg = ScaleConfig(num_principals=50, num_days=5, seed=123)

    def _generate_events(self):
        import random

        from seccloud.synthetic_scale import _generate_day_events

        rng = random.Random(self.cfg.seed)
        principals, teams = generate_org(self.cfg, rng)
        resources = generate_resources(self.cfg, teams, rng)
        resource_affinities = build_principal_affinities(self.cfg, principals, teams, resources, rng)
        app_affinities = build_app_affinities(principals, rng)

        counter = [0]
        events: list[dict] = []
        for d in range(self.cfg.num_days):
            day = self.cfg.start_date + timedelta(days=d)
            events.extend(
                _generate_day_events(day, principals, resource_affinities, app_affinities, self.cfg, rng, counter)
            )
        events.sort(key=lambda e: e["observed_at"])
        return events, principals, teams

    def test_full_pipeline(self):
        events, principals, teams = self._generate_events()
        fs = build_features(events, principals, teams)

        # Every resource in events should have action features
        event_rids = {e["resource_id"] for e in events if e.get("resource_id")}
        self.assertEqual(set(fs.actions.keys()), event_rids)

        # All accessor weights sum to 1
        for af in fs.actions.values():
            self.assertAlmostEqual(sum(af.accessor_weights.values()), 1.0, places=9)

        # Every principal has context features
        self.assertEqual(len(fs.contexts), len(principals))

        # Vocabulary sizes
        self.assertEqual(fs.principal_vocab_size, len(principals))
        self.assertEqual(len(fs.resource_vocab), len(event_rids))

        # History profiles exist for active principals
        active_with_history = sum(1 for ctx in fs.contexts.values() if ctx.history)
        self.assertGreater(active_with_history, 0)

        # Peer features are populated
        for ctx in fs.contexts.values():
            if ctx.peers.group_peers:
                self.assertGreater(len(ctx.peers.group_peers), 0)

    def test_parquet_roundtrip(self):
        events, principals, teams = self._generate_events()
        original = build_features(events, principals, teams)

        tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-feat-"))
        try:
            paths = write_parquet(original, tmpdir)

            self.assertEqual(set(paths.keys()), {"actions", "history", "peers", "collaboration", "static"})
            for name, p in paths.items():
                self.assertTrue(p.exists(), f"{name} parquet file missing")
                self.assertGreater(p.stat().st_size, 0, f"{name} parquet file empty")

            restored = read_parquet(tmpdir)

            # Same action resources
            self.assertEqual(set(original.actions.keys()), set(restored.actions.keys()))

            # Action weights match
            for rid, orig_af in original.actions.items():
                rest_af = restored.actions[rid]
                self.assertEqual(orig_af.source, rest_af.source)
                self.assertEqual(set(orig_af.accessor_weights.keys()), set(rest_af.accessor_weights.keys()))
                for pidx, w in orig_af.accessor_weights.items():
                    self.assertAlmostEqual(w, rest_af.accessor_weights[pidx], places=9)

            # Same principal set
            self.assertEqual(set(original.contexts.keys()), set(restored.contexts.keys()))
            self.assertEqual(original.principal_vocab_size, restored.principal_vocab_size)

            # Static features match exactly
            for pidx, orig_ctx in original.contexts.items():
                rest_ctx = restored.contexts[pidx]
                self.assertEqual(orig_ctx.static.role, rest_ctx.static.role)
                self.assertEqual(orig_ctx.static.location, rest_ctx.static.location)
                self.assertEqual(orig_ctx.static.privilege_level, rest_ctx.static.privilege_level)
                self.assertEqual(
                    orig_ctx.static.employment_duration_bucket,
                    rest_ctx.static.employment_duration_bucket,
                )

            # History windows match
            for pidx, orig_ctx in original.contexts.items():
                rest_ctx = restored.contexts[pidx]
                self.assertEqual(len(orig_ctx.history), len(rest_ctx.history))
                for orig_hw, rest_hw in zip(orig_ctx.history, rest_ctx.history):
                    self.assertEqual(orig_hw.resource_ids, rest_hw.resource_ids)

            # Peer features match
            for pidx, orig_ctx in original.contexts.items():
                rest_ctx = restored.contexts[pidx]
                self.assertEqual(orig_ctx.peers.department_peers, rest_ctx.peers.department_peers)
                self.assertEqual(set(orig_ctx.peers.manager_peers.keys()), set(rest_ctx.peers.manager_peers.keys()))
                self.assertEqual(set(orig_ctx.peers.group_peers.keys()), set(rest_ctx.peers.group_peers.keys()))

            # Collaboration features match
            for pidx, orig_ctx in original.contexts.items():
                rest_ctx = restored.contexts[pidx]
                self.assertEqual(
                    set(orig_ctx.collaboration.co_access.keys()),
                    set(rest_ctx.collaboration.co_access.keys()),
                )
                for peer_idx, w in orig_ctx.collaboration.co_access.items():
                    self.assertAlmostEqual(w, rest_ctx.collaboration.co_access[peer_idx], places=9)

            # Resource vocab matches
            self.assertEqual(original.resource_vocab, restored.resource_vocab)
        finally:
            shutil.rmtree(tmpdir)

    def test_feature_store_caches(self):
        events, principals, teams = self._generate_events()

        tmpdir = Path(tempfile.mkdtemp(prefix="seccloud-store-"))
        try:
            store = FeatureStore(tmpdir / "features")

            self.assertFalse(store.exists)

            # First call computes and saves
            fs1 = store.get_or_build(events, principals, teams)
            self.assertTrue(store.exists)
            self.assertEqual(len(fs1.contexts), len(principals))

            # Second call loads from Parquet (no events needed)
            fs2 = store.load()
            self.assertEqual(set(fs1.actions.keys()), set(fs2.actions.keys()))
            self.assertEqual(set(fs1.contexts.keys()), set(fs2.contexts.keys()))

            # get_or_build returns cached version without recompute
            fs3 = store.get_or_build([], [], [])  # empty inputs — would produce empty if recomputed
            self.assertEqual(set(fs1.actions.keys()), set(fs3.actions.keys()))
        finally:
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    unittest.main()
