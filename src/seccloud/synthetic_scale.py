"""Scaled synthetic data generator for M0 ML validation.

Generates 10^6+ events across 10^3+ principals with realistic distributions:
- Zipf role/access patterns
- Business-hours seasonality
- Org-chart structure with team-based resource affinity
- Preferential attachment (principals return to familiar resources)

Output format matches the existing synthetic.py raw event schema so events
flow through the existing normalization pipeline unchanged.
"""

from __future__ import annotations

import bisect
import math
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from seccloud.storage import format_timestamp
from seccloud.synthetic import SyntheticDataset

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

IC_ROLE_WEIGHTS: list[tuple[str, float]] = [
    ("engineer", 30.0),
    ("analyst", 20.0),
    ("pm", 10.0),
    ("designer", 5.0),
    ("senior-engineer", 15.0),
    ("staff-engineer", 5.0),
    ("security-admin", 2.0),
    ("executive", 1.0),
    ("data-engineer", 4.0),
]

DEPARTMENT_NAMES: list[str] = [
    "engineering",
    "data",
    "product",
    "security",
    "finance",
    "sales",
    "marketing",
    "legal",
    "operations",
    "hr",
    "customer-success",
    "infrastructure",
    "research",
    "compliance",
    "support",
]

LOCATIONS: list[str] = [
    "US-NY",
    "US-CA",
    "US-TX",
    "US-WA",
    "US-IL",
    "GB-LDN",
    "DE-BE",
    "IN-KA",
    "CA-ON",
    "AU-NSW",
]

LOCATION_TZ_OFFSETS: dict[str, float] = {
    "US-NY": -5.0,
    "US-CA": -8.0,
    "US-TX": -6.0,
    "US-WA": -8.0,
    "US-IL": -6.0,
    "GB-LDN": 0.0,
    "DE-BE": 1.0,
    "IN-KA": 5.5,
    "CA-ON": -5.0,
    "AU-NSW": 11.0,
}

APP_CATALOG: list[dict[str, str]] = [
    {"id": "okta:admin-console", "name": "Admin Console", "sensitivity": "high"},
    {"id": "okta:salesforce", "name": "Salesforce", "sensitivity": "internal"},
    {"id": "okta:jira", "name": "Jira", "sensitivity": "internal"},
    {"id": "okta:confluence", "name": "Confluence", "sensitivity": "internal"},
    {"id": "okta:slack", "name": "Slack", "sensitivity": "internal"},
    {"id": "okta:aws-console", "name": "AWS Console", "sensitivity": "high"},
    {"id": "okta:datadog", "name": "Datadog", "sensitivity": "internal"},
    {"id": "okta:pagerduty", "name": "PagerDuty", "sensitivity": "internal"},
    {"id": "okta:workday", "name": "Workday", "sensitivity": "high"},
    {"id": "okta:greenhouse", "name": "Greenhouse", "sensitivity": "high"},
    {"id": "okta:zoom", "name": "Zoom", "sensitivity": "low"},
    {"id": "okta:google-workspace", "name": "Google Workspace", "sensitivity": "internal"},
    {"id": "okta:github", "name": "GitHub", "sensitivity": "internal"},
    {"id": "okta:snowflake", "name": "Snowflake", "sensitivity": "high"},
    {"id": "okta:tableau", "name": "Tableau", "sensitivity": "internal"},
    {"id": "okta:netsuite", "name": "NetSuite", "sensitivity": "high"},
    {"id": "okta:zendesk", "name": "Zendesk", "sensitivity": "internal"},
    {"id": "okta:figma", "name": "Figma", "sensitivity": "internal"},
    {"id": "okta:notion", "name": "Notion", "sensitivity": "internal"},
    {"id": "okta:1password", "name": "1Password", "sensitivity": "high"},
]

WAREHOUSE_NAMES: list[str] = [
    "analytics_wh",
    "etl_wh",
    "reporting_wh",
    "finance_wh",
    "data_science_wh",
    "product_wh",
]

SENSITIVITY_LEVELS: list[str] = ["low", "internal", "high", "critical"]
SENSITIVITY_WEIGHTS: list[float] = [0.1, 0.6, 0.25, 0.05]


@dataclass(slots=True)
class ScaleConfig:
    num_principals: int = 1000
    num_days: int = 30
    start_date: datetime = field(default_factory=lambda: datetime(2026, 1, 1, 0, 0, tzinfo=UTC))
    seed: int = 42

    num_departments: int = 8
    team_size_min: int = 5
    team_size_max: int = 15
    manager_fan_out_min: int = 5
    manager_fan_out_max: int = 8
    num_locations: int = 5

    docs_per_principal: float = 20.0
    repos_per_team: float = 3.0
    tables_per_team: float = 20.0

    events_per_day_median: float = 20.0
    events_per_day_sigma: float = 0.8

    within_team_pct: float = 0.80
    within_dept_pct: float = 0.15

    source_weights: dict[str, float] = field(
        default_factory=lambda: {
            "okta": 0.25,
            "gworkspace": 0.30,
            "github": 0.20,
            "snowflake": 0.25,
        }
    )


# ---------------------------------------------------------------------------
# Weighted sampler (avoids numpy dependency)
# ---------------------------------------------------------------------------


class WeightedSampler:
    """Alias-method-like sampler using cumulative weights for O(log n) sampling."""

    __slots__ = ("items", "cumulative", "total")

    def __init__(self, items: list[Any], weights: list[float]) -> None:
        self.items = items
        self.cumulative: list[float] = []
        running = 0.0
        for w in weights:
            running += w
            self.cumulative.append(running)
        self.total = running

    def sample(self, rng: random.Random) -> Any:
        r = rng.random() * self.total
        idx = bisect.bisect_right(self.cumulative, r)
        return self.items[min(idx, len(self.items) - 1)]

    def sample_n(self, rng: random.Random, n: int) -> list[Any]:
        return [self.sample(rng) for _ in range(n)]


def zipf_weights(n: int, s: float = 1.0) -> list[float]:
    """Return Zipf weights for ranks 1..n with exponent s."""
    return [1.0 / (k ** s) for k in range(1, n + 1)]


# ---------------------------------------------------------------------------
# Organization generator
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class OrgPrincipal:
    idx: int
    email: str
    name: str
    department: str
    team: str
    role: str
    location: str
    is_manager: bool
    manager_idx: int | None
    tz_offset: float


@dataclass(slots=True)
class OrgTeam:
    name: str
    department: str
    member_indices: list[int]
    manager_idx: int | None


def generate_org(cfg: ScaleConfig, rng: random.Random) -> tuple[list[OrgPrincipal], list[OrgTeam]]:
    departments = DEPARTMENT_NAMES[: cfg.num_departments]
    locations = LOCATIONS[: cfg.num_locations]
    location_sampler = WeightedSampler(locations, zipf_weights(len(locations), s=0.8))

    role_names = [r for r, _ in IC_ROLE_WEIGHTS]
    role_wts = [w for _, w in IC_ROLE_WEIGHTS]
    role_sampler = WeightedSampler(role_names, role_wts)

    principals: list[OrgPrincipal] = []
    teams: list[OrgTeam] = []
    idx = 0

    target_per_dept = cfg.num_principals // len(departments)
    remainder = cfg.num_principals - target_per_dept * len(departments)

    for dept_i, dept_name in enumerate(departments):
        dept_count = target_per_dept + (1 if dept_i < remainder else 0)
        if dept_count == 0:
            continue

        team_num = 0
        assigned = 0

        while assigned < dept_count:
            team_size = rng.randint(cfg.team_size_min, cfg.team_size_max)
            team_size = min(team_size, dept_count - assigned)
            if team_size == 0:
                break
            team_num += 1
            team_name = f"{dept_name}-team-{team_num}"
            team_member_indices: list[int] = []

            manager_role = "manager"
            manager_loc = location_sampler.sample(rng)
            manager = OrgPrincipal(
                idx=idx,
                email=f"p{idx:05d}@example.com",
                name=f"Principal {idx:05d}",
                department=dept_name,
                team=team_name,
                role=manager_role,
                location=manager_loc,
                is_manager=True,
                manager_idx=None,
                tz_offset=LOCATION_TZ_OFFSETS.get(manager_loc, 0.0),
            )
            principals.append(manager)
            team_member_indices.append(idx)
            idx += 1
            assigned += 1

            for _ in range(team_size - 1):
                if assigned >= dept_count:
                    break
                loc = location_sampler.sample(rng)
                p = OrgPrincipal(
                    idx=idx,
                    email=f"p{idx:05d}@example.com",
                    name=f"Principal {idx:05d}",
                    department=dept_name,
                    team=team_name,
                    role=role_sampler.sample(rng),
                    location=loc,
                    is_manager=False,
                    manager_idx=manager.idx,
                    tz_offset=LOCATION_TZ_OFFSETS.get(loc, 0.0),
                )
                principals.append(p)
                team_member_indices.append(idx)
                idx += 1
                assigned += 1

            teams.append(OrgTeam(
                name=team_name,
                department=dept_name,
                member_indices=team_member_indices,
                manager_idx=manager.idx,
            ))

    return principals, teams


# ---------------------------------------------------------------------------
# Resource universe generator
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ResourceDef:
    resource_id: str
    name: str
    kind: str
    source: str
    sensitivity: str
    owning_team: str
    owning_department: str


def generate_resources(
    cfg: ScaleConfig,
    teams: list[OrgTeam],
    rng: random.Random,
) -> dict[str, list[ResourceDef]]:
    sensitivity_sampler = WeightedSampler(SENSITIVITY_LEVELS, SENSITIVITY_WEIGHTS)

    resources: dict[str, list[ResourceDef]] = {
        "gworkspace": [],
        "github": [],
        "snowflake": [],
    }

    for team in teams:
        n_docs = max(1, int(len(team.member_indices) * cfg.docs_per_principal))
        for j in range(n_docs):
            doc_id = f"gworkspace:doc/{team.name}-doc-{j:04d}"
            resources["gworkspace"].append(ResourceDef(
                resource_id=doc_id,
                name=f"{team.name} Doc {j}",
                kind="document",
                source="gworkspace",
                sensitivity=sensitivity_sampler.sample(rng),
                owning_team=team.name,
                owning_department=team.department,
            ))

        n_repos = max(1, int(cfg.repos_per_team))
        for j in range(n_repos):
            repo_name = f"{team.name}-repo-{j}"
            resources["github"].append(ResourceDef(
                resource_id=f"github:repo/{repo_name}",
                name=repo_name,
                kind="repo",
                source="github",
                sensitivity=sensitivity_sampler.sample(rng),
                owning_team=team.name,
                owning_department=team.department,
            ))

        n_tables = max(1, int(cfg.tables_per_team))
        for j in range(n_tables):
            table_name = f"{team.name.replace('-', '_')}_table_{j}"
            resources["snowflake"].append(ResourceDef(
                resource_id=f"snowflake:dataset/{table_name}",
                name=table_name,
                kind="dataset",
                source="snowflake",
                sensitivity=sensitivity_sampler.sample(rng),
                owning_team=team.name,
                owning_department=team.department,
            ))

    return resources


# ---------------------------------------------------------------------------
# Per-principal resource affinity (preferential attachment base)
# ---------------------------------------------------------------------------


def _build_resource_index(
    teams: list[OrgTeam],
    resources: dict[str, list[ResourceDef]],
) -> tuple[dict[str, list[ResourceDef]], dict[str, list[ResourceDef]]]:
    """Index resources by team name and department name."""
    by_team: dict[str, list[ResourceDef]] = {}
    by_dept: dict[str, list[ResourceDef]] = {}
    for source_resources in resources.values():
        for r in source_resources:
            by_team.setdefault(r.owning_team, []).append(r)
            by_dept.setdefault(r.owning_department, []).append(r)
    return by_team, by_dept


@dataclass(slots=True)
class PrincipalAffinities:
    """Per-source weighted resource samplers for a single principal."""
    by_source: dict[str, WeightedSampler]


def build_principal_affinities(
    cfg: ScaleConfig,
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
) -> list[PrincipalAffinities]:
    """Build per-source weighted resource samplers per principal.

    80% weight on own-team resources, 15% on same-department (other teams),
    5% on cross-department. Within each tier, weights follow Zipf (some
    resources are accessed much more than others).
    """
    by_team, by_dept = _build_resource_index(teams, resources)
    all_resources: list[ResourceDef] = []
    for source_resources in resources.values():
        all_resources.extend(source_resources)

    # Pre-group cross-department resources by source for faster lookup
    cross_dept_by_source: dict[str, dict[str, list[ResourceDef]]] = {}
    for dept_name in {t.department for t in teams}:
        per_source: dict[str, list[ResourceDef]] = {}
        for r in all_resources:
            if r.owning_department != dept_name:
                per_source.setdefault(r.source, []).append(r)
        cross_dept_by_source[dept_name] = per_source

    affinities: list[PrincipalAffinities] = []
    resource_sources = list(resources.keys())

    for p in principals:
        team_name = p.team
        dept_name = p.department

        own_team = by_team.get(team_name, [])
        same_dept_other = [
            r for r in by_dept.get(dept_name, []) if r.owning_team != team_name
        ]

        # Collect all candidate resources with weights, then split by source
        items: list[ResourceDef] = []
        weights: list[float] = []

        if own_team:
            own_shuffled = list(own_team)
            rng.shuffle(own_shuffled)
            own_zipf = zipf_weights(len(own_shuffled), s=1.0)
            total_own = sum(own_zipf)
            for r, w in zip(own_shuffled, own_zipf):
                items.append(r)
                weights.append(cfg.within_team_pct * w / total_own)

        if same_dept_other:
            dept_shuffled = list(same_dept_other)
            rng.shuffle(dept_shuffled)
            dept_zipf = zipf_weights(len(dept_shuffled), s=1.2)
            total_dept = sum(dept_zipf)
            for r, w in zip(dept_shuffled, dept_zipf):
                items.append(r)
                weights.append(cfg.within_dept_pct * w / total_dept)

        cross_pct = 1.0 - cfg.within_team_pct - cfg.within_dept_pct
        cross_by_src = cross_dept_by_source.get(dept_name, {})
        for src in resource_sources:
            cross_for_src = cross_by_src.get(src, [])
            if cross_for_src:
                n_cross = min(len(cross_for_src), 20)
                cross_sample = rng.sample(cross_for_src, n_cross)
                cross_zipf = zipf_weights(n_cross, s=1.5)
                total_cross = sum(cross_zipf)
                src_share = cross_pct / len(resource_sources)
                for r, w in zip(cross_sample, cross_zipf):
                    items.append(r)
                    weights.append(src_share * w / total_cross)

        # Split into per-source samplers
        source_items: dict[str, list[ResourceDef]] = {}
        source_weights: dict[str, list[float]] = {}
        for r, w in zip(items, weights):
            source_items.setdefault(r.source, []).append(r)
            source_weights.setdefault(r.source, []).append(w)

        by_source: dict[str, WeightedSampler] = {}
        for src in resource_sources:
            si = source_items.get(src, [])
            sw = source_weights.get(src, [])
            if si:
                by_source[src] = WeightedSampler(si, sw)
            else:
                # Fallback to global resources for this source
                fallback = resources[src][:1]
                if fallback:
                    by_source[src] = WeightedSampler(fallback, [1.0])

        affinities.append(PrincipalAffinities(by_source=by_source))

    return affinities


# ---------------------------------------------------------------------------
# App affinity per principal (for Okta login events)
# ---------------------------------------------------------------------------

ROLE_APP_AFFINITIES: dict[str, list[str]] = {
    "engineer": [
        "okta:jira", "okta:github", "okta:slack", "okta:confluence",
        "okta:aws-console", "okta:datadog",
    ],
    "senior-engineer": [
        "okta:jira", "okta:github", "okta:slack", "okta:confluence",
        "okta:aws-console", "okta:datadog", "okta:pagerduty",
    ],
    "staff-engineer": [
        "okta:jira", "okta:github", "okta:slack", "okta:confluence",
        "okta:aws-console", "okta:datadog", "okta:pagerduty",
    ],
    "analyst": [
        "okta:jira", "okta:slack", "okta:confluence", "okta:snowflake", "okta:tableau",
    ],
    "data-engineer": [
        "okta:jira", "okta:github", "okta:slack", "okta:snowflake",
        "okta:aws-console", "okta:datadog",
    ],
    "pm": ["okta:jira", "okta:slack", "okta:confluence", "okta:figma", "okta:notion"],
    "designer": ["okta:jira", "okta:slack", "okta:figma", "okta:notion"],
    "manager": [
        "okta:jira", "okta:slack", "okta:confluence", "okta:workday", "okta:greenhouse",
    ],
    "security-admin": [
        "okta:admin-console", "okta:jira", "okta:slack",
        "okta:aws-console", "okta:1password", "okta:datadog",
    ],
    "executive": [
        "okta:slack", "okta:confluence", "okta:workday",
        "okta:netsuite", "okta:salesforce", "okta:zoom",
    ],
}

APP_BY_ID: dict[str, dict[str, str]] = {app["id"]: app for app in APP_CATALOG}


def build_app_affinities(
    principals: list[OrgPrincipal],
    rng: random.Random,
) -> list[WeightedSampler]:
    """Build per-principal app sampler for Okta login events."""
    common_apps = ["okta:slack", "okta:google-workspace", "okta:zoom"]
    affinities: list[WeightedSampler] = []
    for p in principals:
        role_apps = ROLE_APP_AFFINITIES.get(p.role, common_apps)
        all_apps = list(dict.fromkeys(role_apps + common_apps))
        wts = zipf_weights(len(all_apps), s=0.8)
        affinities.append(WeightedSampler(all_apps, wts))
    return affinities


# ---------------------------------------------------------------------------
# Activity generator
# ---------------------------------------------------------------------------

GITHUB_EVENT_TYPES: list[str] = ["view", "view", "view", "clone", "archive_download"]
GITHUB_EVENT_WEIGHTS: list[float] = [5.0, 5.0, 5.0, 1.0, 0.3]

SNOWFLAKE_EVENT_TYPES: list[str] = ["query", "export"]
SNOWFLAKE_EVENT_WEIGHTS: list[float] = [9.0, 1.0]

WEEKDAY_WEIGHTS: list[float] = [1.0, 1.0, 1.0, 1.0, 1.0, 0.08, 0.04]


def _business_hour(rng: random.Random, tz_offset: float) -> float:
    """Sample a time-of-day in hours (0-24) centered on business hours.

    Uses a truncated normal centered at 12:00 local time, std 2.5h.
    Returns UTC hour as a float.
    """
    while True:
        local_hour = rng.gauss(12.0, 2.5)
        if 6.0 <= local_hour <= 22.0:
            utc_hour = (local_hour - tz_offset) % 24.0
            return utc_hour


def _generate_day_events(
    day: datetime,
    principals: list[OrgPrincipal],
    resource_affinities: list[PrincipalAffinities],
    app_affinities: list[WeightedSampler],
    cfg: ScaleConfig,
    rng: random.Random,
    counter: list[int],
) -> list[dict[str, Any]]:
    """Generate all events for a single day."""
    weekday = day.weekday()
    day_weight = WEEKDAY_WEIGHTS[weekday]

    source_names = list(cfg.source_weights.keys())
    source_wts = [cfg.source_weights[s] for s in source_names]
    source_sampler = WeightedSampler(source_names, source_wts)

    github_sampler = WeightedSampler(GITHUB_EVENT_TYPES, GITHUB_EVENT_WEIGHTS)
    snowflake_sampler = WeightedSampler(SNOWFLAKE_EVENT_TYPES, SNOWFLAKE_EVENT_WEIGHTS)
    warehouse_sampler = WeightedSampler(WAREHOUSE_NAMES, zipf_weights(len(WAREHOUSE_NAMES)))

    events: list[dict[str, Any]] = []

    for p in principals:
        base_count = rng.lognormvariate(
            math.log(cfg.events_per_day_median), cfg.events_per_day_sigma
        )
        day_count = max(0, int(base_count * day_weight + 0.5))
        if day_count == 0:
            continue

        aff = resource_affinities[p.idx]
        app_aff = app_affinities[p.idx]

        for _ in range(day_count):
            source = source_sampler.sample(rng)

            # For non-okta sources, check if this principal has resources for this source
            if source != "okta" and source not in aff.by_source:
                source = "okta"  # fallback to login event

            utc_hour = _business_hour(rng, p.tz_offset)
            hour_int = int(utc_hour)
            minute = int((utc_hour - hour_int) * 60)
            second = rng.randint(0, 59)
            observed_at = day.replace(hour=hour_int % 24, minute=minute, second=second)

            counter[0] += 1
            event_id = f"{source}-{counter[0]:08d}"

            payload: dict[str, Any] = {
                "source": source,
                "source_event_id": event_id,
                "observed_at": format_timestamp(observed_at),
                "received_at": format_timestamp(observed_at + timedelta(minutes=rng.randint(1, 5))),
                "actor_email": p.email,
                "actor_name": p.name,
                "department": p.department,
                "role": p.role,
                "scenario": "baseline",
            }

            if source == "okta":
                app_id = app_aff.sample(rng)
                app_info = APP_BY_ID.get(app_id, {"name": "Unknown", "sensitivity": "internal"})
                payload.update({
                    "event_type": "login",
                    "resource_id": app_id,
                    "resource_name": app_info["name"],
                    "resource_kind": "app",
                    "sensitivity": app_info["sensitivity"],
                    "geo": p.location,
                    "ip": f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
                    "privileged": p.role in ("security-admin", "executive", "manager"),
                })
            elif source == "gworkspace":
                res = aff.by_source["gworkspace"].sample(rng)
                payload.update({
                    "event_type": "view",
                    "resource_id": res.resource_id,
                    "resource_name": res.name,
                    "resource_kind": "document",
                    "sensitivity": res.sensitivity,
                    "external": False,
                })
            elif source == "github":
                res = aff.by_source["github"].sample(rng)
                event_type = github_sampler.sample(rng)
                bytes_mb = rng.randint(1, 20) if event_type == "view" else rng.randint(50, 500)
                payload.update({
                    "event_type": event_type,
                    "resource_id": res.resource_id,
                    "resource_name": res.name,
                    "resource_kind": "repo",
                    "sensitivity": res.sensitivity,
                    "bytes_transferred_mb": bytes_mb,
                })
            elif source == "snowflake":
                res = aff.by_source["snowflake"].sample(rng)
                event_type = snowflake_sampler.sample(rng)
                rows = rng.randint(10, 5000) if event_type == "query" else rng.randint(1000, 100000)
                payload.update({
                    "event_type": event_type,
                    "resource_id": res.resource_id,
                    "resource_name": res.name,
                    "resource_kind": "dataset",
                    "sensitivity": res.sensitivity,
                    "rows_read": rows,
                    "warehouse": warehouse_sampler.sample(rng),
                })

            events.append(payload)

    return events


# ---------------------------------------------------------------------------
# Top-level generator
# ---------------------------------------------------------------------------


def generate_scaled_dataset(cfg: ScaleConfig | None = None) -> SyntheticDataset:
    """Generate a large-scale synthetic dataset.

    Returns a SyntheticDataset with raw_events matching the existing schema
    and expectations set to empty (scenarios not yet injected).
    """
    if cfg is None:
        cfg = ScaleConfig()

    rng = random.Random(cfg.seed)

    principals, teams = generate_org(cfg, rng)
    resources = generate_resources(cfg, teams, rng)
    resource_affinities = build_principal_affinities(cfg, principals, teams, resources, rng)
    app_affinities = build_app_affinities(principals, rng)

    counter = [0]
    all_events: list[dict[str, Any]] = []

    for day_offset in range(cfg.num_days):
        day = cfg.start_date + timedelta(days=day_offset)
        day_events = _generate_day_events(
            day, principals, resource_affinities, app_affinities, cfg, rng, counter,
        )
        all_events.extend(day_events)

    all_events.sort(key=lambda e: e["observed_at"])

    return SyntheticDataset(
        raw_events=all_events,
        expectations={},
    )


def generate_scaled_org_summary(cfg: ScaleConfig | None = None) -> dict[str, Any]:
    """Generate just the org structure for inspection (no events)."""
    if cfg is None:
        cfg = ScaleConfig()
    rng = random.Random(cfg.seed)
    principals, teams = generate_org(cfg, rng)
    resources = generate_resources(cfg, teams, rng)

    dept_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    location_counts: dict[str, int] = {}
    for p in principals:
        dept_counts[p.department] = dept_counts.get(p.department, 0) + 1
        role_counts[p.role] = role_counts.get(p.role, 0) + 1
        location_counts[p.location] = location_counts.get(p.location, 0) + 1

    resource_counts = {source: len(res) for source, res in resources.items()}

    return {
        "num_principals": len(principals),
        "num_teams": len(teams),
        "num_departments": len(dept_counts),
        "departments": dict(sorted(dept_counts.items(), key=lambda x: -x[1])),
        "roles": dict(sorted(role_counts.items(), key=lambda x: -x[1])),
        "locations": dict(sorted(location_counts.items(), key=lambda x: -x[1])),
        "resources": resource_counts,
        "total_resources": sum(resource_counts.values()),
    }
