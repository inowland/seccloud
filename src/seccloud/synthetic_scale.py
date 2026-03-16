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

    inject_scenarios: bool = True

    enable_drift: bool = False
    departure_rate_per_month: float = 0.015
    hire_rate_per_month: float = 0.02
    role_change_rate_per_quarter: float = 0.03
    new_docs_per_month_per_team: float = 3.0
    new_repos_per_quarter_per_team: float = 0.3
    new_tables_per_quarter_per_team: float = 1.0
    quarter_end_spike: float = 1.4
    holiday_dip: float = 0.5


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
    day_multiplier: float = 1.0,
) -> list[dict[str, Any]]:
    """Generate all events for a single day."""
    weekday = day.weekday()
    day_weight = WEEKDAY_WEIGHTS[weekday] * day_multiplier

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
                is_share = rng.random() < 0.05
                payload.update({
                    "event_type": "share_external" if is_share else "view",
                    "resource_id": res.resource_id,
                    "resource_name": res.name,
                    "resource_kind": "document",
                    "sensitivity": res.sensitivity,
                    "external": is_share,
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
# Scenario injector — plant known-bad patterns for ground truth evaluation
# ---------------------------------------------------------------------------

SCENARIO_NAMES: list[str] = [
    "slow_exfiltration",
    "credential_compromise",
    "privilege_escalation",
    "departing_employee",
    "account_takeover",
    "insider_collaboration",
]


@dataclass(slots=True)
class ScenarioSpec:
    """Metadata for an injected attack scenario."""

    name: str
    principal_indices: list[int]
    num_events: int


def _make_scenario_event(
    source: str,
    principal: OrgPrincipal,
    observed_at: datetime,
    scenario: str,
    counter: list[int],
    rng: random.Random,
    **fields: Any,
) -> dict[str, Any]:
    """Build an event dict matching the existing schema."""
    counter[0] += 1
    payload: dict[str, Any] = {
        "source": source,
        "source_event_id": f"{source}-scen-{counter[0]:08d}",
        "observed_at": format_timestamp(observed_at),
        "received_at": format_timestamp(
            observed_at + timedelta(minutes=rng.randint(1, 5))
        ),
        "actor_email": principal.email,
        "actor_name": principal.name,
        "department": principal.department,
        "role": principal.role,
        "scenario": scenario,
    }
    payload.update(fields)
    return payload


def _random_ts(day: datetime, rng: random.Random, tz_offset: float) -> datetime:
    """Generate a business-hours timestamp for the given day."""
    utc_hour = _business_hour(rng, tz_offset)
    h = int(utc_hour)
    m = int((utc_hour - h) * 60)
    return day.replace(hour=h % 24, minute=m, second=rng.randint(0, 59))


def _off_hours_ts(day: datetime, rng: random.Random, tz_offset: float) -> datetime:
    """Generate a timestamp outside business hours in local time."""
    if rng.random() < 0.5:
        local_hour = rng.uniform(0.5, 5.5)
    else:
        local_hour = rng.uniform(22.5, 23.9)
    utc_hour = (local_hour - tz_offset) % 24.0
    h = int(utc_hour)
    m = int((utc_hour - h) * 60)
    return day.replace(hour=h % 24, minute=m, second=rng.randint(0, 59))


def _cross_dept_resources(
    resources: dict[str, list[ResourceDef]],
    dept: str,
    source: str | None = None,
    min_sensitivity: str | None = None,
) -> list[ResourceDef]:
    """Get resources owned by other departments.

    If min_sensitivity is set, only returns resources at or above that level.
    """
    sens_order = {s: i for i, s in enumerate(SENSITIVITY_LEVELS)}
    min_idx = sens_order.get(min_sensitivity, 0) if min_sensitivity else 0
    result: list[ResourceDef] = []
    sources = [source] if source else list(resources.keys())
    for src in sources:
        for r in resources.get(src, []):
            if r.owning_department != dept and sens_order.get(r.sensitivity, 0) >= min_idx:
                result.append(r)
    return result


def _inject_slow_exfiltration(
    cfg: ScaleConfig,
    principal: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Gradual increase in cross-department data access over weeks.

    Signal: volume ramp on sensitive Snowflake tables + shift from query to export.
    """
    scenario = "slow_exfiltration"
    events: list[dict[str, Any]] = []

    targets = _cross_dept_resources(resources, principal.department, "snowflake", "high")
    if not targets:
        targets = _cross_dept_resources(resources, principal.department, "snowflake")
    if not targets:
        targets = resources["snowflake"][:10]

    # Ramp over last 2/3 of the simulation
    start_day = cfg.num_days // 3
    total_ramp_days = cfg.num_days - start_day

    for d in range(start_day, cfg.num_days):
        day = cfg.start_date + timedelta(days=d)
        if day.weekday() >= 5:
            continue

        progress = (d - start_day) / max(1, total_ramp_days - 1)
        n = int(3 + 12 * progress)

        for _ in range(n):
            res = rng.choice(targets)
            ts = _random_ts(day, rng, principal.tz_offset)
            event_type = "export" if rng.random() < 0.15 + 0.45 * progress else "query"
            rows = (
                rng.randint(5000, 200000)
                if event_type == "export"
                else rng.randint(1000, 50000)
            )
            events.append(_make_scenario_event(
                "snowflake", principal, ts, scenario, counter, rng,
                event_type=event_type,
                resource_id=res.resource_id,
                resource_name=res.name,
                resource_kind="dataset",
                sensitivity=res.sensitivity,
                rows_read=rows,
                warehouse=rng.choice(WAREHOUSE_NAMES),
            ))

    return events, ScenarioSpec(scenario, [principal.idx], len(events))


def _inject_credential_compromise(
    cfg: ScaleConfig,
    principal: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Sudden geo shift with access to unfamiliar resources.

    Signal: login from distant location + cross-department resource access.
    """
    scenario = "credential_compromise"
    events: list[dict[str, Any]] = []

    # Pick a location far from the principal's actual location
    other_locations = [loc for loc in LOCATIONS if loc != principal.location]
    other_locations.sort(
        key=lambda loc: abs(LOCATION_TZ_OFFSETS.get(loc, 0) - principal.tz_offset),
        reverse=True,
    )
    new_geo = other_locations[0]
    new_tz = LOCATION_TZ_OFFSETS[new_geo]

    cross_resources = _cross_dept_resources(resources, principal.department)
    if not cross_resources:
        cross_resources = resources["gworkspace"][:20] + resources["snowflake"][:20]

    # Active for ~5 days starting around day 18
    start_day = min(18, cfg.num_days - 6)
    for d in range(start_day, min(start_day + 5, cfg.num_days)):
        day = cfg.start_date + timedelta(days=d)

        # Okta logins from new geo
        for _ in range(rng.randint(3, 6)):
            ts = _random_ts(day, rng, new_tz)
            app_id = rng.choice([
                "okta:aws-console", "okta:admin-console", "okta:snowflake",
                "okta:github", "okta:google-workspace",
            ])
            app_info = APP_BY_ID.get(
                app_id, {"name": "Unknown", "sensitivity": "internal"}
            )
            events.append(_make_scenario_event(
                "okta", principal, ts, scenario, counter, rng,
                event_type="login",
                resource_id=app_id,
                resource_name=app_info["name"],
                resource_kind="app",
                sensitivity=app_info["sensitivity"],
                geo=new_geo,
                ip=f"185.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
                privileged=False,
            ))

        # Access unfamiliar resources
        for _ in range(rng.randint(4, 8)):
            res = rng.choice(cross_resources)
            ts = _random_ts(day, rng, new_tz)
            if res.source == "gworkspace":
                events.append(_make_scenario_event(
                    "gworkspace", principal, ts, scenario, counter, rng,
                    event_type="view",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="document",
                    sensitivity=res.sensitivity,
                    external=False,
                ))
            elif res.source == "snowflake":
                events.append(_make_scenario_event(
                    "snowflake", principal, ts, scenario, counter, rng,
                    event_type="query",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="dataset",
                    sensitivity=res.sensitivity,
                    rows_read=rng.randint(100, 10000),
                    warehouse=rng.choice(WAREHOUSE_NAMES),
                ))
            elif res.source == "github":
                events.append(_make_scenario_event(
                    "github", principal, ts, scenario, counter, rng,
                    event_type="clone",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="repo",
                    sensitivity=res.sensitivity,
                    bytes_transferred_mb=rng.randint(50, 300),
                ))

    return events, ScenarioSpec(scenario, [principal.idx], len(events))


def _inject_privilege_escalation(
    cfg: ScaleConfig,
    principal: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Non-admin accessing admin resources without role change.

    Signal: admin app access (admin-console, aws-console) + critical data,
    role field unchanged.
    """
    scenario = "privilege_escalation"
    events: list[dict[str, Any]] = []

    admin_apps = [
        "okta:admin-console", "okta:aws-console", "okta:workday", "okta:1password",
    ]
    critical_data = _cross_dept_resources(
        resources, principal.department, "snowflake", "critical",
    )
    if not critical_data:
        critical_data = _cross_dept_resources(
            resources, principal.department, "snowflake", "high",
        )
    if not critical_data:
        critical_data = resources["snowflake"][:5]

    start_day = min(15, cfg.num_days - 10)
    for d in range(start_day, min(start_day + 10, cfg.num_days)):
        day = cfg.start_date + timedelta(days=d)
        if day.weekday() >= 5:
            continue

        # Admin app logins
        for _ in range(rng.randint(1, 3)):
            app_id = rng.choice(admin_apps)
            app_info = APP_BY_ID[app_id]
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(_make_scenario_event(
                "okta", principal, ts, scenario, counter, rng,
                event_type="login",
                resource_id=app_id,
                resource_name=app_info["name"],
                resource_kind="app",
                sensitivity=app_info["sensitivity"],
                geo=principal.location,
                ip=f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
                privileged=False,  # role hasn't changed — this is the anomaly
            ))

        # Access critical/high-sensitivity data
        for _ in range(rng.randint(2, 4)):
            res = rng.choice(critical_data)
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(_make_scenario_event(
                "snowflake", principal, ts, scenario, counter, rng,
                event_type="query",
                resource_id=res.resource_id,
                resource_name=res.name,
                resource_kind="dataset",
                sensitivity=res.sensitivity,
                rows_read=rng.randint(100, 5000),
                warehouse=rng.choice(WAREHOUSE_NAMES),
            ))

    return events, ScenarioSpec(scenario, [principal.idx], len(events))


def _inject_departing_employee(
    cfg: ScaleConfig,
    principal: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Broad access sweep in final days — mass downloads across departments.

    Signal: breadth explosion across teams/sources + volume spike + export-heavy.
    """
    scenario = "departing_employee"
    events: list[dict[str, Any]] = []

    all_docs = resources["gworkspace"]
    all_repos = resources["github"]
    all_tables = resources["snowflake"]

    # Last 3 days
    start_day = max(0, cfg.num_days - 3)
    for d in range(start_day, cfg.num_days):
        day = cfg.start_date + timedelta(days=d)

        # Mass doc views across many teams
        sampled_docs = rng.sample(all_docs, min(30, len(all_docs)))
        for doc in sampled_docs:
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(_make_scenario_event(
                "gworkspace", principal, ts, scenario, counter, rng,
                event_type="view",
                resource_id=doc.resource_id,
                resource_name=doc.name,
                resource_kind="document",
                sensitivity=doc.sensitivity,
                external=False,
            ))

        # Clone repos from many teams
        sampled_repos = rng.sample(all_repos, min(8, len(all_repos)))
        for repo in sampled_repos:
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(_make_scenario_event(
                "github", principal, ts, scenario, counter, rng,
                event_type="clone",
                resource_id=repo.resource_id,
                resource_name=repo.name,
                resource_kind="repo",
                sensitivity=repo.sensitivity,
                bytes_transferred_mb=rng.randint(100, 800),
            ))

        # Large data exports
        sampled_tables = rng.sample(all_tables, min(10, len(all_tables)))
        for table in sampled_tables:
            ts = _random_ts(day, rng, principal.tz_offset)
            events.append(_make_scenario_event(
                "snowflake", principal, ts, scenario, counter, rng,
                event_type="export",
                resource_id=table.resource_id,
                resource_name=table.name,
                resource_kind="dataset",
                sensitivity=table.sensitivity,
                rows_read=rng.randint(50000, 500000),
                warehouse=rng.choice(WAREHOUSE_NAMES),
            ))

    return events, ScenarioSpec(scenario, [principal.idx], len(events))


def _inject_account_takeover(
    cfg: ScaleConfig,
    principal: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Access pattern shift — off-hours activity on unfamiliar resources.

    Signal: temporal shift to off-hours + cross-department resource access.
    """
    scenario = "account_takeover"
    events: list[dict[str, Any]] = []

    cross_resources = _cross_dept_resources(resources, principal.department)
    if not cross_resources:
        cross_resources = resources["gworkspace"][:20]

    start_day = min(20, cfg.num_days - 8)
    for d in range(start_day, min(start_day + 7, cfg.num_days)):
        day = cfg.start_date + timedelta(days=d)
        n_events = rng.randint(8, 15)

        for _ in range(n_events):
            ts = _off_hours_ts(day, rng, principal.tz_offset)
            res = rng.choice(cross_resources)

            if res.source == "gworkspace":
                events.append(_make_scenario_event(
                    "gworkspace", principal, ts, scenario, counter, rng,
                    event_type="view",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="document",
                    sensitivity=res.sensitivity,
                    external=False,
                ))
            elif res.source == "snowflake":
                events.append(_make_scenario_event(
                    "snowflake", principal, ts, scenario, counter, rng,
                    event_type="export",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="dataset",
                    sensitivity=res.sensitivity,
                    rows_read=rng.randint(1000, 100000),
                    warehouse=rng.choice(WAREHOUSE_NAMES),
                ))
            elif res.source == "github":
                events.append(_make_scenario_event(
                    "github", principal, ts, scenario, counter, rng,
                    event_type="clone",
                    resource_id=res.resource_id,
                    resource_name=res.name,
                    resource_kind="repo",
                    sensitivity=res.sensitivity,
                    bytes_transferred_mb=rng.randint(50, 400),
                ))

    return events, ScenarioSpec(scenario, [principal.idx], len(events))


def _inject_insider_collaboration(
    cfg: ScaleConfig,
    principal_a: OrgPrincipal,
    principal_b: OrgPrincipal,
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
    counter: list[int],
) -> tuple[list[dict[str, Any]], ScenarioSpec]:
    """Two accounts accessing unusual shared resources in coordination.

    Signal: two principals from different departments both accessing the same
    cross-department resources within short time windows.
    """
    scenario = "insider_collaboration"
    events: list[dict[str, Any]] = []

    # Resources unusual for BOTH principals (not owned by either's dept)
    shared_targets = [
        r
        for src_list in resources.values()
        for r in src_list
        if r.owning_department not in (principal_a.department, principal_b.department)
    ]
    if not shared_targets:
        shared_targets = resources["gworkspace"][:20]

    # Fixed set of ~15 target resources they'll both repeatedly access
    target_set = rng.sample(shared_targets, min(15, len(shared_targets)))

    start_day = min(12, cfg.num_days - 14)
    for d in range(start_day, min(start_day + 13, cfg.num_days)):
        day = cfg.start_date + timedelta(days=d)
        if day.weekday() >= 5:
            continue

        # Both principals access 2-4 of the same targets each day
        day_targets = rng.sample(target_set, min(rng.randint(2, 4), len(target_set)))
        for res in day_targets:
            for p in (principal_a, principal_b):
                ts = _random_ts(day, rng, p.tz_offset)

                if res.source == "gworkspace":
                    events.append(_make_scenario_event(
                        "gworkspace", p, ts, scenario, counter, rng,
                        event_type="view",
                        resource_id=res.resource_id,
                        resource_name=res.name,
                        resource_kind="document",
                        sensitivity=res.sensitivity,
                        external=False,
                    ))
                elif res.source == "snowflake":
                    events.append(_make_scenario_event(
                        "snowflake", p, ts, scenario, counter, rng,
                        event_type="query",
                        resource_id=res.resource_id,
                        resource_name=res.name,
                        resource_kind="dataset",
                        sensitivity=res.sensitivity,
                        rows_read=rng.randint(100, 5000),
                        warehouse=rng.choice(WAREHOUSE_NAMES),
                    ))
                elif res.source == "github":
                    events.append(_make_scenario_event(
                        "github", p, ts, scenario, counter, rng,
                        event_type="view",
                        resource_id=res.resource_id,
                        resource_name=res.name,
                        resource_kind="repo",
                        sensitivity=res.sensitivity,
                        bytes_transferred_mb=rng.randint(1, 20),
                    ))

    return events, ScenarioSpec(
        scenario, [principal_a.idx, principal_b.idx], len(events),
    )


def inject_scenarios(
    cfg: ScaleConfig,
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    resources: dict[str, list[ResourceDef]],
    benign_events: list[dict[str, Any]],
    rng: random.Random,
) -> tuple[list[dict[str, Any]], dict[str, bool]]:
    """Inject all attack scenarios into the benign event stream.

    Selects non-overlapping attacker principals, generates attack events for
    each of the 6 scenario types, and merges them into the sorted event stream.

    Returns (merged_events, expectations) where expectations maps each
    scenario name to True (all injected scenarios should be detected).
    """
    candidates = [p for p in principals if not p.is_manager]
    rng.shuffle(candidates)

    if len(candidates) < 7:
        raise ValueError(
            f"Need >= 7 non-manager principals for scenario injection, "
            f"got {len(candidates)}"
        )

    # Prefer data roles for exfil, non-admin for priv-esc
    data_roles = {"data-engineer", "analyst"}
    admin_roles = {"security-admin", "executive", "manager"}

    def pop_preferred(pred: Any) -> OrgPrincipal:
        for i, p in enumerate(candidates):
            if pred(p):
                return candidates.pop(i)
        return candidates.pop(0)

    p_exfil = pop_preferred(lambda p: p.role in data_roles)
    p_cred = pop_preferred(lambda _: True)
    p_priv = pop_preferred(lambda p: p.role not in admin_roles)
    p_depart = pop_preferred(lambda _: True)
    p_takeover = pop_preferred(lambda _: True)
    p_collab_a = pop_preferred(lambda _: True)
    p_collab_b = pop_preferred(lambda p: p.department != p_collab_a.department)

    counter = [0]
    all_injected: list[dict[str, Any]] = []
    specs: list[ScenarioSpec] = []

    for fn, args in [
        (_inject_slow_exfiltration, (cfg, p_exfil, resources, rng, counter)),
        (_inject_credential_compromise, (cfg, p_cred, resources, rng, counter)),
        (_inject_privilege_escalation, (cfg, p_priv, resources, rng, counter)),
        (_inject_departing_employee, (cfg, p_depart, resources, rng, counter)),
        (_inject_account_takeover, (cfg, p_takeover, resources, rng, counter)),
        (
            _inject_insider_collaboration,
            (cfg, p_collab_a, p_collab_b, resources, rng, counter),
        ),
    ]:
        evts, spec = fn(*args)
        all_injected.extend(evts)
        specs.append(spec)

    merged = benign_events + all_injected
    merged.sort(key=lambda e: e["observed_at"])

    expectations = {spec.name: True for spec in specs}

    return merged, expectations


# ---------------------------------------------------------------------------
# Drift simulator — evolve the org over time
# ---------------------------------------------------------------------------


def _seasonal_multiplier(day: datetime, cfg: ScaleConfig) -> float:
    """Compute daily activity multiplier from seasonal patterns.

    Quarter-end months (last 2 weeks): spike.
    Late December holidays: dip.
    """
    month, dom = day.month, day.day
    if month == 12 and dom >= 24:
        return cfg.holiday_dip
    if month in (3, 6, 9, 12) and dom > 15:
        return cfg.quarter_end_spike
    return 1.0


def _apply_drift(
    cfg: ScaleConfig,
    months_elapsed: int,
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    resources: dict[str, list[ResourceDef]],
    active_indices: set[int],
    resource_affinities: list[PrincipalAffinities],
    app_affinities: list[WeightedSampler],
    rng: random.Random,
) -> None:
    """Apply one month of organizational drift. Mutates all arguments in place."""
    locations = LOCATIONS[: cfg.num_locations]
    loc_sampler = WeightedSampler(locations, zipf_weights(len(locations), s=0.8))
    role_names = [r for r, _ in IC_ROLE_WEIGHTS]
    role_wts = [w for _, w in IC_ROLE_WEIGHTS]
    role_sampler = WeightedSampler(role_names, role_wts)
    sens_sampler = WeightedSampler(SENSITIVITY_LEVELS, SENSITIVITY_WEIGHTS)
    common_apps = ["okta:slack", "okta:google-workspace", "okta:zoom"]

    # --- Departures ---
    n_departures = max(0, int(len(active_indices) * cfg.departure_rate_per_month))
    # Keep a floor so the org doesn't collapse
    if n_departures > 0 and len(active_indices) > n_departures + 20:
        departing = rng.sample(sorted(active_indices), n_departures)
        active_indices -= set(departing)

    # --- New hires ---
    n_hires = max(0, int(len(active_indices) * cfg.hire_rate_per_month))
    if n_hires > 0:
        new_principals: list[OrgPrincipal] = []
        for _ in range(n_hires):
            team = rng.choice(teams)
            new_idx = len(principals)
            loc = loc_sampler.sample(rng)
            new_p = OrgPrincipal(
                idx=new_idx,
                email=f"p{new_idx:05d}@example.com",
                name=f"Principal {new_idx:05d}",
                department=team.department,
                team=team.name,
                role=role_sampler.sample(rng),
                location=loc,
                is_manager=False,
                manager_idx=team.manager_idx,
                tz_offset=LOCATION_TZ_OFFSETS.get(loc, 0.0),
            )
            principals.append(new_p)
            team.member_indices.append(new_idx)
            active_indices.add(new_idx)
            new_principals.append(new_p)

        resource_affinities.extend(
            build_principal_affinities(cfg, new_principals, teams, resources, rng)
        )
        app_affinities.extend(build_app_affinities(new_principals, rng))

    # --- Role changes (quarterly) ---
    if months_elapsed % 3 == 0:
        n_changes = max(
            0, int(len(active_indices) * cfg.role_change_rate_per_quarter)
        )
        changeable = [i for i in active_indices if not principals[i].is_manager]
        if n_changes > 0 and changeable:
            changing = rng.sample(changeable, min(n_changes, len(changeable)))
            for idx in changing:
                new_role = role_sampler.sample(rng)
                principals[idx].role = new_role
                ra = ROLE_APP_AFFINITIES.get(new_role, common_apps)
                all_apps = list(dict.fromkeys(ra + common_apps))
                app_affinities[idx] = WeightedSampler(
                    all_apps, zipf_weights(len(all_apps), s=0.8),
                )

    # --- New resources ---
    for team in teams:
        n_docs = max(0, int(rng.gauss(cfg.new_docs_per_month_per_team, 1.5) + 0.5))
        for j in range(n_docs):
            doc_id = f"gworkspace:doc/{team.name}-m{months_elapsed}-doc-{j:03d}"
            resources["gworkspace"].append(
                ResourceDef(
                    resource_id=doc_id,
                    name=f"{team.name} Doc m{months_elapsed}-{j}",
                    kind="document",
                    source="gworkspace",
                    sensitivity=sens_sampler.sample(rng),
                    owning_team=team.name,
                    owning_department=team.department,
                )
            )

        if months_elapsed % 3 == 0:
            if rng.random() < cfg.new_repos_per_quarter_per_team:
                repo_name = f"{team.name}-repo-q{months_elapsed // 3}"
                resources["github"].append(
                    ResourceDef(
                        resource_id=f"github:repo/{repo_name}",
                        name=repo_name,
                        kind="repo",
                        source="github",
                        sensitivity=sens_sampler.sample(rng),
                        owning_team=team.name,
                        owning_department=team.department,
                    )
                )

            n_tables = max(
                0,
                int(rng.gauss(cfg.new_tables_per_quarter_per_team, 0.5) + 0.5),
            )
            for j in range(n_tables):
                tname = f"{team.name.replace('-', '_')}_q{months_elapsed // 3}_t{j}"
                resources["snowflake"].append(
                    ResourceDef(
                        resource_id=f"snowflake:dataset/{tname}",
                        name=tname,
                        kind="dataset",
                        source="snowflake",
                        sensitivity=sens_sampler.sample(rng),
                        owning_team=team.name,
                        owning_department=team.department,
                    )
                )


def _generate_with_drift(
    cfg: ScaleConfig,
    principals: list[OrgPrincipal],
    teams: list[OrgTeam],
    resources: dict[str, list[ResourceDef]],
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Generate events with organizational drift applied at month boundaries."""
    active_indices = set(range(len(principals)))

    resource_affinities = build_principal_affinities(
        cfg, principals, teams, resources, rng,
    )
    app_affinities = build_app_affinities(principals, rng)

    counter = [0]
    all_events: list[dict[str, Any]] = []
    prev_month: tuple[int, int] | None = None
    months_elapsed = 0
    active_principals = list(principals)

    for day_offset in range(cfg.num_days):
        day = cfg.start_date + timedelta(days=day_offset)
        current_month = (day.year, day.month)

        if current_month != prev_month:
            if prev_month is not None:
                months_elapsed += 1
                _apply_drift(
                    cfg,
                    months_elapsed,
                    principals,
                    teams,
                    resources,
                    active_indices,
                    resource_affinities,
                    app_affinities,
                    rng,
                )
            active_principals = [principals[i] for i in sorted(active_indices)]
            prev_month = current_month

        multiplier = _seasonal_multiplier(day, cfg)

        day_events = _generate_day_events(
            day,
            active_principals,
            resource_affinities,
            app_affinities,
            cfg,
            rng,
            counter,
            day_multiplier=multiplier,
        )
        all_events.extend(day_events)

    all_events.sort(key=lambda e: e["observed_at"])
    return all_events


# ---------------------------------------------------------------------------
# Top-level generator
# ---------------------------------------------------------------------------


def generate_scaled_dataset(cfg: ScaleConfig | None = None) -> SyntheticDataset:
    """Generate a large-scale synthetic dataset.

    Returns a SyntheticDataset with raw_events matching the existing schema.
    When inject_scenarios is True, attack events are injected and expectations
    are populated. When enable_drift is True, the org evolves over time with
    hires, departures, role changes, new resources, and seasonal patterns.
    """
    if cfg is None:
        cfg = ScaleConfig()

    rng = random.Random(cfg.seed)

    principals, teams = generate_org(cfg, rng)
    resources = generate_resources(cfg, teams, rng)

    if cfg.enable_drift:
        all_events = _generate_with_drift(cfg, principals, teams, resources, rng)
    else:
        resource_affinities = build_principal_affinities(
            cfg, principals, teams, resources, rng,
        )
        app_affinities = build_app_affinities(principals, rng)

        counter = [0]
        all_events: list[dict[str, Any]] = []

        for day_offset in range(cfg.num_days):
            day = cfg.start_date + timedelta(days=day_offset)
            day_events = _generate_day_events(
                day, principals, resource_affinities, app_affinities,
                cfg, rng, counter,
            )
            all_events.extend(day_events)

        all_events.sort(key=lambda e: e["observed_at"])

    if cfg.inject_scenarios:
        all_events, expectations = inject_scenarios(
            cfg, principals, teams, resources, all_events, rng,
        )
    else:
        expectations = {}

    return SyntheticDataset(
        raw_events=all_events,
        expectations=expectations,
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
