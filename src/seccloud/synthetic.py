from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from seccloud.storage import format_timestamp

SOURCE_PACK = ("okta", "gworkspace", "github", "snowflake")
SCENARIO_EXPECTATIONS = {
    "compromised_privileged_identity": True,
    "unusual_repo_export": True,
    "unusual_data_access": True,
    "unusual_external_sharing": True,
    "benign_role_change_control": False,
    "benign_travel_with_access": False,
    "benign_incident_response_access": False,
    "benign_finance_close_access": False,
    "source_noise_control": False,
}


@dataclass(slots=True)
class SyntheticDataset:
    raw_events: list[dict[str, Any]]
    expectations: dict[str, bool]


def _principal(email: str, name: str, department: str, role: str) -> dict[str, str]:
    return {
        "actor_email": email,
        "actor_name": name,
        "department": department,
        "role": role,
    }


def generate_synthetic_dataset() -> SyntheticDataset:
    start = datetime(2026, 1, 1, 14, 0, tzinfo=UTC)
    principals = {
        "alice": _principal("alice@example.com", "Alice Admin", "security", "security-admin"),
        "bob": _principal("bob@example.com", "Bob Builder", "engineering", "staff-engineer"),
        "carol": _principal("carol@example.com", "Carol Query", "data", "data-analyst"),
        "dave": _principal("dave@example.com", "Dave Share", "product", "pm"),
        "emma": _principal("emma@example.com", "Emma Move", "engineering", "senior-engineer"),
    }
    raw_events: list[dict[str, Any]] = []
    counter = 0

    def emit(source: str, observed_at: datetime, payload: dict[str, Any]) -> None:
        nonlocal counter
        counter += 1
        raw_events.append(
            {
                "source": source,
                "source_event_id": f"{source}-{counter:04d}",
                "observed_at": format_timestamp(observed_at),
                "received_at": format_timestamp(observed_at + timedelta(minutes=2)),
                **payload,
            }
        )

    for day in range(8):
        current = start + timedelta(days=day)
        emit(
            "okta",
            current,
            {
                **principals["alice"],
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-NY",
                "ip": f"10.0.0.{day + 10}",
                "privileged": True,
                "scenario": "baseline",
            },
        )
        emit(
            "github",
            current + timedelta(hours=1),
            {
                **principals["bob"],
                "event_type": "view",
                "resource_id": "github:repo/payments-service",
                "resource_name": "payments-service",
                "resource_kind": "repo",
                "sensitivity": "internal",
                "bytes_transferred_mb": 5,
                "scenario": "baseline",
            },
        )
        emit(
            "snowflake",
            current + timedelta(hours=2),
            {
                **principals["carol"],
                "event_type": "query",
                "resource_id": "snowflake:dataset/sales_summary",
                "resource_name": "sales_summary",
                "resource_kind": "dataset",
                "sensitivity": "internal",
                "rows_read": 1200,
                "warehouse": "analytics_wh",
                "scenario": "baseline",
            },
        )
        emit(
            "gworkspace",
            current + timedelta(hours=3),
            {
                **principals["dave"],
                "event_type": "view",
                "resource_id": "gworkspace:doc/product-plan",
                "resource_name": "Product Plan",
                "resource_kind": "document",
                "sensitivity": "internal",
                "external": False,
                "scenario": "baseline",
            },
        )
        emit(
            "github",
            current + timedelta(hours=4),
            {
                **principals["emma"],
                "event_type": "view",
                "resource_id": "github:repo/ml-platform",
                "resource_name": "ml-platform",
                "resource_kind": "repo",
                "sensitivity": "internal",
                "bytes_transferred_mb": 3,
                "scenario": "baseline",
            },
        )

    attack_day = start + timedelta(days=8)
    emit(
        "okta",
        attack_day,
        {
            **principals["alice"],
            "event_type": "login",
            "resource_id": "okta:admin-console",
            "resource_name": "Admin Console",
            "resource_kind": "app",
            "sensitivity": "high",
            "geo": "DE-BE",
            "ip": "185.220.100.7",
            "privileged": True,
            "scenario": "compromised_privileged_identity",
        },
    )
    emit(
        "okta",
        attack_day + timedelta(minutes=10),
        {
            **principals["alice"],
            "event_type": "login",
            "resource_id": "okta:admin-console",
            "resource_name": "Admin Console",
            "resource_kind": "app",
            "sensitivity": "high",
            "geo": "US-CA",
            "ip": "10.99.0.22",
            "privileged": True,
            "approved_travel": True,
            "travel_context": "customer-site-visit",
            "scenario": "benign_travel_with_access",
        },
    )
    emit(
        "snowflake",
        attack_day + timedelta(minutes=20),
        {
            **principals["alice"],
            "event_type": "query",
            "resource_id": "snowflake:dataset/payroll",
            "resource_name": "payroll",
            "resource_kind": "dataset",
            "sensitivity": "critical",
            "rows_read": 150000,
            "warehouse": "finance_wh",
            "scenario": "compromised_privileged_identity",
        },
    )
    emit(
        "github",
        attack_day + timedelta(minutes=50),
        {
            **principals["bob"],
            "event_type": "archive_download",
            "resource_id": "github:repo/incident-runbooks",
            "resource_name": "incident-runbooks",
            "resource_kind": "repo",
            "sensitivity": "high",
            "bytes_transferred_mb": 410,
            "incident_response_window": True,
            "incident_ticket": "INC-4242",
            "scenario": "benign_incident_response_access",
        },
    )
    emit(
        "github",
        attack_day + timedelta(hours=1),
        {
            **principals["bob"],
            "event_type": "archive_download",
            "resource_id": "github:repo/security-agent",
            "resource_name": "security-agent",
            "resource_kind": "repo",
            "sensitivity": "high",
            "bytes_transferred_mb": 850,
            "scenario": "unusual_repo_export",
        },
    )
    emit(
        "snowflake",
        attack_day + timedelta(hours=1, minutes=40),
        {
            **principals["carol"],
            "event_type": "export",
            "resource_id": "snowflake:dataset/revenue_forecast",
            "resource_name": "revenue_forecast",
            "resource_kind": "dataset",
            "sensitivity": "high",
            "rows_read": 86000,
            "warehouse": "finance_wh",
            "quarter_close_window": True,
            "approved_workflow": True,
            "scenario": "benign_finance_close_access",
        },
    )
    emit(
        "snowflake",
        attack_day + timedelta(hours=2),
        {
            **principals["carol"],
            "event_type": "export",
            "resource_id": "snowflake:dataset/customer_pii",
            "resource_name": "customer_pii",
            "resource_kind": "dataset",
            "sensitivity": "critical",
            "rows_read": 220000,
            "warehouse": "analytics_wh",
            "scenario": "unusual_data_access",
        },
    )
    emit(
        "gworkspace",
        attack_day + timedelta(hours=3),
        {
            **principals["dave"],
            "event_type": "share_external",
            "resource_id": "gworkspace:doc/board-notes",
            "resource_name": "Board Notes",
            "resource_kind": "document",
            "sensitivity": "high",
            "external": True,
            "target_domain": "gmail.com",
            "scenario": "unusual_external_sharing",
        },
    )
    emit(
        "github",
        attack_day + timedelta(hours=3, minutes=10),
        {
            **principals["bob"],
            "event_type": "view",
            "resource_id": "github:repo/payments-service",
            "resource_name": "payments-service",
            "resource_kind": "repo",
            "sensitivity": "internal",
            "bytes_transferred_mb": 5,
            "scenario": "source_noise_control",
        },
    )
    emit(
        "github",
        attack_day + timedelta(hours=4),
        {
            **principals["emma"],
            "event_type": "clone",
            "resource_id": "github:repo/forecasting",
            "resource_name": "forecasting",
            "resource_kind": "repo",
            "sensitivity": "internal",
            "bytes_transferred_mb": 120,
            "expected_transition": True,
            "role_change_window": True,
            "scenario": "benign_role_change_control",
        },
    )
    emit(
        "snowflake",
        attack_day + timedelta(hours=4, minutes=30),
        {
            **principals["emma"],
            "event_type": "query",
            "resource_id": "snowflake:dataset/forecasting",
            "resource_name": "forecasting",
            "resource_kind": "dataset",
            "sensitivity": "internal",
            "rows_read": 1800,
            "warehouse": "forecast_wh",
            "expected_transition": True,
            "role_change_window": True,
            "scenario": "benign_role_change_control",
        },
    )

    raw_events.append(
        {
            "source": "github",
            "source_event_id": "github-semantic-dup-0001",
            "observed_at": format_timestamp(start + timedelta(hours=1)),
            "received_at": format_timestamp(attack_day + timedelta(hours=6)),
            **principals["bob"],
            "event_type": "view",
            "resource_id": "github:repo/payments-service",
            "resource_name": "payments-service",
            "resource_kind": "repo",
            "sensitivity": "internal",
            "bytes_transferred_mb": 5,
            "scenario": "source_noise_control",
        }
    )
    raw_events.append(
        {
            "source": "snowflake",
            "source_event_id": "snowflake-late-0001",
            "observed_at": format_timestamp(start + timedelta(days=2, hours=2, minutes=5)),
            "received_at": format_timestamp(attack_day + timedelta(hours=5)),
            **principals["carol"],
            "event_type": "query",
            "resource_id": "snowflake:dataset/sales_summary",
            "resource_name": "sales_summary",
            "resource_kind": "dataset",
            "sensitivity": "internal",
            "rows_read": 1200,
            "warehouse": "analytics_wh",
            "scenario": "source_noise_control",
        }
    )
    raw_events.append(
        {
            "source": "gworkspace",
            "source_event_id": "gworkspace-bad-0001",
            "observed_at": format_timestamp(attack_day + timedelta(hours=5, minutes=5)),
            "received_at": format_timestamp(attack_day + timedelta(hours=5, minutes=6)),
            "actor_email": principals["dave"]["actor_email"],
            "actor_name": principals["dave"]["actor_name"],
            "department": principals["dave"]["department"],
            "role": principals["dave"]["role"],
            "event_type": "view",
            "resource_id": "gworkspace:doc/malformed",
            "resource_name": "Malformed Document",
            "sensitivity": "internal",
            "external": False,
            "scenario": "source_noise_control",
        }
    )
    raw_events.append(
        {
            "source": "okta",
            "source_event_id": "okta-unsupported-0001",
            "observed_at": format_timestamp(attack_day + timedelta(hours=5, minutes=15)),
            "received_at": format_timestamp(attack_day + timedelta(hours=5, minutes=16)),
            **principals["alice"],
            "event_type": "mfa_challenge",
            "resource_id": "okta:admin-console",
            "resource_name": "Admin Console",
            "resource_kind": "app",
            "sensitivity": "high",
            "geo": "US-NY",
            "ip": "10.0.0.88",
            "privileged": True,
            "scenario": "source_noise_control",
        }
    )

    raw_events.sort(key=lambda item: item.get("received_at", item["observed_at"]))
    return SyntheticDataset(raw_events=raw_events, expectations=dict(SCENARIO_EXPECTATIONS))
