use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Timelike, Utc};
use seccloud_lake::schema::{
    ActionFeatureRow, CollaborationFeatureRow, HistoryFeatureRow, PeerGroupFeatureRow,
    StaticPrincipalFeatureRow,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const HISTORY_WINDOW_HOURS: u32 = 2;
const DEFAULT_DURATION_BUCKET: &str = "1-3yr";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RuntimePrincipalRecord {
    pub idx: i64,
    pub email: String,
    pub manager_idx: Option<i64>,
    pub department: Option<String>,
    pub role: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RuntimeTeamRecord {
    pub member_indices: Vec<i64>,
}

fn privilege_level(role: &str) -> &str {
    match role {
        "security-admin" => "admin",
        "executive" | "manager" | "staff-engineer" => "elevated",
        _ => "regular",
    }
}

fn employment_duration_bucket_from_idx(principal_idx: i64) -> &'static str {
    let tenure_years = ((principal_idx as f64) * 7.3) % 6.0;
    if tenure_years < 0.25 {
        "<3mo"
    } else if tenure_years < 1.0 {
        "3-12mo"
    } else if tenure_years < 3.0 {
        "1-3yr"
    } else if tenure_years < 5.0 {
        "3-5yr"
    } else {
        "5yr+"
    }
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> anyhow::Result<&'a Value> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .ok_or_else(|| anyhow::anyhow!("missing field: {}", path.join(".")))?;
    }
    Ok(current)
}

fn string_at_path(value: &Value, path: &[&str]) -> anyhow::Result<String> {
    value_at_path(value, path)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("field is not a string: {}", path.join(".")))
}

fn optional_string_at_path(value: &Value, path: &[&str], default: &str) -> String {
    value_at_path(value, path)
        .ok()
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn parse_observed_at(value: &str) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn history_window_start(ts: DateTime<Utc>) -> String {
    let hour = (ts.hour() / HISTORY_WINDOW_HOURS) * HISTORY_WINDOW_HOURS;
    ts.with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .and_then(|value| value.with_hour(hour))
        .expect("valid history window timestamp")
        .format("%Y-%m-%dT%H:00:00Z")
        .to_string()
}

pub fn build_action_feature_rows(
    events: &[Value],
    tenant_id: &str,
    schema_version: &str,
) -> anyhow::Result<Vec<ActionFeatureRow>> {
    let mut per_resource: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    let mut resource_sources: BTreeMap<String, String> = BTreeMap::new();

    for event in events {
        let resource_entity_key = string_at_path(event, &["resource", "entity_key"])?;
        let principal_entity_key = string_at_path(event, &["principal", "entity_key"])?;
        let source = string_at_path(event, &["source"])?;
        *per_resource
            .entry(resource_entity_key.clone())
            .or_default()
            .entry(principal_entity_key)
            .or_insert(0) += 1;
        resource_sources
            .entry(resource_entity_key)
            .or_insert(source);
    }

    let mut rows = Vec::new();
    for (resource_entity_key, principals) in per_resource {
        let total: i64 = principals.values().sum();
        if total == 0 {
            continue;
        }
        let source = resource_sources
            .get(&resource_entity_key)
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        for (principal_entity_key, access_count) in principals {
            rows.push(ActionFeatureRow {
                feature_schema_version: schema_version.to_string(),
                tenant_id: tenant_id.to_string(),
                source: source.clone(),
                resource_entity_key: resource_entity_key.clone(),
                principal_entity_key,
                access_count,
                accessor_weight: access_count as f64 / total as f64,
            });
        }
    }
    Ok(rows)
}

pub fn build_history_feature_rows(
    events: &[Value],
    tenant_id: &str,
    schema_version: &str,
) -> anyhow::Result<Vec<HistoryFeatureRow>> {
    let mut grouped: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();

    for event in events {
        let principal_entity_key = string_at_path(event, &["principal", "entity_key"])?;
        let resource_entity_key = string_at_path(event, &["resource", "entity_key"])?;
        let observed_at = string_at_path(event, &["observed_at"])?;
        let window_start = history_window_start(parse_observed_at(&observed_at)?);
        grouped
            .entry((principal_entity_key, window_start))
            .or_default()
            .insert(resource_entity_key);
    }

    let mut rows = Vec::new();
    for ((principal_entity_key, window_start), resources) in grouped {
        for resource_entity_key in resources {
            rows.push(HistoryFeatureRow {
                feature_schema_version: schema_version.to_string(),
                tenant_id: tenant_id.to_string(),
                principal_entity_key: principal_entity_key.clone(),
                window_start: window_start.clone(),
                resource_entity_key,
            });
        }
    }
    Ok(rows)
}

pub fn build_collaboration_feature_rows(
    events: &[Value],
    tenant_id: &str,
    schema_version: &str,
) -> anyhow::Result<Vec<CollaborationFeatureRow>> {
    let mut resource_accessors: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();

    for event in events {
        let resource_entity_key = string_at_path(event, &["resource", "entity_key"])?;
        let principal_entity_key = string_at_path(event, &["principal", "entity_key"])?;
        *resource_accessors
            .entry(resource_entity_key)
            .or_default()
            .entry(principal_entity_key)
            .or_insert(0) += 1;
    }

    let mut weights: BTreeMap<(String, String), f64> = BTreeMap::new();
    for accessors in resource_accessors.values() {
        let popularity = accessors.len();
        if popularity <= 1 {
            continue;
        }
        let inv_popularity = 1.0 / popularity as f64;
        let accessor_items: Vec<_> = accessors.iter().collect();
        for (index, (principal_a, count_a)) in accessor_items.iter().enumerate() {
            for (principal_b, count_b) in accessor_items.iter().skip(index + 1) {
                let weight = (**count_a).min(**count_b) as f64 * inv_popularity;
                *weights
                    .entry(((*principal_a).clone(), (*principal_b).clone()))
                    .or_insert(0.0) += weight;
                *weights
                    .entry(((*principal_b).clone(), (*principal_a).clone()))
                    .or_insert(0.0) += weight;
            }
        }
    }

    Ok(weights
        .into_iter()
        .map(
            |((principal_entity_key, collaborator_entity_key), co_access_weight)| {
                CollaborationFeatureRow {
                    feature_schema_version: schema_version.to_string(),
                    tenant_id: tenant_id.to_string(),
                    principal_entity_key,
                    collaborator_entity_key,
                    co_access_weight,
                }
            },
        )
        .collect())
}

pub fn build_static_principal_feature_rows(
    events: &[Value],
    runtime_principals: &[RuntimePrincipalRecord],
    tenant_id: &str,
    schema_version: &str,
) -> anyhow::Result<Vec<StaticPrincipalFeatureRow>> {
    let mut latest_by_principal: BTreeMap<String, (String, String, String, String, String)> =
        BTreeMap::new();
    let runtime_by_email: BTreeMap<&str, &RuntimePrincipalRecord> = runtime_principals
        .iter()
        .map(|principal| (principal.email.as_str(), principal))
        .collect();

    for event in events {
        let principal_entity_key = string_at_path(event, &["principal", "entity_key"])?;
        let observed_at = string_at_path(event, &["observed_at"])?;
        let principal_id = optional_string_at_path(event, &["principal", "id"], "unknown");
        let department = optional_string_at_path(event, &["principal", "department"], "unknown");
        let role = optional_string_at_path(event, &["principal", "attributes", "role"], "unknown");
        let location = optional_string_at_path(event, &["attributes", "geo"], "unknown");

        let should_replace = latest_by_principal
            .get(&principal_entity_key)
            .map(|current| observed_at >= current.0)
            .unwrap_or(true);
        if should_replace {
            latest_by_principal.insert(
                principal_entity_key,
                (observed_at, principal_id, department, role, location),
            );
        }
    }

    Ok(latest_by_principal
        .into_iter()
        .map(
            |(principal_entity_key, (_observed_at, principal_id, department, role, location))| {
                let runtime = runtime_by_email.get(principal_id.as_str()).copied();
                let department = runtime
                    .and_then(|principal| principal.department.as_deref())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(&department)
                    .to_string();
                let role = runtime
                    .and_then(|principal| principal.role.as_deref())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(&role)
                    .to_string();
                let location = runtime
                    .and_then(|principal| principal.location.as_deref())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(&location)
                    .to_string();
                let employment_duration_bucket = runtime
                    .map(|principal| employment_duration_bucket_from_idx(principal.idx))
                    .unwrap_or(DEFAULT_DURATION_BUCKET)
                    .to_string();
                StaticPrincipalFeatureRow {
                    feature_schema_version: schema_version.to_string(),
                    tenant_id: tenant_id.to_string(),
                    principal_entity_key,
                    principal_id,
                    department,
                    role: role.clone(),
                    location,
                    employment_duration_bucket,
                    privilege_level: privilege_level(&role).to_string(),
                }
            },
        )
        .collect())
}

pub fn build_peer_group_feature_rows(
    static_rows: &[StaticPrincipalFeatureRow],
    runtime_principals: &[RuntimePrincipalRecord],
    runtime_teams: &[RuntimeTeamRecord],
    tenant_id: &str,
    schema_version: &str,
) -> Vec<PeerGroupFeatureRow> {
    let mut rows = Vec::new();
    let mut by_department: BTreeMap<String, Vec<&StaticPrincipalFeatureRow>> = BTreeMap::new();
    for row in static_rows {
        by_department
            .entry(row.department.clone())
            .or_default()
            .push(row);
    }
    for peers in by_department.values() {
        for principal in peers {
            for peer in peers {
                if principal.principal_entity_key == peer.principal_entity_key {
                    continue;
                }
                rows.push(PeerGroupFeatureRow {
                    feature_schema_version: schema_version.to_string(),
                    tenant_id: tenant_id.to_string(),
                    principal_entity_key: principal.principal_entity_key.clone(),
                    peer_entity_key: peer.principal_entity_key.clone(),
                    peer_type: "department".into(),
                    peer_weight: 1.0,
                });
            }
        }
    }

    if runtime_principals.is_empty() {
        rows.sort_by(|left, right| {
            (
                &left.principal_entity_key,
                &left.peer_type,
                &left.peer_entity_key,
            )
                .cmp(&(
                    &right.principal_entity_key,
                    &right.peer_type,
                    &right.peer_entity_key,
                ))
        });
        return rows;
    }

    let principal_id_to_row: BTreeMap<String, &StaticPrincipalFeatureRow> = static_rows
        .iter()
        .map(|row| (row.principal_id.clone(), row))
        .collect();
    let runtime_by_idx: BTreeMap<i64, &RuntimePrincipalRecord> = runtime_principals
        .iter()
        .map(|principal| (principal.idx, principal))
        .collect();
    let mut manager_reports: BTreeMap<i64, Vec<&RuntimePrincipalRecord>> = BTreeMap::new();
    for principal in runtime_principals {
        if let Some(manager_idx) = principal.manager_idx {
            manager_reports
                .entry(manager_idx)
                .or_default()
                .push(principal);
        }
    }

    for principal in runtime_principals {
        let Some(principal_row) = principal_id_to_row.get(&principal.email) else {
            continue;
        };
        if let Some(manager_idx) = principal.manager_idx {
            if let Some(siblings) = manager_reports.get(&manager_idx) {
                for sibling in siblings {
                    if sibling.email == principal.email {
                        continue;
                    }
                    if let Some(peer_row) = principal_id_to_row.get(&sibling.email) {
                        rows.push(PeerGroupFeatureRow {
                            feature_schema_version: schema_version.to_string(),
                            tenant_id: tenant_id.to_string(),
                            principal_entity_key: principal_row.principal_entity_key.clone(),
                            peer_entity_key: peer_row.principal_entity_key.clone(),
                            peer_type: "manager".into(),
                            peer_weight: 1.0,
                        });
                    }
                }
            }
            if let Some(manager) = runtime_by_idx.get(&manager_idx) {
                if let Some(grand_manager_idx) = manager.manager_idx {
                    if let Some(uncles) = manager_reports.get(&grand_manager_idx) {
                        for uncle in uncles {
                            if uncle.idx == manager_idx {
                                continue;
                            }
                            if let Some(cousins) = manager_reports.get(&uncle.idx) {
                                for cousin in cousins {
                                    if cousin.email == principal.email {
                                        continue;
                                    }
                                    if let Some(peer_row) = principal_id_to_row.get(&cousin.email) {
                                        if !rows.iter().any(|row| {
                                            row.principal_entity_key
                                                == principal_row.principal_entity_key
                                                && row.peer_entity_key
                                                    == peer_row.principal_entity_key
                                                && row.peer_type == "manager"
                                        }) {
                                            rows.push(PeerGroupFeatureRow {
                                                feature_schema_version: schema_version.to_string(),
                                                tenant_id: tenant_id.to_string(),
                                                principal_entity_key: principal_row
                                                    .principal_entity_key
                                                    .clone(),
                                                peer_entity_key: peer_row
                                                    .principal_entity_key
                                                    .clone(),
                                                peer_type: "manager".into(),
                                                peer_weight: 0.5,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    for principal in runtime_principals {
        let Some(principal_row) = principal_id_to_row.get(&principal.email) else {
            continue;
        };
        for team in runtime_teams {
            if !team.member_indices.contains(&principal.idx) || team.member_indices.len() <= 1 {
                continue;
            }
            let weight = 1.0 / team.member_indices.len() as f64;
            for member_idx in &team.member_indices {
                if *member_idx == principal.idx {
                    continue;
                }
                if let Some(member) = runtime_by_idx.get(member_idx) {
                    if let Some(peer_row) = principal_id_to_row.get(&member.email) {
                        rows.push(PeerGroupFeatureRow {
                            feature_schema_version: schema_version.to_string(),
                            tenant_id: tenant_id.to_string(),
                            principal_entity_key: principal_row.principal_entity_key.clone(),
                            peer_entity_key: peer_row.principal_entity_key.clone(),
                            peer_type: "group".into(),
                            peer_weight: weight,
                        });
                    }
                }
            }
        }
    }

    rows.sort_by(|left, right| {
        (
            &left.principal_entity_key,
            &left.peer_type,
            &left.peer_entity_key,
        )
            .cmp(&(
                &right.principal_entity_key,
                &right.peer_type,
                &right.peer_entity_key,
            ))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use seccloud_lake::schema::FEATURE_SCHEMA_VERSION;

    fn normalized_event(
        principal_entity_key: &str,
        resource_entity_key: &str,
        observed_at: &str,
        source: &str,
    ) -> Value {
        serde_json::json!({
            "event_id": format!("evt_{principal_entity_key}_{resource_entity_key}_{observed_at}"),
            "event_key": format!("evk_{principal_entity_key}_{resource_entity_key}_{observed_at}"),
            "source": source,
            "source_event_id": "src_1",
            "principal": {"entity_key": principal_entity_key},
            "resource": {"entity_key": resource_entity_key},
            "action": {"source": source, "verb": "read", "category": "access"},
            "observed_at": observed_at,
        })
    }

    #[test]
    fn action_features_are_weighted_per_resource() {
        let rows = build_action_feature_rows(
            &[
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:05:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:06:00Z", "gworkspace"),
                normalized_event("enk_p2", "enk_r1", "2026-03-15T14:07:00Z", "gworkspace"),
            ],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].resource_entity_key, "enk_r1");
        assert_eq!(rows[0].access_count, 2);
        assert!((rows[0].accessor_weight - (2.0 / 3.0)).abs() < 1e-9);
        assert_eq!(rows[1].access_count, 1);
        assert!((rows[1].accessor_weight - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn history_features_group_by_two_hour_windows() {
        let rows = build_history_feature_rows(
            &[
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:05:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r2", "2026-03-15T15:05:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r3", "2026-03-15T16:00:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:30:00Z", "gworkspace"),
            ],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].window_start, "2026-03-15T14:00:00Z");
        assert_eq!(rows[0].resource_entity_key, "enk_r1");
        assert_eq!(rows[1].window_start, "2026-03-15T14:00:00Z");
        assert_eq!(rows[1].resource_entity_key, "enk_r2");
        assert_eq!(rows[2].window_start, "2026-03-15T16:00:00Z");
        assert_eq!(rows[2].resource_entity_key, "enk_r3");
    }

    #[test]
    fn collaboration_features_sum_shared_resource_weights() {
        let rows = build_collaboration_feature_rows(
            &[
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:05:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r1", "2026-03-15T14:06:00Z", "gworkspace"),
                normalized_event("enk_p2", "enk_r1", "2026-03-15T14:07:00Z", "gworkspace"),
                normalized_event("enk_p1", "enk_r2", "2026-03-15T14:08:00Z", "gworkspace"),
                normalized_event("enk_p2", "enk_r2", "2026-03-15T14:09:00Z", "gworkspace"),
                normalized_event("enk_p3", "enk_r2", "2026-03-15T14:10:00Z", "gworkspace"),
            ],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        )
        .unwrap();

        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0].principal_entity_key, "enk_p1");
        assert_eq!(rows[0].collaborator_entity_key, "enk_p2");
        assert!((rows[0].co_access_weight - (0.5 + (1.0 / 3.0))).abs() < 1e-9);
        assert_eq!(rows[1].principal_entity_key, "enk_p1");
        assert_eq!(rows[1].collaborator_entity_key, "enk_p3");
        assert!((rows[1].co_access_weight - (1.0 / 3.0)).abs() < 1e-9);
        assert_eq!(rows[2].principal_entity_key, "enk_p2");
        assert_eq!(rows[2].collaborator_entity_key, "enk_p1");
        assert!((rows[2].co_access_weight - (0.5 + (1.0 / 3.0))).abs() < 1e-9);
        assert_eq!(rows[3].principal_entity_key, "enk_p2");
        assert_eq!(rows[3].collaborator_entity_key, "enk_p3");
        assert!((rows[3].co_access_weight - (1.0 / 3.0)).abs() < 1e-9);
        assert_eq!(rows[4].principal_entity_key, "enk_p3");
        assert_eq!(rows[4].collaborator_entity_key, "enk_p1");
        assert!((rows[4].co_access_weight - (1.0 / 3.0)).abs() < 1e-9);
        assert_eq!(rows[5].principal_entity_key, "enk_p3");
        assert_eq!(rows[5].collaborator_entity_key, "enk_p2");
        assert!((rows[5].co_access_weight - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn static_features_use_latest_principal_attributes() {
        let rows = build_static_principal_feature_rows(
            &[
                serde_json::json!({
                    "principal": {
                        "entity_key": "enk_p1",
                        "id": "alice@example.com",
                        "department": "security",
                        "attributes": {"role": "security-admin"}
                    },
                    "attributes": {"geo": "US-NY"},
                    "observed_at": "2026-03-15T14:05:00Z"
                }),
                serde_json::json!({
                    "principal": {
                        "entity_key": "enk_p1",
                        "id": "alice@example.com",
                        "department": "security",
                        "attributes": {"role": "manager"}
                    },
                    "attributes": {"geo": "US-CA"},
                    "observed_at": "2026-03-15T15:05:00Z"
                }),
            ],
            &[],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].principal_entity_key, "enk_p1");
        assert_eq!(rows[0].principal_id, "alice@example.com");
        assert_eq!(rows[0].department, "security");
        assert_eq!(rows[0].role, "manager");
        assert_eq!(rows[0].location, "US-CA");
        assert_eq!(rows[0].employment_duration_bucket, "1-3yr");
        assert_eq!(rows[0].privilege_level, "elevated");
    }

    #[test]
    fn static_features_apply_runtime_org_overrides() {
        let rows = build_static_principal_feature_rows(
            &[serde_json::json!({
                "principal": {
                    "entity_key": "enk_p1",
                    "id": "alice@example.com",
                    "department": "security",
                    "attributes": {"role": "security-admin"}
                },
                "attributes": {"geo": "US-NY"},
                "observed_at": "2026-03-15T14:05:00Z"
            })],
            &[RuntimePrincipalRecord {
                idx: 0,
                email: "alice@example.com".into(),
                manager_idx: None,
                department: Some("secops".into()),
                role: Some("manager".into()),
                location: Some("US-CA".into()),
            }],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].department, "secops");
        assert_eq!(rows[0].role, "manager");
        assert_eq!(rows[0].location, "US-CA");
        assert_eq!(rows[0].employment_duration_bucket, "<3mo");
        assert_eq!(rows[0].privilege_level, "elevated");
    }

    #[test]
    fn peer_group_features_include_department_manager_and_group_relationships() {
        let rows = build_peer_group_feature_rows(
            &[
                StaticPrincipalFeatureRow {
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    tenant_id: "tenant-1".into(),
                    principal_entity_key: "enk_p1".into(),
                    principal_id: "alice@example.com".into(),
                    department: "security".into(),
                    role: "security-admin".into(),
                    location: "US-NY".into(),
                    employment_duration_bucket: "1-3yr".into(),
                    privilege_level: "admin".into(),
                },
                StaticPrincipalFeatureRow {
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    tenant_id: "tenant-1".into(),
                    principal_entity_key: "enk_p2".into(),
                    principal_id: "charlie@example.com".into(),
                    department: "security".into(),
                    role: "engineer".into(),
                    location: "US-IL".into(),
                    employment_duration_bucket: "3-5yr".into(),
                    privilege_level: "regular".into(),
                },
                StaticPrincipalFeatureRow {
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    tenant_id: "tenant-1".into(),
                    principal_entity_key: "enk_p3".into(),
                    principal_id: "bob@example.com".into(),
                    department: "security".into(),
                    role: "manager".into(),
                    location: "US-CA".into(),
                    employment_duration_bucket: "5yr+".into(),
                    privilege_level: "elevated".into(),
                },
            ],
            &[
                RuntimePrincipalRecord {
                    idx: 0,
                    email: "alice@example.com".into(),
                    manager_idx: Some(2),
                    department: None,
                    role: None,
                    location: None,
                },
                RuntimePrincipalRecord {
                    idx: 1,
                    email: "charlie@example.com".into(),
                    manager_idx: Some(2),
                    department: None,
                    role: None,
                    location: None,
                },
                RuntimePrincipalRecord {
                    idx: 2,
                    email: "bob@example.com".into(),
                    manager_idx: None,
                    department: None,
                    role: None,
                    location: None,
                },
            ],
            &[RuntimeTeamRecord {
                member_indices: vec![0, 1, 2],
            }],
            "tenant-1",
            FEATURE_SCHEMA_VERSION,
        );

        assert!(rows.iter().any(|row| {
            row.principal_entity_key == "enk_p1"
                && row.peer_entity_key == "enk_p2"
                && row.peer_type == "department"
                && (row.peer_weight - 1.0).abs() < 1e-9
        }));
        assert!(rows.iter().any(|row| {
            row.principal_entity_key == "enk_p1"
                && row.peer_entity_key == "enk_p2"
                && row.peer_type == "manager"
                && (row.peer_weight - 1.0).abs() < 1e-9
        }));
        assert!(rows.iter().any(|row| {
            row.principal_entity_key == "enk_p1"
                && row.peer_entity_key == "enk_p3"
                && row.peer_type == "group"
                && (row.peer_weight - (1.0 / 3.0)).abs() < 1e-9
        }));
    }
}
