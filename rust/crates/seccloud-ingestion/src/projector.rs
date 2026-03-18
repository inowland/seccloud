use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::Utc;
use seccloud_lake::manifest::{DetectionManifest, NormalizedManifest};
use seccloud_lake::schema::{
    NORMALIZED_SCHEMA_VERSION, detection_record_batch_to_rows, normalized_record_batch_to_rows,
    read_parquet_bytes,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::event_index::ensure_event_index;

const PROJECTED_EVENTS_TABLE: &str = "projected_events";
const PROJECTED_DETECTIONS_TABLE: &str = "projected_detections";
const PROJECTION_STATE_TABLE: &str = "projection_state";
const HOT_EVENT_INDEX_TABLE: &str = "hot_event_index";
const DETECTION_EVENT_EDGE_TABLE: &str = "detection_event_edge";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ProjectionSyncResult {
    pub dsn: String,
    pub event_count: usize,
    pub detection_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RawPointer {
    pointer_version: usize,
    object_key: String,
    object_format: String,
    sha256: String,
    manifest_key: String,
    retention_class: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_locator: Option<BTreeMap<String, usize>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct NormalizedPointer {
    pointer_version: usize,
    object_key: String,
    object_format: String,
    sha256: String,
    manifest_key: String,
    record_locator: BTreeMap<String, usize>,
    retention_class: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct ProjectionEventRow {
    tenant_id: String,
    event_id: String,
    event_key: String,
    normalized_schema_version: String,
    source: String,
    integration_id: Option<String>,
    source_event_id: String,
    observed_at: String,
    principal_entity_id: String,
    principal_id: String,
    principal_display_name: String,
    principal_email: String,
    principal_department: String,
    resource_entity_id: String,
    resource_id: String,
    resource_name: String,
    resource_kind: String,
    resource_sensitivity: String,
    action_verb: String,
    action_category: String,
    environment: Value,
    attributes: Value,
    event_payload: Value,
    normalized_pointer: NormalizedPointer,
    raw_pointer: Option<RawPointer>,
    raw_retention_state: String,
    indexed_at: String,
    updated_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct ProjectionDetectionRow {
    tenant_id: String,
    detection_id: String,
    observed_at: String,
    payload: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct DetectionEdgeRow {
    tenant_id: String,
    detection_id: String,
    event_id: String,
    ordinal: usize,
    link_role: String,
    observed_at: String,
    created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct ProjectionStateRow {
    key: String,
    payload: Value,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ProjectionPayload {
    events: Vec<ProjectionEventRow>,
    detections: Vec<ProjectionDetectionRow>,
    edges: Vec<DetectionEdgeRow>,
    states: Vec<ProjectionStateRow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct StreamStateFile {
    cursor: usize,
    total_source_events: usize,
    complete: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct OpsMetadataFile {
    workspace: Option<String>,
    #[serde(default)]
    event_counts_by_source: BTreeMap<String, usize>,
    dead_letter_count: usize,
    #[serde(default)]
    dead_letter_counts_by_source: BTreeMap<String, usize>,
    contains_raw_payloads: bool,
}

#[derive(Debug, Clone)]
struct NormalizedPointerInfo {
    manifest_key: String,
    object_key: String,
    object_format: String,
    sha256: String,
}

fn read_json_or_default<T>(path: &Path) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    if !path.exists() {
        return Ok(T::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn detections_dir(workspace: &Path) -> PathBuf {
    workspace.join("detections")
}

fn runtime_stream_manifest_path(workspace: &Path) -> PathBuf {
    workspace
        .join("manifests")
        .join("runtime_stream_manifest.json")
}

fn ops_metadata_path(workspace: &Path) -> PathBuf {
    workspace.join("ops").join("metadata.json")
}

fn read_detection_files(workspace: &Path) -> anyhow::Result<Vec<Value>> {
    let mut detections_by_id = BTreeMap::new();
    let manifest_root = workspace
        .join("lake")
        .join("manifests")
        .join("layout=v1")
        .join("type=detection");
    for manifest_path in collect_files(&manifest_root, "json")? {
        let manifest: DetectionManifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        for descriptor in manifest.objects {
            let object_path = workspace.join(&descriptor.object_key);
            if !object_path.exists() {
                continue;
            }
            let batches = read_parquet_bytes(&std::fs::read(&object_path)?)?;
            for batch in batches {
                for row in detection_record_batch_to_rows(&batch)? {
                    let payload: Value = serde_json::from_str(&row.payload_json)?;
                    if let Some(detection_id) = payload.get("detection_id").and_then(Value::as_str)
                    {
                        detections_by_id.insert(detection_id.to_string(), payload);
                    }
                }
            }
        }
    }
    let root = detections_dir(workspace);
    if !root.exists() {
        return Ok(detections_by_id.into_values().collect());
    }
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let payload: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        if let Some(detection_id) = payload.get("detection_id").and_then(Value::as_str) {
            detections_by_id.insert(detection_id.to_string(), payload);
        }
    }
    Ok(detections_by_id.into_values().collect())
}

fn collect_files(root: &Path, extension: &str) -> anyhow::Result<Vec<PathBuf>> {
    fn visit(dir: &Path, extension: &str, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(&path, extension, out)?;
            } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, extension, &mut files)?;
    files.sort();
    Ok(files)
}

fn load_normalized_pointer_map(
    workspace: &Path,
) -> anyhow::Result<BTreeMap<String, NormalizedPointerInfo>> {
    let mut pointers = BTreeMap::new();
    let manifest_root = workspace
        .join("lake")
        .join("manifests")
        .join("layout=v1")
        .join("type=normalized");
    for manifest_path in collect_files(&manifest_root, "json")? {
        let manifest_key = manifest_path
            .strip_prefix(workspace)?
            .to_string_lossy()
            .replace('\\', "/");
        let manifest: NormalizedManifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        let Some(descriptor) = manifest.objects.first() else {
            continue;
        };
        let object_path = workspace.join(&descriptor.object_key);
        if !object_path.exists() {
            continue;
        }
        let batches = read_parquet_bytes(&std::fs::read(&object_path)?)?;
        for batch in batches {
            for row in normalized_record_batch_to_rows(&batch)? {
                pointers.insert(
                    row.event_id,
                    NormalizedPointerInfo {
                        manifest_key: manifest_key.clone(),
                        object_key: descriptor.object_key.clone(),
                        object_format: descriptor.object_format.clone(),
                        sha256: descriptor.sha256.clone(),
                    },
                );
            }
        }
    }
    Ok(pointers)
}

fn now_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn raw_pointer_for_event(workspace: &Path, event: &Value) -> (Option<RawPointer>, String) {
    let Some(attributes) = event.get("attributes").and_then(Value::as_object) else {
        return (None, "unknown".to_string());
    };
    let Some(manifest_key) = attributes.get("raw_manifest_key").and_then(Value::as_str) else {
        return (None, "unknown".to_string());
    };
    let Some(object_key) = attributes
        .get("raw_batch_object_key")
        .and_then(Value::as_str)
    else {
        return (None, "unknown".to_string());
    };
    let object_sha256 = attributes
        .get("raw_object_sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let object_format = attributes
        .get("raw_object_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if object_sha256.is_empty() || object_format.is_empty() {
        return (None, "unknown".to_string());
    }
    let raw_available = workspace.join(object_key).exists();
    let record_locator = attributes
        .get("raw_record_ordinal")
        .and_then(Value::as_u64)
        .map(|ordinal| BTreeMap::from([("ordinal".to_string(), ordinal as usize)]));
    (
        Some(RawPointer {
            pointer_version: 1,
            object_key: object_key.to_string(),
            object_format: object_format.to_string(),
            sha256: object_sha256.to_string(),
            manifest_key: manifest_key.to_string(),
            retention_class: "raw_hot".to_string(),
            record_locator,
        }),
        if raw_available {
            "available"
        } else {
            "expired"
        }
        .to_string(),
    )
}

fn projection_event_row(
    workspace: &Path,
    tenant_id: &str,
    event: &Value,
    normalized_pointers: &BTreeMap<String, NormalizedPointerInfo>,
) -> anyhow::Result<ProjectionEventRow> {
    let principal = event
        .get("principal")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("event principal missing"))?;
    let resource = event
        .get("resource")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("event resource missing"))?;
    let action = event
        .get("action")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("event action missing"))?;
    let event_id = event
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("event_id missing"))?;
    let observed_at = event
        .get("observed_at")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("observed_at missing"))?;
    let normalized_info = normalized_pointers
        .get(event_id)
        .ok_or_else(|| anyhow::anyhow!("normalized pointer missing for {}", event_id))?;
    let (raw_pointer, raw_retention_state) = raw_pointer_for_event(workspace, event);
    let indexed_at = now_timestamp();

    Ok(ProjectionEventRow {
        tenant_id: tenant_id.to_string(),
        event_id: event_id.to_string(),
        event_key: event
            .get("event_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        normalized_schema_version: NORMALIZED_SCHEMA_VERSION.to_string(),
        source: event
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        integration_id: event
            .get("integration_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        source_event_id: event
            .get("source_event_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        observed_at: observed_at.to_string(),
        principal_entity_id: principal
            .get("entity_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        principal_id: principal
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        principal_display_name: principal
            .get("display_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        principal_email: principal
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        principal_department: principal
            .get("department")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_entity_id: resource
            .get("entity_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_id: resource
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_name: resource
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_kind: resource
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        resource_sensitivity: resource
            .get("sensitivity")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        action_verb: action
            .get("verb")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        action_category: action
            .get("category")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        environment: event
            .get("environment")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default())),
        attributes: event
            .get("attributes")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default())),
        event_payload: event.clone(),
        normalized_pointer: NormalizedPointer {
            pointer_version: 1,
            object_key: normalized_info.object_key.clone(),
            object_format: normalized_info.object_format.clone(),
            sha256: normalized_info.sha256.clone(),
            manifest_key: normalized_info.manifest_key.clone(),
            record_locator: BTreeMap::from([
                ("ordinal".to_string(), 0),
                ("row_group".to_string(), 0),
            ]),
            retention_class: "normalized_retained".to_string(),
        },
        raw_pointer,
        raw_retention_state,
        indexed_at: indexed_at.clone(),
        updated_at: indexed_at,
    })
}

fn detection_observed_at(
    detection: &Value,
    event_lookup: &BTreeMap<String, ProjectionEventRow>,
) -> Option<String> {
    let anchor_id = detection
        .get("event_ids")
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(Value::as_str);
    if let Some(anchor_id) = anchor_id {
        if let Some(row) = event_lookup.get(anchor_id) {
            return Some(row.observed_at.clone());
        }
    }
    detection
        .get("evidence")
        .and_then(Value::as_array)
        .and_then(|evidence| {
            evidence
                .iter()
                .find_map(|item| item.get("observed_at").and_then(Value::as_str))
        })
        .map(ToOwned::to_owned)
}

fn build_projection_rows(workspace: &Path, tenant_id: &str) -> anyhow::Result<ProjectionPayload> {
    let event_index = ensure_event_index(workspace)?;
    let normalized_pointers = load_normalized_pointer_map(workspace)?;
    let mut events = Vec::new();
    let mut event_lookup = BTreeMap::new();
    let mut ordered_events: Vec<&Value> = event_index
        .events_by_id
        .values()
        .filter(|value| value.is_object())
        .collect();
    ordered_events.sort_by_key(|event| {
        (
            event
                .get("observed_at")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            event
                .get("event_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        )
    });
    for event in ordered_events {
        let row = projection_event_row(workspace, tenant_id, event, &normalized_pointers)?;
        event_lookup.insert(row.event_id.clone(), row.clone());
        events.push(row);
    }

    let mut detections = Vec::new();
    let mut edges = Vec::new();
    for detection in read_detection_files(workspace)? {
        let Some(detection_id) = detection.get("detection_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(observed_at) = detection_observed_at(&detection, &event_lookup) else {
            continue;
        };
        detections.push(ProjectionDetectionRow {
            tenant_id: tenant_id.to_string(),
            detection_id: detection_id.to_string(),
            observed_at: observed_at.clone(),
            payload: detection.clone(),
        });
        if let Some(event_ids) = detection.get("event_ids").and_then(Value::as_array) {
            for (ordinal, event_id) in event_ids.iter().filter_map(Value::as_str).enumerate() {
                if let Some(event_row) = event_lookup.get(event_id) {
                    edges.push(DetectionEdgeRow {
                        tenant_id: tenant_id.to_string(),
                        detection_id: detection_id.to_string(),
                        event_id: event_id.to_string(),
                        ordinal,
                        link_role: if ordinal == 0 {
                            "anchor".to_string()
                        } else {
                            "supporting".to_string()
                        },
                        observed_at: event_row.observed_at.clone(),
                        created_at: now_timestamp(),
                    });
                }
            }
        }
    }

    let stream_state: StreamStateFile =
        read_json_or_default(&runtime_stream_manifest_path(workspace))?;
    let ops_metadata: OpsMetadataFile = read_json_or_default(&ops_metadata_path(workspace))?;
    let states = vec![
        ProjectionStateRow {
            key: "stream_state".to_string(),
            payload: serde_json::to_value(stream_state)?,
        },
        ProjectionStateRow {
            key: "ops_metadata".to_string(),
            payload: serde_json::to_value(ops_metadata)?,
        },
    ];

    Ok(ProjectionPayload {
        events,
        detections,
        edges,
        states,
    })
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_json_block(value: &Value) -> anyhow::Result<String> {
    let payload = serde_json::to_string(value)?;
    let mut tag = "seccloud_json".to_string();
    while payload.contains(&format!("${tag}$")) {
        tag.push('_');
    }
    Ok(format!("${tag}${payload}${tag}$"))
}

fn ensure_projection_schema_sql() -> &'static str {
    r#"
create table if not exists projected_events (
  event_id text primary key,
  observed_at timestamptz not null,
  payload jsonb not null
);
create table if not exists projected_detections (
  tenant_id text not null,
  detection_id text primary key,
  observed_at timestamptz not null,
  payload jsonb not null
);
create table if not exists projection_state (
  key text primary key,
  payload jsonb not null
);
create table if not exists hot_event_index (
  tenant_id text not null,
  event_id text primary key,
  event_key text not null,
  normalized_schema_version text not null,
  source text not null,
  integration_id text null,
  source_event_id text not null,
  observed_at timestamptz not null,
  principal_entity_id text not null,
  principal_id text not null,
  principal_display_name text not null,
  principal_email text not null,
  principal_department text not null,
  resource_entity_id text not null,
  resource_id text not null,
  resource_name text not null,
  resource_kind text not null,
  resource_sensitivity text not null,
  action_verb text not null,
  action_category text not null,
  environment jsonb not null,
  attributes jsonb not null,
  event_payload jsonb not null,
  normalized_pointer jsonb not null,
  raw_pointer jsonb null,
  raw_retention_state text not null,
  indexed_at timestamptz not null,
  updated_at timestamptz not null
);
create table if not exists detection_event_edge (
  tenant_id text not null,
  detection_id text not null,
  event_id text not null references hot_event_index(event_id),
  ordinal integer not null,
  link_role text not null,
  observed_at timestamptz not null,
  created_at timestamptz not null,
  primary key (tenant_id, detection_id, event_id)
);
create unique index if not exists hot_event_index_tenant_event_key_idx
  on hot_event_index (tenant_id, event_key);
create index if not exists hot_event_index_tenant_observed_idx
  on hot_event_index (tenant_id, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_source_observed_idx
  on hot_event_index (tenant_id, source, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_integration_observed_idx
  on hot_event_index (tenant_id, integration_id, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_principal_entity_observed_idx
  on hot_event_index (tenant_id, principal_entity_id, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_resource_entity_observed_idx
  on hot_event_index (tenant_id, resource_entity_id, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_action_category_observed_idx
  on hot_event_index (tenant_id, action_category, observed_at desc, event_id desc);
create index if not exists hot_event_index_tenant_action_verb_observed_idx
  on hot_event_index (tenant_id, action_verb, observed_at desc, event_id desc);
create unique index if not exists detection_event_edge_tenant_detection_ordinal_idx
  on detection_event_edge (tenant_id, detection_id, ordinal);
create index if not exists detection_event_edge_tenant_event_idx
  on detection_event_edge (tenant_id, event_id, observed_at desc, detection_id desc);
alter table projected_detections
  add column if not exists tenant_id text;
update projected_detections
  set tenant_id = coalesce(tenant_id, payload->>'tenant_id')
  where tenant_id is null;
create index if not exists projected_detections_tenant_observed_idx
  on projected_detections (tenant_id, observed_at desc, detection_id desc);
"#
}

fn projection_sql(
    tenant_id: &str,
    events: &[ProjectionEventRow],
    detections: &[ProjectionDetectionRow],
    edges: &[DetectionEdgeRow],
    states: &[ProjectionStateRow],
) -> anyhow::Result<String> {
    let events_json = sql_json_block(&serde_json::to_value(events)?)?;
    let detections_json = sql_json_block(&serde_json::to_value(detections)?)?;
    let edges_json = sql_json_block(&serde_json::to_value(edges)?)?;
    let states_json = sql_json_block(&serde_json::to_value(states)?)?;
    let tenant = sql_string(tenant_id);

    Ok(format!(
        r#"
\set ON_ERROR_STOP on
begin;
{schema}
delete from {dee} where tenant_id = {tenant};
delete from {pd} where tenant_id = {tenant};
delete from {pe}
using {hei}
where {hei}.tenant_id = {tenant}
  and {pe}.event_id = {hei}.event_id;
delete from {hei} where tenant_id = {tenant};

create temp table st_events as
select * from jsonb_to_recordset({events_json}::jsonb) as x(
  tenant_id text,
  event_id text,
  event_key text,
  normalized_schema_version text,
  source text,
  integration_id text,
  source_event_id text,
  observed_at timestamptz,
  principal_entity_id text,
  principal_id text,
  principal_display_name text,
  principal_email text,
  principal_department text,
  resource_entity_id text,
  resource_id text,
  resource_name text,
  resource_kind text,
  resource_sensitivity text,
  action_verb text,
  action_category text,
  environment jsonb,
  attributes jsonb,
  event_payload jsonb,
  normalized_pointer jsonb,
  raw_pointer jsonb,
  raw_retention_state text,
  indexed_at timestamptz,
  updated_at timestamptz
);
insert into {hei} (
  tenant_id, event_id, event_key, normalized_schema_version, source, integration_id,
  source_event_id, observed_at, principal_entity_id, principal_id,
  principal_display_name, principal_email, principal_department,
  resource_entity_id, resource_id, resource_name, resource_kind,
  resource_sensitivity, action_verb, action_category, environment,
  attributes, event_payload, normalized_pointer, raw_pointer,
  raw_retention_state, indexed_at, updated_at
)
select
  tenant_id, event_id, event_key, normalized_schema_version, source, integration_id,
  source_event_id, observed_at, principal_entity_id, principal_id,
  principal_display_name, principal_email, principal_department,
  resource_entity_id, resource_id, resource_name, resource_kind,
  resource_sensitivity, action_verb, action_category, environment,
  attributes, event_payload, normalized_pointer, raw_pointer,
  raw_retention_state, indexed_at, updated_at
from st_events;
insert into {pe} (event_id, observed_at, payload)
select event_id, observed_at, event_payload from st_events;

create temp table st_detections as
select * from jsonb_to_recordset({detections_json}::jsonb) as x(
  tenant_id text,
  detection_id text,
  observed_at timestamptz,
  payload jsonb
);
insert into {pd} (tenant_id, detection_id, observed_at, payload)
select tenant_id, detection_id, observed_at, payload from st_detections;

create temp table st_edges as
select * from jsonb_to_recordset({edges_json}::jsonb) as x(
  tenant_id text,
  detection_id text,
  event_id text,
  ordinal integer,
  link_role text,
  observed_at timestamptz,
  created_at timestamptz
);
insert into {dee} (
  tenant_id, detection_id, event_id, ordinal, link_role, observed_at, created_at
)
select tenant_id, detection_id, event_id, ordinal, link_role, observed_at, created_at
from st_edges;

create temp table st_states as
select * from jsonb_to_recordset({states_json}::jsonb) as x(
  key text,
  payload jsonb
);
insert into {ps} (key, payload)
select key, payload from st_states
on conflict (key) do update set payload = excluded.payload;
commit;
"#,
        schema = ensure_projection_schema_sql(),
        tenant = tenant,
        pe = PROJECTED_EVENTS_TABLE,
        pd = PROJECTED_DETECTIONS_TABLE,
        ps = PROJECTION_STATE_TABLE,
        hei = HOT_EVENT_INDEX_TABLE,
        dee = DETECTION_EVENT_EDGE_TABLE,
        events_json = events_json,
        detections_json = detections_json,
        edges_json = edges_json,
        states_json = states_json,
    ))
}

pub fn sync_workspace_projection(
    workspace: &Path,
    tenant_id: &str,
    dsn: &str,
) -> anyhow::Result<ProjectionSyncResult> {
    let payload = build_projection_rows(workspace, tenant_id)?;
    let sql = projection_sql(
        tenant_id,
        &payload.events,
        &payload.detections,
        &payload.edges,
        &payload.states,
    )?;
    let script_path = std::env::temp_dir().join(format!(
        "seccloud-projector-{}-{}.sql",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    std::fs::write(&script_path, sql)?;
    let output = Command::new("psql")
        .arg("-X")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(dsn)
        .arg("-f")
        .arg(&script_path)
        .output()?;
    let _ = std::fs::remove_file(&script_path);
    if !output.status.success() {
        anyhow::bail!(
            "psql projection sync failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(ProjectionSyncResult {
        dsn: dsn.to_string(),
        event_count: payload.events.len(),
        detection_count: payload.detections.len(),
    })
}
