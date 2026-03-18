use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::feature_runtime::load_workspace_normalized_events;
use crate::local_runtime::load_ingest_manifest;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventIndexFile {
    pub index_version: usize,
    pub input_signature: String,
    pub event_count: usize,
    #[serde(default)]
    pub events_by_id: BTreeMap<String, Value>,
    #[serde(default)]
    pub principal_event_ids: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub resource_event_ids: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub department_event_ids: BTreeMap<String, Vec<String>>,
}

fn write_json<T: Serialize>(path: &Path, payload: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(payload)?)?;
    Ok(())
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

pub fn event_index_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("event_index.json")
}

fn event_index_input_signature(workspace: &Path) -> anyhow::Result<String> {
    let manifest = load_ingest_manifest(workspace)?;
    let mut payload = BTreeMap::new();
    let mut normalized_event_ids = manifest.normalized_event_ids;
    let mut normalized_event_keys = manifest.normalized_event_keys;
    normalized_event_ids.sort();
    normalized_event_keys.sort();
    payload.insert(
        "normalized_event_ids",
        serde_json::to_value(normalized_event_ids)?,
    );
    payload.insert(
        "normalized_event_keys",
        serde_json::to_value(normalized_event_keys)?,
    );
    let bytes = serde_json::to_vec(&payload)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

pub fn load_event_index(workspace: &Path) -> anyhow::Result<EventIndexFile> {
    read_json_or_default(&event_index_path(workspace))
}

pub fn save_event_index(workspace: &Path, payload: &EventIndexFile) -> anyhow::Result<()> {
    write_json(&event_index_path(workspace), payload)
}

pub fn rebuild_event_index(workspace: &Path) -> anyhow::Result<EventIndexFile> {
    let mut events = load_workspace_normalized_events(workspace)?;
    events.sort_by(|left: &Value, right: &Value| {
        right
            .get("observed_at")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(
                left.get("observed_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .then(
                right
                    .get("event_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .cmp(
                        left.get("event_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    ),
            )
    });

    let mut events_by_id = BTreeMap::new();
    let mut principal_event_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut resource_event_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut department_event_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for event in events {
        let Some(event_id) = event.get("event_id").and_then(Value::as_str) else {
            continue;
        };
        if event_id.is_empty() {
            continue;
        }
        events_by_id.insert(event_id.to_string(), event.clone());
        let principal = event
            .get("principal")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let resource = event
            .get("resource")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let department = principal
            .get("department")
            .and_then(Value::as_str)
            .unwrap_or_default();
        for reference in ["id", "entity_id"] {
            if let Some(value) = principal.get(reference).and_then(Value::as_str) {
                if !value.is_empty() {
                    principal_event_ids
                        .entry(value.to_string())
                        .or_default()
                        .push(event_id.to_string());
                }
            }
            if let Some(value) = resource.get(reference).and_then(Value::as_str) {
                if !value.is_empty() {
                    resource_event_ids
                        .entry(value.to_string())
                        .or_default()
                        .push(event_id.to_string());
                }
            }
        }
        if !department.is_empty() {
            department_event_ids
                .entry(department.to_string())
                .or_default()
                .push(event_id.to_string());
        }
    }

    let payload = EventIndexFile {
        index_version: 1,
        input_signature: event_index_input_signature(workspace)?,
        event_count: events_by_id.len(),
        events_by_id,
        principal_event_ids,
        resource_event_ids,
        department_event_ids,
    };
    save_event_index(workspace, &payload)?;
    Ok(payload)
}

pub fn ensure_event_index(workspace: &Path) -> anyhow::Result<EventIndexFile> {
    let current = load_event_index(workspace)?;
    let expected_signature = event_index_input_signature(workspace)?;
    if current.input_signature == expected_signature {
        return Ok(current);
    }
    rebuild_event_index(workspace)
}
