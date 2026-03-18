use std::path::{Path, PathBuf};

use chrono::Utc;
use seccloud_lake::batch_writer::{
    FeatureBatchDescriptor, FeatureBatchWriteInput, write_action_feature_batch,
    write_collaboration_feature_batch, write_history_feature_batch, write_peer_group_feature_batch,
    write_static_principal_feature_batch,
};
use seccloud_lake::object_store::{LocalObjectStore, ObjectStore};
use seccloud_lake::schema::FEATURE_SCHEMA_VERSION;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::features::{
    RuntimePrincipalRecord, RuntimeTeamRecord, build_action_feature_rows,
    build_collaboration_feature_rows, build_history_feature_rows, build_peer_group_feature_rows,
    build_static_principal_feature_rows,
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FeatureBuildResult {
    pub normalized_event_count: usize,
    pub action_feature_row_count: usize,
    pub history_feature_row_count: usize,
    pub collaboration_feature_row_count: usize,
    pub static_feature_row_count: usize,
    pub peer_group_feature_row_count: usize,
    pub action_manifest_key: Option<String>,
    pub history_manifest_key: Option<String>,
    pub collaboration_manifest_key: Option<String>,
    pub static_manifest_key: Option<String>,
    pub peer_group_manifest_key: Option<String>,
    pub skipped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedInputs {
    events: Vec<Value>,
    batch_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct FeatureVocab {
    principal_entity_keys: Vec<String>,
    resource_entity_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeOrg {
    principals: Vec<RuntimePrincipalRecord>,
    teams: Vec<RuntimeTeamRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct FeatureBuildState {
    input_signature: String,
    normalized_event_count: usize,
    action_feature_row_count: usize,
    history_feature_row_count: usize,
    collaboration_feature_row_count: usize,
    static_feature_row_count: usize,
    peer_group_feature_row_count: usize,
    action_manifest_key: Option<String>,
    history_manifest_key: Option<String>,
    collaboration_manifest_key: Option<String>,
    static_manifest_key: Option<String>,
    peer_group_manifest_key: Option<String>,
}

fn feature_state_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("feature_state.json")
}

fn feature_vocab_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("feature_vocab.json")
}

fn load_feature_state(workspace: &Path) -> anyhow::Result<FeatureBuildState> {
    let path = feature_state_path(workspace);
    if !path.exists() {
        return Ok(FeatureBuildState::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn save_feature_state(workspace: &Path, state: &FeatureBuildState) -> anyhow::Result<()> {
    let path = feature_state_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

fn save_feature_vocab(workspace: &Path, vocab: &FeatureVocab) -> anyhow::Result<()> {
    let path = feature_vocab_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(vocab)?)?;
    Ok(())
}

fn identity_profiles_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("identity_profiles.json")
}

fn load_identity_profiles(workspace: &Path) -> anyhow::Result<RuntimeOrg> {
    let path = identity_profiles_path(workspace);
    if path.exists() {
        let profiles: RuntimeOrg = serde_json::from_slice(&std::fs::read(&path)?)?;
        if !profiles.principals.is_empty() || !profiles.teams.is_empty() {
            return Ok(profiles);
        }
    }
    Ok(RuntimeOrg::default())
}

fn input_signature(events: &[Value], runtime_org: &RuntimeOrg) -> anyhow::Result<String> {
    let mut event_ids: Vec<String> = events
        .iter()
        .filter_map(|event| {
            event
                .get("event_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect();
    event_ids.sort();
    let mut hasher = Sha256::new();
    hasher.update(event_ids.join("|").as_bytes());
    hasher.update(b"|identity_profiles|");
    hasher.update(serde_json::to_vec(runtime_org)?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn manifest_exists(workspace: &Path, key: &Option<String>) -> bool {
    key.as_ref()
        .map(|path| workspace.join(path).exists())
        .unwrap_or(false)
}

fn feature_manifest_ready(workspace: &Path, key: &Option<String>, row_count: usize) -> bool {
    row_count == 0 || manifest_exists(workspace, key)
}

fn feature_manifests_exist(workspace: &Path, state: &FeatureBuildState) -> bool {
    feature_manifest_ready(
        workspace,
        &state.action_manifest_key,
        state.action_feature_row_count,
    ) && feature_manifest_ready(
        workspace,
        &state.history_manifest_key,
        state.history_feature_row_count,
    ) && feature_manifest_ready(
        workspace,
        &state.collaboration_manifest_key,
        state.collaboration_feature_row_count,
    ) && feature_manifest_ready(
        workspace,
        &state.static_manifest_key,
        state.static_feature_row_count,
    ) && feature_manifest_ready(
        workspace,
        &state.peer_group_manifest_key,
        state.peer_group_feature_row_count,
    ) && feature_vocab_path(workspace).exists()
}

fn build_feature_vocab(
    static_rows: &[seccloud_lake::schema::StaticPrincipalFeatureRow],
    action_rows: &[seccloud_lake::schema::ActionFeatureRow],
    history_rows: &[seccloud_lake::schema::HistoryFeatureRow],
    collaboration_rows: &[seccloud_lake::schema::CollaborationFeatureRow],
    peer_group_rows: &[seccloud_lake::schema::PeerGroupFeatureRow],
) -> FeatureVocab {
    let mut principal_entity_keys: Vec<String> = static_rows
        .iter()
        .map(|row| row.principal_entity_key.clone())
        .chain(
            action_rows
                .iter()
                .map(|row| row.principal_entity_key.clone()),
        )
        .chain(
            history_rows
                .iter()
                .map(|row| row.principal_entity_key.clone()),
        )
        .chain(
            collaboration_rows
                .iter()
                .map(|row| row.principal_entity_key.clone()),
        )
        .chain(
            collaboration_rows
                .iter()
                .map(|row| row.collaborator_entity_key.clone()),
        )
        .chain(
            peer_group_rows
                .iter()
                .map(|row| row.principal_entity_key.clone()),
        )
        .chain(
            peer_group_rows
                .iter()
                .map(|row| row.peer_entity_key.clone()),
        )
        .collect();
    principal_entity_keys.sort();
    principal_entity_keys.dedup();

    let mut resource_entity_keys: Vec<String> = action_rows
        .iter()
        .map(|row| row.resource_entity_key.clone())
        .chain(
            history_rows
                .iter()
                .map(|row| row.resource_entity_key.clone()),
        )
        .collect();
    resource_entity_keys.sort();
    resource_entity_keys.dedup();

    FeatureVocab {
        principal_entity_keys,
        resource_entity_keys,
    }
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
            } else if let Some(ext) = path.extension().and_then(|value| value.to_str()) {
                if ext == extension {
                    out.push(path);
                }
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, extension, &mut files)?;
    files.sort();
    Ok(files)
}

fn load_upstream_normalized_batch_ids(workspace: &Path) -> anyhow::Result<Vec<String>> {
    let mut batch_ids = Vec::new();
    for path in collect_files(
        &workspace
            .join("lake")
            .join("manifests")
            .join("layout=v1")
            .join("type=normalized"),
        "json",
    )? {
        let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        if let Some(batch_id) = value.get("batch_id").and_then(Value::as_str) {
            batch_ids.push(batch_id.to_string());
        }
    }
    batch_ids.sort();
    batch_ids.dedup();
    Ok(batch_ids)
}

fn load_workspace_normalized_inputs(workspace: &Path) -> anyhow::Result<NormalizedInputs> {
    let mut events = Vec::new();
    for path in collect_files(&workspace.join("lake").join("normalized"), "parquet")? {
        let bytes = std::fs::read(path)?;
        for batch in seccloud_lake::schema::read_parquet_bytes(&bytes)? {
            for row in seccloud_lake::schema::normalized_record_batch_to_rows(&batch)? {
                events.push(serde_json::from_str(&row.payload_json)?);
            }
        }
    }
    let batch_ids = load_upstream_normalized_batch_ids(workspace)?;
    if !events.is_empty() {
        return Ok(NormalizedInputs { events, batch_ids });
    }

    for path in collect_files(&workspace.join("normalized"), "json")? {
        let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        events.push(value);
    }
    Ok(NormalizedInputs { events, batch_ids })
}

pub fn load_workspace_normalized_events(workspace: &Path) -> anyhow::Result<Vec<Value>> {
    Ok(load_workspace_normalized_inputs(workspace)?.events)
}

pub async fn build_workspace_feature_tables(
    workspace: &Path,
    tenant_id: &str,
) -> anyhow::Result<FeatureBuildResult> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(workspace));
    let inputs = load_workspace_normalized_inputs(workspace)?;
    let events = inputs.events;
    let runtime_org = load_identity_profiles(workspace)?;
    let signature = input_signature(&events, &runtime_org)?;
    let prior_state = load_feature_state(workspace)?;
    if prior_state.input_signature == signature && feature_manifests_exist(workspace, &prior_state)
    {
        return Ok(FeatureBuildResult {
            normalized_event_count: prior_state.normalized_event_count,
            action_feature_row_count: prior_state.action_feature_row_count,
            history_feature_row_count: prior_state.history_feature_row_count,
            collaboration_feature_row_count: prior_state.collaboration_feature_row_count,
            static_feature_row_count: prior_state.static_feature_row_count,
            peer_group_feature_row_count: prior_state.peer_group_feature_row_count,
            action_manifest_key: prior_state.action_manifest_key,
            history_manifest_key: prior_state.history_manifest_key,
            collaboration_manifest_key: prior_state.collaboration_manifest_key,
            static_manifest_key: prior_state.static_manifest_key,
            peer_group_manifest_key: prior_state.peer_group_manifest_key,
            skipped: true,
        });
    }

    let action_rows = build_action_feature_rows(&events, tenant_id, FEATURE_SCHEMA_VERSION)?;
    let history_rows = build_history_feature_rows(&events, tenant_id, FEATURE_SCHEMA_VERSION)?;
    let collaboration_rows =
        build_collaboration_feature_rows(&events, tenant_id, FEATURE_SCHEMA_VERSION)?;
    let static_rows = build_static_principal_feature_rows(
        &events,
        &runtime_org.principals,
        tenant_id,
        FEATURE_SCHEMA_VERSION,
    )?;
    let peer_group_rows = build_peer_group_feature_rows(
        &static_rows,
        &runtime_org.principals,
        &runtime_org.teams,
        tenant_id,
        FEATURE_SCHEMA_VERSION,
    );
    let checkpoint_event_id = events
        .iter()
        .filter_map(|event| event.get("event_id").and_then(Value::as_str))
        .max()
        .unwrap_or("")
        .to_string();

    let action_manifest_key = if action_rows.is_empty() {
        None
    } else {
        Some(
            write_action_feature_batch(
                store.as_ref(),
                FeatureBatchWriteInput {
                    descriptor: FeatureBatchDescriptor {
                        batch_id: format!("feat_{}", uuid::Uuid::now_v7()),
                        tenant_id: tenant_id.to_string(),
                        table_name: "action-accessor-set".into(),
                        source: None,
                        generated_at: Utc::now(),
                    },
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    upstream_normalized_batches: inputs.batch_ids.clone(),
                    checkpoint_extra: serde_json::Map::new(),
                    checkpoint_event_id: checkpoint_event_id.clone(),
                    rows: action_rows.clone(),
                },
            )
            .await?
            .manifest_key,
        )
    };

    let history_manifest_key = if history_rows.is_empty() {
        None
    } else {
        Some(
            write_history_feature_batch(
                store.as_ref(),
                FeatureBatchWriteInput {
                    descriptor: FeatureBatchDescriptor {
                        batch_id: format!("feat_{}", uuid::Uuid::now_v7()),
                        tenant_id: tenant_id.to_string(),
                        table_name: "history-window".into(),
                        source: None,
                        generated_at: Utc::now(),
                    },
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    upstream_normalized_batches: inputs.batch_ids.clone(),
                    checkpoint_extra: serde_json::Map::new(),
                    checkpoint_event_id: checkpoint_event_id.clone(),
                    rows: history_rows.clone(),
                },
            )
            .await?
            .manifest_key,
        )
    };

    let collaboration_manifest_key = if collaboration_rows.is_empty() {
        None
    } else {
        Some(
            write_collaboration_feature_batch(
                store.as_ref(),
                FeatureBatchWriteInput {
                    descriptor: FeatureBatchDescriptor {
                        batch_id: format!("feat_{}", uuid::Uuid::now_v7()),
                        tenant_id: tenant_id.to_string(),
                        table_name: "collaboration".into(),
                        source: None,
                        generated_at: Utc::now(),
                    },
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    upstream_normalized_batches: inputs.batch_ids.clone(),
                    checkpoint_extra: serde_json::Map::new(),
                    checkpoint_event_id: checkpoint_event_id.clone(),
                    rows: collaboration_rows.clone(),
                },
            )
            .await?
            .manifest_key,
        )
    };

    let static_manifest_key = if static_rows.is_empty() {
        None
    } else {
        Some(
            write_static_principal_feature_batch(
                store.as_ref(),
                FeatureBatchWriteInput {
                    descriptor: FeatureBatchDescriptor {
                        batch_id: format!("feat_{}", uuid::Uuid::now_v7()),
                        tenant_id: tenant_id.to_string(),
                        table_name: "principal-static".into(),
                        source: None,
                        generated_at: Utc::now(),
                    },
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    upstream_normalized_batches: inputs.batch_ids.clone(),
                    checkpoint_extra: serde_json::Map::new(),
                    checkpoint_event_id: checkpoint_event_id.clone(),
                    rows: static_rows.clone(),
                },
            )
            .await?
            .manifest_key,
        )
    };

    let peer_group_manifest_key = if peer_group_rows.is_empty() {
        None
    } else {
        Some(
            write_peer_group_feature_batch(
                store.as_ref(),
                FeatureBatchWriteInput {
                    descriptor: FeatureBatchDescriptor {
                        batch_id: format!("feat_{}", uuid::Uuid::now_v7()),
                        tenant_id: tenant_id.to_string(),
                        table_name: "peer-group".into(),
                        source: None,
                        generated_at: Utc::now(),
                    },
                    feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                    upstream_normalized_batches: inputs.batch_ids.clone(),
                    checkpoint_extra: serde_json::Map::new(),
                    checkpoint_event_id: checkpoint_event_id.clone(),
                    rows: peer_group_rows.clone(),
                },
            )
            .await?
            .manifest_key,
        )
    };

    let result = FeatureBuildResult {
        normalized_event_count: events.len(),
        action_feature_row_count: action_rows.len(),
        history_feature_row_count: history_rows.len(),
        collaboration_feature_row_count: collaboration_rows.len(),
        static_feature_row_count: static_rows.len(),
        peer_group_feature_row_count: peer_group_rows.len(),
        action_manifest_key,
        history_manifest_key,
        collaboration_manifest_key,
        static_manifest_key,
        peer_group_manifest_key,
        skipped: false,
    };
    save_feature_vocab(
        workspace,
        &build_feature_vocab(
            &static_rows,
            &action_rows,
            &history_rows,
            &collaboration_rows,
            &peer_group_rows,
        ),
    )?;
    save_feature_state(
        workspace,
        &FeatureBuildState {
            input_signature: signature,
            normalized_event_count: result.normalized_event_count,
            action_feature_row_count: result.action_feature_row_count,
            history_feature_row_count: result.history_feature_row_count,
            collaboration_feature_row_count: result.collaboration_feature_row_count,
            static_feature_row_count: result.static_feature_row_count,
            peer_group_feature_row_count: result.peer_group_feature_row_count,
            action_manifest_key: result.action_manifest_key.clone(),
            history_manifest_key: result.history_manifest_key.clone(),
            collaboration_manifest_key: result.collaboration_manifest_key.clone(),
            static_manifest_key: result.static_manifest_key.clone(),
            peer_group_manifest_key: result.peer_group_manifest_key.clone(),
        },
    )?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use seccloud_lake::schema::{
        NORMALIZED_SCHEMA_VERSION, NormalizedEventRow, normalized_event_record_batch,
        write_parquet_bytes,
    };

    fn test_workspace() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "seccloud-feature-runtime-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(root.join("normalized/source=gworkspace")).unwrap();
        std::fs::create_dir_all(root.join("lake/normalized")).unwrap();
        root
    }

    fn write_event(root: &Path, name: &str, value: &Value) {
        let path = root
            .join("normalized/source=gworkspace")
            .join(format!("{name}.json"));
        std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    fn write_normalized_manifest(root: &Path, batch_id: &str) {
        let path = root.join(
            "lake/manifests/layout=v1/type=normalized/tenant=tenant-1/source=gworkspace/integration=default/dt=2026-03-15/hour=14",
        );
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(
            path.join(format!("batch={batch_id}.json")),
            serde_json::to_vec(&serde_json::json!({
                "batch_id": batch_id,
                "manifest_type": "normalized"
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn write_parquet_event(root: &Path, name: &str, value: &Value) {
        let row = NormalizedEventRow {
            normalized_schema_version: NORMALIZED_SCHEMA_VERSION.into(),
            tenant_id: "tenant-1".into(),
            source: "gworkspace".into(),
            integration_id: "default".into(),
            observed_at: value["observed_at"].as_str().unwrap().into(),
            event_id: value["event_id"].as_str().unwrap().into(),
            event_key: value["event_key"].as_str().unwrap().into(),
            source_event_id: value["source_event_id"].as_str().unwrap().into(),
            principal_entity_key: value["principal"]["entity_key"].as_str().unwrap().into(),
            resource_entity_key: value["resource"]["entity_key"].as_str().unwrap().into(),
            action_source: value["action"]["source"].as_str().unwrap().into(),
            action_verb: value["action"]["verb"].as_str().unwrap().into(),
            action_category: value["action"]["category"].as_str().unwrap().into(),
            payload_json: serde_json::to_string(value).unwrap(),
        };
        let batch = normalized_event_record_batch(&[row]).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let path = root.join("lake/normalized").join(format!("{name}.parquet"));
        std::fs::write(path, bytes).unwrap();
    }

    #[tokio::test]
    async fn workspace_feature_builder_writes_feature_tables() {
        let root = test_workspace();
        let evt1 = serde_json::json!({
            "event_id": "evt_1",
            "event_key": "evk_1",
            "source": "gworkspace",
            "source_event_id": "src_1",
            "principal": {"entity_key": "enk_p1"},
            "resource": {"entity_key": "enk_r1"},
            "action": {"source": "gworkspace", "verb": "read", "category": "access"},
            "observed_at": "2026-03-15T14:05:00Z"
        });
        let evt2 = serde_json::json!({
            "event_id": "evt_2",
            "event_key": "evk_2",
            "source": "gworkspace",
            "source_event_id": "src_2",
            "principal": {"entity_key": "enk_p2"},
            "resource": {"entity_key": "enk_r1"},
            "action": {"source": "gworkspace", "verb": "read", "category": "access"},
            "observed_at": "2026-03-15T14:15:00Z"
        });
        write_parquet_event(&root, "evt1", &evt1);
        write_parquet_event(&root, "evt2", &evt2);
        write_normalized_manifest(&root, "norm_1");
        write_normalized_manifest(&root, "norm_2");

        let result = build_workspace_feature_tables(&root, "tenant-1")
            .await
            .unwrap();

        assert_eq!(result.normalized_event_count, 2);
        assert_eq!(result.action_feature_row_count, 2);
        assert_eq!(result.history_feature_row_count, 2);
        assert_eq!(result.collaboration_feature_row_count, 2);
        assert_eq!(result.static_feature_row_count, 2);
        assert_eq!(result.peer_group_feature_row_count, 2);
        assert!(result.action_manifest_key.is_some());
        assert!(result.history_manifest_key.is_some());
        assert!(result.collaboration_manifest_key.is_some());
        assert!(result.static_manifest_key.is_some());
        assert!(result.peer_group_manifest_key.is_some());
        assert!(!result.skipped);

        let action_manifest_path = root.join(result.action_manifest_key.unwrap());
        let history_manifest_path = root.join(result.history_manifest_key.unwrap());
        let collaboration_manifest_path = root.join(result.collaboration_manifest_key.unwrap());
        let static_manifest_path = root.join(result.static_manifest_key.unwrap());
        let peer_group_manifest_path = root.join(result.peer_group_manifest_key.unwrap());
        let feature_vocab_path = root.join("manifests/feature_vocab.json");
        assert!(action_manifest_path.exists());
        assert!(history_manifest_path.exists());
        assert!(collaboration_manifest_path.exists());
        assert!(static_manifest_path.exists());
        assert!(peer_group_manifest_path.exists());
        assert!(feature_vocab_path.exists());

        let action_manifest: Value =
            serde_json::from_slice(&std::fs::read(action_manifest_path).unwrap()).unwrap();
        let history_manifest: Value =
            serde_json::from_slice(&std::fs::read(history_manifest_path).unwrap()).unwrap();
        let collaboration_manifest: Value =
            serde_json::from_slice(&std::fs::read(collaboration_manifest_path).unwrap()).unwrap();
        let static_manifest: Value =
            serde_json::from_slice(&std::fs::read(static_manifest_path).unwrap()).unwrap();
        let peer_group_manifest: Value =
            serde_json::from_slice(&std::fs::read(peer_group_manifest_path).unwrap()).unwrap();
        assert_eq!(
            action_manifest["upstream_normalized_batches"],
            serde_json::json!(["norm_1", "norm_2"])
        );
        assert_eq!(
            history_manifest["upstream_normalized_batches"],
            serde_json::json!(["norm_1", "norm_2"])
        );
        assert_eq!(
            collaboration_manifest["upstream_normalized_batches"],
            serde_json::json!(["norm_1", "norm_2"])
        );
        assert_eq!(
            static_manifest["upstream_normalized_batches"],
            serde_json::json!(["norm_1", "norm_2"])
        );
        assert_eq!(
            peer_group_manifest["upstream_normalized_batches"],
            serde_json::json!(["norm_1", "norm_2"])
        );
        let action_object_key = action_manifest["objects"][0]["object_key"]
            .as_str()
            .unwrap();
        let history_object_key = history_manifest["objects"][0]["object_key"]
            .as_str()
            .unwrap();
        let collaboration_object_key = collaboration_manifest["objects"][0]["object_key"]
            .as_str()
            .unwrap();
        let static_object_key = static_manifest["objects"][0]["object_key"]
            .as_str()
            .unwrap();
        let peer_group_object_key = peer_group_manifest["objects"][0]["object_key"]
            .as_str()
            .unwrap();

        let action_bytes = std::fs::read(root.join(action_object_key)).unwrap();
        let history_bytes = std::fs::read(root.join(history_object_key)).unwrap();
        let collaboration_bytes = std::fs::read(root.join(collaboration_object_key)).unwrap();
        let static_bytes = std::fs::read(root.join(static_object_key)).unwrap();
        let peer_group_bytes = std::fs::read(root.join(peer_group_object_key)).unwrap();
        let feature_vocab: Value =
            serde_json::from_slice(&std::fs::read(feature_vocab_path).unwrap()).unwrap();
        assert_eq!(
            seccloud_lake::schema::read_parquet_bytes(&action_bytes).unwrap()[0].num_rows(),
            2
        );
        assert_eq!(
            seccloud_lake::schema::read_parquet_bytes(&history_bytes).unwrap()[0].num_rows(),
            2
        );
        assert_eq!(
            seccloud_lake::schema::read_parquet_bytes(&collaboration_bytes).unwrap()[0].num_rows(),
            2
        );
        assert_eq!(
            seccloud_lake::schema::read_parquet_bytes(&static_bytes).unwrap()[0].num_rows(),
            2
        );
        assert_eq!(
            seccloud_lake::schema::read_parquet_bytes(&peer_group_bytes).unwrap()[0].num_rows(),
            2
        );
        assert_eq!(
            feature_vocab["principal_entity_keys"],
            serde_json::json!(["enk_p1", "enk_p2"])
        );
        assert_eq!(
            feature_vocab["resource_entity_keys"],
            serde_json::json!(["enk_r1"])
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn second_feature_build_skips_when_inputs_are_unchanged() {
        let root = test_workspace();
        let evt = serde_json::json!({
            "event_id": "evt_1",
            "event_key": "evk_1",
            "source": "gworkspace",
            "source_event_id": "src_1",
            "principal": {"entity_key": "enk_p1"},
            "resource": {"entity_key": "enk_r1"},
            "action": {"source": "gworkspace", "verb": "read", "category": "access"},
            "observed_at": "2026-03-15T14:05:00Z"
        });
        write_parquet_event(&root, "evt1", &evt);

        let first = build_workspace_feature_tables(&root, "tenant-1")
            .await
            .unwrap();
        let second = build_workspace_feature_tables(&root, "tenant-1")
            .await
            .unwrap();

        assert!(!first.skipped);
        assert!(second.skipped);
        assert_eq!(first.action_manifest_key, second.action_manifest_key);
        assert_eq!(first.history_manifest_key, second.history_manifest_key);
        assert_eq!(
            first.collaboration_manifest_key,
            second.collaboration_manifest_key
        );
        assert_eq!(first.static_manifest_key, second.static_manifest_key);
        assert_eq!(
            first.peer_group_manifest_key,
            second.peer_group_manifest_key
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn feature_build_recomputes_when_identity_profiles_change() {
        let root = test_workspace();
        let evt1 = serde_json::json!({
            "event_id": "evt_1",
            "event_key": "evk_1",
            "source": "gworkspace",
            "source_event_id": "src_1",
            "principal": {
                "entity_key": "enk_p1",
                "id": "alice@example.com",
                "department": "security",
                "attributes": {"role": "security-admin"}
            },
            "resource": {"entity_key": "enk_r1"},
            "action": {"source": "gworkspace", "verb": "read", "category": "access"},
            "attributes": {"geo": "US-NY"},
            "observed_at": "2026-03-15T14:05:00Z"
        });
        let evt2 = serde_json::json!({
            "event_id": "evt_2",
            "event_key": "evk_2",
            "source": "gworkspace",
            "source_event_id": "src_2",
            "principal": {
                "entity_key": "enk_p2",
                "id": "bob@example.com",
                "department": "security",
                "attributes": {"role": "manager"}
            },
            "resource": {"entity_key": "enk_r1"},
            "action": {"source": "gworkspace", "verb": "read", "category": "access"},
            "attributes": {"geo": "US-CA"},
            "observed_at": "2026-03-15T14:15:00Z"
        });
        write_parquet_event(&root, "evt1", &evt1);
        write_parquet_event(&root, "evt2", &evt2);

        let first = build_workspace_feature_tables(&root, "tenant-1")
            .await
            .unwrap();
        assert_eq!(first.peer_group_feature_row_count, 2);

        std::fs::create_dir_all(root.join("manifests")).unwrap();
        std::fs::write(
            root.join("manifests/identity_profiles.json"),
            serde_json::to_vec(&serde_json::json!({
                "manifest_version": 1,
                "source": "test",
                "generated_at": "2026-03-15T14:16:00Z",
                "principals": [
                    {"idx": 0, "email": "alice@example.com", "manager_idx": 1},
                    {"idx": 1, "email": "bob@example.com", "manager_idx": null}
                ],
                "teams": [
                    {"member_indices": [0, 1]}
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let second = build_workspace_feature_tables(&root, "tenant-1")
            .await
            .unwrap();
        assert!(!second.skipped);
        assert_eq!(second.peer_group_feature_row_count, 4);
        assert_ne!(
            first.peer_group_manifest_key,
            second.peer_group_manifest_key
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn normalized_loader_falls_back_to_json_workspace_tree() {
        let root = test_workspace();
        write_event(
            &root,
            "evt1",
            &serde_json::json!({
                "event_id": "evt_1",
                "event_key": "evk_1",
                "source": "gworkspace",
                "source_event_id": "src_1",
                "principal": {"entity_key": "enk_p1"},
                "resource": {"entity_key": "enk_r1"},
                "action": {"source": "gworkspace", "verb": "read", "category": "access"},
                "observed_at": "2026-03-15T14:05:00Z"
            }),
        );

        let events = load_workspace_normalized_events(&root).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_id"], "evt_1");

        std::fs::remove_dir_all(root).unwrap();
    }
}
