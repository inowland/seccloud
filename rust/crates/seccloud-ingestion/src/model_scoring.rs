use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::process::Command;

use arrow::record_batch::RecordBatch;
use ndarray::{Array1, Array2, Array3, Ix2};
use ort::session::{Session, builder::GraphOptimizationLevel};
use ort::value::TensorRef;
use seccloud_lake::schema::{
    ActionFeatureRow, CollaborationFeatureRow, HistoryFeatureRow, PeerGroupFeatureRow,
    StaticPrincipalFeatureRow, action_feature_record_batch_to_rows,
    collaboration_feature_record_batch_to_rows, history_feature_record_batch_to_rows,
    peer_group_feature_record_batch_to_rows, read_parquet_bytes,
    static_principal_feature_record_batch_to_rows,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::detector::DetectionContextFile;
use crate::event_index::EventIndexFile;
use crate::model_runtime::{ModelArtifactMetadata, load_active_model_bundle};

#[cfg(test)]
pub(crate) fn bridge_env_lock() -> &'static std::sync::Mutex<()> {
    use std::sync::{Mutex, OnceLock};

    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FeatureStateFile {
    action_manifest_key: Option<String>,
    history_manifest_key: Option<String>,
    collaboration_manifest_key: Option<String>,
    static_manifest_key: Option<String>,
    peer_group_manifest_key: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CategoricalVocabs {
    #[serde(default)]
    role: BTreeMap<String, i64>,
    #[serde(default)]
    location: BTreeMap<String, i64>,
    #[serde(default, rename = "duration_bucket")]
    duration_bucket: BTreeMap<String, i64>,
    #[serde(default, rename = "privilege_level")]
    privilege_level: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Default)]
struct LoadedFeatureTables {
    action_rows: Vec<ActionFeatureRow>,
    history_rows: Vec<HistoryFeatureRow>,
    collaboration_rows: Vec<CollaborationFeatureRow>,
    static_rows: Vec<StaticPrincipalFeatureRow>,
    peer_group_rows: Vec<PeerGroupFeatureRow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelScoreItem {
    pub event_id: String,
    pub source: String,
    pub action_indices: Vec<i64>,
    pub action_weights: Vec<f32>,
    pub action_mask: Vec<bool>,
    pub hist_window_indices: Vec<Vec<i64>>,
    pub hist_window_mask: Vec<Vec<bool>>,
    pub hist_num_windows: i64,
    pub peer_indices: Vec<Vec<i64>>,
    pub peer_weights: Vec<Vec<f32>>,
    pub peer_mask: Vec<Vec<bool>>,
    pub role: i64,
    pub location: i64,
    pub duration: i64,
    pub privilege: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelScoreRequest {
    pub items: Vec<ModelScoreItem>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelScoreResponseItem {
    pub event_id: String,
    pub model_score: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelScoreResponse {
    pub items: Vec<ModelScoreResponseItem>,
}

fn feature_state_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("feature_state.json")
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

fn load_feature_state(workspace: &Path) -> anyhow::Result<FeatureStateFile> {
    read_json_or_default(&feature_state_path(workspace))
}

fn load_manifest_rows<T>(
    workspace: &Path,
    manifest_key: &Option<String>,
    decode: impl Fn(&RecordBatch) -> anyhow::Result<Vec<T>>,
) -> anyhow::Result<Vec<T>> {
    let Some(manifest_key) = manifest_key.as_ref() else {
        return Ok(Vec::new());
    };
    let manifest: Value = serde_json::from_slice(&std::fs::read(workspace.join(manifest_key))?)?;
    let mut rows = Vec::new();
    let objects = manifest
        .get("objects")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for object in objects {
        let Some(object_key) = object.get("object_key").and_then(Value::as_str) else {
            continue;
        };
        let bytes = std::fs::read(workspace.join(object_key))?;
        for batch in read_parquet_bytes(&bytes)? {
            rows.extend(decode(&batch)?);
        }
    }
    Ok(rows)
}

fn load_feature_tables(workspace: &Path) -> anyhow::Result<LoadedFeatureTables> {
    let state = load_feature_state(workspace)?;
    Ok(LoadedFeatureTables {
        action_rows: load_manifest_rows(workspace, &state.action_manifest_key, |batch| {
            action_feature_record_batch_to_rows(batch)
        })?,
        history_rows: load_manifest_rows(workspace, &state.history_manifest_key, |batch| {
            history_feature_record_batch_to_rows(batch)
        })?,
        collaboration_rows: load_manifest_rows(
            workspace,
            &state.collaboration_manifest_key,
            collaboration_feature_record_batch_to_rows,
        )?,
        static_rows: load_manifest_rows(workspace, &state.static_manifest_key, |batch| {
            static_principal_feature_record_batch_to_rows(batch)
        })?,
        peer_group_rows: load_manifest_rows(workspace, &state.peer_group_manifest_key, |batch| {
            peer_group_feature_record_batch_to_rows(batch)
        })?,
    })
}

fn load_json_list(path: &Path) -> anyhow::Result<Vec<String>> {
    let payload: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(payload
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
        .collect())
}

fn load_categorical_vocabs(path: &Path) -> anyhow::Result<CategoricalVocabs> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
}

fn optional_string<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_str()
}

fn i64_or_default(map: &BTreeMap<String, i64>, key: &str) -> i64 {
    map.get(key).copied().unwrap_or(0)
}

fn pad_weighted_set(items: &[(usize, f64)], max_len: usize) -> (Vec<i64>, Vec<f32>, Vec<bool>) {
    let mut indices = Vec::with_capacity(max_len);
    let mut weights = Vec::with_capacity(max_len);
    let mut mask = Vec::with_capacity(max_len);
    for (idx, weight) in items.iter().take(max_len) {
        indices.push((*idx as i64) + 1);
        weights.push(*weight as f32);
        mask.push(true);
    }
    while indices.len() < max_len {
        indices.push(0);
        weights.push(0.0);
        mask.push(false);
    }
    (indices, weights, mask)
}

fn bundle_root<'a>(metadata_path: &'a Path, workspace: &'a Path) -> &'a Path {
    metadata_path.parent().unwrap_or(workspace)
}

fn open_session(model_path: &Path) -> anyhow::Result<Session> {
    Ok(Session::builder()?
        .with_optimization_level(GraphOptimizationLevel::Level3)?
        .commit_from_file(model_path)?)
}

fn action_output_name<'a>(metadata: &'a ModelArtifactMetadata, source: &str) -> &'a str {
    metadata
        .action_towers
        .get(source)
        .map(|tower| tower.output_name.as_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("embedding")
}

fn context_output_name(metadata: &ModelArtifactMetadata) -> &str {
    if metadata.context_tower.output_name.is_empty() {
        "embedding"
    } else {
        metadata.context_tower.output_name.as_str()
    }
}

fn action_arrays(
    items: &[&ModelScoreItem],
) -> anyhow::Result<(Array2<i64>, Array2<f32>, Array2<bool>)> {
    let token_count = items
        .first()
        .map(|item| item.action_indices.len())
        .ok_or_else(|| anyhow::anyhow!("cannot build action tensors for empty items"))?;
    let indices = Array2::from_shape_vec(
        (items.len(), token_count),
        items
            .iter()
            .flat_map(|item| item.action_indices.iter().copied())
            .collect(),
    )?;
    let weights = Array2::from_shape_vec(
        (items.len(), token_count),
        items
            .iter()
            .flat_map(|item| item.action_weights.iter().copied())
            .collect(),
    )?;
    let mask = Array2::from_shape_vec(
        (items.len(), token_count),
        items
            .iter()
            .flat_map(|item| item.action_mask.iter().copied())
            .collect(),
    )?;
    Ok((indices, weights, mask))
}

type ContextArrays = (
    Array3<i64>,
    Array3<bool>,
    Array1<i64>,
    Array3<i64>,
    Array3<f32>,
    Array3<bool>,
    Array1<i64>,
    Array1<i64>,
    Array1<i64>,
    Array1<i64>,
);

fn context_arrays(items: &[&ModelScoreItem]) -> anyhow::Result<ContextArrays> {
    let first = items
        .first()
        .ok_or_else(|| anyhow::anyhow!("cannot build context tensors for empty items"))?;
    let max_windows = first.hist_window_indices.len();
    let max_res_per_window = first
        .hist_window_indices
        .first()
        .map(Vec::len)
        .unwrap_or_default();
    let peer_group_count = first.peer_indices.len();
    let max_peers = first.peer_indices.first().map(Vec::len).unwrap_or_default();
    let hist_window_indices = Array3::from_shape_vec(
        (items.len(), max_windows, max_res_per_window),
        items
            .iter()
            .flat_map(|item| {
                item.hist_window_indices
                    .iter()
                    .flat_map(|row| row.iter().copied())
            })
            .collect(),
    )?;
    let hist_window_mask = Array3::from_shape_vec(
        (items.len(), max_windows, max_res_per_window),
        items
            .iter()
            .flat_map(|item| {
                item.hist_window_mask
                    .iter()
                    .flat_map(|row| row.iter().copied())
            })
            .collect(),
    )?;
    let hist_num_windows =
        Array1::from_vec(items.iter().map(|item| item.hist_num_windows).collect());
    let peer_indices = Array3::from_shape_vec(
        (items.len(), peer_group_count, max_peers),
        items
            .iter()
            .flat_map(|item| item.peer_indices.iter().flat_map(|row| row.iter().copied()))
            .collect(),
    )?;
    let peer_weights = Array3::from_shape_vec(
        (items.len(), peer_group_count, max_peers),
        items
            .iter()
            .flat_map(|item| item.peer_weights.iter().flat_map(|row| row.iter().copied()))
            .collect(),
    )?;
    let peer_mask = Array3::from_shape_vec(
        (items.len(), peer_group_count, max_peers),
        items
            .iter()
            .flat_map(|item| item.peer_mask.iter().flat_map(|row| row.iter().copied()))
            .collect(),
    )?;
    let role = Array1::from_vec(items.iter().map(|item| item.role).collect());
    let location = Array1::from_vec(items.iter().map(|item| item.location).collect());
    let duration = Array1::from_vec(items.iter().map(|item| item.duration).collect());
    let privilege = Array1::from_vec(items.iter().map(|item| item.privilege).collect());
    Ok((
        hist_window_indices,
        hist_window_mask,
        hist_num_windows,
        peer_indices,
        peer_weights,
        peer_mask,
        role,
        location,
        duration,
        privilege,
    ))
}

fn extract_embeddings(
    outputs: &ort::session::SessionOutputs<'_>,
    output_name: &str,
) -> anyhow::Result<Array2<f32>> {
    let view = outputs[output_name].try_extract_array::<f32>()?;
    Ok(view.to_owned().into_dimensionality::<Ix2>()?)
}

fn run_context_tower(
    session: &mut Session,
    metadata: &ModelArtifactMetadata,
    items: &[&ModelScoreItem],
) -> anyhow::Result<Array2<f32>> {
    let (
        hist_window_indices,
        hist_window_mask,
        hist_num_windows,
        peer_indices,
        peer_weights,
        peer_mask,
        role,
        location,
        duration,
        privilege,
    ) = context_arrays(items)?;
    let outputs = session.run(ort::inputs! {
        "hist_window_indices" => TensorRef::from_array_view(hist_window_indices.view())?,
        "hist_window_mask" => TensorRef::from_array_view(hist_window_mask.view())?,
        "hist_num_windows" => TensorRef::from_array_view(hist_num_windows.view())?,
        "peer_indices" => TensorRef::from_array_view(peer_indices.view())?,
        "peer_weights" => TensorRef::from_array_view(peer_weights.view())?,
        "peer_mask" => TensorRef::from_array_view(peer_mask.view())?,
        "role" => TensorRef::from_array_view(role.view())?,
        "location" => TensorRef::from_array_view(location.view())?,
        "duration" => TensorRef::from_array_view(duration.view())?,
        "privilege" => TensorRef::from_array_view(privilege.view())?,
    })?;
    extract_embeddings(&outputs, context_output_name(metadata))
}

fn run_action_tower(
    session: &mut Session,
    metadata: &ModelArtifactMetadata,
    source: &str,
    items: &[&ModelScoreItem],
) -> anyhow::Result<Array2<f32>> {
    let (indices, weights, mask) = action_arrays(items)?;
    let outputs = session.run(ort::inputs! {
        "indices" => TensorRef::from_array_view(indices.view())?,
        "weights" => TensorRef::from_array_view(weights.view())?,
        "mask" => TensorRef::from_array_view(mask.view())?,
    })?;
    extract_embeddings(&outputs, action_output_name(metadata, source))
}

pub fn build_model_score_request(
    workspace: &Path,
    event_index: &EventIndexFile,
    detection_context: &DetectionContextFile,
) -> anyhow::Result<Option<(ModelArtifactMetadata, PathBuf, ModelScoreRequest)>> {
    let Some((_manifest, metadata, metadata_path)) = load_active_model_bundle(workspace)? else {
        return Ok(None);
    };
    let bundle_root = metadata_path.parent().unwrap_or(workspace);
    let principal_vocab_path = bundle_root.join(
        metadata
            .input_vocabs
            .principal_entity_keys_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("model bundle missing principal vocab path"))?,
    );
    let resource_vocab_path = bundle_root.join(
        metadata
            .input_vocabs
            .resource_entity_keys_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("model bundle missing resource vocab path"))?,
    );
    let categorical_vocabs_path = bundle_root.join(
        metadata
            .input_vocabs
            .categorical_vocabs_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("model bundle missing categorical vocabs path"))?,
    );
    let principal_vocab = load_json_list(&principal_vocab_path)?;
    let resource_vocab = load_json_list(&resource_vocab_path)?;
    let categorical_vocabs = load_categorical_vocabs(&categorical_vocabs_path)?;
    let principal_to_idx: BTreeMap<String, usize> = principal_vocab
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), index))
        .collect();
    let resource_to_idx: BTreeMap<String, usize> = resource_vocab
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), index))
        .collect();

    let feature_tables = load_feature_tables(workspace)?;
    let max_tokens = metadata.padding.get("max_tokens").copied().unwrap_or(64);
    let max_windows = metadata.padding.get("max_windows").copied().unwrap_or(64);
    let max_res_per_window = metadata
        .padding
        .get("max_res_per_window")
        .copied()
        .unwrap_or(16);
    let max_peers = metadata.padding.get("max_peers").copied().unwrap_or(64);

    let mut action_by_resource: BTreeMap<String, (String, Vec<(usize, f64)>)> = BTreeMap::new();
    for row in &feature_tables.action_rows {
        let Some(principal_idx) = principal_to_idx.get(&row.principal_entity_key).copied() else {
            continue;
        };
        let entry = action_by_resource
            .entry(row.resource_entity_key.clone())
            .or_insert_with(|| (row.source.clone(), Vec::new()));
        entry.1.push((principal_idx, row.accessor_weight));
    }
    for (_source, accessors) in action_by_resource.values_mut() {
        accessors.sort_by_key(|(idx, _)| *idx);
    }

    let mut history_by_principal: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> =
        BTreeMap::new();
    for row in &feature_tables.history_rows {
        history_by_principal
            .entry(row.principal_entity_key.clone())
            .or_default()
            .entry(row.window_start.clone())
            .or_default()
            .insert(row.resource_entity_key.clone());
    }

    let mut collaboration_by_principal: BTreeMap<String, Vec<(usize, f64)>> = BTreeMap::new();
    for row in &feature_tables.collaboration_rows {
        let Some(peer_idx) = principal_to_idx.get(&row.collaborator_entity_key).copied() else {
            continue;
        };
        collaboration_by_principal
            .entry(row.principal_entity_key.clone())
            .or_default()
            .push((peer_idx, row.co_access_weight));
    }
    for peers in collaboration_by_principal.values_mut() {
        peers.sort_by_key(|(idx, _)| *idx);
    }

    let mut peers_by_principal: BTreeMap<String, BTreeMap<String, Vec<(usize, f64)>>> =
        BTreeMap::new();
    for row in &feature_tables.peer_group_rows {
        let Some(peer_idx) = principal_to_idx.get(&row.peer_entity_key).copied() else {
            continue;
        };
        peers_by_principal
            .entry(row.principal_entity_key.clone())
            .or_default()
            .entry(row.peer_type.clone())
            .or_default()
            .push((peer_idx, row.peer_weight));
    }
    for peer_types in peers_by_principal.values_mut() {
        for peers in peer_types.values_mut() {
            peers.sort_by_key(|(idx, _)| *idx);
        }
    }

    let static_by_principal: BTreeMap<String, &StaticPrincipalFeatureRow> = feature_tables
        .static_rows
        .iter()
        .map(|row| (row.principal_entity_key.clone(), row))
        .collect();

    let mut items = Vec::new();
    for event_id in &detection_context.ordered_event_ids {
        let Some(event) = event_index.events_by_id.get(event_id) else {
            continue;
        };
        let Some(source) = event.get("source").and_then(Value::as_str) else {
            continue;
        };
        if !metadata.action_towers.contains_key(source) {
            continue;
        }
        let Some(principal_entity_key) = optional_string(event, &["principal", "entity_key"])
        else {
            continue;
        };
        let Some(resource_entity_key) = optional_string(event, &["resource", "entity_key"]) else {
            continue;
        };
        let Some((action_source, accessors)) = action_by_resource.get(resource_entity_key) else {
            continue;
        };
        if action_source != source {
            continue;
        }
        let Some(static_row) = static_by_principal.get(principal_entity_key) else {
            continue;
        };

        let (action_indices, action_weights, action_mask) = pad_weighted_set(accessors, max_tokens);
        let mut hist_window_indices = Vec::with_capacity(max_windows);
        let mut hist_window_mask = Vec::with_capacity(max_windows);
        let mut hist_num_windows = 0_i64;
        if let Some(windows) = history_by_principal.get(principal_entity_key) {
            for resources in windows.values().take(max_windows) {
                let mut indices = Vec::with_capacity(max_res_per_window);
                let mut mask = Vec::with_capacity(max_res_per_window);
                for resource in resources.iter().take(max_res_per_window) {
                    indices.push(resource_to_idx.get(resource).copied().unwrap_or(0) as i64 + 1);
                    mask.push(true);
                }
                while indices.len() < max_res_per_window {
                    indices.push(0);
                    mask.push(false);
                }
                hist_window_indices.push(indices);
                hist_window_mask.push(mask);
                hist_num_windows += 1;
            }
        }
        while hist_window_indices.len() < max_windows {
            hist_window_indices.push(vec![0; max_res_per_window]);
            hist_window_mask.push(vec![false; max_res_per_window]);
        }

        let peer_types = peers_by_principal.get(principal_entity_key);
        let empty_peers: Vec<(usize, f64)> = Vec::new();
        let department_peers = peer_types
            .and_then(|value| value.get("department"))
            .unwrap_or(&empty_peers);
        let manager_peers = peer_types
            .and_then(|value| value.get("manager"))
            .unwrap_or(&empty_peers);
        let group_peers = peer_types
            .and_then(|value| value.get("group"))
            .unwrap_or(&empty_peers);
        let collaboration_peers = collaboration_by_principal
            .get(principal_entity_key)
            .unwrap_or(&empty_peers);
        let peer_sets = [
            department_peers,
            manager_peers,
            group_peers,
            collaboration_peers,
        ];
        let mut peer_indices = Vec::with_capacity(4);
        let mut peer_weights = Vec::with_capacity(4);
        let mut peer_mask = Vec::with_capacity(4);
        for peer_set in peer_sets {
            let (indices, weights, mask) = pad_weighted_set(peer_set, max_peers);
            peer_indices.push(indices);
            peer_weights.push(weights);
            peer_mask.push(mask);
        }

        items.push(ModelScoreItem {
            event_id: event_id.clone(),
            source: source.to_string(),
            action_indices,
            action_weights,
            action_mask,
            hist_window_indices,
            hist_window_mask,
            hist_num_windows,
            peer_indices,
            peer_weights,
            peer_mask,
            role: i64_or_default(&categorical_vocabs.role, &static_row.role),
            location: i64_or_default(&categorical_vocabs.location, &static_row.location),
            duration: i64_or_default(
                &categorical_vocabs.duration_bucket,
                &static_row.employment_duration_bucket,
            ),
            privilege: i64_or_default(
                &categorical_vocabs.privilege_level,
                &static_row.privilege_level,
            ),
        });
    }

    Ok(Some((metadata, metadata_path, ModelScoreRequest { items })))
}

#[cfg(test)]
pub fn run_model_score_bridge(
    metadata_path: &Path,
    request: &ModelScoreRequest,
) -> anyhow::Result<ModelScoreResponse> {
    let temp_root = std::env::temp_dir().join(format!(
        "seccloud-model-score-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&temp_root)?;
    let input_path = temp_root.join("input.json");
    let output_path = temp_root.join("output.json");
    std::fs::write(&input_path, serde_json::to_vec_pretty(request)?)?;

    let bridge = std::env::var("SECCLOUD_ONNX_SCORE_BRIDGE")
        .map_err(|_| anyhow::anyhow!("test bridge requested without SECCLOUD_ONNX_SCORE_BRIDGE"))?;
    let status = Command::new(bridge)
        .arg("--metadata")
        .arg(metadata_path)
        .arg("--input")
        .arg(&input_path)
        .arg("--output")
        .arg(&output_path)
        .status()?;
    if !status.success() {
        anyhow::bail!("ONNX score bridge failed with status: {status}");
    }
    let response: ModelScoreResponse = serde_json::from_slice(&std::fs::read(&output_path)?)?;
    let _ = std::fs::remove_dir_all(temp_root);
    Ok(response)
}

pub fn run_model_score_native(
    metadata_path: &Path,
    metadata: &ModelArtifactMetadata,
    request: &ModelScoreRequest,
) -> anyhow::Result<ModelScoreResponse> {
    if request.items.is_empty() {
        return Ok(ModelScoreResponse::default());
    }
    let workspace_root = repo_root();
    let bundle_root = bundle_root(metadata_path, workspace_root);
    let mut context_session = open_session(&bundle_root.join(&metadata.context_tower.path))?;
    let item_refs: Vec<&ModelScoreItem> = request.items.iter().collect();
    let context_embeddings = run_context_tower(&mut context_session, metadata, &item_refs)?;
    let mut scores_by_index = vec![0.0_f64; request.items.len()];

    let mut indexes_by_source: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, item) in request.items.iter().enumerate() {
        indexes_by_source
            .entry(item.source.as_str())
            .or_default()
            .push(index);
    }

    for (source, indexes) in indexes_by_source {
        let tower = metadata
            .action_towers
            .get(source)
            .ok_or_else(|| anyhow::anyhow!("missing action tower for source: {source}"))?;
        let mut action_session = open_session(&bundle_root.join(&tower.path))?;
        let source_items: Vec<&ModelScoreItem> =
            indexes.iter().map(|index| &request.items[*index]).collect();
        let action_embeddings =
            run_action_tower(&mut action_session, metadata, source, &source_items)?;
        for (local_index, global_index) in indexes.iter().enumerate() {
            let action = action_embeddings.row(local_index);
            let context = context_embeddings.row(*global_index);
            let similarity: f32 = action
                .iter()
                .zip(context.iter())
                .map(|(left, right)| left * right)
                .sum();
            scores_by_index[*global_index] = f64::from(1.0_f32 - similarity);
        }
    }

    Ok(ModelScoreResponse {
        items: request
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| ModelScoreResponseItem {
                event_id: item.event_id.clone(),
                model_score: scores_by_index[index].clamp(0.0, 1.0),
            })
            .collect(),
    })
}

pub fn run_model_score(
    metadata_path: &Path,
    metadata: &ModelArtifactMetadata,
    request: &ModelScoreRequest,
) -> anyhow::Result<ModelScoreResponse> {
    #[cfg(test)]
    if std::env::var_os("SECCLOUD_ONNX_SCORE_BRIDGE").is_some() {
        return run_model_score_bridge(metadata_path, request);
    }
    run_model_score_native(metadata_path, metadata, request)
}

#[cfg(test)]
mod tests {
    use super::{
        ModelArtifactMetadata, ModelScoreItem, ModelScoreRequest, bridge_env_lock, run_model_score,
        run_model_score_bridge,
    };
    use crate::model_runtime::ModelTowerMetadata;

    #[test]
    fn bridge_executes_override_command() {
        let _guard = bridge_env_lock().lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("bridge.py");
        let metadata = root.path().join("metadata.json");
        std::fs::write(&metadata, b"{}").unwrap();
        std::fs::write(
            &script,
            br#"#!/usr/bin/env python3
import argparse, json
parser = argparse.ArgumentParser()
parser.add_argument("--metadata")
parser.add_argument("--input")
parser.add_argument("--output")
args = parser.parse_args()
with open(args.input, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
with open(args.output, "w", encoding="utf-8") as fh:
    json.dump({"items": [{"event_id": item["event_id"], "model_score": 0.91} for item in payload["items"]]}, fh)
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&script, permissions).unwrap();
        }
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &script);
        }
        let response = run_model_score_bridge(
            &metadata,
            &ModelScoreRequest {
                items: vec![ModelScoreItem {
                    event_id: "evt_1".into(),
                    source: "okta".into(),
                    action_indices: vec![1],
                    action_weights: vec![1.0],
                    action_mask: vec![true],
                    hist_window_indices: vec![vec![1]],
                    hist_window_mask: vec![vec![true]],
                    hist_num_windows: 1,
                    peer_indices: vec![vec![0], vec![0], vec![0], vec![0]],
                    peer_weights: vec![vec![0.0], vec![0.0], vec![0.0], vec![0.0]],
                    peer_mask: vec![vec![false], vec![false], vec![false], vec![false]],
                    role: 0,
                    location: 0,
                    duration: 0,
                    privilege: 0,
                }],
            },
        )
        .unwrap();
        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].event_id, "evt_1");
        assert!((response.items[0].model_score - 0.91).abs() < f64::EPSILON);
    }

    #[test]
    fn run_model_score_prefers_test_bridge_override() {
        let _guard = bridge_env_lock().lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("bridge.py");
        let metadata = root.path().join("metadata.json");
        std::fs::write(&metadata, b"{}").unwrap();
        std::fs::write(
            &script,
            br#"#!/usr/bin/env python3
import argparse, json
parser = argparse.ArgumentParser()
parser.add_argument("--metadata")
parser.add_argument("--input")
parser.add_argument("--output")
args = parser.parse_args()
with open(args.input, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
with open(args.output, "w", encoding="utf-8") as fh:
    json.dump({"items": [{"event_id": item["event_id"], "model_score": 0.42} for item in payload["items"]]}, fh)
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&script, permissions).unwrap();
        }
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &script);
        }
        let response = run_model_score(
            &metadata,
            &ModelArtifactMetadata {
                action_towers: std::collections::BTreeMap::from([(
                    "okta".into(),
                    ModelTowerMetadata {
                        path: "action_tower_okta.onnx".into(),
                        sha256: String::new(),
                        input_names: vec![],
                        output_name: "embedding".into(),
                    },
                )]),
                context_tower: ModelTowerMetadata {
                    path: "context_tower.onnx".into(),
                    sha256: String::new(),
                    input_names: vec![],
                    output_name: "embedding".into(),
                },
                ..ModelArtifactMetadata::default()
            },
            &ModelScoreRequest {
                items: vec![ModelScoreItem {
                    event_id: "evt_1".into(),
                    source: "okta".into(),
                    action_indices: vec![1],
                    action_weights: vec![1.0],
                    action_mask: vec![true],
                    hist_window_indices: vec![vec![1]],
                    hist_window_mask: vec![vec![true]],
                    hist_num_windows: 1,
                    peer_indices: vec![vec![0], vec![0], vec![0], vec![0]],
                    peer_weights: vec![vec![0.0], vec![0.0], vec![0.0], vec![0.0]],
                    peer_mask: vec![vec![false], vec![false], vec![false], vec![false]],
                    role: 0,
                    location: 0,
                    duration: 0,
                    privilege: 0,
                }],
            },
        )
        .unwrap();
        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }
        assert_eq!(response.items[0].event_id, "evt_1");
        assert!((response.items[0].model_score - 0.42).abs() < f64::EPSILON);
    }
}
