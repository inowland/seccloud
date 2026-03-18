use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelArtifactManifest {
    #[serde(default)]
    pub manifest_version: usize,
    #[serde(default)]
    pub requested_mode: String,
    #[serde(default)]
    pub active_model_id: Option<String>,
    #[serde(default)]
    pub active_metadata_path: Option<String>,
    #[serde(default)]
    pub activated_at: Option<String>,
    #[serde(default)]
    pub activation_source: Option<String>,
    #[serde(default)]
    pub activation_history: Vec<ModelActivationRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelActivationRecord {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub metadata_path: Option<String>,
    #[serde(default)]
    pub requested_mode: String,
    #[serde(default)]
    pub activation_source: Option<String>,
    #[serde(default)]
    pub activated_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelTowerMetadata {
    pub path: String,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub input_names: Vec<String>,
    #[serde(default)]
    pub output_name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ModelArtifactMetadata {
    pub artifact_version: usize,
    pub tenant_id: String,
    pub model_id: String,
    pub model_version: String,
    pub model_family: String,
    pub scoring_mode: String,
    pub exported_at: Option<String>,
    pub feature_schema_version: String,
    #[serde(default)]
    pub required_feature_tables: Vec<String>,
    #[serde(default)]
    pub padding: BTreeMap<String, usize>,
    #[serde(default)]
    pub action_towers: BTreeMap<String, ModelTowerMetadata>,
    pub context_tower: ModelTowerMetadata,
    #[serde(default)]
    pub eval_report_path: String,
    #[serde(default)]
    pub input_vocabs: ModelInputVocabs,
    #[serde(default)]
    pub score_policy: ModelScorePolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelInputVocabs {
    pub principal_entity_keys_path: Option<String>,
    pub resource_entity_keys_path: Option<String>,
    pub categorical_vocabs_path: Option<String>,
    #[serde(default)]
    pub principal_vocab_count: usize,
    #[serde(default)]
    pub resource_vocab_count: usize,
    #[serde(default)]
    pub categorical_vocab_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelScorePolicy {
    #[serde(default = "default_detection_threshold")]
    pub detection_threshold: f64,
    #[serde(default = "default_high_severity_threshold")]
    pub high_severity_threshold: f64,
    #[serde(default)]
    pub calibration_source: Option<String>,
    #[serde(default)]
    pub calibration_reason: Option<String>,
    #[serde(default)]
    pub positive_distance_p95: Option<f64>,
    #[serde(default)]
    pub negative_distance_p50: Option<f64>,
    #[serde(default)]
    pub negative_distance_p90: Option<f64>,
    #[serde(default)]
    pub source_policies: BTreeMap<String, SourceScorePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceScorePolicy {
    #[serde(default = "default_detection_threshold")]
    pub detection_threshold: f64,
    #[serde(default = "default_high_severity_threshold")]
    pub high_severity_threshold: f64,
    #[serde(default)]
    pub calibration_source: Option<String>,
    #[serde(default)]
    pub calibration_reason: Option<String>,
    #[serde(default)]
    pub positive_distance_p95: Option<f64>,
    #[serde(default)]
    pub negative_distance_p50: Option<f64>,
    #[serde(default)]
    pub negative_distance_p90: Option<f64>,
    #[serde(default)]
    pub evaluation_pair_count: Option<usize>,
}

impl Default for ModelScorePolicy {
    fn default() -> Self {
        Self {
            detection_threshold: default_detection_threshold(),
            high_severity_threshold: default_high_severity_threshold(),
            calibration_source: None,
            calibration_reason: None,
            positive_distance_p95: None,
            negative_distance_p50: None,
            negative_distance_p90: None,
            source_policies: BTreeMap::new(),
        }
    }
}

impl Default for SourceScorePolicy {
    fn default() -> Self {
        Self {
            detection_threshold: default_detection_threshold(),
            high_severity_threshold: default_high_severity_threshold(),
            calibration_source: None,
            calibration_reason: None,
            positive_distance_p95: None,
            negative_distance_p50: None,
            negative_distance_p90: None,
            evaluation_pair_count: None,
        }
    }
}

fn default_detection_threshold() -> f64 {
    0.60
}

fn default_high_severity_threshold() -> f64 {
    0.80
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelRuntimeStatus {
    pub available: bool,
    pub requested_mode: String,
    pub effective_mode: String,
    pub reason: String,
    pub model_id: Option<String>,
    pub model_version: Option<String>,
    pub model_family: Option<String>,
    pub exported_at: Option<String>,
    pub metadata_path: Option<String>,
    pub activated_at: Option<String>,
    pub activation_source: Option<String>,
    pub action_source_count: usize,
    pub principal_vocab_count: usize,
    pub resource_vocab_count: usize,
    pub installed_model_count: usize,
    pub installed_model_ids: Vec<String>,
}

pub fn model_artifact_manifest_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("model_artifact.json")
}

pub fn load_model_artifact_manifest(workspace: &Path) -> anyhow::Result<ModelArtifactManifest> {
    let path = model_artifact_manifest_path(workspace);
    if !path.exists() {
        return Ok(ModelArtifactManifest::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn coerce_requested_mode(manifest: &ModelArtifactManifest) -> String {
    match manifest.requested_mode.as_str() {
        "onnx" | "heuristic" => manifest.requested_mode.clone(),
        _ if manifest.active_metadata_path.is_some() => "onnx".to_string(),
        _ => "heuristic".to_string(),
    }
}

fn load_model_artifact_metadata(path: &Path) -> anyhow::Result<ModelArtifactMetadata> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn resolve_metadata_path(workspace: &Path, manifest: &ModelArtifactManifest) -> Option<PathBuf> {
    let relative = manifest.active_metadata_path.as_ref()?;
    Some(workspace.join(relative))
}

pub fn load_active_model_bundle(
    workspace: &Path,
) -> anyhow::Result<Option<(ModelArtifactManifest, ModelArtifactMetadata, PathBuf)>> {
    let manifest = load_model_artifact_manifest(workspace)?;
    let Some(metadata_path) = resolve_metadata_path(workspace, &manifest) else {
        return Ok(None);
    };
    if !metadata_path.exists() {
        return Ok(None);
    }
    let metadata = load_model_artifact_metadata(&metadata_path)?;
    Ok(Some((manifest, metadata, metadata_path)))
}

pub fn resolve_model_runtime_status(workspace: &Path) -> anyhow::Result<ModelRuntimeStatus> {
    let manifest = load_model_artifact_manifest(workspace)?;
    let requested_mode = coerce_requested_mode(&manifest);
    let no_active_reason =
        if manifest.activation_source.is_some() || !manifest.activation_history.is_empty() {
            "requested_heuristic_mode".to_string()
        } else {
            "no_active_model".to_string()
        };
    let installed_model_ids = std::fs::read_dir(workspace.join("models"))
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().join("metadata.json").exists())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect::<Vec<_>>();
    let Some((manifest, metadata, metadata_path)) = load_active_model_bundle(workspace)? else {
        return Ok(ModelRuntimeStatus {
            requested_mode,
            effective_mode: "heuristic".to_string(),
            reason: if manifest.active_metadata_path.is_none() {
                no_active_reason
            } else if manifest.requested_mode == "heuristic" {
                "requested_heuristic_mode".to_string()
            } else {
                "active_metadata_missing".to_string()
            },
            model_id: manifest.active_model_id.clone(),
            metadata_path: manifest.active_metadata_path.clone(),
            activated_at: manifest.activated_at.clone(),
            activation_source: manifest.activation_source.clone(),
            installed_model_count: installed_model_ids.len(),
            installed_model_ids,
            ..ModelRuntimeStatus::default()
        });
    };
    let mut required_paths: Vec<PathBuf> = metadata
        .action_towers
        .values()
        .map(|tower| {
            metadata_path
                .parent()
                .unwrap_or(workspace)
                .join(&tower.path)
        })
        .collect();
    required_paths.push(
        metadata_path
            .parent()
            .unwrap_or(workspace)
            .join(&metadata.context_tower.path),
    );
    for path in [
        metadata.input_vocabs.principal_entity_keys_path.as_ref(),
        metadata.input_vocabs.resource_entity_keys_path.as_ref(),
        metadata.input_vocabs.categorical_vocabs_path.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        required_paths.push(metadata_path.parent().unwrap_or(workspace).join(path));
    }
    let has_complete_vocab_contract = metadata.input_vocabs.principal_entity_keys_path.is_some()
        && metadata.input_vocabs.resource_entity_keys_path.is_some()
        && metadata.input_vocabs.categorical_vocabs_path.is_some();
    let available = !metadata.action_towers.is_empty()
        && has_complete_vocab_contract
        && required_paths.iter().all(|path| path.exists());
    let effective_mode = if available && requested_mode == "onnx" {
        "onnx_native".to_string()
    } else {
        "heuristic".to_string()
    };
    let reason = if requested_mode == "heuristic" {
        "requested_heuristic_mode".to_string()
    } else if available {
        "model_artifact_loaded_onnx_native".to_string()
    } else {
        "model_artifact_incomplete".to_string()
    };
    Ok(ModelRuntimeStatus {
        available,
        requested_mode,
        effective_mode,
        reason,
        model_id: Some(metadata.model_id),
        model_version: Some(metadata.model_version),
        model_family: Some(metadata.model_family),
        exported_at: metadata.exported_at,
        metadata_path: manifest.active_metadata_path,
        activated_at: manifest.activated_at,
        activation_source: manifest.activation_source,
        action_source_count: metadata.action_towers.len(),
        principal_vocab_count: metadata.input_vocabs.principal_vocab_count,
        resource_vocab_count: metadata.input_vocabs.resource_vocab_count,
        installed_model_count: installed_model_ids.len(),
        installed_model_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::{model_artifact_manifest_path, resolve_model_runtime_status};

    #[test]
    fn no_active_model_uses_heuristic_runtime() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(workspace.path().join("manifests")).unwrap();

        let status = resolve_model_runtime_status(workspace.path()).unwrap();

        assert!(!status.available);
        assert_eq!(status.requested_mode, "heuristic");
        assert_eq!(status.effective_mode, "heuristic");
        assert_eq!(status.reason, "no_active_model");
    }

    #[test]
    fn complete_active_model_is_loaded() {
        let workspace = tempfile::tempdir().unwrap();
        let manifests = workspace.path().join("manifests");
        let model_dir = workspace.path().join("models").join("demo-v1");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("action_tower_okta.onnx"), b"onnx").unwrap();
        std::fs::write(model_dir.join("context_tower.onnx"), b"onnx").unwrap();
        std::fs::write(model_dir.join("principal-vocab.json"), b"[\"principal-1\"]").unwrap();
        std::fs::write(model_dir.join("resource-vocab.json"), b"[\"resource-1\"]").unwrap();
        std::fs::write(
            model_dir.join("categorical-vocabs.json"),
            b"{\"role\":{\"analyst\":0}}",
        )
        .unwrap();
        std::fs::write(
            model_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "artifact_version": 1,
                "tenant_id": "tenant-1",
                "model_id": "demo-v1",
                "model_version": "demo-v1",
                "model_family": "contrastive-facade",
                "scoring_mode": "onnx",
                "exported_at": "2026-03-17T00:00:00Z",
                "feature_schema_version": "feature.v1",
                "required_feature_tables": ["action", "history", "static", "peer_group"],
                "padding": {"max_tokens": 8},
                "action_towers": {
                    "okta": {
                        "path": "action_tower_okta.onnx",
                        "sha256": "sha256:test",
                        "input_names": ["indices", "weights", "mask"],
                        "output_name": "embedding"
                    }
                },
                "context_tower": {
                    "path": "context_tower.onnx",
                    "sha256": "sha256:test",
                    "input_names": ["hist_window_indices"],
                    "output_name": "embedding"
                },
                "eval_report_path": "eval-report.json",
                "input_vocabs": {
                    "principal_entity_keys_path": "principal-vocab.json",
                    "resource_entity_keys_path": "resource-vocab.json",
                    "categorical_vocabs_path": "categorical-vocabs.json",
                    "principal_vocab_count": 1,
                    "resource_vocab_count": 1,
                    "categorical_vocab_counts": {"role": 1}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            model_artifact_manifest_path(workspace.path()),
            serde_json::to_vec_pretty(&serde_json::json!({
                "manifest_version": 2,
                "requested_mode": "onnx",
                "active_model_id": "demo-v1",
                "active_metadata_path": "models/demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "test",
                "activation_history": []
            }))
            .unwrap(),
        )
        .unwrap();

        let status = resolve_model_runtime_status(workspace.path()).unwrap();

        assert!(status.available);
        assert_eq!(status.requested_mode, "onnx");
        assert_eq!(status.effective_mode, "onnx_native");
        assert_eq!(status.reason, "model_artifact_loaded_onnx_native");
        assert_eq!(status.model_id.as_deref(), Some("demo-v1"));
        assert_eq!(status.action_source_count, 1);
        assert_eq!(status.principal_vocab_count, 1);
        assert_eq!(status.resource_vocab_count, 1);
        assert_eq!(status.installed_model_count, 1);
        assert_eq!(status.installed_model_ids, vec!["demo-v1".to_string()]);
    }

    #[test]
    fn requested_heuristic_mode_disables_active_model() {
        let workspace = tempfile::tempdir().unwrap();
        let manifests = workspace.path().join("manifests");
        let model_dir = workspace.path().join("models").join("demo-v1");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("action_tower_okta.onnx"), b"onnx").unwrap();
        std::fs::write(model_dir.join("context_tower.onnx"), b"onnx").unwrap();
        std::fs::write(model_dir.join("principal-vocab.json"), b"[\"principal-1\"]").unwrap();
        std::fs::write(model_dir.join("resource-vocab.json"), b"[\"resource-1\"]").unwrap();
        std::fs::write(
            model_dir.join("categorical-vocabs.json"),
            b"{\"role\":{\"analyst\":0}}",
        )
        .unwrap();
        std::fs::write(
            model_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "artifact_version": 1,
                "tenant_id": "tenant-1",
                "model_id": "demo-v1",
                "model_version": "demo-v1",
                "model_family": "contrastive-facade",
                "scoring_mode": "onnx",
                "feature_schema_version": "feature.v1",
                "action_towers": {
                    "okta": {
                        "path": "action_tower_okta.onnx"
                    }
                },
                "context_tower": {
                    "path": "context_tower.onnx"
                },
                "input_vocabs": {
                    "principal_entity_keys_path": "principal-vocab.json",
                    "resource_entity_keys_path": "resource-vocab.json",
                    "categorical_vocabs_path": "categorical-vocabs.json"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            model_artifact_manifest_path(workspace.path()),
            serde_json::to_vec_pretty(&serde_json::json!({
                "manifest_version": 2,
                "requested_mode": "heuristic",
                "active_model_id": "demo-v1",
                "active_metadata_path": "models/demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "manual_deactivate",
                "activation_history": [{
                    "action": "deactivate",
                    "requested_mode": "heuristic",
                    "activated_at": "2026-03-17T00:00:00Z"
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let status = resolve_model_runtime_status(workspace.path()).unwrap();

        assert!(status.available);
        assert_eq!(status.requested_mode, "heuristic");
        assert_eq!(status.effective_mode, "heuristic");
        assert_eq!(status.reason, "requested_heuristic_mode");
    }
}
