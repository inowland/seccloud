use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use seccloud_lake::schema::{normalized_record_batch_to_rows, read_parquet_bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::worker::NormalizationState;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestManifestState {
    pub raw_event_ids: Vec<String>,
    pub raw_event_keys: Vec<String>,
    pub normalized_event_ids: Vec<String>,
    pub normalized_event_keys: Vec<String>,
    pub semantic_event_keys: Vec<String>,
    pub dead_letter_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SourceStatsFile {
    pub sources: BTreeMap<String, SourceStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SourceStats {
    pub seen_event_types: Vec<String>,
    pub seen_raw_fields: Vec<String>,
    pub raw_event_count: usize,
    pub normalized_event_count: usize,
    pub dead_letter_count: usize,
    pub raw_last_seen_at: Option<String>,
    pub normalized_last_seen_at: Option<String>,
    pub recent_window_anchor_at: Option<String>,
    pub raw_daily_counts: BTreeMap<String, usize>,
    pub normalized_daily_counts: BTreeMap<String, usize>,
    pub dead_letter_daily_counts: BTreeMap<String, usize>,
    pub dead_letter_reason_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerStateFile {
    pub normalization_runs: usize,
    pub feature_runs: usize,
    pub detection_runs: usize,
    pub source_stats_runs: usize,
    pub projection_runs: usize,
    pub service_runs: usize,
    pub last_submitted_batch_id: Option<String>,
    pub last_processed_batch_id: Option<String>,
    pub last_normalization_at: Option<String>,
    pub last_feature_at: Option<String>,
    pub last_detection_at: Option<String>,
    pub last_source_stats_at: Option<String>,
    pub last_projection_at: Option<String>,
    pub last_service_at: Option<String>,
    pub last_service_status: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerControlFile {
    pub projection_refresh_requested: bool,
    pub projection_refresh_requested_at: Option<String>,
    pub source_stats_refresh_requested: bool,
    pub source_stats_refresh_requested_at: Option<String>,
}

pub fn ingest_manifest_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("ingest_manifest.json")
}

pub fn source_stats_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("source_stats.json")
}

pub fn worker_state_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("worker_state.json")
}

pub fn worker_control_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("worker_control.json")
}

pub fn load_ingest_manifest(workspace: &Path) -> anyhow::Result<IngestManifestState> {
    let path = ingest_manifest_path(workspace);
    if !path.exists() {
        return Ok(IngestManifestState::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

pub fn save_ingest_manifest(
    workspace: &Path,
    manifest: &IngestManifestState,
) -> anyhow::Result<()> {
    let path = ingest_manifest_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(manifest)?)?;
    Ok(())
}

pub fn state_from_manifest(manifest: IngestManifestState) -> NormalizationState {
    NormalizationState {
        raw_event_ids: manifest.raw_event_ids.into_iter().collect(),
        raw_event_keys: manifest.raw_event_keys.into_iter().collect(),
        normalized_event_ids: manifest.normalized_event_ids.into_iter().collect(),
        normalized_event_keys: manifest.normalized_event_keys.into_iter().collect(),
        semantic_event_keys: manifest.semantic_event_keys.into_iter().collect(),
        dead_letter_ids: manifest.dead_letter_ids.into_iter().collect(),
    }
}

pub fn manifest_from_state(state: NormalizationState) -> IngestManifestState {
    IngestManifestState {
        raw_event_ids: state.raw_event_ids.into_iter().collect(),
        raw_event_keys: state.raw_event_keys.into_iter().collect(),
        normalized_event_ids: state.normalized_event_ids.into_iter().collect(),
        normalized_event_keys: state.normalized_event_keys.into_iter().collect(),
        semantic_event_keys: state.semantic_event_keys.into_iter().collect(),
        dead_letter_ids: state.dead_letter_ids.into_iter().collect(),
    }
}

pub fn load_source_stats(workspace: &Path) -> anyhow::Result<SourceStatsFile> {
    let path = source_stats_path(workspace);
    if !path.exists() {
        return Ok(SourceStatsFile::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

pub fn save_source_stats(workspace: &Path, stats: &SourceStatsFile) -> anyhow::Result<()> {
    let path = source_stats_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(stats)?)?;
    Ok(())
}

pub fn load_worker_state(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    let path = worker_state_path(workspace);
    if !path.exists() {
        return Ok(WorkerStateFile::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

pub fn save_worker_state(workspace: &Path, state: &WorkerStateFile) -> anyhow::Result<()> {
    let path = worker_state_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

pub fn mutate_worker_state(
    workspace: &Path,
    mutate: impl FnOnce(&mut WorkerStateFile),
) -> anyhow::Result<WorkerStateFile> {
    let mut worker_state = load_worker_state(workspace)?;
    mutate(&mut worker_state);
    save_worker_state(workspace, &worker_state)?;
    Ok(worker_state)
}

pub fn record_normalization_run(
    workspace: &Path,
    last_processed_batch_id: Option<String>,
) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.normalization_runs += 1;
        worker_state.last_processed_batch_id = last_processed_batch_id;
        worker_state.last_normalization_at = Some(now_timestamp());
    })
}

pub fn record_feature_run(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.feature_runs += 1;
        worker_state.last_feature_at = Some(now_timestamp());
    })
}

pub fn record_detection_run(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.detection_runs += 1;
        worker_state.last_detection_at = Some(now_timestamp());
    })
}

pub fn record_source_stats_run(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.source_stats_runs += 1;
        worker_state.last_source_stats_at = Some(now_timestamp());
    })
}

pub fn record_projection_run(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.projection_runs += 1;
        worker_state.last_projection_at = Some(now_timestamp());
    })
}

pub fn mark_service_started(workspace: &Path) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.service_runs += 1;
        worker_state.last_service_at = Some(now_timestamp());
    })
}

pub fn finalize_service_status(workspace: &Path, status: &str) -> anyhow::Result<WorkerStateFile> {
    mutate_worker_state(workspace, |worker_state| {
        worker_state.last_service_at = Some(now_timestamp());
        worker_state.last_service_status = Some(status.to_string());
    })
}

pub fn load_worker_control(workspace: &Path) -> anyhow::Result<WorkerControlFile> {
    let path = worker_control_path(workspace);
    if !path.exists() {
        return Ok(WorkerControlFile::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

pub fn save_worker_control(workspace: &Path, state: &WorkerControlFile) -> anyhow::Result<()> {
    let path = worker_control_path(workspace);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

pub fn clear_source_stats_refresh_request(workspace: &Path) -> anyhow::Result<()> {
    let mut control = load_worker_control(workspace)?;
    control.source_stats_refresh_requested = false;
    control.source_stats_refresh_requested_at = None;
    save_worker_control(workspace, &control)
}

pub fn clear_projection_refresh_request(workspace: &Path) -> anyhow::Result<()> {
    let mut control = load_worker_control(workspace)?;
    control.projection_refresh_requested = false;
    control.projection_refresh_requested_at = None;
    save_worker_control(workspace, &control)
}

pub fn now_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

pub fn source_stats_summary(stats: &SourceStatsFile) -> serde_json::Value {
    serde_json::json!({
        "source_count": stats.sources.len(),
        "sources": stats.sources.keys().cloned().collect::<Vec<_>>(),
        "window_days": {"raw": 1, "normalized": 1, "dead_letters": 7},
    })
}

fn merge_latest(current: Option<String>, candidate: Option<&str>) -> Option<String> {
    match (current, candidate) {
        (None, None) => None,
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate.to_string()),
        (Some(current), Some(candidate)) => Some(current.max(candidate.to_string())),
    }
}

fn increment_bucket(buckets: &mut BTreeMap<String, usize>, timestamp: Option<&str>) {
    if let Some(timestamp) = timestamp {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(timestamp) {
            let bucket = parsed
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d")
                .to_string();
            *buckets.entry(bucket).or_insert(0) += 1;
        }
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
            } else if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
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

fn collect_json_files(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    collect_files(root, "json")
}

fn as_object<'a>(
    value: &'a Value,
    context: &str,
) -> anyhow::Result<&'a serde_json::Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{context} should be an object"))
}

pub fn rebuild_source_stats(workspace: &Path) -> anyhow::Result<SourceStatsFile> {
    let mut stats = SourceStatsFile::default();

    for path in collect_json_files(&workspace.join("raw"))? {
        let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let event = as_object(&value, "raw event")?;
        let source = event
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("raw event missing source"))?;
        let entry = stats.sources.entry(source.to_string()).or_default();
        let observed_at = event
            .get("received_at")
            .or_else(|| event.get("observed_at"))
            .and_then(Value::as_str);
        entry.raw_event_count += 1;
        entry.raw_last_seen_at = merge_latest(entry.raw_last_seen_at.take(), observed_at);
        entry.recent_window_anchor_at =
            merge_latest(entry.recent_window_anchor_at.take(), observed_at);
        increment_bucket(&mut entry.raw_daily_counts, observed_at);
        let mut seen_fields: BTreeSet<String> = entry.seen_raw_fields.iter().cloned().collect();
        seen_fields.extend(event.keys().cloned());
        entry.seen_raw_fields = seen_fields.into_iter().collect();
        if let Some(event_type) = event.get("event_type").and_then(Value::as_str) {
            let mut seen_event_types: BTreeSet<String> =
                entry.seen_event_types.iter().cloned().collect();
            seen_event_types.insert(event_type.to_string());
            entry.seen_event_types = seen_event_types.into_iter().collect();
        }
    }

    fn record_normalized_event(
        stats: &mut SourceStatsFile,
        event: &serde_json::Map<String, Value>,
    ) -> anyhow::Result<()> {
        let source = event
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("normalized event missing source"))?;
        let entry = stats.sources.entry(source.to_string()).or_default();
        let observed_at = event.get("observed_at").and_then(Value::as_str);
        entry.normalized_event_count += 1;
        entry.normalized_last_seen_at =
            merge_latest(entry.normalized_last_seen_at.take(), observed_at);
        entry.recent_window_anchor_at =
            merge_latest(entry.recent_window_anchor_at.take(), observed_at);
        increment_bucket(&mut entry.normalized_daily_counts, observed_at);
        Ok(())
    }

    let normalized_json_files = collect_json_files(&workspace.join("normalized"))?;
    if normalized_json_files.is_empty() {
        for path in collect_files(&workspace.join("lake").join("normalized"), "parquet")? {
            let bytes = std::fs::read(path)?;
            for batch in read_parquet_bytes(&bytes)? {
                for row in normalized_record_batch_to_rows(&batch)? {
                    let value: Value = serde_json::from_str(&row.payload_json)?;
                    let event = as_object(&value, "normalized event")?;
                    record_normalized_event(&mut stats, event)?;
                }
            }
        }
    } else {
        for path in normalized_json_files {
            let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            let event = as_object(&value, "normalized event")?;
            record_normalized_event(&mut stats, event)?;
        }
    }

    for path in collect_json_files(&workspace.join("dead_letters"))? {
        let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let event = as_object(&value, "dead letter")?;
        let source = event
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("dead letter missing source"))?;
        let entry = stats.sources.entry(source.to_string()).or_default();
        let raw_event = event
            .get("raw_event")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("dead letter missing raw_event"))?;
        let observed_at = raw_event
            .get("received_at")
            .or_else(|| raw_event.get("observed_at"))
            .and_then(Value::as_str);
        entry.dead_letter_count += 1;
        entry.recent_window_anchor_at =
            merge_latest(entry.recent_window_anchor_at.take(), observed_at);
        increment_bucket(&mut entry.dead_letter_daily_counts, observed_at);
        if let Some(reason) = event.get("reason").and_then(Value::as_str) {
            *entry
                .dead_letter_reason_counts
                .entry(reason.to_string())
                .or_insert(0) += 1;
        }
    }

    Ok(stats)
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
            "seccloud-local-runtime-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(root.join("raw/source=okta")).unwrap();
        std::fs::create_dir_all(root.join("normalized/source=okta")).unwrap();
        std::fs::create_dir_all(root.join("dead_letters/source=okta")).unwrap();
        std::fs::create_dir_all(root.join("manifests")).unwrap();
        root
    }

    #[test]
    fn rebuild_source_stats_rolls_up_workspace_json_files() {
        let root = test_workspace();
        std::fs::write(
            root.join("raw/source=okta/raw.json"),
            serde_json::to_vec(&serde_json::json!({
                "source": "okta",
                "event_type": "login",
                "received_at": "2026-03-15T10:00:00Z",
                "actor_email": "alice@example.com"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("normalized/source=okta/normalized.json"),
            serde_json::to_vec(&serde_json::json!({
                "source": "okta",
                "observed_at": "2026-03-15T09:59:00Z"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("dead_letters/source=okta/dead.json"),
            serde_json::to_vec(&serde_json::json!({
                "source": "okta",
                "reason": "missing_required_field:actor_email",
                "raw_event": {
                    "source": "okta",
                    "received_at": "2026-03-15T10:01:00Z"
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let stats = rebuild_source_stats(&root).unwrap();
        let okta = stats.sources.get("okta").unwrap();
        assert_eq!(okta.raw_event_count, 1);
        assert_eq!(okta.normalized_event_count, 1);
        assert_eq!(okta.dead_letter_count, 1);
        assert_eq!(
            okta.raw_last_seen_at.as_deref(),
            Some("2026-03-15T10:00:00Z")
        );
        assert_eq!(
            okta.normalized_last_seen_at.as_deref(),
            Some("2026-03-15T09:59:00Z")
        );
        assert_eq!(
            okta.recent_window_anchor_at.as_deref(),
            Some("2026-03-15T10:01:00Z")
        );
        assert_eq!(
            okta.dead_letter_reason_counts
                .get("missing_required_field:actor_email"),
            Some(&1)
        );
        assert!(okta.seen_event_types.iter().any(|value| value == "login"));
        assert!(
            okta.seen_raw_fields
                .iter()
                .any(|value| value == "actor_email")
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_source_stats_accepts_partial_source_entries() {
        let root = test_workspace();
        std::fs::write(
            source_stats_path(&root),
            serde_json::to_vec(&serde_json::json!({
                "sources": {
                    "github": {
                        "normalized_event_count": 1,
                        "dead_letter_count": 0,
                        "last_seen_at": "2026-03-15T10:00:00Z"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let stats = load_source_stats(&root).unwrap();
        let github = stats.sources.get("github").unwrap();
        assert_eq!(github.normalized_event_count, 1);
        assert_eq!(github.dead_letter_count, 0);
        assert!(github.seen_event_types.is_empty());
        assert!(github.seen_raw_fields.is_empty());
        assert_eq!(github.raw_event_count, 0);
        assert_eq!(github.normalized_last_seen_at, None);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clear_source_stats_refresh_request_resets_worker_control_flag() {
        let root = test_workspace();
        save_worker_control(
            &root,
            &WorkerControlFile {
                source_stats_refresh_requested: true,
                source_stats_refresh_requested_at: Some("2026-03-15T10:00:00Z".into()),
                ..WorkerControlFile::default()
            },
        )
        .unwrap();

        clear_source_stats_refresh_request(&root).unwrap();

        let control = load_worker_control(&root).unwrap();
        assert!(!control.source_stats_refresh_requested);
        assert_eq!(control.source_stats_refresh_requested_at, None);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clear_projection_refresh_request_resets_worker_control_flag() {
        let root = test_workspace();
        save_worker_control(
            &root,
            &WorkerControlFile {
                projection_refresh_requested: true,
                projection_refresh_requested_at: Some("2026-03-15T10:00:00Z".into()),
                ..WorkerControlFile::default()
            },
        )
        .unwrap();

        clear_projection_refresh_request(&root).unwrap();

        let control = load_worker_control(&root).unwrap();
        assert!(!control.projection_refresh_requested);
        assert_eq!(control.projection_refresh_requested_at, None);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rebuild_source_stats_falls_back_to_lake_normalized_parquet() {
        let root = test_workspace();
        std::fs::remove_dir_all(root.join("normalized")).unwrap();
        std::fs::create_dir_all(root.join("lake/normalized")).unwrap();

        let batch = normalized_event_record_batch(&[NormalizedEventRow {
            normalized_schema_version: NORMALIZED_SCHEMA_VERSION.to_string(),
            tenant_id: "tenant-1".to_string(),
            source: "okta".to_string(),
            integration_id: "okta-primary".to_string(),
            observed_at: "2026-03-15T09:59:00Z".to_string(),
            event_id: "evt_1".to_string(),
            event_key: "okta:e1:okta-primary".to_string(),
            source_event_id: "e1".to_string(),
            principal_entity_key: "principal:alice".to_string(),
            resource_entity_key: "resource:admin".to_string(),
            action_source: "okta".to_string(),
            action_verb: "login".to_string(),
            action_category: "authentication".to_string(),
            payload_json: serde_json::json!({
                "source": "okta",
                "observed_at": "2026-03-15T09:59:00Z",
                "event_id": "evt_1",
            })
            .to_string(),
        }])
        .unwrap();
        std::fs::write(
            root.join("lake/normalized/normalized.parquet"),
            write_parquet_bytes(&batch).unwrap(),
        )
        .unwrap();

        let stats = rebuild_source_stats(&root).unwrap();
        let okta = stats.sources.get("okta").unwrap();
        assert_eq!(okta.normalized_event_count, 1);
        assert_eq!(
            okta.normalized_last_seen_at.as_deref(),
            Some("2026-03-15T09:59:00Z")
        );

        std::fs::remove_dir_all(root).unwrap();
    }
}
