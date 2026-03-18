use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use seccloud_lake::batch_writer::{
    DetectionBatchDescriptor, DetectionBatchWriteInput, write_detection_batch,
};
use seccloud_lake::object_store::LocalObjectStore;
use seccloud_lake::schema::{DETECTION_SCHEMA_VERSION, DetectionRow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::event_index::ensure_event_index;
use crate::local_runtime::{SourceStatsFile, load_source_stats};
use crate::model_runtime::{
    ModelRuntimeStatus, ModelScorePolicy, SourceScorePolicy, load_active_model_bundle,
    resolve_model_runtime_status,
};
use crate::model_scoring::{build_model_score_request, run_model_score};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectionContextEntry {
    pub principal_total_events: usize,
    pub action_count: usize,
    pub resource_count: usize,
    pub peer_resource_count: usize,
    pub geo_history_count: usize,
    pub geo_seen_before: bool,
    pub peer_group: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectionContextFile {
    pub context_version: usize,
    pub input_signature: String,
    pub event_count: usize,
    pub ordered_event_ids: Vec<String>,
    pub contexts_by_event_id: BTreeMap<String, DetectionContextEntry>,
    pub aggregates: DetectionAggregates,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectionAggregates {
    pub events_by_source: BTreeMap<String, usize>,
    pub total_events: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectionResult {
    pub normalized_event_count: usize,
    pub new_detection_count: usize,
    pub total_detection_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionWorkerResult {
    pub detect: DetectionResult,
    pub ops_metadata: OpsMetadata,
    pub scoring_runtime: ModelRuntimeStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionModeSummary {
    pub detection_count: usize,
    pub distinct_event_count: usize,
    pub by_source: BTreeMap<String, usize>,
    pub by_scenario: BTreeMap<String, usize>,
    pub model_backed_detection_count: usize,
    pub heuristic_fallback_detection_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionComparisonSample {
    pub detection_id: String,
    pub event_id: String,
    pub source: String,
    pub scenario: String,
    pub title: String,
    pub score: f64,
    pub severity: String,
    pub scoring_mode: String,
    pub policy_scope: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionModeComparisonResult {
    pub event_count: usize,
    pub scoring_runtime: ModelRuntimeStatus,
    pub heuristic: DetectionModeSummary,
    pub model: DetectionModeSummary,
    pub overlap_detection_count: usize,
    pub overlap_event_count: usize,
    pub heuristic_only_detection_count: usize,
    pub model_only_detection_count: usize,
    pub heuristic_only_samples: Vec<DetectionComparisonSample>,
    pub model_only_samples: Vec<DetectionComparisonSample>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ScoreQuantiles {
    pub count: usize,
    pub mean: f64,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ScenarioRecallSummary {
    pub total_event_count: usize,
    pub detected_event_count: usize,
    pub recall: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SourceBehaviorSummary {
    pub baseline_event_count: usize,
    pub baseline_detected_event_count: usize,
    pub baseline_alert_rate: f64,
    pub baseline_score_quantiles: ScoreQuantiles,
    pub attack_event_count: usize,
    pub attack_detected_event_count: usize,
    pub attack_recall: f64,
    pub attack_score_quantiles: ScoreQuantiles,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionEvaluationModeSummary {
    pub detection_count: usize,
    pub detection_rate: f64,
    pub model_backed_event_count: usize,
    pub heuristic_fallback_event_count: usize,
    pub scenario_recall: BTreeMap<String, ScenarioRecallSummary>,
    pub source_behavior: BTreeMap<String, SourceBehaviorSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectionEvaluationReport {
    pub event_count: usize,
    pub scoring_runtime: ModelRuntimeStatus,
    pub heuristic: DetectionEvaluationModeSummary,
    pub model: DetectionEvaluationModeSummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ThresholdSweepPoint {
    pub threshold: f64,
    pub baseline_alert_rate: f64,
    pub attack_recall: f64,
    pub baseline_detected_event_count: usize,
    pub attack_detected_event_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SourceThresholdSweep {
    pub current_policy_threshold: f64,
    pub candidate_count: usize,
    pub recommended_threshold_for_5pct_budget: f64,
    pub recommended_attack_recall_for_5pct_budget: f64,
    pub points: Vec<ThresholdSweepPoint>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ModelThresholdSweepReport {
    pub event_count: usize,
    pub scoring_runtime: ModelRuntimeStatus,
    pub source_sweeps: BTreeMap<String, SourceThresholdSweep>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpsMetadata {
    pub workspace: String,
    pub event_counts_by_source: BTreeMap<String, usize>,
    pub dead_letter_count: usize,
    pub dead_letter_counts_by_source: BTreeMap<String, usize>,
    pub contains_raw_payloads: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct DerivedStateFile {
    #[serde(default)]
    pub principal_profiles: BTreeMap<String, Value>,
    #[serde(default)]
    pub peer_groups: BTreeMap<String, Value>,
    #[serde(default)]
    pub access_histories: BTreeMap<String, Value>,
    #[serde(default)]
    pub aggregates: BTreeMap<String, Value>,
    #[serde(default)]
    pub embeddings: BTreeMap<String, Value>,
    #[serde(default)]
    pub case_artifacts: BTreeMap<String, Value>,
    #[serde(default)]
    pub feedback_labels: BTreeMap<String, Value>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct DetectionBaseline {
    prior_event_count: usize,
    prior_action_count: usize,
    prior_resource_count: usize,
    peer_resource_count: usize,
    geo_history_count: usize,
    geo_seen_before: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredEventObservation {
    event_id: String,
    source: String,
    truth_scenario: String,
    score: f64,
    detected: bool,
    scoring_mode: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct DetectorContribution {
    score_delta: f64,
    reasons: Vec<String>,
    attributions: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct EvidencePointer {
    source: String,
    object_key: String,
    raw_event_id: String,
    observed_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct DetectionPayload {
    detection_id: String,
    scenario: String,
    title: String,
    score: f64,
    confidence: f64,
    severity: String,
    reasons: Vec<String>,
    feature_attributions: BTreeMap<String, f64>,
    event_ids: Vec<String>,
    related_entity_ids: Vec<String>,
    evidence: Vec<EvidencePointer>,
    model_version: String,
    model_rationale: Option<DetectionModelRationale>,
    status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct DetectionModelRationale {
    scoring_mode: String,
    policy_scope: String,
    detection_threshold: f64,
    high_severity_threshold: f64,
    model_score: f64,
    score_margin: f64,
    calibration_source: Option<String>,
    calibration_reason: Option<String>,
}

struct DetectionBuildInput {
    score: f64,
    detection_threshold: f64,
    high_severity_threshold: f64,
    reasons: Vec<String>,
    attributions: BTreeMap<String, f64>,
    model_version: String,
    model_rationale: Option<DetectionModelRationale>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PrincipalProfileState {
    prior_event_count: usize,
    action_counts: BTreeMap<String, usize>,
    resource_counts: BTreeMap<String, usize>,
    seen_geos: BTreeSet<String>,
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

fn write_json<T: Serialize>(path: &Path, payload: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(payload)?)?;
    Ok(())
}

fn detection_context_path(workspace: &Path) -> PathBuf {
    workspace.join("manifests").join("detection_context.json")
}

fn derived_state_path(workspace: &Path) -> PathBuf {
    workspace.join("derived").join("derived_state.json")
}

fn detections_dir(workspace: &Path) -> PathBuf {
    workspace.join("detections")
}

fn ops_metadata_path(workspace: &Path) -> PathBuf {
    workspace.join("ops").join("metadata.json")
}

fn load_detection_context(workspace: &Path) -> anyhow::Result<DetectionContextFile> {
    read_json_or_default(&detection_context_path(workspace))
}

fn save_detection_context(workspace: &Path, context: &DetectionContextFile) -> anyhow::Result<()> {
    write_json(&detection_context_path(workspace), context)
}

fn load_derived_state(workspace: &Path) -> anyhow::Result<DerivedStateFile> {
    read_json_or_default(&derived_state_path(workspace))
}

fn save_derived_state(workspace: &Path, state: &DerivedStateFile) -> anyhow::Result<()> {
    write_json(&derived_state_path(workspace), state)
}

fn read_timestamp_key(event: &Value) -> (&str, &str) {
    let observed_at = event
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let event_id = event
        .get("event_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    (observed_at, event_id)
}

pub fn build_detection_context(workspace: &Path) -> anyhow::Result<DetectionContextFile> {
    let event_index = ensure_event_index(workspace)?;
    let mut ordered_events: Vec<&Value> = event_index
        .events_by_id
        .values()
        .filter(|value| value.is_object())
        .collect();
    ordered_events.sort_by_key(|event| read_timestamp_key(event));

    let mut peer_groups: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    let mut aggregate_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut contexts_by_event_id = BTreeMap::new();
    let mut ordered_event_ids = Vec::new();
    let mut profiles: BTreeMap<String, PrincipalProfileState> = BTreeMap::new();

    for event in ordered_events {
        let Some(event_id) = event.get("event_id").and_then(Value::as_str) else {
            continue;
        };
        if event_id.is_empty() {
            continue;
        }
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
        let action = event
            .get("action")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let attributes = event
            .get("attributes")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let Some(principal_id) = principal.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(resource_id) = resource.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(action_verb) = action.get("verb").and_then(Value::as_str) else {
            continue;
        };
        let peer_group = principal
            .get("department")
            .and_then(Value::as_str)
            .unwrap_or("__unknown__");
        let peer_group_key = if peer_group.is_empty() {
            "__unknown__"
        } else {
            peer_group
        };

        let profile = profiles.entry(principal_id.to_string()).or_default();
        let peer_state = peer_groups.entry(peer_group_key.to_string()).or_default();
        let geo = attributes.get("geo").and_then(Value::as_str);

        contexts_by_event_id.insert(
            event_id.to_string(),
            DetectionContextEntry {
                principal_total_events: profile.prior_event_count,
                action_count: profile.action_counts.get(action_verb).copied().unwrap_or(0),
                resource_count: profile
                    .resource_counts
                    .get(resource_id)
                    .copied()
                    .unwrap_or(0),
                peer_resource_count: peer_state.get(resource_id).copied().unwrap_or(0),
                geo_history_count: profile.seen_geos.len(),
                geo_seen_before: geo
                    .map(|candidate| profile.seen_geos.contains(candidate))
                    .unwrap_or(false),
                peer_group: peer_group_key.to_string(),
                source: event
                    .get("source")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            },
        );
        ordered_event_ids.push(event_id.to_string());

        profile.prior_event_count += 1;
        *profile
            .action_counts
            .entry(action_verb.to_string())
            .or_insert(0) += 1;
        *profile
            .resource_counts
            .entry(resource_id.to_string())
            .or_insert(0) += 1;
        if let Some(geo) = geo.filter(|candidate| !candidate.is_empty()) {
            profile.seen_geos.insert(geo.to_string());
        }
        *peer_state.entry(resource_id.to_string()).or_insert(0) += 1;
        if let Some(source) = event
            .get("source")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            *aggregate_counts.entry(source.to_string()).or_insert(0) += 1;
        }
    }

    let payload = DetectionContextFile {
        context_version: 1,
        input_signature: event_index.input_signature,
        event_count: ordered_event_ids.len(),
        ordered_event_ids,
        contexts_by_event_id,
        aggregates: DetectionAggregates {
            total_events: aggregate_counts.values().sum(),
            events_by_source: aggregate_counts,
        },
    };
    save_detection_context(workspace, &payload)?;
    Ok(payload)
}

pub fn ensure_detection_context(workspace: &Path) -> anyhow::Result<DetectionContextFile> {
    let event_index = ensure_event_index(workspace)?;
    let current = load_detection_context(workspace)?;
    if current.input_signature == event_index.input_signature {
        return Ok(current);
    }
    build_detection_context(workspace)
}

fn existing_detection_ids(workspace: &Path) -> anyhow::Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    let root = detections_dir(workspace);
    if !root.exists() {
        return Ok(ids);
    }
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let payload: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        if let Some(detection_id) = payload.get("detection_id").and_then(Value::as_str) {
            ids.insert(detection_id.to_string());
        }
    }
    Ok(ids)
}

fn save_detection_payload(workspace: &Path, detection: &DetectionPayload) -> anyhow::Result<()> {
    let path = detections_dir(workspace).join(format!("{}.json", detection.detection_id));
    write_json(&path, detection)
}

fn workspace_tenant_id(workspace: &Path) -> String {
    if let Ok(tenant_id) = std::env::var("SECCLOUD_TENANT_ID") {
        if !tenant_id.trim().is_empty() {
            return tenant_id;
        }
    }
    if let Ok(Some((_manifest, metadata, _metadata_path))) = load_active_model_bundle(workspace) {
        if !metadata.tenant_id.trim().is_empty() {
            return metadata.tenant_id;
        }
    }
    "local".to_string()
}

fn parse_detected_at(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc))
}

fn detection_row_from_payload(
    tenant_id: &str,
    detection: &DetectionPayload,
) -> anyhow::Result<DetectionRow> {
    let observed_at = detection
        .evidence
        .first()
        .map(|evidence| evidence.observed_at.clone())
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
    Ok(DetectionRow {
        detection_schema_version: DETECTION_SCHEMA_VERSION.to_string(),
        tenant_id: tenant_id.to_string(),
        detection_id: detection.detection_id.clone(),
        observed_at,
        scenario: detection.scenario.clone(),
        title: detection.title.clone(),
        score: detection.score,
        confidence: detection.confidence,
        severity: detection.severity.clone(),
        status: detection.status.clone(),
        model_version: detection.model_version.clone(),
        primary_event_id: detection.event_ids.first().cloned().unwrap_or_default(),
        payload_json: serde_json::to_string(detection)?,
    })
}

fn persist_detections(
    workspace: &Path,
    detections: &[DetectionPayload],
    detection_context: &DetectionContextFile,
    scoring_runtime_mode: &str,
) -> anyhow::Result<()> {
    if detections.is_empty() {
        return Ok(());
    }
    for detection in detections {
        save_detection_payload(workspace, detection)?;
    }
    let tenant_id = workspace_tenant_id(workspace);
    let rows: Vec<DetectionRow> = detections
        .iter()
        .map(|detection| detection_row_from_payload(&tenant_id, detection))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let detected_at = detections
        .iter()
        .flat_map(|detection| detection.evidence.iter())
        .filter_map(|evidence| parse_detected_at(&evidence.observed_at))
        .max()
        .unwrap_or_else(chrono::Utc::now);
    let model_version = detections
        .first()
        .map(|detection| detection.model_version.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let store = LocalObjectStore::new(workspace);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(write_detection_batch(
        &store,
        DetectionBatchWriteInput {
            descriptor: DetectionBatchDescriptor {
                batch_id: format!("det_{}", uuid::Uuid::now_v7()),
                tenant_id,
                detected_at,
            },
            detection_schema_version: DETECTION_SCHEMA_VERSION.to_string(),
            upstream_detection_context_signature: detection_context.input_signature.clone(),
            scoring_runtime_mode: scoring_runtime_mode.to_string(),
            model_version,
            rows,
        },
    ))?;
    Ok(())
}

fn object<'a>(value: &'a Value, field: &str) -> Option<&'a Map<String, Value>> {
    value.get(field)?.as_object()
}

fn str_field<'a>(value: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    value.get(field)?.as_str()
}

fn bool_attr(event: &Value, key: &str) -> bool {
    object(event, "attributes")
        .and_then(|attrs| attrs.get(key))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn string_attr<'a>(event: &'a Value, key: &str) -> Option<&'a str> {
    object(event, "attributes")
        .and_then(|attrs| attrs.get(key))
        .and_then(Value::as_str)
}

fn number_attr(event: &Value, key: &str) -> i64 {
    object(event, "attributes")
        .and_then(|attrs| attrs.get(key))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

fn sensitivity(event: &Value) -> Option<&str> {
    object(event, "resource").and_then(|resource| str_field(resource, "sensitivity"))
}

fn source(event: &Value) -> Option<&str> {
    event.get("source").and_then(Value::as_str)
}

fn action_verb(event: &Value) -> Option<&str> {
    object(event, "action").and_then(|action| str_field(action, "verb"))
}

fn evidence_pointer(event: &Value) -> Option<EvidencePointer> {
    let evidence = object(event, "evidence")?;
    Some(EvidencePointer {
        source: str_field(evidence, "source")?.to_string(),
        object_key: str_field(evidence, "object_key")?.to_string(),
        raw_event_id: str_field(evidence, "raw_event_id")?.to_string(),
        observed_at: str_field(evidence, "observed_at")?.to_string(),
    })
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn principal_novelty_detector(
    _event: &Value,
    baseline: &DetectionBaseline,
) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    if baseline.prior_event_count >= 3 && baseline.prior_action_count == 0 {
        contribution
            .reasons
            .push("principal_has_not_performed_this_action_before".to_string());
        contribution
            .attributions
            .insert("new_action_for_principal".to_string(), 0.25);
        contribution.score_delta += 0.25;
    }
    if baseline.prior_event_count >= 3 && baseline.prior_resource_count == 0 {
        contribution
            .reasons
            .push("principal_has_not_accessed_this_resource_before".to_string());
        contribution
            .attributions
            .insert("new_resource_for_principal".to_string(), 0.25);
        contribution.score_delta += 0.25;
    }
    contribution
}

fn peer_rarity_detector(total_events: usize, peer_resource_count: usize) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    if peer_resource_count == 0 && total_events >= 3 {
        contribution
            .reasons
            .push("resource_is_rare_for_peer_group".to_string());
        contribution
            .attributions
            .insert("peer_group_rarity".to_string(), 0.15);
        contribution.score_delta += 0.15;
    }
    contribution
}

fn sensitivity_and_exfil_detector(event: &Value) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    if matches!(sensitivity(event), Some("high" | "critical")) {
        contribution
            .reasons
            .push("resource_is_sensitive".to_string());
        contribution
            .attributions
            .insert("sensitive_resource".to_string(), 0.15);
        contribution.score_delta += 0.15;
    }
    if bool_attr(event, "privileged") {
        contribution
            .reasons
            .push("privileged_identity_activity".to_string());
        contribution
            .attributions
            .insert("privileged_activity".to_string(), 0.2);
        contribution.score_delta += 0.2;
    }
    if matches!(
        action_verb(event),
        Some("clone" | "archive_download" | "share_external" | "export")
    ) {
        contribution
            .reasons
            .push("action_has_exfiltration_characteristics".to_string());
        contribution
            .attributions
            .insert("high_risk_action".to_string(), 0.2);
        contribution.score_delta += 0.2;
    }
    if bool_attr(event, "external") {
        contribution
            .reasons
            .push("resource_shared_externally".to_string());
        contribution
            .attributions
            .insert("external_share".to_string(), 0.2);
        contribution.score_delta += 0.2;
    }
    contribution
}

fn geo_anomaly_detector(event: &Value, baseline: &DetectionBaseline) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    let has_geo_history = baseline.geo_history_count > 0;
    if matches!(source(event), Some("okta"))
        && baseline.prior_event_count >= 3
        && has_geo_history
        && !baseline.geo_seen_before
    {
        contribution
            .reasons
            .push("login_from_unfamiliar_geography".to_string());
        contribution.attributions.insert("new_geo".to_string(), 0.2);
        contribution.score_delta += 0.2;
    }
    if matches!(source(event), Some("okta"))
        && baseline.prior_event_count >= 3
        && has_geo_history
        && !baseline.geo_seen_before
        && bool_attr(event, "privileged")
    {
        contribution
            .reasons
            .push("privileged_login_combines_unfamiliar_geo_and_sensitive_access".to_string());
        contribution
            .attributions
            .insert("privileged_geo_combo".to_string(), 0.15);
        contribution.score_delta += 0.15;
    }
    contribution
}

fn volume_detector(event: &Value) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    if matches!(source(event), Some("snowflake")) && number_attr(event, "rows_read") >= 100_000 {
        contribution
            .reasons
            .push("large_sensitive_query".to_string());
        contribution
            .attributions
            .insert("large_query".to_string(), 0.15);
        contribution.score_delta += 0.15;
    }
    contribution
}

fn context_suppression_detector(event: &Value) -> DetectorContribution {
    let mut contribution = DetectorContribution::default();
    if bool_attr(event, "expected_transition") || bool_attr(event, "role_change_window") {
        contribution
            .reasons
            .push("activity_occurs_during_expected_transition_window".to_string());
        contribution
            .attributions
            .insert("expected_transition".to_string(), -0.55);
        contribution.score_delta -= 0.55;
    }
    if bool_attr(event, "approved_travel") {
        contribution
            .reasons
            .push("activity_occurs_during_approved_travel".to_string());
        contribution
            .attributions
            .insert("approved_travel".to_string(), -0.55);
        contribution.score_delta -= 0.55;
    }
    if bool_attr(event, "incident_response_window")
        && string_attr(event, "incident_ticket").is_some()
    {
        contribution
            .reasons
            .push("activity_occurs_during_documented_incident_response".to_string());
        contribution
            .attributions
            .insert("incident_response".to_string(), -0.65);
        contribution.score_delta -= 0.65;
    }
    if bool_attr(event, "quarter_close_window") && bool_attr(event, "approved_workflow") {
        contribution
            .reasons
            .push("activity_occurs_during_approved_finance_close_workflow".to_string());
        contribution
            .attributions
            .insert("finance_close_workflow".to_string(), -0.60);
        contribution.score_delta -= 0.60;
    }
    contribution
}

fn fuse_contributions(
    contributions: Vec<DetectorContribution>,
) -> (f64, Vec<String>, BTreeMap<String, f64>) {
    let mut score = 0.0;
    let mut reasons = Vec::new();
    let mut attributions = BTreeMap::new();
    for contribution in contributions {
        score += contribution.score_delta;
        reasons.extend(contribution.reasons);
        attributions.extend(contribution.attributions);
    }
    (score.clamp(0.0, 0.99), reasons, attributions)
}

fn collect_contributions(event: &Value, baseline: &DetectionBaseline) -> Vec<DetectorContribution> {
    vec![
        principal_novelty_detector(event, baseline),
        peer_rarity_detector(baseline.prior_event_count, baseline.peer_resource_count),
        sensitivity_and_exfil_detector(event),
        geo_anomaly_detector(event, baseline),
        volume_detector(event),
        context_suppression_detector(event),
    ]
}

fn scenario_and_title(
    event: &Value,
    reasons: &[String],
) -> anyhow::Result<(&'static str, &'static str)> {
    let source = event
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("event source missing"))?;
    let action_verb = action_verb(event).unwrap_or_default();
    Ok(
        if source == "okta"
            && string_attr(event, "geo").is_some()
            && reasons
                .iter()
                .any(|reason| reason == "login_from_unfamiliar_geography")
        {
            (
                "compromised_privileged_identity",
                "Privileged identity used from an unfamiliar geography",
            )
        } else if source == "github" && matches!(action_verb, "clone" | "archive_download") {
            ("unusual_repo_export", "Unusual repository clone or export")
        } else if source == "gworkspace" && bool_attr(event, "external") {
            (
                "unusual_external_sharing",
                "Unusual external sharing of a sensitive document",
            )
        } else if source == "snowflake" {
            ("unusual_data_access", "Unusual sensitive dataset access")
        } else {
            ("general_behavioral_anomaly", "Behavioral anomaly")
        },
    )
}

fn truth_scenario(event: &Value) -> String {
    string_attr(event, "scenario")
        .unwrap_or("baseline")
        .to_string()
}

fn quantile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let clamped = percentile.clamp(0.0, 1.0);
    let index = ((sorted_values.len() - 1) as f64 * clamped).round() as usize;
    sorted_values[index]
}

fn score_quantiles(values: &[f64]) -> ScoreQuantiles {
    if values.is_empty() {
        return ScoreQuantiles::default();
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let sum: f64 = sorted.iter().sum();
    ScoreQuantiles {
        count: sorted.len(),
        mean: round2(sum / sorted.len() as f64),
        min: round2(*sorted.first().unwrap_or(&0.0)),
        p50: round2(quantile(&sorted, 0.50)),
        p90: round2(quantile(&sorted, 0.90)),
        p95: round2(quantile(&sorted, 0.95)),
        p99: round2(quantile(&sorted, 0.99)),
        max: round2(*sorted.last().unwrap_or(&0.0)),
    }
}

fn rounded_threshold(value: f64) -> f64 {
    ((value.clamp(0.0, 1.0) * 10_000.0).round()) / 10_000.0
}

fn threshold_candidates(values: &[f64], current_threshold: f64) -> Vec<f64> {
    let quantiles = if values.is_empty() {
        vec![0.50, 0.75, 0.90, 0.95, 0.99]
    } else {
        let mut sorted = values.to_vec();
        sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        vec![
            quantile(&sorted, 0.50),
            quantile(&sorted, 0.75),
            quantile(&sorted, 0.90),
            quantile(&sorted, 0.95),
            quantile(&sorted, 0.99),
            *sorted.last().unwrap_or(&current_threshold),
        ]
    };
    let mut candidates: Vec<f64> = vec![0.60, 0.70, 0.80, 0.90, 0.95, 0.99, current_threshold];
    candidates.extend(quantiles);
    candidates.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    candidates
        .into_iter()
        .map(rounded_threshold)
        .fold(Vec::new(), |mut deduped, threshold| {
            if deduped.last().copied() != Some(threshold) {
                deduped.push(threshold);
            }
            deduped
        })
}

fn baseline_for_context(context: &DetectionContextEntry) -> DetectionBaseline {
    DetectionBaseline {
        prior_event_count: context.principal_total_events,
        prior_action_count: context.action_count,
        prior_resource_count: context.resource_count,
        peer_resource_count: context.peer_resource_count,
        geo_history_count: context.geo_history_count,
        geo_seen_before: context.geo_seen_before,
    }
}

fn collect_scored_detections(
    event_index: &crate::event_index::EventIndexFile,
    detection_context: &DetectionContextFile,
    mut score_fn: impl FnMut(
        &str,
        &Value,
        &DetectionBaseline,
    ) -> anyhow::Result<Option<DetectionPayload>>,
) -> anyhow::Result<Vec<DetectionPayload>> {
    let mut detections = Vec::new();
    for event_id in &detection_context.ordered_event_ids {
        let Some(event) = event_index.events_by_id.get(event_id) else {
            continue;
        };
        let Some(context) = detection_context.contexts_by_event_id.get(event_id) else {
            continue;
        };
        let baseline = baseline_for_context(context);
        if let Some(detection) = score_fn(event_id, event, &baseline)? {
            detections.push(detection);
        }
    }
    Ok(detections)
}

fn summarize_detections(detections: &[DetectionPayload]) -> DetectionModeSummary {
    let mut by_source = BTreeMap::new();
    let mut by_scenario = BTreeMap::new();
    let mut distinct_event_ids = BTreeSet::new();
    let mut model_backed_detection_count = 0;
    let mut heuristic_fallback_detection_count = 0;

    for detection in detections {
        if let Some(event_id) = detection.event_ids.first() {
            distinct_event_ids.insert(event_id.clone());
        }
        if let Some(source) = detection
            .evidence
            .first()
            .map(|evidence| evidence.source.clone())
        {
            *by_source.entry(source).or_insert(0) += 1;
        }
        *by_scenario.entry(detection.scenario.clone()).or_insert(0) += 1;
        if detection.model_rationale.is_some() {
            model_backed_detection_count += 1;
        } else {
            heuristic_fallback_detection_count += 1;
        }
    }

    DetectionModeSummary {
        detection_count: detections.len(),
        distinct_event_count: distinct_event_ids.len(),
        by_source,
        by_scenario,
        model_backed_detection_count,
        heuristic_fallback_detection_count,
    }
}

fn comparison_sample(detection: &DetectionPayload) -> DetectionComparisonSample {
    DetectionComparisonSample {
        detection_id: detection.detection_id.clone(),
        event_id: detection.event_ids.first().cloned().unwrap_or_default(),
        source: detection
            .evidence
            .first()
            .map(|evidence| evidence.source.clone())
            .unwrap_or_default(),
        scenario: detection.scenario.clone(),
        title: detection.title.clone(),
        score: detection.score,
        severity: detection.severity.clone(),
        scoring_mode: detection
            .model_rationale
            .as_ref()
            .map(|rationale| rationale.scoring_mode.clone())
            .unwrap_or_else(|| "heuristic".to_string()),
        policy_scope: detection
            .model_rationale
            .as_ref()
            .map(|rationale| rationale.policy_scope.clone()),
    }
}

fn score_policy_for_source<'a>(
    score_policy: &'a ModelScorePolicy,
    source: &'a str,
) -> (&'a str, f64, f64, Option<&'a String>, Option<&'a String>) {
    if let Some(source_policy) = score_policy.source_policies.get(source) {
        return policy_components(source_policy, source);
    }
    (
        "global",
        score_policy.detection_threshold,
        score_policy
            .high_severity_threshold
            .max(score_policy.detection_threshold),
        score_policy.calibration_source.as_ref(),
        score_policy.calibration_reason.as_ref(),
    )
}

fn policy_components<'a>(
    score_policy: &'a SourceScorePolicy,
    source: &'a str,
) -> (&'a str, f64, f64, Option<&'a String>, Option<&'a String>) {
    (
        source,
        score_policy.detection_threshold,
        score_policy
            .high_severity_threshold
            .max(score_policy.detection_threshold),
        score_policy.calibration_source.as_ref(),
        score_policy.calibration_reason.as_ref(),
    )
}

fn build_detection_payload(
    event: &Value,
    input: DetectionBuildInput,
) -> anyhow::Result<Option<DetectionPayload>> {
    let DetectionBuildInput {
        score,
        detection_threshold,
        high_severity_threshold,
        mut reasons,
        mut attributions,
        model_version,
        model_rationale,
    } = input;
    if score < detection_threshold {
        return Ok(None);
    }
    if reasons.is_empty() {
        reasons.push("embedding_distance_anomaly".to_string());
    }
    attributions
        .entry("model_distance".to_string())
        .or_insert(score);
    let (scenario, title) = scenario_and_title(event, &reasons)?;
    let event_id = event
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("event_id missing"))?;
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{event_id}:{scenario}").as_bytes())
    );
    let evidence = evidence_pointer(event).ok_or_else(|| anyhow::anyhow!("evidence missing"))?;
    let principal =
        object(event, "principal").ok_or_else(|| anyhow::anyhow!("principal missing"))?;
    let resource = object(event, "resource").ok_or_else(|| anyhow::anyhow!("resource missing"))?;

    Ok(Some(DetectionPayload {
        detection_id: format!("det-{}", &digest[..12]),
        scenario: scenario.to_string(),
        title: title.to_string(),
        score: round2(score),
        confidence: round2((0.55 + score / 2.0).min(0.99)),
        severity: if score >= high_severity_threshold {
            "high".to_string()
        } else {
            "medium".to_string()
        },
        reasons,
        feature_attributions: attributions,
        event_ids: vec![event_id.to_string()],
        related_entity_ids: vec![
            str_field(principal, "id").unwrap_or_default().to_string(),
            str_field(resource, "id").unwrap_or_default().to_string(),
        ],
        evidence: vec![evidence],
        model_version,
        model_rationale,
        status: "open".to_string(),
    }))
}

fn score_event(
    event: &Value,
    baseline: &DetectionBaseline,
) -> anyhow::Result<Option<DetectionPayload>> {
    let contributions = collect_contributions(event, baseline);
    let (score, reasons, attributions) = fuse_contributions(contributions);
    build_detection_payload(
        event,
        DetectionBuildInput {
            score,
            detection_threshold: 0.60,
            high_severity_threshold: 0.80,
            reasons,
            attributions,
            model_version: "rules-heuristics-fusion-v1".to_string(),
            model_rationale: None,
        },
    )
}

fn heuristic_observation(event: &Value, baseline: &DetectionBaseline) -> ScoredEventObservation {
    let contributions = collect_contributions(event, baseline);
    let (score, _reasons, _attributions) = fuse_contributions(contributions);
    ScoredEventObservation {
        event_id: event
            .get("event_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        source: source(event).unwrap_or_default().to_string(),
        truth_scenario: truth_scenario(event),
        score: round2(score),
        detected: score >= 0.60,
        scoring_mode: "heuristic".to_string(),
    }
}

fn score_event_with_model(
    event: &Value,
    baseline: &DetectionBaseline,
    model_score: f64,
    score_policy: &ModelScorePolicy,
    model_version: &str,
) -> anyhow::Result<Option<DetectionPayload>> {
    let contributions = collect_contributions(event, baseline);
    let (_heuristic_score, reasons, attributions) = fuse_contributions(contributions);
    let source = event
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (
        policy_scope,
        detection_threshold,
        high_severity_threshold,
        calibration_source,
        calibration_reason,
    ) = score_policy_for_source(score_policy, source);
    let mut attributions = attributions;
    attributions.insert("score_policy_threshold".to_string(), detection_threshold);
    if !source.is_empty() {
        attributions.insert("model_distance".to_string(), model_score);
    }
    build_detection_payload(
        event,
        DetectionBuildInput {
            score: model_score,
            detection_threshold,
            high_severity_threshold,
            reasons,
            attributions,
            model_version: model_version.to_string(),
            model_rationale: Some(DetectionModelRationale {
                scoring_mode: "onnx_native".to_string(),
                policy_scope: policy_scope.to_string(),
                detection_threshold,
                high_severity_threshold,
                model_score,
                score_margin: (model_score - detection_threshold).max(0.0),
                calibration_source: calibration_source.cloned(),
                calibration_reason: calibration_reason.cloned(),
            }),
        },
    )
}

fn model_observation(
    event: &Value,
    baseline: &DetectionBaseline,
    model_score: Option<f64>,
    score_policy: &ModelScorePolicy,
) -> ScoredEventObservation {
    if let Some(model_score) = model_score {
        let src = source(event).unwrap_or_default();
        let (
            _policy_scope,
            detection_threshold,
            _high_severity_threshold,
            _cal_source,
            _cal_reason,
        ) = score_policy_for_source(score_policy, src);
        return ScoredEventObservation {
            event_id: event
                .get("event_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            source: src.to_string(),
            truth_scenario: truth_scenario(event),
            score: round2(model_score),
            detected: model_score >= detection_threshold,
            scoring_mode: "onnx_native".to_string(),
        };
    }
    heuristic_observation(event, baseline)
}

fn heuristic_detections(
    event_index: &crate::event_index::EventIndexFile,
    detection_context: &DetectionContextFile,
) -> anyhow::Result<Vec<DetectionPayload>> {
    collect_scored_detections(
        event_index,
        detection_context,
        |_event_id, event, baseline| score_event(event, baseline),
    )
}

fn model_detections(
    workspace: &Path,
    event_index: &crate::event_index::EventIndexFile,
    detection_context: &DetectionContextFile,
) -> anyhow::Result<Vec<DetectionPayload>> {
    let Some((metadata, metadata_path, request)) =
        build_model_score_request(workspace, event_index, detection_context)?
    else {
        anyhow::bail!("model comparison requires an active runtime-ready model bundle")
    };
    if request.items.is_empty() {
        anyhow::bail!("model comparison requires feature-lake rows for the current events")
    }
    let response = run_model_score(&metadata_path, &metadata, &request)?;
    let score_by_event_id: BTreeMap<String, f64> = response
        .items
        .into_iter()
        .map(|item| (item.event_id, item.model_score))
        .collect();

    collect_scored_detections(
        event_index,
        detection_context,
        |event_id, event, baseline| {
            if let Some(model_score) = score_by_event_id.get(event_id).copied() {
                score_event_with_model(
                    event,
                    baseline,
                    model_score,
                    &metadata.score_policy,
                    &metadata.model_version,
                )
            } else {
                score_event(event, baseline)
            }
        },
    )
}

fn heuristic_observations(
    event_index: &crate::event_index::EventIndexFile,
    detection_context: &DetectionContextFile,
) -> Vec<ScoredEventObservation> {
    detection_context
        .ordered_event_ids
        .iter()
        .filter_map(|event_id| {
            let event = event_index.events_by_id.get(event_id)?;
            let context = detection_context.contexts_by_event_id.get(event_id)?;
            Some(heuristic_observation(event, &baseline_for_context(context)))
        })
        .collect()
}

fn model_observations(
    workspace: &Path,
    event_index: &crate::event_index::EventIndexFile,
    detection_context: &DetectionContextFile,
) -> anyhow::Result<Vec<ScoredEventObservation>> {
    let Some((metadata, _metadata_path, request)) =
        build_model_score_request(workspace, event_index, detection_context)?
    else {
        anyhow::bail!("model evaluation requires an active runtime-ready model bundle")
    };
    if request.items.is_empty() {
        anyhow::bail!("model evaluation requires feature-lake rows for the current events")
    }
    let response = run_model_score(&_metadata_path, &metadata, &request)?;
    let score_by_event_id: BTreeMap<String, f64> = response
        .items
        .into_iter()
        .map(|item| (item.event_id, item.model_score))
        .collect();

    Ok(detection_context
        .ordered_event_ids
        .iter()
        .filter_map(|event_id| {
            let event = event_index.events_by_id.get(event_id)?;
            let context = detection_context.contexts_by_event_id.get(event_id)?;
            Some(model_observation(
                event,
                &baseline_for_context(context),
                score_by_event_id.get(event_id).copied(),
                &metadata.score_policy,
            ))
        })
        .collect())
}

fn evaluate_observations(
    observations: &[ScoredEventObservation],
) -> DetectionEvaluationModeSummary {
    let event_count = observations.len();
    let detection_count = observations
        .iter()
        .filter(|observation| observation.detected)
        .count();
    let model_backed_event_count = observations
        .iter()
        .filter(|observation| observation.scoring_mode == "onnx_native")
        .count();
    let heuristic_fallback_event_count = observations
        .iter()
        .filter(|observation| observation.scoring_mode != "onnx_native")
        .count();

    let mut scenario_totals: BTreeMap<String, usize> = BTreeMap::new();
    let mut scenario_detected: BTreeMap<String, usize> = BTreeMap::new();
    let mut source_baseline_scores: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut source_attack_scores: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut source_baseline_total: BTreeMap<String, usize> = BTreeMap::new();
    let mut source_baseline_detected: BTreeMap<String, usize> = BTreeMap::new();
    let mut source_attack_total: BTreeMap<String, usize> = BTreeMap::new();
    let mut source_attack_detected: BTreeMap<String, usize> = BTreeMap::new();

    for observation in observations {
        let is_attack = observation.truth_scenario != "baseline";
        if is_attack {
            *scenario_totals
                .entry(observation.truth_scenario.clone())
                .or_insert(0) += 1;
            if observation.detected {
                *scenario_detected
                    .entry(observation.truth_scenario.clone())
                    .or_insert(0) += 1;
            }
            source_attack_scores
                .entry(observation.source.clone())
                .or_default()
                .push(observation.score);
            *source_attack_total
                .entry(observation.source.clone())
                .or_insert(0) += 1;
            if observation.detected {
                *source_attack_detected
                    .entry(observation.source.clone())
                    .or_insert(0) += 1;
            }
        } else {
            source_baseline_scores
                .entry(observation.source.clone())
                .or_default()
                .push(observation.score);
            *source_baseline_total
                .entry(observation.source.clone())
                .or_insert(0) += 1;
            if observation.detected {
                *source_baseline_detected
                    .entry(observation.source.clone())
                    .or_insert(0) += 1;
            }
        }
    }

    let scenario_recall = scenario_totals
        .into_iter()
        .map(|(scenario, total_event_count)| {
            let detected_event_count = scenario_detected.get(&scenario).copied().unwrap_or(0);
            let recall = if total_event_count == 0 {
                0.0
            } else {
                detected_event_count as f64 / total_event_count as f64
            };
            (
                scenario,
                ScenarioRecallSummary {
                    total_event_count,
                    detected_event_count,
                    recall: round2(recall),
                },
            )
        })
        .collect();

    let sources: BTreeSet<String> = source_baseline_total
        .keys()
        .chain(source_attack_total.keys())
        .cloned()
        .collect();
    let source_behavior = sources
        .into_iter()
        .map(|source| {
            let baseline_event_count = source_baseline_total.get(&source).copied().unwrap_or(0);
            let baseline_detected_event_count =
                source_baseline_detected.get(&source).copied().unwrap_or(0);
            let attack_event_count = source_attack_total.get(&source).copied().unwrap_or(0);
            let attack_detected_event_count =
                source_attack_detected.get(&source).copied().unwrap_or(0);
            let baseline_alert_rate = if baseline_event_count == 0 {
                0.0
            } else {
                baseline_detected_event_count as f64 / baseline_event_count as f64
            };
            let attack_recall = if attack_event_count == 0 {
                0.0
            } else {
                attack_detected_event_count as f64 / attack_event_count as f64
            };
            (
                source.clone(),
                SourceBehaviorSummary {
                    baseline_event_count,
                    baseline_detected_event_count,
                    baseline_alert_rate: round2(baseline_alert_rate),
                    baseline_score_quantiles: score_quantiles(
                        source_baseline_scores
                            .get(&source)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                    ),
                    attack_event_count,
                    attack_detected_event_count,
                    attack_recall: round2(attack_recall),
                    attack_score_quantiles: score_quantiles(
                        source_attack_scores
                            .get(&source)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                    ),
                },
            )
        })
        .collect();

    DetectionEvaluationModeSummary {
        detection_count,
        detection_rate: if event_count == 0 {
            0.0
        } else {
            round2(detection_count as f64 / event_count as f64)
        },
        model_backed_event_count,
        heuristic_fallback_event_count,
        scenario_recall,
        source_behavior,
    }
}

fn build_derived_state(
    prior_state: DerivedStateFile,
    context: &DetectionContextFile,
) -> DerivedStateFile {
    let mut metadata = prior_state.metadata;
    metadata.insert(
        "rebuild_source".to_string(),
        Value::String("event_index_detection_context".to_string()),
    );
    metadata.insert(
        "normalized_event_count".to_string(),
        Value::Number(context.event_count.into()),
    );
    metadata.insert(
        "detection_context_version".to_string(),
        Value::Number(context.context_version.into()),
    );

    let mut aggregates = BTreeMap::new();
    if let Ok(value) = serde_json::to_value(&context.aggregates) {
        if let Some(map) = value.as_object() {
            for (key, value) in map {
                aggregates.insert(key.clone(), value.clone());
            }
        }
    }

    DerivedStateFile {
        principal_profiles: BTreeMap::new(),
        peer_groups: BTreeMap::new(),
        access_histories: BTreeMap::new(),
        aggregates,
        embeddings: BTreeMap::new(),
        case_artifacts: prior_state.case_artifacts,
        feedback_labels: prior_state.feedback_labels,
        metadata,
    }
}

fn collect_json_files(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    fn visit(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(&path, out)?;
            } else if path.extension().and_then(|value| value.to_str()) == Some("json") {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_ops_metadata(workspace: &Path) -> anyhow::Result<OpsMetadata> {
    let source_stats: SourceStatsFile = load_source_stats(workspace)?;
    let event_counts_by_source = source_stats
        .sources
        .into_iter()
        .map(|(source, stats)| (source, stats.normalized_event_count))
        .collect();

    let mut dead_letter_counts_by_source = BTreeMap::new();
    let mut dead_letter_count = 0;
    for path in collect_json_files(&workspace.join("dead_letters"))? {
        let payload: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        if let Some(source) = payload.get("source").and_then(Value::as_str) {
            *dead_letter_counts_by_source
                .entry(source.to_string())
                .or_insert(0) += 1;
        }
        dead_letter_count += 1;
    }

    let metadata = OpsMetadata {
        workspace: workspace.display().to_string(),
        event_counts_by_source,
        dead_letter_count,
        dead_letter_counts_by_source,
        contains_raw_payloads: false,
    };
    write_json(&ops_metadata_path(workspace), &metadata)?;
    Ok(metadata)
}

fn run_heuristic_detection(workspace: &Path) -> anyhow::Result<DetectionResult> {
    let event_index = ensure_event_index(workspace)?;
    let detection_context = ensure_detection_context(workspace)?;
    let prior_state = load_derived_state(workspace)?;
    let mut existing_detection_ids = existing_detection_ids(workspace)?;
    let mut new_detections = 0;
    let mut new_detection_payloads = Vec::new();

    for detection in heuristic_detections(&event_index, &detection_context)? {
        if existing_detection_ids.contains(&detection.detection_id) {
            continue;
        }
        new_detection_payloads.push(detection.clone());
        existing_detection_ids.insert(detection.detection_id);
        new_detections += 1;
    }

    persist_detections(
        workspace,
        &new_detection_payloads,
        &detection_context,
        "heuristic",
    )?;

    let derived_state = build_derived_state(prior_state, &detection_context);
    save_derived_state(workspace, &derived_state)?;
    Ok(DetectionResult {
        normalized_event_count: detection_context.event_count,
        new_detection_count: new_detections,
        total_detection_count: existing_detection_ids.len(),
    })
}

fn run_model_detection(workspace: &Path) -> anyhow::Result<DetectionResult> {
    let event_index = ensure_event_index(workspace)?;
    let detection_context = ensure_detection_context(workspace)?;
    let prior_state = load_derived_state(workspace)?;
    let mut existing_detection_ids = existing_detection_ids(workspace)?;
    let model_detections = match model_detections(workspace, &event_index, &detection_context) {
        Ok(detections) => detections,
        Err(_) => return run_heuristic_detection(workspace),
    };
    let mut new_detections = 0;
    let mut new_detection_payloads = Vec::new();

    for detection in model_detections {
        if existing_detection_ids.contains(&detection.detection_id) {
            continue;
        }
        new_detection_payloads.push(detection.clone());
        existing_detection_ids.insert(detection.detection_id);
        new_detections += 1;
    }

    persist_detections(
        workspace,
        &new_detection_payloads,
        &detection_context,
        "onnx_native",
    )?;

    let derived_state = build_derived_state(prior_state, &detection_context);
    save_derived_state(workspace, &derived_state)?;
    Ok(DetectionResult {
        normalized_event_count: detection_context.event_count,
        new_detection_count: new_detections,
        total_detection_count: existing_detection_ids.len(),
    })
}

pub fn run_detection_worker(workspace: &Path) -> anyhow::Result<DetectionWorkerResult> {
    let scoring_runtime = resolve_model_runtime_status(workspace)?;
    let detect = if scoring_runtime.effective_mode == "onnx_native" {
        run_model_detection(workspace)?
    } else {
        run_heuristic_detection(workspace)?
    };
    let ops_metadata = collect_ops_metadata(workspace)?;
    Ok(DetectionWorkerResult {
        detect,
        ops_metadata,
        scoring_runtime,
    })
}

pub fn compare_detection_modes(workspace: &Path) -> anyhow::Result<DetectionModeComparisonResult> {
    let scoring_runtime = resolve_model_runtime_status(workspace)?;
    if scoring_runtime.effective_mode != "onnx_native" {
        anyhow::bail!(
            "detection mode comparison requires an active native ONNX model; current mode is {}",
            scoring_runtime.effective_mode
        );
    }

    let event_index = ensure_event_index(workspace)?;
    let detection_context = ensure_detection_context(workspace)?;
    let heuristic = heuristic_detections(&event_index, &detection_context)?;
    let model = model_detections(workspace, &event_index, &detection_context)?;

    let heuristic_ids: BTreeSet<String> = heuristic
        .iter()
        .map(|detection| detection.detection_id.clone())
        .collect();
    let model_ids: BTreeSet<String> = model
        .iter()
        .map(|detection| detection.detection_id.clone())
        .collect();
    let heuristic_event_ids: BTreeSet<String> = heuristic
        .iter()
        .filter_map(|detection| detection.event_ids.first().cloned())
        .collect();
    let model_event_ids: BTreeSet<String> = model
        .iter()
        .filter_map(|detection| detection.event_ids.first().cloned())
        .collect();

    let heuristic_only_samples = heuristic
        .iter()
        .filter(|detection| !model_ids.contains(&detection.detection_id))
        .take(10)
        .map(comparison_sample)
        .collect();
    let model_only_samples = model
        .iter()
        .filter(|detection| !heuristic_ids.contains(&detection.detection_id))
        .take(10)
        .map(comparison_sample)
        .collect();

    Ok(DetectionModeComparisonResult {
        event_count: detection_context.event_count,
        scoring_runtime,
        heuristic: summarize_detections(&heuristic),
        model: summarize_detections(&model),
        overlap_detection_count: heuristic_ids.intersection(&model_ids).count(),
        overlap_event_count: heuristic_event_ids.intersection(&model_event_ids).count(),
        heuristic_only_detection_count: heuristic_ids.difference(&model_ids).count(),
        model_only_detection_count: model_ids.difference(&heuristic_ids).count(),
        heuristic_only_samples,
        model_only_samples,
    })
}

pub fn evaluate_detection_modes(workspace: &Path) -> anyhow::Result<DetectionEvaluationReport> {
    let scoring_runtime = resolve_model_runtime_status(workspace)?;
    if scoring_runtime.effective_mode != "onnx_native" {
        anyhow::bail!(
            "detection evaluation requires an active native ONNX model; current mode is {}",
            scoring_runtime.effective_mode
        );
    }

    let event_index = ensure_event_index(workspace)?;
    let detection_context = ensure_detection_context(workspace)?;
    let heuristic = heuristic_observations(&event_index, &detection_context);
    let model = model_observations(workspace, &event_index, &detection_context)?;

    Ok(DetectionEvaluationReport {
        event_count: detection_context.event_count,
        scoring_runtime,
        heuristic: evaluate_observations(&heuristic),
        model: evaluate_observations(&model),
    })
}

pub fn sweep_model_thresholds(workspace: &Path) -> anyhow::Result<ModelThresholdSweepReport> {
    let scoring_runtime = resolve_model_runtime_status(workspace)?;
    if scoring_runtime.effective_mode != "onnx_native" {
        anyhow::bail!(
            "model threshold sweep requires an active native ONNX model; current mode is {}",
            scoring_runtime.effective_mode
        );
    }

    let event_index = ensure_event_index(workspace)?;
    let detection_context = ensure_detection_context(workspace)?;
    let observations = model_observations(workspace, &event_index, &detection_context)?;
    let Some((_manifest, metadata, _metadata_path)) = load_active_model_bundle(workspace)? else {
        anyhow::bail!("model threshold sweep requires an active model bundle");
    };

    let mut source_sweeps = BTreeMap::new();
    let sources: BTreeSet<String> = observations
        .iter()
        .map(|observation| observation.source.clone())
        .collect();
    for source in sources {
        let source_observations: Vec<&ScoredEventObservation> = observations
            .iter()
            .filter(|observation| observation.source == source)
            .collect();
        let baseline_scores: Vec<f64> = source_observations
            .iter()
            .filter(|observation| observation.truth_scenario == "baseline")
            .map(|observation| observation.score)
            .collect();
        let current_policy_threshold = metadata
            .score_policy
            .source_policies
            .get(&source)
            .map(|policy| policy.detection_threshold)
            .unwrap_or(metadata.score_policy.detection_threshold);
        let thresholds = threshold_candidates(&baseline_scores, current_policy_threshold);
        let baseline_event_count = source_observations
            .iter()
            .filter(|observation| observation.truth_scenario == "baseline")
            .count();
        let attack_event_count = source_observations
            .iter()
            .filter(|observation| observation.truth_scenario != "baseline")
            .count();

        let points: Vec<ThresholdSweepPoint> = thresholds
            .iter()
            .map(|threshold| {
                let baseline_detected_event_count = source_observations
                    .iter()
                    .filter(|observation| {
                        observation.truth_scenario == "baseline" && observation.score >= *threshold
                    })
                    .count();
                let attack_detected_event_count = source_observations
                    .iter()
                    .filter(|observation| {
                        observation.truth_scenario != "baseline" && observation.score >= *threshold
                    })
                    .count();
                let baseline_alert_rate = if baseline_event_count == 0 {
                    0.0
                } else {
                    baseline_detected_event_count as f64 / baseline_event_count as f64
                };
                let attack_recall = if attack_event_count == 0 {
                    0.0
                } else {
                    attack_detected_event_count as f64 / attack_event_count as f64
                };
                ThresholdSweepPoint {
                    threshold: *threshold,
                    baseline_alert_rate: round2(baseline_alert_rate),
                    attack_recall: round2(attack_recall),
                    baseline_detected_event_count,
                    attack_detected_event_count,
                }
            })
            .collect();

        let recommended = points
            .iter()
            .filter(|point| point.baseline_alert_rate <= 0.05)
            .max_by(|left, right| {
                left.attack_recall
                    .partial_cmp(&right.attack_recall)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        right
                            .threshold
                            .partial_cmp(&left.threshold)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            })
            .or_else(|| {
                points.iter().min_by(|left, right| {
                    left.baseline_alert_rate
                        .partial_cmp(&right.baseline_alert_rate)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
            })
            .cloned()
            .unwrap_or_default();

        source_sweeps.insert(
            source,
            SourceThresholdSweep {
                current_policy_threshold: rounded_threshold(current_policy_threshold),
                candidate_count: points.len(),
                recommended_threshold_for_5pct_budget: recommended.threshold,
                recommended_attack_recall_for_5pct_budget: recommended.attack_recall,
                points,
            },
        );
    }

    Ok(ModelThresholdSweepReport {
        event_count: detection_context.event_count,
        scoring_runtime,
        source_sweeps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    use seccloud_lake::schema::{
        ActionFeatureRow, HistoryFeatureRow, StaticPrincipalFeatureRow,
        action_feature_record_batch, history_feature_record_batch,
        static_principal_feature_record_batch, write_parquet_bytes,
    };

    fn test_workspace() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "seccloud-detector-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(root.join("manifests")).unwrap();
        std::fs::create_dir_all(root.join("derived")).unwrap();
        std::fs::create_dir_all(root.join("detections")).unwrap();
        std::fs::create_dir_all(root.join("dead_letters/source=okta")).unwrap();
        std::fs::create_dir_all(root.join("normalized/source=github")).unwrap();
        root
    }

    fn repo_root() -> &'static Path {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("repo root")
    }

    fn write_feature_manifest(root: &Path, manifest_key: &str, object_key: &str, payload: Vec<u8>) {
        let object_path = root.join(object_key);
        if let Some(parent) = object_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&object_path, payload).unwrap();
        write_json(
            &root.join(manifest_key),
            &serde_json::json!({
                "objects": [{"object_key": object_key}],
            }),
        )
        .unwrap();
    }

    fn normalized_event(
        event_id: &str,
        observed_at: &str,
        action_verb: &str,
        resource_id: &str,
    ) -> Value {
        serde_json::json!({
            "event_id": event_id,
            "event_key": format!("evk_{event_id}"),
            "source": "github",
            "observed_at": observed_at,
            "principal": {
                "id": "alice@example.com",
                "entity_id": "ent_alice",
                "department": "security",
            },
            "resource": {
                "id": resource_id,
                "entity_id": format!("ent_{resource_id}"),
                "sensitivity": if action_verb == "archive_download" { "high" } else { "internal" }
            },
            "action": {
                "verb": action_verb
            },
            "attributes": {},
            "evidence": {
                "source": "github",
                "object_key": "raw/object.json",
                "raw_event_id": event_id,
                "observed_at": observed_at
            }
        })
    }

    fn write_active_model_manifest(root: &Path) {
        write_json(
            &root.join("manifests").join("model_artifact.json"),
            &serde_json::json!({
                "manifest_version": 1,
                "active_model_id": "demo-v1",
                "active_metadata_path": "models/demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "test"
            }),
        )
        .unwrap();
    }

    fn setup_model_scoring_fixture(root: &Path) {
        let mut event =
            normalized_event("evt_model", "2026-03-10T10:00:00Z", "clone", "repo-secret");
        event["principal"]["entity_key"] = Value::String("enk_alice@example.com".into());
        event["resource"]["entity_key"] = Value::String("enk_repo-secret".into());
        event["resource"]["sensitivity"] = Value::String("critical".into());
        event["principal"]["department"] = Value::String("engineering".into());
        event["principal"]["role"] = Value::String("staff-engineer".into());
        write_json(
            &root
                .join("normalized")
                .join("source=github")
                .join("evt_model.json"),
            &event,
        )
        .unwrap();
        write_json(
            &crate::local_runtime::source_stats_path(root),
            &SourceStatsFile::default(),
        )
        .unwrap();

        let action_rows = vec![ActionFeatureRow {
            feature_schema_version: "feature.v1".into(),
            tenant_id: "tenant-1".into(),
            source: "github".into(),
            resource_entity_key: "enk_repo-secret".into(),
            principal_entity_key: "enk_alice@example.com".into(),
            access_count: 1,
            accessor_weight: 1.0,
        }];
        let history_rows = vec![HistoryFeatureRow {
            feature_schema_version: "feature.v1".into(),
            tenant_id: "tenant-1".into(),
            principal_entity_key: "enk_alice@example.com".into(),
            window_start: "2026-03-10T10:00:00Z".into(),
            resource_entity_key: "enk_repo-secret".into(),
        }];
        let static_rows = vec![StaticPrincipalFeatureRow {
            feature_schema_version: "feature.v1".into(),
            tenant_id: "tenant-1".into(),
            principal_entity_key: "enk_alice@example.com".into(),
            principal_id: "alice@example.com".into(),
            department: "engineering".into(),
            role: "staff-engineer".into(),
            location: "unknown".into(),
            employment_duration_bucket: "1-3yr".into(),
            privilege_level: "elevated".into(),
        }];
        write_feature_manifest(
            root,
            "lake/manifests/action.json",
            "lake/features/action.parquet",
            write_parquet_bytes(&action_feature_record_batch(&action_rows).unwrap()).unwrap(),
        );
        write_feature_manifest(
            root,
            "lake/manifests/history.json",
            "lake/features/history.parquet",
            write_parquet_bytes(&history_feature_record_batch(&history_rows).unwrap()).unwrap(),
        );
        write_feature_manifest(
            root,
            "lake/manifests/static.json",
            "lake/features/static.parquet",
            write_parquet_bytes(&static_principal_feature_record_batch(&static_rows).unwrap())
                .unwrap(),
        );
        write_json(
            &root.join("manifests").join("feature_state.json"),
            &serde_json::json!({
                "action_manifest_key": "lake/manifests/action.json",
                "history_manifest_key": "lake/manifests/history.json",
                "static_manifest_key": "lake/manifests/static.json",
            }),
        )
        .unwrap();
    }

    fn setup_model_scoring_workspace(root: &Path, bridge_score: f64) -> PathBuf {
        setup_model_scoring_fixture(root);
        let model_dir = root.join("models").join("demo-v1");
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("action_tower_github.onnx"), b"onnx").unwrap();
        std::fs::write(model_dir.join("context_tower.onnx"), b"onnx").unwrap();
        std::fs::write(
            model_dir.join("principal-vocab.json"),
            b"[\"enk_alice@example.com\"]",
        )
        .unwrap();
        std::fs::write(
            model_dir.join("resource-vocab.json"),
            b"[\"enk_repo-secret\"]",
        )
        .unwrap();
        std::fs::write(
            model_dir.join("categorical-vocabs.json"),
            br#"{"role":{"staff-engineer":0},"location":{"unknown":0},"duration_bucket":{"1-3yr":0},"privilege_level":{"elevated":0}}"#,
        )
        .unwrap();
        write_json(
            &model_dir.join("metadata.json"),
            &serde_json::json!({
                "artifact_version": 1,
                "tenant_id": "tenant-1",
                "model_id": "demo-v1",
                "model_version": "demo-v1",
                "model_family": "contrastive-facade",
                "scoring_mode": "onnx",
                "feature_schema_version": "feature.v1",
                "padding": {"max_tokens": 8, "max_windows": 4, "max_res_per_window": 4, "max_peers": 4},
                "action_towers": {
                    "github": {"path": "action_tower_github.onnx", "sha256": "sha256:test", "input_names": ["indices"], "output_name": "embedding"}
                },
                "context_tower": {"path": "context_tower.onnx", "sha256": "sha256:test", "input_names": ["hist_window_indices"], "output_name": "embedding"},
                "eval_report_path": "eval-report.json",
                "score_policy": {
                    "detection_threshold": 0.8,
                    "high_severity_threshold": 0.9,
                    "calibration_source": "heldout",
                    "calibration_reason": "test_threshold",
                    "source_policies": {
                        "github": {
                            "detection_threshold": 0.35,
                            "high_severity_threshold": 0.7,
                            "calibration_source": "heldout:github",
                            "calibration_reason": "source_specific_threshold",
                            "evaluation_pair_count": 4
                        }
                    }
                },
                "input_vocabs": {
                    "principal_entity_keys_path": "principal-vocab.json",
                    "resource_entity_keys_path": "resource-vocab.json",
                    "categorical_vocabs_path": "categorical-vocabs.json",
                    "principal_vocab_count": 1,
                    "resource_vocab_count": 1,
                    "categorical_vocab_counts": {"role": 1}
                }
            }),
        )
        .unwrap();
        write_active_model_manifest(root);

        let bridge = root.join("bridge.py");
        std::fs::write(
            &bridge,
            format!(
                "#!/usr/bin/env python3\nimport argparse, json\nparser = argparse.ArgumentParser()\nparser.add_argument(\"--metadata\")\nparser.add_argument(\"--input\")\nparser.add_argument(\"--output\")\nargs = parser.parse_args()\nwith open(args.input, \"r\", encoding=\"utf-8\") as fh:\n    payload = json.load(fh)\nwith open(args.output, \"w\", encoding=\"utf-8\") as fh:\n    json.dump({{\"items\": [{{\"event_id\": item[\"event_id\"], \"model_score\": {bridge_score}}} for item in payload[\"items\"]]}}, fh)\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&bridge).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&bridge, permissions).unwrap();
        }
        bridge
    }

    fn setup_native_model_scoring_workspace(root: &Path) {
        setup_model_scoring_fixture(root);
        let workspace_path = root.display().to_string();
        let status = Command::new("uv")
            .arg("run")
            .arg("python")
            .arg("-c")
            .arg(format!(
                r#"
from pathlib import Path

from seccloud.contrastive_model import FacadeModel, ModelConfig
from seccloud.onnx_export import export_model_artifact_bundle

root = Path({workspace_path:?})
model_dir = root / "models" / "demo-v1"
model = FacadeModel(
    ModelConfig(
        principal_vocab_size=2,
        resource_vocab_size=2,
        embed_dim=8,
        token_dim=4,
        static_embed_dim=4,
        action_hidden=[16],
        context_hidden=[16],
        num_roles=2,
        num_locations=2,
        num_duration_buckets=2,
        num_privilege_levels=2,
        sources=["github"],
    )
)
model.eval()
export_model_artifact_bundle(
    model,
    model_dir,
    tenant_id="tenant-1",
    model_id="demo-v1",
    principal_entity_keys=["enk_alice@example.com"],
    resource_entity_keys=["enk_repo-secret"],
    categorical_vocabs={{
        "role": {{"staff-engineer": 0}},
        "location": {{"unknown": 0}},
        "duration_bucket": {{"1-3yr": 0}},
        "privilege_level": {{"elevated": 0}},
    }},
    score_policy={{
        "detection_threshold": 0.0,
        "high_severity_threshold": 0.7,
        "calibration_source": "unit_test",
        "calibration_reason": "force_native_detector_path",
        "source_policies": {{
            "github": {{
                "detection_threshold": 0.0,
                "high_severity_threshold": 0.7,
                "calibration_source": "unit_test:github",
                "calibration_reason": "force_native_detector_path",
                "evaluation_pair_count": 1,
            }}
        }},
    }},
    max_tokens=8,
    max_windows=4,
    max_res=4,
    max_peers=4,
)
"#,
            ))
            .env("PYTHONPATH", repo_root().join("src"))
            .current_dir(repo_root())
            .status()
            .unwrap();
        assert!(status.success(), "failed to export native ONNX test bundle");
        write_active_model_manifest(root);
    }

    #[test]
    fn heuristic_detection_worker_builds_context_and_detection() {
        let root = test_workspace();
        let events = [
            normalized_event("evt_1", "2026-03-10T10:00:00Z", "read", "repo-1"),
            normalized_event("evt_2", "2026-03-10T10:01:00Z", "read", "repo-2"),
            normalized_event("evt_3", "2026-03-10T10:02:00Z", "read", "repo-3"),
            normalized_event(
                "evt_4",
                "2026-03-10T10:03:00Z",
                "archive_download",
                "repo-sensitive",
            ),
        ];
        for (index, event) in events.iter().enumerate() {
            write_json(
                &root
                    .join("normalized/source=github")
                    .join(format!("event-{index}.json")),
                event,
            )
            .unwrap();
        }
        crate::local_runtime::save_ingest_manifest(
            &root,
            &crate::local_runtime::IngestManifestState {
                normalized_event_ids: vec![
                    "evt_1".into(),
                    "evt_2".into(),
                    "evt_3".into(),
                    "evt_4".into(),
                ],
                normalized_event_keys: vec![
                    "evk_evt_1".into(),
                    "evk_evt_2".into(),
                    "evk_evt_3".into(),
                    "evk_evt_4".into(),
                ],
                ..crate::local_runtime::IngestManifestState::default()
            },
        )
        .unwrap();
        write_json(
            &crate::local_runtime::source_stats_path(&root),
            &SourceStatsFile {
                sources: BTreeMap::from([(
                    "github".into(),
                    crate::local_runtime::SourceStats {
                        normalized_event_count: 4,
                        ..crate::local_runtime::SourceStats::default()
                    },
                )]),
            },
        )
        .unwrap();

        let result = run_detection_worker(&root).unwrap();

        assert_eq!(result.detect.normalized_event_count, 4);
        assert_eq!(result.detect.new_detection_count, 1);
        assert_eq!(result.detect.total_detection_count, 1);
        assert_eq!(result.scoring_runtime.requested_mode, "heuristic");
        assert_eq!(result.scoring_runtime.reason, "no_active_model");
        let context = load_detection_context(&root).unwrap();
        assert_eq!(context.event_count, 4);
        assert_eq!(
            context
                .contexts_by_event_id
                .get("evt_4")
                .unwrap()
                .principal_total_events,
            3
        );
        let detection: Value = serde_json::from_slice(
            &std::fs::read(
                root.join("detections").join(
                    std::fs::read_dir(root.join("detections"))
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap()
                        .path(),
                ),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(detection["scenario"], "unusual_repo_export");
        assert_eq!(detection["severity"], "high");
        let detection_manifests = collect_json_files(
            &root
                .join("lake")
                .join("manifests")
                .join("layout=v1")
                .join("type=detection"),
        )
        .unwrap();
        assert_eq!(detection_manifests.len(), 1);
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(&detection_manifests[0]).unwrap()).unwrap();
        let object_key = manifest["objects"][0]["object_key"].as_str().unwrap();
        assert!(root.join(object_key).exists());
        let derived_state = load_derived_state(&root).unwrap();
        assert_eq!(
            derived_state
                .metadata
                .get("rebuild_source")
                .and_then(Value::as_str),
            Some("event_index_detection_context")
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detection_worker_uses_model_score_test_bridge_when_active_model_exists() {
        let _guard = crate::model_scoring::bridge_env_lock().lock().unwrap();
        let root = test_workspace();
        let bridge = setup_model_scoring_workspace(&root, 0.42);
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &bridge);
        }

        let result = run_detection_worker(&root).unwrap();

        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }
        assert_eq!(result.detect.new_detection_count, 1);
        assert_eq!(result.scoring_runtime.effective_mode, "onnx_native");
        let detection_path = std::fs::read_dir(root.join("detections"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let detection: DetectionPayload =
            serde_json::from_slice(&std::fs::read(detection_path).unwrap()).unwrap();
        assert_eq!(detection.model_version, "demo-v1");
        assert_eq!(detection.score, 0.42);
        assert_eq!(
            detection
                .feature_attributions
                .get("score_policy_threshold")
                .copied(),
            Some(0.35)
        );
        assert!(
            detection
                .feature_attributions
                .contains_key("model_distance")
        );
        let rationale = detection.model_rationale.expect("model rationale");
        assert_eq!(rationale.policy_scope, "github");
        assert_eq!(rationale.detection_threshold, 0.35);
        assert!(rationale.score_margin > 0.0);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detection_worker_uses_native_onnx_when_real_model_bundle_exists() {
        let root = test_workspace();
        setup_native_model_scoring_workspace(&root);

        let result = run_detection_worker(&root).unwrap();

        assert_eq!(result.detect.new_detection_count, 1);
        assert_eq!(result.scoring_runtime.effective_mode, "onnx_native");
        let detection_path = std::fs::read_dir(root.join("detections"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let detection: DetectionPayload =
            serde_json::from_slice(&std::fs::read(detection_path).unwrap()).unwrap();
        assert_eq!(detection.model_version, "demo-v1");
        assert!((0.0..=1.0).contains(&detection.score));
        assert_eq!(
            detection
                .feature_attributions
                .get("score_policy_threshold")
                .copied(),
            Some(0.0)
        );
        assert!(
            detection
                .feature_attributions
                .contains_key("model_distance")
        );
        let rationale = detection.model_rationale.expect("model rationale");
        assert_eq!(rationale.scoring_mode, "onnx_native");
        assert_eq!(rationale.policy_scope, "github");
        assert_eq!(rationale.detection_threshold, 0.0);
        assert!(rationale.score_margin >= 0.0);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compare_detection_modes_reports_overlap_and_divergence() {
        let _guard = crate::model_scoring::bridge_env_lock().lock().unwrap();
        let root = test_workspace();
        let bridge = setup_model_scoring_workspace(&root, 0.42);
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &bridge);
        }

        let comparison = compare_detection_modes(&root).unwrap();

        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }

        assert_eq!(comparison.event_count, 1);
        assert_eq!(comparison.scoring_runtime.effective_mode, "onnx_native");
        assert_eq!(comparison.heuristic.detection_count, 0);
        assert_eq!(comparison.model.detection_count, 1);
        assert_eq!(comparison.model.model_backed_detection_count, 1);
        assert_eq!(comparison.model.heuristic_fallback_detection_count, 0);
        assert_eq!(comparison.overlap_detection_count, 0);
        assert_eq!(comparison.model_only_detection_count, 1);
        assert_eq!(comparison.heuristic_only_detection_count, 0);
        assert_eq!(comparison.model.by_source.get("github").copied(), Some(1));
        assert_eq!(comparison.model_only_samples.len(), 1);
        assert_eq!(comparison.model_only_samples[0].scoring_mode, "onnx_native");
        assert_eq!(
            comparison.model_only_samples[0].policy_scope.as_deref(),
            Some("github")
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn evaluate_detection_modes_reports_truth_recall_and_source_behavior() {
        let _guard = crate::model_scoring::bridge_env_lock().lock().unwrap();
        let root = test_workspace();
        let bridge = setup_model_scoring_workspace(&root, 0.42);
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &bridge);
        }

        let report = evaluate_detection_modes(&root).unwrap();

        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }

        assert_eq!(report.event_count, 1);
        assert_eq!(report.model.detection_count, 1);
        assert_eq!(report.heuristic.detection_count, 0);
        assert_eq!(
            report
                .model
                .scenario_recall
                .get("baseline")
                .map(|summary| summary.total_event_count),
            None
        );
        let github_behavior = report.model.source_behavior.get("github").unwrap();
        assert_eq!(github_behavior.attack_event_count, 0);
        assert_eq!(github_behavior.baseline_event_count, 1);
        assert_eq!(github_behavior.baseline_detected_event_count, 1);
        assert_eq!(github_behavior.baseline_alert_rate, 1.0);
        assert_eq!(github_behavior.baseline_score_quantiles.count, 1);
        assert_eq!(report.model.model_backed_event_count, 1);
        assert_eq!(report.model.heuristic_fallback_event_count, 0);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sweep_model_thresholds_reports_source_candidates() {
        let _guard = crate::model_scoring::bridge_env_lock().lock().unwrap();
        let root = test_workspace();
        let bridge = setup_model_scoring_workspace(&root, 0.42);
        unsafe {
            std::env::set_var("SECCLOUD_ONNX_SCORE_BRIDGE", &bridge);
        }

        let report = sweep_model_thresholds(&root).unwrap();

        unsafe {
            std::env::remove_var("SECCLOUD_ONNX_SCORE_BRIDGE");
        }

        let github = report.source_sweeps.get("github").unwrap();
        assert_eq!(report.event_count, 1);
        assert_eq!(report.scoring_runtime.effective_mode, "onnx_native");
        assert!(github.candidate_count >= 1);
        assert!(!github.points.is_empty());
        assert!(github.current_policy_threshold >= 0.35);
        assert!(github.recommended_threshold_for_5pct_budget >= github.current_policy_threshold);

        std::fs::remove_dir_all(root).unwrap();
    }
}
