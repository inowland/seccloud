use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use seccloud_pipeline::envelope::Envelope;

use crate::intake_queue::IntakeQueue;
use crate::key;
use crate::manifest::{
    CheckpointInfo, DetectionManifest, FeatureManifest, IntakeManifest, IntakeQueueEntry,
    NormalizedCheckpointInfo, NormalizedManifest, ObjectDescriptor, Partition, Producer,
    TimeBounds,
};
use crate::object_store::ObjectStore;
use crate::schema::{
    ActionFeatureRow, CollaborationFeatureRow, DeadLetterRow, DetectionRow, HistoryFeatureRow,
    PeerGroupFeatureRow, RawEventRow, StaticPrincipalFeatureRow, action_feature_record_batch,
    collaboration_feature_record_batch, dead_letter_record_batch, detection_record_batch,
    history_feature_record_batch, normalized_event_record_batch, normalized_event_row_from_value,
    peer_group_feature_record_batch, raw_event_record_batch, static_principal_feature_record_batch,
    write_parquet_bytes,
};

const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

#[derive(Debug, Clone)]
pub struct BatchDescriptor {
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: Option<String>,
    pub received_at: DateTime<Utc>,
}

impl BatchDescriptor {
    fn normalized_integration_id(&self) -> String {
        key::partition_value(self.integration_id.as_deref())
    }

    fn raw_object_key(&self) -> String {
        key::raw_batch_object_key(
            &self.tenant_id,
            &self.source,
            self.integration_id.as_deref(),
            &self.batch_id,
            &self.received_at,
        )
    }

    fn raw_manifest_key(&self) -> String {
        key::raw_manifest_key(
            &self.tenant_id,
            &self.source,
            self.integration_id.as_deref(),
            &self.batch_id,
            &self.received_at,
        )
    }

    fn dead_letter_object_key(&self) -> String {
        key::dead_letter_object_key(
            &self.tenant_id,
            &self.source,
            self.integration_id.as_deref(),
            &self.batch_id,
            &self.received_at,
        )
    }

    fn partition(&self) -> Partition {
        Partition {
            dt: self.received_at.format("%Y-%m-%d").to_string(),
            hour: self.received_at.format("%H").to_string(),
        }
    }

    fn time_bounds(&self) -> TimeBounds {
        TimeBounds {
            min: self.received_at.format("%Y-%m-%dT%H:00:00Z").to_string(),
            max: self.received_at.format("%Y-%m-%dT%H:59:59Z").to_string(),
        }
    }

    fn received_at_bounds(&self) -> TimeBounds {
        TimeBounds {
            min: format_ts(self.received_at),
            max: format_ts(self.received_at),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NormalizedBatchDescriptor {
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl NormalizedBatchDescriptor {
    fn normalized_integration_id(&self) -> String {
        key::partition_value(self.integration_id.as_deref())
    }

    fn normalized_object_key(&self, schema_version: &str) -> String {
        key::normalized_batch_object_key(
            &self.tenant_id,
            &self.source,
            self.integration_id.as_deref(),
            &self.observed_at,
            &self.batch_id,
            schema_version,
        )
    }

    fn normalized_manifest_key(&self) -> String {
        key::normalized_manifest_key(
            &self.tenant_id,
            &self.source,
            self.integration_id.as_deref(),
            &self.observed_at,
            &self.batch_id,
        )
    }

    fn partition(&self) -> Partition {
        Partition {
            dt: self.observed_at.format("%Y-%m-%d").to_string(),
            hour: self.observed_at.format("%H").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawBatchWriteInput {
    pub descriptor: BatchDescriptor,
    pub intake_kind: String,
    pub producer_run_id: String,
    pub metadata_json: String,
    pub idempotency_key: String,
    pub payload_sha256: String,
    pub checkpoint_extra: serde_json::Map<String, serde_json::Value>,
    pub envelopes: Vec<Envelope>,
}

#[derive(Debug, Clone)]
pub struct BatchWriteResult {
    pub manifest_key: String,
    pub object_key: String,
    pub dead_letter_object_key: Option<String>,
    pub record_count: usize,
    pub queue_submitted: bool,
}

#[derive(Debug, Clone)]
pub struct NormalizedBatchWriteInput {
    pub descriptor: NormalizedBatchDescriptor,
    pub normalized_schema_version: String,
    pub idempotency_key: String,
    pub upstream_raw_batches: Vec<String>,
    pub checkpoint_extra: serde_json::Map<String, serde_json::Value>,
    pub events: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct NormalizedBatchWriteResult {
    pub manifest_key: String,
    pub object_key: String,
    pub record_count: usize,
}

#[derive(Debug, Clone)]
pub struct FeatureBatchDescriptor {
    pub batch_id: String,
    pub tenant_id: String,
    pub table_name: String,
    pub source: Option<String>,
    pub generated_at: DateTime<Utc>,
}

impl FeatureBatchDescriptor {
    fn normalized_source(&self) -> String {
        key::partition_value(self.source.as_deref())
    }

    fn feature_object_key(&self, schema_version: &str) -> String {
        key::feature_batch_object_key(
            &self.tenant_id,
            &self.table_name,
            self.source.as_deref(),
            &self.generated_at,
            &self.batch_id,
            schema_version,
        )
    }

    fn feature_manifest_key(&self) -> String {
        key::feature_manifest_key(
            &self.tenant_id,
            &self.table_name,
            self.source.as_deref(),
            &self.generated_at,
            &self.batch_id,
        )
    }

    fn partition(&self) -> Partition {
        Partition {
            dt: self.generated_at.format("%Y-%m-%d").to_string(),
            hour: self.generated_at.format("%H").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeatureBatchWriteInput<T> {
    pub descriptor: FeatureBatchDescriptor,
    pub feature_schema_version: String,
    pub upstream_normalized_batches: Vec<String>,
    pub checkpoint_extra: serde_json::Map<String, serde_json::Value>,
    pub checkpoint_event_id: String,
    pub rows: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct FeatureBatchWriteResult {
    pub manifest_key: String,
    pub object_key: String,
    pub record_count: usize,
}

#[derive(Debug, Clone)]
pub struct DetectionBatchDescriptor {
    pub batch_id: String,
    pub tenant_id: String,
    pub detected_at: DateTime<Utc>,
}

impl DetectionBatchDescriptor {
    fn detection_object_key(&self, schema_version: &str) -> String {
        key::detection_batch_object_key(
            &self.tenant_id,
            &self.detected_at,
            &self.batch_id,
            schema_version,
        )
    }

    fn detection_manifest_key(&self) -> String {
        key::detection_manifest_key(&self.tenant_id, &self.detected_at, &self.batch_id)
    }

    fn partition(&self) -> Partition {
        Partition {
            dt: self.detected_at.format("%Y-%m-%d").to_string(),
            hour: self.detected_at.format("%H").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DetectionBatchWriteInput {
    pub descriptor: DetectionBatchDescriptor,
    pub detection_schema_version: String,
    pub upstream_detection_context_signature: String,
    pub scoring_runtime_mode: String,
    pub model_version: String,
    pub rows: Vec<DetectionRow>,
}

#[derive(Debug, Clone)]
pub struct DetectionBatchWriteResult {
    pub manifest_key: String,
    pub object_key: String,
    pub record_count: usize,
}

struct FeatureWritePayload {
    feature_schema_version: String,
    upstream_normalized_batches: Vec<String>,
    checkpoint_extra: serde_json::Map<String, serde_json::Value>,
    checkpoint_event_id: String,
    row_count: usize,
    time_bounds: TimeBounds,
    parquet_bytes: Vec<u8>,
}

fn format_ts(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn single_time_bounds(t: DateTime<Utc>) -> TimeBounds {
    let ts = format_ts(t);
    TimeBounds {
        min: ts.clone(),
        max: ts,
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("sha256:{hash:x}")
}

fn source_event_id_hash(rows: &[RawEventRow]) -> String {
    let mut digests = Vec::with_capacity(rows.len());
    for row in rows {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&row.record_json) {
            if let Some(id) = parsed.get("source_event_id").and_then(|v| v.as_str()) {
                if !id.is_empty() {
                    digests.push(id.to_string());
                    continue;
                }
            }
        }
        digests.push(row.record_json.clone());
    }
    sha256_hex(digests.join("|").as_bytes())
}

fn string_bounds(values: impl IntoIterator<Item = String>) -> anyhow::Result<TimeBounds> {
    let mut values: Vec<String> = values.into_iter().collect();
    values.sort();
    let min = values
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("cannot compute bounds for empty values"))?;
    let max = values
        .last()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("cannot compute bounds for empty values"))?;
    Ok(TimeBounds { min, max })
}

fn sorted_unique_non_empty(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut values: Vec<String> = values
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect();
    values.sort();
    values.dedup();
    values
}

fn integration_id_for_row(descriptor: &BatchDescriptor, envelope: &Envelope) -> String {
    key::partition_value(
        descriptor
            .integration_id
            .as_deref()
            .or(envelope.metadata.integration_id.as_deref()),
    )
}

fn raw_row(
    envelope: &Envelope,
    descriptor: &BatchDescriptor,
    intake_kind: &str,
    metadata_json: &str,
) -> RawEventRow {
    RawEventRow {
        raw_envelope_version: 1,
        tenant_id: descriptor.tenant_id.clone(),
        source: descriptor.source.clone(),
        integration_id: integration_id_for_row(descriptor, envelope),
        intake_kind: intake_kind.into(),
        batch_id: descriptor.batch_id.clone(),
        received_at: format_ts(descriptor.received_at),
        record_ordinal: 0,
        metadata_json: metadata_json.into(),
        record_json: String::from_utf8_lossy(&envelope.payload).into_owned(),
    }
}

fn raw_rows(
    envelopes: &[Envelope],
    descriptor: &BatchDescriptor,
    intake_kind: &str,
    metadata_json: &str,
) -> Vec<RawEventRow> {
    envelopes
        .iter()
        .enumerate()
        .map(|(ordinal, envelope)| {
            let mut row = raw_row(envelope, descriptor, intake_kind, metadata_json);
            row.record_ordinal = ordinal as i32;
            row
        })
        .collect()
}

fn dead_letter_rows(
    envelopes: &[Envelope],
    descriptor: &BatchDescriptor,
    intake_kind: &str,
    metadata_json: &str,
) -> Vec<DeadLetterRow> {
    envelopes
        .iter()
        .enumerate()
        .map(|(ordinal, envelope)| {
            let mut raw = raw_row(envelope, descriptor, intake_kind, metadata_json);
            raw.record_ordinal = ordinal as i32;
            DeadLetterRow {
                raw,
                dead_letter_reason: envelope
                    .metadata
                    .dead_letter_reason
                    .clone()
                    .unwrap_or_else(|| "unknown".into()),
            }
        })
        .collect()
}

pub async fn write_dead_letter_batch(
    store: &dyn ObjectStore,
    descriptor: &BatchDescriptor,
    intake_kind: &str,
    metadata_json: &str,
    envelopes: &[Envelope],
) -> anyhow::Result<Option<String>> {
    if envelopes.is_empty() {
        return Ok(None);
    }

    let rows = dead_letter_rows(envelopes, descriptor, intake_kind, metadata_json);
    let batch = dead_letter_record_batch(&rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let object_key = descriptor.dead_letter_object_key();
    store
        .put_bytes(&object_key, &parquet_bytes, PARQUET_CONTENT_TYPE)
        .await?;
    Ok(Some(object_key))
}

pub async fn write_raw_batch(
    store: &dyn ObjectStore,
    queue: &dyn IntakeQueue,
    input: RawBatchWriteInput,
) -> anyhow::Result<BatchWriteResult> {
    let rows = raw_rows(
        &input.envelopes,
        &input.descriptor,
        &input.intake_kind,
        &input.metadata_json,
    );
    let batch = raw_event_record_batch(&rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;

    let object_key = input.descriptor.raw_object_key();
    store
        .put_bytes(&object_key, &parquet_bytes, PARQUET_CONTENT_TYPE)
        .await?;

    let manifest_key = input.descriptor.raw_manifest_key();

    let object_sha256 = sha256_hex(&parquet_bytes);
    let manifest = IntakeManifest {
        manifest_version: 1,
        manifest_type: "raw".into(),
        layout_version: 1,
        manifest_id: format!("man_{}", uuid::Uuid::now_v7()),
        batch_id: input.descriptor.batch_id.clone(),
        tenant_id: input.descriptor.tenant_id.clone(),
        source: input.descriptor.source.clone(),
        integration_id: input.descriptor.normalized_integration_id(),
        produced_at: format_ts(Utc::now()),
        partition: input.descriptor.partition(),
        time_bounds: input.descriptor.time_bounds(),
        received_at_bounds: input.descriptor.received_at_bounds(),
        record_count: rows.len(),
        idempotency_key: input.idempotency_key.clone(),
        source_event_id_hash: source_event_id_hash(&rows),
        raw_envelope_version: 1,
        producer: Producer {
            kind: input.intake_kind.clone(),
            run_id: input.producer_run_id.clone(),
        },
        checkpoint: CheckpointInfo {
            request_payload_sha256: input.payload_sha256.clone(),
            extra: input.checkpoint_extra.clone(),
        },
        objects: vec![ObjectDescriptor {
            object_key: object_key.clone(),
            object_format: "parquet".into(),
            sha256: object_sha256,
            size_bytes: parquet_bytes.len() as u64,
            record_count: rows.len(),
            first_record_ordinal: 0,
            last_record_ordinal: rows.len().saturating_sub(1),
        }],
    };

    store
        .put_json(&manifest_key, &serde_json::to_value(&manifest)?)
        .await?;

    let mut queue_submitted = false;
    if !rows.is_empty() {
        let queue_entry = IntakeQueueEntry {
            batch_id: manifest.batch_id.clone(),
            tenant_id: manifest.tenant_id.clone(),
            source: manifest.source.clone(),
            integration_id: manifest.integration_id.clone(),
            received_at: manifest.received_at_bounds.min.clone(),
            record_count: manifest.record_count,
            idempotency_key: manifest.idempotency_key.clone(),
            payload_sha256: manifest.checkpoint.request_payload_sha256.clone(),
            manifest_key: manifest_key.clone(),
            object_key: object_key.clone(),
            producer: manifest.producer.clone(),
        };
        queue.submit(&queue_entry).await?;
        queue_submitted = true;
    }

    Ok(BatchWriteResult {
        manifest_key,
        object_key,
        dead_letter_object_key: None,
        record_count: rows.len(),
        queue_submitted,
    })
}

pub async fn write_normalized_batch(
    store: &dyn ObjectStore,
    input: NormalizedBatchWriteInput,
) -> anyhow::Result<NormalizedBatchWriteResult> {
    if input.events.is_empty() {
        anyhow::bail!("cannot write empty normalized batch");
    }

    let schema_version = if input.normalized_schema_version.is_empty() {
        crate::schema::NORMALIZED_SCHEMA_VERSION.to_string()
    } else {
        input.normalized_schema_version.clone()
    };
    let normalized_integration_id = input.descriptor.normalized_integration_id();
    let rows = input
        .events
        .iter()
        .map(|event| {
            normalized_event_row_from_value(
                event,
                &input.descriptor.tenant_id,
                &normalized_integration_id,
                &schema_version,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let batch = normalized_event_record_batch(&rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;

    let object_key = input.descriptor.normalized_object_key(&schema_version);
    store
        .put_bytes(&object_key, &parquet_bytes, PARQUET_CONTENT_TYPE)
        .await?;

    let manifest_key = input.descriptor.normalized_manifest_key();
    let observed_at_bounds = string_bounds(rows.iter().map(|row| row.observed_at.clone()))?;
    let event_id_bounds = string_bounds(rows.iter().map(|row| row.event_id.clone()))?;
    let upstream_raw_batches = sorted_unique_non_empty(input.upstream_raw_batches);

    let manifest = NormalizedManifest {
        manifest_version: 1,
        manifest_type: "normalized".into(),
        layout_version: 1,
        manifest_id: format!("man_{}", uuid::Uuid::now_v7()),
        batch_id: input.descriptor.batch_id.clone(),
        tenant_id: input.descriptor.tenant_id.clone(),
        source: input.descriptor.source.clone(),
        integration_id: normalized_integration_id,
        produced_at: format_ts(Utc::now()),
        partition: input.descriptor.partition(),
        time_bounds: observed_at_bounds.clone(),
        observed_at_bounds,
        record_count: rows.len(),
        idempotency_key: input.idempotency_key.clone(),
        normalized_schema_version: schema_version,
        event_id_min: event_id_bounds.min.clone(),
        event_id_max: event_id_bounds.max.clone(),
        upstream_raw_batches,
        checkpoint: NormalizedCheckpointInfo {
            event_id: event_id_bounds.max,
            extra: input.checkpoint_extra,
        },
        objects: vec![ObjectDescriptor {
            object_key: object_key.clone(),
            object_format: "parquet".into(),
            sha256: sha256_hex(&parquet_bytes),
            size_bytes: parquet_bytes.len() as u64,
            record_count: rows.len(),
            first_record_ordinal: 0,
            last_record_ordinal: rows.len().saturating_sub(1),
        }],
    };

    store
        .put_json(&manifest_key, &serde_json::to_value(&manifest)?)
        .await?;

    Ok(NormalizedBatchWriteResult {
        manifest_key,
        object_key,
        record_count: rows.len(),
    })
}

async fn write_feature_batch(
    store: &dyn ObjectStore,
    descriptor: &FeatureBatchDescriptor,
    payload: FeatureWritePayload,
) -> anyhow::Result<FeatureBatchWriteResult> {
    if payload.row_count == 0 {
        anyhow::bail!("cannot write empty feature batch");
    }

    let object_key = descriptor.feature_object_key(&payload.feature_schema_version);
    store
        .put_bytes(&object_key, &payload.parquet_bytes, PARQUET_CONTENT_TYPE)
        .await?;

    let manifest_key = descriptor.feature_manifest_key();
    let manifest = FeatureManifest {
        manifest_version: 1,
        manifest_type: "feature".into(),
        layout_version: 1,
        manifest_id: format!("man_{}", uuid::Uuid::now_v7()),
        batch_id: descriptor.batch_id.clone(),
        tenant_id: descriptor.tenant_id.clone(),
        source: descriptor.normalized_source(),
        produced_at: format_ts(Utc::now()),
        partition: descriptor.partition(),
        time_bounds: payload.time_bounds,
        record_count: payload.row_count,
        feature_table: descriptor.table_name.clone(),
        feature_schema_version: payload.feature_schema_version,
        upstream_normalized_batches: sorted_unique_non_empty(payload.upstream_normalized_batches),
        checkpoint: NormalizedCheckpointInfo {
            event_id: payload.checkpoint_event_id,
            extra: payload.checkpoint_extra,
        },
        objects: vec![ObjectDescriptor {
            object_key: object_key.clone(),
            object_format: "parquet".into(),
            sha256: sha256_hex(&payload.parquet_bytes),
            size_bytes: payload.parquet_bytes.len() as u64,
            record_count: payload.row_count,
            first_record_ordinal: 0,
            last_record_ordinal: payload.row_count.saturating_sub(1),
        }],
    };

    store
        .put_json(&manifest_key, &serde_json::to_value(&manifest)?)
        .await?;

    Ok(FeatureBatchWriteResult {
        manifest_key,
        object_key,
        record_count: payload.row_count,
    })
}

pub async fn write_action_feature_batch(
    store: &dyn ObjectStore,
    input: FeatureBatchWriteInput<ActionFeatureRow>,
) -> anyhow::Result<FeatureBatchWriteResult> {
    let schema_version = if input.feature_schema_version.is_empty() {
        crate::schema::FEATURE_SCHEMA_VERSION.to_string()
    } else {
        input.feature_schema_version
    };
    let batch = action_feature_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = single_time_bounds(input.descriptor.generated_at);
    write_feature_batch(
        store,
        &input.descriptor,
        FeatureWritePayload {
            feature_schema_version: schema_version,
            upstream_normalized_batches: input.upstream_normalized_batches,
            checkpoint_extra: input.checkpoint_extra,
            checkpoint_event_id: input.checkpoint_event_id,
            row_count: input.rows.len(),
            time_bounds,
            parquet_bytes,
        },
    )
    .await
}

pub async fn write_history_feature_batch(
    store: &dyn ObjectStore,
    input: FeatureBatchWriteInput<HistoryFeatureRow>,
) -> anyhow::Result<FeatureBatchWriteResult> {
    let schema_version = if input.feature_schema_version.is_empty() {
        crate::schema::FEATURE_SCHEMA_VERSION.to_string()
    } else {
        input.feature_schema_version
    };
    let batch = history_feature_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = string_bounds(input.rows.iter().map(|row| row.window_start.clone()))?;
    write_feature_batch(
        store,
        &input.descriptor,
        FeatureWritePayload {
            feature_schema_version: schema_version,
            upstream_normalized_batches: input.upstream_normalized_batches,
            checkpoint_extra: input.checkpoint_extra,
            checkpoint_event_id: input.checkpoint_event_id,
            row_count: input.rows.len(),
            time_bounds,
            parquet_bytes,
        },
    )
    .await
}

pub async fn write_collaboration_feature_batch(
    store: &dyn ObjectStore,
    input: FeatureBatchWriteInput<CollaborationFeatureRow>,
) -> anyhow::Result<FeatureBatchWriteResult> {
    let schema_version = if input.feature_schema_version.is_empty() {
        crate::schema::FEATURE_SCHEMA_VERSION.to_string()
    } else {
        input.feature_schema_version
    };
    let batch = collaboration_feature_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = single_time_bounds(input.descriptor.generated_at);
    write_feature_batch(
        store,
        &input.descriptor,
        FeatureWritePayload {
            feature_schema_version: schema_version,
            upstream_normalized_batches: input.upstream_normalized_batches,
            checkpoint_extra: input.checkpoint_extra,
            checkpoint_event_id: input.checkpoint_event_id,
            row_count: input.rows.len(),
            time_bounds,
            parquet_bytes,
        },
    )
    .await
}

pub async fn write_static_principal_feature_batch(
    store: &dyn ObjectStore,
    input: FeatureBatchWriteInput<StaticPrincipalFeatureRow>,
) -> anyhow::Result<FeatureBatchWriteResult> {
    let schema_version = if input.feature_schema_version.is_empty() {
        crate::schema::FEATURE_SCHEMA_VERSION.to_string()
    } else {
        input.feature_schema_version
    };
    let batch = static_principal_feature_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = single_time_bounds(input.descriptor.generated_at);
    write_feature_batch(
        store,
        &input.descriptor,
        FeatureWritePayload {
            feature_schema_version: schema_version,
            upstream_normalized_batches: input.upstream_normalized_batches,
            checkpoint_extra: input.checkpoint_extra,
            checkpoint_event_id: input.checkpoint_event_id,
            row_count: input.rows.len(),
            time_bounds,
            parquet_bytes,
        },
    )
    .await
}

pub async fn write_peer_group_feature_batch(
    store: &dyn ObjectStore,
    input: FeatureBatchWriteInput<PeerGroupFeatureRow>,
) -> anyhow::Result<FeatureBatchWriteResult> {
    let schema_version = if input.feature_schema_version.is_empty() {
        crate::schema::FEATURE_SCHEMA_VERSION.to_string()
    } else {
        input.feature_schema_version
    };
    let batch = peer_group_feature_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = single_time_bounds(input.descriptor.generated_at);
    write_feature_batch(
        store,
        &input.descriptor,
        FeatureWritePayload {
            feature_schema_version: schema_version,
            upstream_normalized_batches: input.upstream_normalized_batches,
            checkpoint_extra: input.checkpoint_extra,
            checkpoint_event_id: input.checkpoint_event_id,
            row_count: input.rows.len(),
            time_bounds,
            parquet_bytes,
        },
    )
    .await
}

pub async fn write_detection_batch(
    store: &dyn ObjectStore,
    input: DetectionBatchWriteInput,
) -> anyhow::Result<DetectionBatchWriteResult> {
    if input.rows.is_empty() {
        anyhow::bail!("cannot write empty detection batch");
    }
    let schema_version = if input.detection_schema_version.is_empty() {
        crate::schema::DETECTION_SCHEMA_VERSION.to_string()
    } else {
        input.detection_schema_version
    };
    let batch = detection_record_batch(&input.rows)?;
    let parquet_bytes = write_parquet_bytes(&batch)?;
    let time_bounds = string_bounds(input.rows.iter().map(|row| row.observed_at.clone()))?;
    let object_key = input.descriptor.detection_object_key(&schema_version);
    store
        .put_bytes(&object_key, &parquet_bytes, PARQUET_CONTENT_TYPE)
        .await?;

    let manifest_key = input.descriptor.detection_manifest_key();
    let manifest = DetectionManifest {
        manifest_version: 1,
        manifest_type: "detection".into(),
        layout_version: 1,
        manifest_id: format!("man_{}", uuid::Uuid::now_v7()),
        batch_id: input.descriptor.batch_id.clone(),
        tenant_id: input.descriptor.tenant_id.clone(),
        produced_at: format_ts(Utc::now()),
        partition: input.descriptor.partition(),
        time_bounds,
        record_count: input.rows.len(),
        detection_schema_version: schema_version,
        upstream_detection_context_signature: input.upstream_detection_context_signature,
        scoring_runtime_mode: input.scoring_runtime_mode,
        model_version: input.model_version,
        objects: vec![ObjectDescriptor {
            object_key: object_key.clone(),
            object_format: "parquet".into(),
            sha256: sha256_hex(&parquet_bytes),
            size_bytes: parquet_bytes.len() as u64,
            record_count: input.rows.len(),
            first_record_ordinal: 0,
            last_record_ordinal: input.rows.len().saturating_sub(1),
        }],
    };

    store
        .put_json(&manifest_key, &serde_json::to_value(&manifest)?)
        .await?;

    Ok(DetectionBatchWriteResult {
        manifest_key,
        object_key,
        record_count: input.rows.len(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use seccloud_pipeline::envelope::{Envelope, EventMetadata};

    use super::*;
    use crate::intake_queue::InMemoryIntakeQueue;
    use crate::object_store::InMemoryObjectStore;
    use crate::schema::NORMALIZED_SCHEMA_VERSION;

    fn descriptor() -> BatchDescriptor {
        BatchDescriptor {
            batch_id: "raw_test".into(),
            tenant_id: "tenant".into(),
            source: "okta".into(),
            integration_id: Some("okta-primary".into()),
            received_at: Utc.with_ymd_and_hms(2026, 3, 15, 14, 30, 0).unwrap(),
        }
    }

    fn envelope(payload: &[u8]) -> Envelope {
        let mut meta = EventMetadata::new("raw_test", "okta", "tenant");
        meta.integration_id = Some("okta-primary".into());
        Envelope::new(payload.to_vec(), meta)
    }

    fn normalized_descriptor() -> NormalizedBatchDescriptor {
        NormalizedBatchDescriptor {
            batch_id: "norm_test".into(),
            tenant_id: "tenant".into(),
            source: "okta".into(),
            integration_id: Some("okta-primary".into()),
            observed_at: Utc.with_ymd_and_hms(2026, 3, 15, 14, 0, 0).unwrap(),
        }
    }

    fn feature_descriptor(table_name: &str) -> FeatureBatchDescriptor {
        FeatureBatchDescriptor {
            batch_id: "feat_test".into(),
            tenant_id: "tenant".into(),
            table_name: table_name.into(),
            source: Some("gworkspace".into()),
            generated_at: Utc.with_ymd_and_hms(2026, 3, 15, 14, 30, 0).unwrap(),
        }
    }

    fn normalized_event(event_id: &str, observed_at: &str) -> serde_json::Value {
        serde_json::json!({
            "event_id": event_id,
            "event_key": format!("evk_{event_id}"),
            "integration_id": null,
            "source": "okta",
            "source_event_id": format!("src_{event_id}"),
            "principal": {
                "entity_key": format!("enk_principal_{event_id}")
            },
            "resource": {
                "entity_key": format!("enk_resource_{event_id}")
            },
            "action": {
                "source": "okta",
                "verb": "login",
                "category": "authentication"
            },
            "observed_at": observed_at,
            "environment": {
                "source_kind": "identity"
            },
            "attributes": {
                "intake_batch_id": "raw_123"
            },
            "evidence": {
                "source": "okta",
                "object_key": "lake/raw/layout=v1/tenant=tenant/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_123/part-00000.parquet",
                "raw_event_id": format!("src_{event_id}"),
                "observed_at": observed_at
            }
        })
    }

    #[tokio::test]
    async fn raw_batch_manifest_has_real_object_metadata() {
        let store = InMemoryObjectStore::new();
        let queue = InMemoryIntakeQueue::new();
        let result = write_raw_batch(
            &store,
            &queue,
            RawBatchWriteInput {
                descriptor: descriptor(),
                intake_kind: "push_gateway".into(),
                producer_run_id: "run-1".into(),
                metadata_json: "{}".into(),
                idempotency_key: "k".into(),
                payload_sha256: "sha256:p".into(),
                checkpoint_extra: serde_json::Map::new(),
                envelopes: vec![envelope(
                    br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z"}"#,
                )],
            },
        )
        .await
        .unwrap();

        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_ne!(
            manifest["objects"][0]["sha256"],
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(manifest["objects"][0]["size_bytes"].as_u64().unwrap() > 0);
        assert_eq!(queue.pending_count("tenant"), 1);
    }

    #[tokio::test]
    async fn dead_letter_batch_writes_file() {
        let store = InMemoryObjectStore::new();
        let mut env = envelope(br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z"}"#);
        env.mark_dead_letter("invalid");
        let key = write_dead_letter_batch(&store, &descriptor(), "push_gateway", "{}", &[env])
            .await
            .unwrap()
            .unwrap();
        assert!(key.contains("dead-letter"));
    }

    #[tokio::test]
    async fn empty_raw_batch_does_not_submit_queue_entry() {
        let store = InMemoryObjectStore::new();
        let queue = InMemoryIntakeQueue::new();
        let result = write_raw_batch(
            &store,
            &queue,
            RawBatchWriteInput {
                descriptor: descriptor(),
                intake_kind: "push_gateway".into(),
                producer_run_id: "run-1".into(),
                metadata_json: "{}".into(),
                idempotency_key: "k-empty".into(),
                payload_sha256: "sha256:empty".into(),
                checkpoint_extra: serde_json::Map::new(),
                envelopes: vec![],
            },
        )
        .await
        .unwrap();

        assert!(!result.queue_submitted);
        assert_eq!(queue.pending_count("tenant"), 0);
    }

    #[tokio::test]
    async fn normalized_batch_manifest_has_real_object_metadata() {
        let store = InMemoryObjectStore::new();
        let result = write_normalized_batch(
            &store,
            NormalizedBatchWriteInput {
                descriptor: normalized_descriptor(),
                normalized_schema_version: NORMALIZED_SCHEMA_VERSION.into(),
                idempotency_key: "norm:key".into(),
                upstream_raw_batches: vec!["raw_123".into(), "raw_123".into()],
                checkpoint_extra: serde_json::Map::new(),
                events: vec![
                    normalized_event("evt_2", "2026-03-15T14:10:00Z"),
                    normalized_event("evt_1", "2026-03-15T14:05:00Z"),
                ],
            },
        )
        .await
        .unwrap();

        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(result.record_count, 2);
        assert_eq!(manifest["manifest_type"], "normalized");
        assert_eq!(
            manifest["normalized_schema_version"],
            NORMALIZED_SCHEMA_VERSION
        );
        assert_eq!(manifest["event_id_min"], "evt_1");
        assert_eq!(manifest["event_id_max"], "evt_2");
        assert_eq!(
            manifest["upstream_raw_batches"],
            serde_json::json!(["raw_123"])
        );
        assert!(manifest["objects"][0]["size_bytes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn normalized_batch_writes_parquet_payload() {
        let store = InMemoryObjectStore::new();
        let result = write_normalized_batch(
            &store,
            NormalizedBatchWriteInput {
                descriptor: normalized_descriptor(),
                normalized_schema_version: NORMALIZED_SCHEMA_VERSION.into(),
                idempotency_key: "norm:key".into(),
                upstream_raw_batches: vec!["raw_123".into()],
                checkpoint_extra: serde_json::Map::new(),
                events: vec![normalized_event("evt_1", "2026-03-15T14:05:00Z")],
            },
        )
        .await
        .unwrap();

        let parquet_bytes = store.get_bytes(&result.object_key).await.unwrap().unwrap();
        let batches = crate::schema::read_parquet_bytes(&parquet_bytes).unwrap();
        let rows = crate::schema::normalized_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "evt_1");

        let payload: serde_json::Value = serde_json::from_str(&rows[0].payload_json).unwrap();
        assert_eq!(payload["action"]["verb"], "login");
    }

    #[tokio::test]
    async fn empty_normalized_batch_is_rejected() {
        let store = InMemoryObjectStore::new();
        let err = write_normalized_batch(
            &store,
            NormalizedBatchWriteInput {
                descriptor: normalized_descriptor(),
                normalized_schema_version: NORMALIZED_SCHEMA_VERSION.into(),
                idempotency_key: "norm:empty".into(),
                upstream_raw_batches: vec![],
                checkpoint_extra: serde_json::Map::new(),
                events: vec![],
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("empty normalized batch"));
    }

    #[tokio::test]
    async fn action_feature_batch_writes_parquet_and_manifest() {
        let store = InMemoryObjectStore::new();
        let result = write_action_feature_batch(
            &store,
            FeatureBatchWriteInput {
                descriptor: feature_descriptor("action-accessor-set"),
                feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                upstream_normalized_batches: vec!["norm_1".into()],
                checkpoint_extra: serde_json::Map::new(),
                checkpoint_event_id: "evt_2".into(),
                rows: vec![
                    ActionFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        source: "gworkspace".into(),
                        resource_entity_key: "enk_resource_1".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        access_count: 2,
                        accessor_weight: 2.0 / 3.0,
                    },
                    ActionFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        source: "gworkspace".into(),
                        resource_entity_key: "enk_resource_1".into(),
                        principal_entity_key: "enk_principal_2".into(),
                        access_count: 1,
                        accessor_weight: 1.0 / 3.0,
                    },
                ],
            },
        )
        .await
        .unwrap();

        assert!(result.object_key.contains("table=action-accessor-set"));
        assert_eq!(result.record_count, 2);
        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(manifest["manifest_type"], "feature");
        assert_eq!(manifest["feature_table"], "action-accessor-set");
        assert_eq!(manifest["record_count"], 2);
    }

    #[tokio::test]
    async fn history_feature_batch_writes_parquet_and_manifest() {
        let store = InMemoryObjectStore::new();
        let result = write_history_feature_batch(
            &store,
            FeatureBatchWriteInput {
                descriptor: feature_descriptor("history-window"),
                feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                upstream_normalized_batches: vec!["norm_1".into()],
                checkpoint_extra: serde_json::Map::new(),
                checkpoint_event_id: "evt_2".into(),
                rows: vec![
                    HistoryFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        window_start: "2026-03-15T14:00:00Z".into(),
                        resource_entity_key: "enk_resource_1".into(),
                    },
                    HistoryFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        window_start: "2026-03-15T14:00:00Z".into(),
                        resource_entity_key: "enk_resource_2".into(),
                    },
                ],
            },
        )
        .await
        .unwrap();

        assert!(result.object_key.contains("table=history-window"));
        assert_eq!(result.record_count, 2);
        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(manifest["manifest_type"], "feature");
        assert_eq!(manifest["feature_table"], "history-window");
        assert_eq!(manifest["record_count"], 2);
    }

    #[tokio::test]
    async fn collaboration_feature_batch_writes_parquet_and_manifest() {
        let store = InMemoryObjectStore::new();
        let result = write_collaboration_feature_batch(
            &store,
            FeatureBatchWriteInput {
                descriptor: feature_descriptor("collaboration"),
                feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                upstream_normalized_batches: vec!["norm_1".into()],
                checkpoint_extra: serde_json::Map::new(),
                checkpoint_event_id: "evt_2".into(),
                rows: vec![
                    CollaborationFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        collaborator_entity_key: "enk_principal_2".into(),
                        co_access_weight: 1.5,
                    },
                    CollaborationFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_2".into(),
                        collaborator_entity_key: "enk_principal_1".into(),
                        co_access_weight: 1.5,
                    },
                ],
            },
        )
        .await
        .unwrap();

        assert!(result.object_key.contains("table=collaboration"));
        assert_eq!(result.record_count, 2);
        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(manifest["manifest_type"], "feature");
        assert_eq!(manifest["feature_table"], "collaboration");
        assert_eq!(manifest["record_count"], 2);
    }

    #[tokio::test]
    async fn static_principal_feature_batch_writes_parquet_and_manifest() {
        let store = InMemoryObjectStore::new();
        let result = write_static_principal_feature_batch(
            &store,
            FeatureBatchWriteInput {
                descriptor: feature_descriptor("principal-static"),
                feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                upstream_normalized_batches: vec!["norm_1".into()],
                checkpoint_extra: serde_json::Map::new(),
                checkpoint_event_id: "evt_2".into(),
                rows: vec![
                    StaticPrincipalFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        principal_id: "alice@example.com".into(),
                        department: "security".into(),
                        role: "security-admin".into(),
                        location: "US-NY".into(),
                        employment_duration_bucket: "3-12mo".into(),
                        privilege_level: "admin".into(),
                    },
                    StaticPrincipalFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_2".into(),
                        principal_id: "bob@example.com".into(),
                        department: "engineering".into(),
                        role: "engineer".into(),
                        location: "US-CA".into(),
                        employment_duration_bucket: "1-3yr".into(),
                        privilege_level: "regular".into(),
                    },
                ],
            },
        )
        .await
        .unwrap();

        assert!(result.object_key.contains("table=principal-static"));
        assert_eq!(result.record_count, 2);
        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(manifest["manifest_type"], "feature");
        assert_eq!(manifest["feature_table"], "principal-static");
        assert_eq!(manifest["record_count"], 2);
    }

    #[tokio::test]
    async fn peer_group_feature_batch_writes_parquet_and_manifest() {
        let store = InMemoryObjectStore::new();
        let result = write_peer_group_feature_batch(
            &store,
            FeatureBatchWriteInput {
                descriptor: feature_descriptor("peer-group"),
                feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                upstream_normalized_batches: vec!["norm_1".into()],
                checkpoint_extra: serde_json::Map::new(),
                checkpoint_event_id: "evt_2".into(),
                rows: vec![
                    PeerGroupFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        peer_entity_key: "enk_principal_2".into(),
                        peer_type: "department".into(),
                        peer_weight: 1.0,
                    },
                    PeerGroupFeatureRow {
                        feature_schema_version: crate::schema::FEATURE_SCHEMA_VERSION.into(),
                        tenant_id: "tenant".into(),
                        principal_entity_key: "enk_principal_1".into(),
                        peer_entity_key: "enk_principal_3".into(),
                        peer_type: "manager".into(),
                        peer_weight: 0.5,
                    },
                ],
            },
        )
        .await
        .unwrap();

        assert!(result.object_key.contains("table=peer-group"));
        assert_eq!(result.record_count, 2);
        let manifest = store.get_json(&result.manifest_key).await.unwrap().unwrap();
        assert_eq!(manifest["manifest_type"], "feature");
        assert_eq!(manifest["feature_table"], "peer-group");
        assert_eq!(manifest["record_count"], 2);
    }
}
