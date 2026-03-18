use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Duration, Timelike, Utc};
use flate2::read::GzDecoder;
use seccloud_lake::batch_writer::{
    BatchDescriptor, NormalizedBatchDescriptor, NormalizedBatchWriteInput, write_dead_letter_batch,
    write_normalized_batch,
};
use seccloud_lake::intake_queue::IntakeQueue;
use seccloud_lake::key;
use seccloud_lake::manifest::{IntakeManifest, IntakeQueueEntry, ObjectDescriptor};
use seccloud_lake::object_store::ObjectStore;
use seccloud_lake::schema::{read_parquet_bytes, record_batch_to_rows};
use seccloud_pipeline::envelope::{Envelope, EventMetadata};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::normalize;

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NormalizationWorkerResult {
    pub processed_batch_count: usize,
    pub processed_batch_ids: Vec<String>,
    pub raw_events_seen: usize,
    pub added_raw_events: usize,
    pub normalized_events_seen: usize,
    pub normalized_event_count: usize,
    pub duplicate_semantic_events: usize,
    pub late_arrival_count: usize,
    pub dead_letter_count: usize,
    pub dead_letter_reasons: BTreeMap<String, usize>,
    pub raw_event_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NormalizationState {
    pub raw_event_ids: BTreeSet<String>,
    pub raw_event_keys: BTreeSet<String>,
    pub normalized_event_ids: BTreeSet<String>,
    pub normalized_event_keys: BTreeSet<String>,
    pub semantic_event_keys: BTreeSet<String>,
    pub dead_letter_ids: BTreeSet<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct LegacyRawEnvelope {
    source: String,
    integration_id: String,
    intake_kind: String,
    batch_id: String,
    received_at: String,
    record_ordinal: i32,
    metadata: Value,
    record: Value,
}

#[derive(Debug, Clone)]
struct RawEnvelopeRecord {
    integration_id: String,
    intake_kind: String,
    received_at: String,
    record_ordinal: i32,
    metadata: Value,
    record: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionKey {
    source: String,
    integration_id: String,
    dt: String,
    hour: String,
}

fn parse_timestamp(value: &str) -> anyhow::Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(value)?;
    Ok(parsed.with_timezone(&Utc))
}

async fn load_manifest(
    store: &dyn ObjectStore,
    manifest_key: &str,
) -> anyhow::Result<Option<IntakeManifest>> {
    match store.get_json(manifest_key).await? {
        Some(value) => Ok(Some(serde_json::from_value(value)?)),
        None => Ok(None),
    }
}

fn find_object_descriptor<'a>(
    manifest: Option<&'a IntakeManifest>,
    object_key: &str,
) -> Option<&'a ObjectDescriptor> {
    manifest.and_then(|manifest| {
        manifest
            .objects
            .iter()
            .find(|descriptor| descriptor.object_key == object_key)
    })
}

async fn read_raw_batch_records(
    store: &dyn ObjectStore,
    batch: &IntakeQueueEntry,
) -> anyhow::Result<Vec<RawEnvelopeRecord>> {
    let payload = store
        .get_bytes(&batch.object_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("raw batch object not found: {}", batch.object_key))?;

    if batch.object_key.ends_with(".parquet") {
        let batches = read_parquet_bytes(&payload)?;
        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(record_batch_to_rows(&batch)?);
        }
        return rows
            .into_iter()
            .map(|row| {
                let metadata =
                    serde_json::from_str(&row.metadata_json).unwrap_or_else(|_| json!({}));
                let record = serde_json::from_str(&row.record_json)?;
                Ok(RawEnvelopeRecord {
                    integration_id: row.integration_id,
                    intake_kind: row.intake_kind,
                    received_at: row.received_at,
                    record_ordinal: row.record_ordinal,
                    metadata,
                    record,
                })
            })
            .collect();
    }

    let mut decoder = GzDecoder::new(payload.as_slice());
    let mut body = String::new();
    use std::io::Read;
    decoder.read_to_string(&mut body)?;

    body.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let row: LegacyRawEnvelope = serde_json::from_str(line)?;
            Ok(RawEnvelopeRecord {
                integration_id: row.integration_id,
                intake_kind: row.intake_kind,
                received_at: row.received_at,
                record_ordinal: row.record_ordinal,
                metadata: row.metadata,
                record: row.record,
            })
        })
        .collect()
}

fn landed_raw_record(
    batch: &IntakeQueueEntry,
    envelope: &RawEnvelopeRecord,
    raw_descriptor: Option<&ObjectDescriptor>,
) -> anyhow::Result<Value> {
    let raw_record = envelope
        .record
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("raw record should be an object"))?;
    let mut record = Map::new();
    record.insert(
        "integration_id".into(),
        Value::String(envelope.integration_id.clone()),
    );
    record.insert(
        "intake_batch_id".into(),
        Value::String(batch.batch_id.clone()),
    );
    record.insert(
        "intake_kind".into(),
        Value::String(envelope.intake_kind.clone()),
    );
    record.insert(
        "received_at".into(),
        Value::String(envelope.received_at.clone()),
    );
    record.insert(
        "raw_manifest_key".into(),
        Value::String(batch.manifest_key.clone()),
    );
    record.insert(
        "raw_batch_object_key".into(),
        Value::String(batch.object_key.clone()),
    );
    record.insert(
        "raw_record_ordinal".into(),
        Value::Number(envelope.record_ordinal.into()),
    );
    if let Some(descriptor) = raw_descriptor {
        record.insert(
            "raw_object_sha256".into(),
            Value::String(descriptor.sha256.clone()),
        );
        record.insert(
            "raw_object_format".into(),
            Value::String(descriptor.object_format.clone()),
        );
    }
    record.extend(raw_record);
    let source = record
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or(&batch.source)
        .to_string();
    record.insert("source".into(), Value::String(source));
    Ok(Value::Object(record))
}

fn raw_event_object_key(tenant_id: &str, record: &Map<String, Value>) -> anyhow::Result<String> {
    let source = record
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing source"))?;
    let source_event_id = record
        .get("source_event_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing source_event_id"))?;
    let integration_id = record.get("integration_id").and_then(Value::as_str);
    let received_at = record
        .get("received_at")
        .or_else(|| record.get("observed_at"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing received_at"))?;
    let received_at = parse_timestamp(received_at)?;
    let event_key = normalize::event_key(source, source_event_id, integration_id);
    Ok(key::raw_event_json_key(
        tenant_id,
        source,
        integration_id,
        &received_at,
        &event_key,
    ))
}

fn dead_letter_object_key(tenant_id: &str, record: &Map<String, Value>) -> anyhow::Result<String> {
    let source = record
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing source"))?;
    let source_event_id = record
        .get("source_event_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let integration_id = record.get("integration_id").and_then(Value::as_str);
    let received_at = record
        .get("received_at")
        .or_else(|| record.get("observed_at"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing received_at"))?;
    let received_at = parse_timestamp(received_at)?;
    Ok(key::dead_letter_json_key(
        tenant_id,
        source,
        integration_id,
        &received_at,
        &format!("{source}-{source_event_id}"),
    ))
}

fn partition_key_for_event(event: &Map<String, Value>) -> anyhow::Result<PartitionKey> {
    let source = event
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("normalized event missing source"))?
        .to_string();
    let observed_at = event
        .get("observed_at")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("normalized event missing observed_at"))?;
    let observed_at = parse_timestamp(observed_at)?;
    let integration_id = key::partition_value(event.get("integration_id").and_then(Value::as_str));
    Ok(PartitionKey {
        source,
        integration_id,
        dt: observed_at.format("%Y-%m-%d").to_string(),
        hour: format!("{:02}", observed_at.hour()),
    })
}

fn batch_id_for_partition(raw_batch_id: &str, partition: &PartitionKey) -> String {
    format!(
        "norm_{}_{}_{}",
        raw_batch_id,
        partition.dt.replace('-', ""),
        partition.hour
    )
}

fn semantic_event_key(record: &Map<String, Value>) -> anyhow::Result<String> {
    let source = record
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing source"))?;
    let actor_email = record
        .get("actor_email")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing actor_email"))?;
    let event_type = record
        .get("event_type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing event_type"))?;
    let resource_id = record
        .get("resource_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing resource_id"))?;
    let observed_at = record
        .get("observed_at")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("raw record missing observed_at"))?;
    Ok(format!(
        "{source}|{actor_email}|{event_type}|{resource_id}|{observed_at}"
    ))
}

async fn ensure_json_object(
    store: &dyn ObjectStore,
    key: &str,
    payload: &Value,
) -> anyhow::Result<bool> {
    if store.get_json(key).await?.is_some() {
        return Ok(false);
    }
    store.put_json(key, payload).await?;
    Ok(true)
}

fn dead_letter_envelope(
    batch: &IntakeQueueEntry,
    envelope: &RawEnvelopeRecord,
    record: &Value,
    reason: &str,
) -> anyhow::Result<Envelope> {
    let source = record
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or(&batch.source);
    let mut metadata = EventMetadata::new(&batch.batch_id, source, &batch.tenant_id);
    metadata.integration_id = Some(envelope.integration_id.clone());
    metadata.intake_ts = parse_timestamp(&envelope.received_at)?.into();
    metadata.dead_letter_reason = Some(reason.to_string());
    Ok(Envelope::new(serde_json::to_vec(record)?, metadata))
}

pub async fn process_pending_batches_with_state(
    tenant_id: &str,
    store: &Arc<dyn ObjectStore>,
    queue: &Arc<dyn IntakeQueue>,
    mut state: NormalizationState,
    max_batches: Option<usize>,
) -> anyhow::Result<(NormalizationWorkerResult, NormalizationState)> {
    let mut pending = queue.list_pending(tenant_id).await?;
    if let Some(limit) = max_batches {
        pending.truncate(limit);
    }

    let mut result = NormalizationWorkerResult::default();
    for batch in pending {
        let manifest = load_manifest(store.as_ref(), &batch.manifest_key).await?;
        let raw_descriptor = find_object_descriptor(manifest.as_ref(), &batch.object_key);
        let raw_rows = read_raw_batch_records(store.as_ref(), &batch).await?;
        let mut normalized_groups: BTreeMap<PartitionKey, Vec<Value>> = BTreeMap::new();
        let mut dead_letter_envelopes = Vec::new();
        let dead_letter_metadata_json = raw_rows
            .first()
            .map(|row| serde_json::to_string(&row.metadata))
            .transpose()?
            .unwrap_or_else(|| "{}".to_string());

        for raw_row in raw_rows {
            let landed = landed_raw_record(&batch, &raw_row, raw_descriptor)?;
            let landed_object = landed
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("landed raw record should be an object"))?;
            let raw_key = raw_event_object_key(&batch.tenant_id, &landed_object)?;
            ensure_json_object(
                store.as_ref(),
                &raw_key,
                &Value::Object(landed_object.clone()),
            )
            .await?;
            result.raw_event_count += 1;
            if let Some(source_event_id) =
                landed_object.get("source_event_id").and_then(Value::as_str)
                && state.raw_event_ids.insert(source_event_id.to_string())
            {
                result.added_raw_events += 1;
            }
            let raw_event_key = normalize::event_key(
                landed_object
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or(&batch.source),
                landed_object
                    .get("source_event_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                landed_object.get("integration_id").and_then(Value::as_str),
            );
            state.raw_event_keys.insert(raw_event_key.clone());

            match normalize::validate_raw_event(&Value::Object(landed_object.clone())) {
                Ok(()) => {
                    let semantic_key = semantic_event_key(&landed_object)?;
                    if state.semantic_event_keys.contains(&semantic_key) {
                        result.duplicate_semantic_events += 1;
                        continue;
                    }
                    state.semantic_event_keys.insert(semantic_key);

                    let received_at = parse_timestamp(
                        landed_object
                            .get("received_at")
                            .or_else(|| landed_object.get("observed_at"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("raw record missing received_at"))?,
                    )?;
                    let observed_at = parse_timestamp(
                        landed_object
                            .get("observed_at")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("raw record missing observed_at"))?,
                    )?;
                    if received_at - observed_at > Duration::hours(12) {
                        result.late_arrival_count += 1;
                    }

                    let event = normalize::normalize_raw_event(
                        &Value::Object(landed_object.clone()),
                        landed_object.get("integration_id").and_then(Value::as_str),
                        &raw_key,
                    )?;
                    let event_object = event
                        .as_object()
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("normalized event should be an object"))?;
                    let normalized_event_key = event_object
                        .get("event_key")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("normalized event missing event_key"))?
                        .to_string();
                    if state.normalized_event_keys.contains(&normalized_event_key) {
                        continue;
                    }
                    state.normalized_event_keys.insert(normalized_event_key);
                    if let Some(event_id) = event_object.get("event_id").and_then(Value::as_str) {
                        state.normalized_event_ids.insert(event_id.to_string());
                    }
                    normalized_groups
                        .entry(partition_key_for_event(&event_object)?)
                        .or_default()
                        .push(Value::Object(event_object));
                    result.normalized_event_count += 1;
                }
                Err(error) => {
                    let reason = error.to_string();
                    *result
                        .dead_letter_reasons
                        .entry(reason.clone())
                        .or_insert(0) += 1;
                    let dead_letter_key = dead_letter_object_key(&batch.tenant_id, &landed_object)?;
                    let source = landed_object
                        .get("source")
                        .and_then(Value::as_str)
                        .unwrap_or(&batch.source);
                    let source_event_id = landed_object
                        .get("source_event_id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    let payload = json!({
                        "dead_letter_id": format!("{source}-{source_event_id}"),
                        "source": source,
                        "reason": reason.clone(),
                        "raw_event": Value::Object(landed_object.clone()),
                    });
                    ensure_json_object(store.as_ref(), &dead_letter_key, &payload).await?;
                    let dead_letter_manifest_id = format!("{source}:{raw_event_key}");
                    if state.dead_letter_ids.insert(dead_letter_manifest_id) {
                        result.dead_letter_count += 1;
                    }
                    dead_letter_envelopes.push(dead_letter_envelope(
                        &batch,
                        &raw_row,
                        &Value::Object(landed_object),
                        &reason,
                    )?);
                }
            }
        }

        for (partition, events) in normalized_groups {
            let observed_at =
                DateTime::from_str(&format!("{}T{}:00:00Z", partition.dt, partition.hour))?;
            write_normalized_batch(
                store.as_ref(),
                NormalizedBatchWriteInput {
                    descriptor: NormalizedBatchDescriptor {
                        batch_id: batch_id_for_partition(&batch.batch_id, &partition),
                        tenant_id: batch.tenant_id.clone(),
                        source: partition.source.clone(),
                        integration_id: Some(partition.integration_id.clone()),
                        observed_at,
                    },
                    normalized_schema_version: seccloud_lake::schema::NORMALIZED_SCHEMA_VERSION
                        .into(),
                    idempotency_key: format!(
                        "norm:{}:{}:{}",
                        batch.batch_id, partition.dt, partition.hour
                    ),
                    upstream_raw_batches: vec![batch.batch_id.clone()],
                    checkpoint_extra: Map::new(),
                    events,
                },
            )
            .await?;
        }

        if !dead_letter_envelopes.is_empty() {
            let received_at = parse_timestamp(&batch.received_at)?;
            let descriptor = BatchDescriptor {
                batch_id: format!("normdl_{}", batch.batch_id),
                tenant_id: batch.tenant_id.clone(),
                source: batch.source.clone(),
                integration_id: Some(batch.integration_id.clone()),
                received_at,
            };
            write_dead_letter_batch(
                store.as_ref(),
                &descriptor,
                &batch.producer.kind,
                &dead_letter_metadata_json,
                &dead_letter_envelopes,
            )
            .await?;
        }

        queue
            .mark_processed(&batch.tenant_id, &batch.batch_id)
            .await?;
        result.processed_batch_count += 1;
        result.processed_batch_ids.push(batch.batch_id);
    }

    result.raw_events_seen = state.raw_event_ids.len();
    result.normalized_events_seen = state.normalized_event_ids.len();
    Ok((result, state))
}

pub async fn process_pending_batches(
    tenant_id: &str,
    store: &Arc<dyn ObjectStore>,
    queue: &Arc<dyn IntakeQueue>,
    max_batches: Option<usize>,
) -> anyhow::Result<NormalizationWorkerResult> {
    let (result, _) = process_pending_batches_with_state(
        tenant_id,
        store,
        queue,
        NormalizationState::default(),
        max_batches,
    )
    .await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use seccloud_lake::batch_writer::{BatchDescriptor, RawBatchWriteInput, write_raw_batch};
    use seccloud_lake::intake_queue::InMemoryIntakeQueue;
    use seccloud_lake::object_store::{InMemoryObjectStore, ObjectStore};
    use seccloud_pipeline::envelope::{Envelope, EventMetadata};

    fn raw_envelope(
        payload: &[u8],
        ordinal: usize,
        observed_at: &str,
        integration_id: &str,
    ) -> Envelope {
        let mut meta = EventMetadata::new("raw_test", "okta", "tenant");
        meta.integration_id = Some(integration_id.to_string());
        meta.intake_ts = parse_timestamp(observed_at).unwrap().into();
        meta.attributes
            .insert("ordinal".into(), ordinal.to_string());
        Envelope::new(payload.to_vec(), meta)
    }

    #[tokio::test]
    async fn processes_pending_batch_into_raw_normalized_and_dead_letter_outputs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemoryObjectStore::new());
        let queue = Arc::new(InMemoryIntakeQueue::new());

        write_raw_batch(
            store.as_ref(),
            queue.as_ref(),
            RawBatchWriteInput {
                descriptor: BatchDescriptor {
                    batch_id: "raw_test".into(),
                    tenant_id: "tenant".into(),
                    source: "okta".into(),
                    integration_id: Some("okta-primary".into()),
                    received_at: Utc.with_ymd_and_hms(2026, 3, 15, 14, 30, 0).unwrap(),
                },
                intake_kind: "push_gateway".into(),
                producer_run_id: "run-1".into(),
                metadata_json: "{}".into(),
                idempotency_key: "raw:key".into(),
                payload_sha256: "sha256:payload".into(),
                checkpoint_extra: Map::new(),
                envelopes: vec![
                    raw_envelope(
                        br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:05:00Z","actor_email":"alice@example.com","actor_name":"Alice","department":"security","role":"security-admin","event_type":"login","resource_id":"okta:app/admin","resource_name":"Admin","resource_kind":"app","sensitivity":"high"}"#,
                        0,
                        "2026-03-15T14:05:00Z",
                        "okta-primary",
                    ),
                    raw_envelope(
                        br#"{"source_event_id":"e2","observed_at":"2026-03-15T14:06:00Z","actor_email":"bob@example.com","actor_name":"Bob","department":"security","role":"analyst","event_type":"password_reset","resource_id":"okta:app/helpdesk","resource_name":"Helpdesk","resource_kind":"app","sensitivity":"high"}"#,
                        1,
                        "2026-03-15T14:06:00Z",
                        "okta-primary",
                    ),
                ],
            },
        )
        .await
        .unwrap();

        let (result, state) = process_pending_batches_with_state(
            "tenant",
            &store,
            &(queue.clone() as Arc<dyn IntakeQueue>),
            NormalizationState::default(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            NormalizationWorkerResult {
                processed_batch_count: 1,
                processed_batch_ids: vec!["raw_test".into()],
                raw_events_seen: 2,
                added_raw_events: 2,
                normalized_events_seen: 1,
                normalized_event_count: 1,
                duplicate_semantic_events: 0,
                late_arrival_count: 0,
                dead_letter_count: 1,
                dead_letter_reasons: BTreeMap::from([("unsupported_event_type".into(), 1)]),
                raw_event_count: 2,
            }
        );
        assert_eq!(state.semantic_event_keys.len(), 1);
        assert_eq!(state.dead_letter_ids.len(), 1);

        let mem_store = store
            .as_any()
            .downcast_ref::<InMemoryObjectStore>()
            .expect("in-memory store");
        let keys = mem_store.keys();
        assert!(keys.iter().any(|key| key.starts_with("raw/tenant=tenant/")));
        assert!(
            !keys
                .iter()
                .any(|key| key.starts_with("normalized/tenant=tenant/"))
        );
        assert!(
            keys.iter()
                .any(|key| key.starts_with("dead_letters/tenant=tenant/"))
        );
        assert!(
            keys.iter()
                .any(|key| key.contains("lake/normalized/layout=v1/"))
        );
        assert!(keys.iter().any(|key| key.contains("type=normalized/")));
        assert_eq!(queue.pending_count("tenant"), 0);
    }

    #[tokio::test]
    async fn splits_normalized_output_by_observed_hour() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemoryObjectStore::new());
        let queue = Arc::new(InMemoryIntakeQueue::new());

        write_raw_batch(
            store.as_ref(),
            queue.as_ref(),
            RawBatchWriteInput {
                descriptor: BatchDescriptor {
                    batch_id: "raw_test".into(),
                    tenant_id: "tenant".into(),
                    source: "okta".into(),
                    integration_id: Some("okta-primary".into()),
                    received_at: Utc.with_ymd_and_hms(2026, 3, 15, 15, 0, 0).unwrap(),
                },
                intake_kind: "push_gateway".into(),
                producer_run_id: "run-1".into(),
                metadata_json: "{}".into(),
                idempotency_key: "raw:key".into(),
                payload_sha256: "sha256:payload".into(),
                checkpoint_extra: Map::new(),
                envelopes: vec![
                    raw_envelope(
                        br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:05:00Z","actor_email":"alice@example.com","actor_name":"Alice","department":"security","role":"security-admin","event_type":"login","resource_id":"okta:app/admin","resource_name":"Admin","resource_kind":"app","sensitivity":"high"}"#,
                        0,
                        "2026-03-15T14:05:00Z",
                        "okta-primary",
                    ),
                    raw_envelope(
                        br#"{"source_event_id":"e2","observed_at":"2026-03-15T15:06:00Z","actor_email":"bob@example.com","actor_name":"Bob","department":"security","role":"analyst","event_type":"login","resource_id":"okta:app/helpdesk","resource_name":"Helpdesk","resource_kind":"app","sensitivity":"high"}"#,
                        1,
                        "2026-03-15T15:06:00Z",
                        "okta-primary",
                    ),
                ],
            },
        )
        .await
        .unwrap();

        process_pending_batches(
            "tenant",
            &store,
            &(queue.clone() as Arc<dyn IntakeQueue>),
            None,
        )
        .await
        .unwrap();

        let mem_store = store
            .as_any()
            .downcast_ref::<InMemoryObjectStore>()
            .expect("in-memory store");
        let normalized_manifests = mem_store
            .keys()
            .into_iter()
            .filter(|key| key.contains("type=normalized/"))
            .collect::<Vec<_>>();
        assert_eq!(normalized_manifests.len(), 2);
    }

    #[tokio::test]
    async fn tracks_duplicate_semantics_and_late_arrivals() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemoryObjectStore::new());
        let queue = Arc::new(InMemoryIntakeQueue::new());

        write_raw_batch(
            store.as_ref(),
            queue.as_ref(),
            RawBatchWriteInput {
                descriptor: BatchDescriptor {
                    batch_id: "raw_test".into(),
                    tenant_id: "tenant".into(),
                    source: "okta".into(),
                    integration_id: Some("okta-primary".into()),
                    received_at: Utc.with_ymd_and_hms(2026, 3, 16, 15, 0, 0).unwrap(),
                },
                intake_kind: "push_gateway".into(),
                producer_run_id: "run-1".into(),
                metadata_json: "{}".into(),
                idempotency_key: "raw:key".into(),
                payload_sha256: "sha256:payload".into(),
                checkpoint_extra: Map::new(),
                envelopes: vec![
                    raw_envelope(
                        br#"{"source_event_id":"e1","observed_at":"2026-03-15T01:00:00Z","received_at":"2026-03-16T15:00:00Z","actor_email":"alice@example.com","actor_name":"Alice","department":"security","role":"security-admin","event_type":"login","resource_id":"okta:app/admin","resource_name":"Admin","resource_kind":"app","sensitivity":"high"}"#,
                        0,
                        "2026-03-16T15:00:00Z",
                        "okta-primary",
                    ),
                    raw_envelope(
                        br#"{"source_event_id":"e2","observed_at":"2026-03-15T01:00:00Z","received_at":"2026-03-16T15:00:00Z","actor_email":"alice@example.com","actor_name":"Alice","department":"security","role":"security-admin","event_type":"login","resource_id":"okta:app/admin","resource_name":"Admin","resource_kind":"app","sensitivity":"high"}"#,
                        1,
                        "2026-03-16T15:00:00Z",
                        "okta-primary",
                    ),
                ],
            },
        )
        .await
        .unwrap();

        let result = process_pending_batches(
            "tenant",
            &store,
            &(queue.clone() as Arc<dyn IntakeQueue>),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.added_raw_events, 2);
        assert_eq!(result.normalized_event_count, 1);
        assert_eq!(result.duplicate_semantic_events, 1);
        assert_eq!(result.late_arrival_count, 1);
    }
}
