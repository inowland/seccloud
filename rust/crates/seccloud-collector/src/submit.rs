use std::sync::Arc;

use chrono::Utc;
use sha2::{Digest, Sha256};

use seccloud_lake::accepted_batch::{AcceptedBatch, AcceptedBatchStore};
use seccloud_lake::batch_writer::{BatchDescriptor, RawBatchWriteInput, write_raw_batch};
use seccloud_lake::intake_queue::IntakeQueue;
use seccloud_lake::key;
use seccloud_lake::object_store::ObjectStore;
use seccloud_pipeline::envelope::{Envelope, EventMetadata};

/// Result of a successful batch submission.
#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub batch_id: String,
    pub manifest_key: String,
    pub object_key: String,
    pub idempotency_key: String,
    pub record_count: usize,
    pub duplicate: bool,
}

fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("sha256:{hash:x}")
}

/// Submit a batch of collected records as Parquet to the lake.
#[allow(clippy::too_many_arguments)]
pub async fn submit_collected_batch(
    store: &Arc<dyn ObjectStore>,
    queue: &Arc<dyn IntakeQueue>,
    accepted_batches: &Arc<dyn AcceptedBatchStore>,
    tenant_id: &str,
    source: &str,
    integration_id: &str,
    records: &[serde_json::Value],
    idempotency_key: &str,
    collector_kind: &str,
    producer_run_id: &str,
    checkpoint_payload: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<SubmitResult> {
    let now = Utc::now();
    let integration_normalized = key::partition_value(Some(integration_id));

    let payload_sha256 = sha256_hex(
        serde_json::to_string(records)
            .unwrap_or_default()
            .as_bytes(),
    );

    if let Some(existing) = accepted_batches.lookup(idempotency_key).await? {
        if existing.payload_sha256 != payload_sha256 {
            anyhow::bail!("idempotency conflict for key {idempotency_key}");
        }
        return Ok(SubmitResult {
            batch_id: existing.batch_id,
            manifest_key: existing.manifest_key,
            object_key: existing.object_key,
            idempotency_key: existing.idempotency_key,
            record_count: existing.record_count,
            duplicate: true,
        });
    }

    let batch_id = format!("raw_{}", uuid::Uuid::now_v7());
    let envelopes: Vec<Envelope> = records
        .iter()
        .map(|record| {
            let mut metadata = EventMetadata::new(&batch_id, source, tenant_id);
            metadata.integration_id = Some(integration_normalized.clone());
            metadata.intake_ts = now.into();
            Envelope::new(serde_json::to_vec(record).unwrap_or_default(), metadata)
        })
        .collect();

    let write_result = write_raw_batch(
        store.as_ref(),
        queue.as_ref(),
        RawBatchWriteInput {
            descriptor: BatchDescriptor {
                batch_id: batch_id.clone(),
                tenant_id: tenant_id.into(),
                source: source.into(),
                integration_id: Some(integration_id.into()),
                received_at: now,
            },
            intake_kind: "collector_pull".into(),
            producer_run_id: producer_run_id.into(),
            metadata_json: serde_json::json!({"collector_kind": collector_kind}).to_string(),
            idempotency_key: idempotency_key.into(),
            payload_sha256: payload_sha256.clone(),
            checkpoint_extra: checkpoint_payload.clone(),
            envelopes,
        },
    )
    .await?;

    accepted_batches
        .register(&AcceptedBatch {
            batch_id: batch_id.clone(),
            tenant_id: tenant_id.into(),
            source: source.into(),
            integration_id: integration_normalized,
            record_count: write_result.record_count,
            dead_letter_count: 0,
            manifest_key: write_result.manifest_key.clone(),
            object_key: write_result.object_key.clone(),
            idempotency_key: idempotency_key.into(),
            payload_sha256,
            dead_letter_object_key: None,
        })
        .await?;

    Ok(SubmitResult {
        batch_id,
        manifest_key: write_result.manifest_key,
        object_key: write_result.object_key,
        idempotency_key: idempotency_key.into(),
        record_count: write_result.record_count,
        duplicate: false,
    })
}
