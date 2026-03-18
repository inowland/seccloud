use std::io::Read;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use seccloud_lake::accepted_batch::{AcceptedBatch, AcceptedBatchStore};
use seccloud_lake::batch_writer::{
    BatchDescriptor, RawBatchWriteInput, write_dead_letter_batch, write_raw_batch,
};
use seccloud_lake::intake_queue::IntakeQueue;
use seccloud_lake::key;
use seccloud_lake::object_store::ObjectStore;
use seccloud_pipeline::Chain;
use seccloud_pipeline::context::Context;
use seccloud_pipeline::envelope::{ControlSignal, Envelope, EventMetadata};
use seccloud_pipeline::transforms::buffer::{Buffer, BufferConfig};

use crate::auth::AuthCredentials;
use crate::validate;

/// Request body for the intake endpoint (matches PoC `IntakeRequest`).
#[derive(Debug, serde::Deserialize)]
pub struct IntakeRequest {
    pub source: String,
    pub records: Vec<serde_json::Value>,
    #[serde(default = "default_intake_kind")]
    pub intake_kind: String,
    pub integration_id: Option<String>,
    pub received_at: Option<String>,
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

fn default_intake_kind() -> String {
    "push_gateway".into()
}

/// Response body for a successful intake (matches PoC `IntakeAccepted`).
#[derive(Debug, serde::Serialize)]
pub struct IntakeAccepted {
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub record_count: usize,
    pub dead_letter_count: usize,
    pub manifest_key: String,
    pub object_key: String,
    pub idempotency_key: String,
    pub duplicate: bool,
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)] // IdempotencyConflict, ValidationError used when idempotency check is wired
pub enum IngestError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("idempotency conflict: {0}")]
    IdempotencyConflict(String),
    #[error("validation error in record {index}: {reason}")]
    ValidationError { index: usize, reason: String },
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IngestError {
    pub fn status_code(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::IdempotencyConflict(_) => StatusCode::CONFLICT,
            Self::ValidationError { .. } => StatusCode::BAD_REQUEST,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Decompress a gzipped body, or return the body as-is if not gzipped.
pub fn maybe_decompress(
    body: &[u8],
    content_encoding: Option<&str>,
) -> Result<Vec<u8>, IngestError> {
    match content_encoding {
        Some("gzip") => {
            let mut decoder = flate2::read::GzDecoder::new(body);
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| IngestError::BadRequest(format!("invalid gzip: {e}")))?;
            Ok(decompressed)
        }
        Some(other) => Err(IngestError::BadRequest(format!(
            "unsupported content encoding: {other}"
        ))),
        None => Ok(body.to_vec()),
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("sha256:{hash:x}")
}

/// Parameters for the intake processing function.
pub struct IntakeParams<'a> {
    pub creds: &'a AuthCredentials,
    pub store: &'a Arc<dyn ObjectStore>,
    pub queue: &'a Arc<dyn IntakeQueue>,
    pub accepted_batches: &'a Arc<dyn AcceptedBatchStore>,
    pub allowed_sources: &'a [String],
    pub max_records: usize,
    pub buffer_max_bytes: usize,
    pub producer_run_id: &'a str,
    pub idempotency_key_header: Option<&'a str>,
}

/// Core ingestion logic: validate, write Parquet + manifest, submit to queue.
pub async fn process_intake(
    params: IntakeParams<'_>,
    request: IntakeRequest,
) -> Result<IntakeAccepted, IngestError> {
    let IntakeParams {
        creds,
        store,
        queue,
        accepted_batches,
        allowed_sources,
        max_records,
        buffer_max_bytes,
        producer_run_id,
        idempotency_key_header,
    } = params;
    // Validate record count
    if request.records.is_empty() {
        return Err(IngestError::BadRequest("records must not be empty".into()));
    }
    if request.records.len() > max_records {
        return Err(IngestError::PayloadTooLarge(format!(
            "record count {} exceeds limit {max_records}",
            request.records.len()
        )));
    }

    // Integration ID: request overrides token default
    let integration_id = request
        .integration_id
        .as_deref()
        .unwrap_or(&creds.integration_id);
    let integration_normalized = key::partition_value(Some(integration_id));

    let received_at_str = request
        .received_at
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
    let received_at: DateTime<Utc> = received_at_str
        .replace('Z', "+00:00")
        .parse::<DateTime<chrono::FixedOffset>>()
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    // Generate batch ID and idempotency key
    let batch_id = format!("raw_{}", uuid::Uuid::now_v7());
    let payload_sha256 = sha256_hex(
        serde_json::to_string(&request.records)
            .unwrap_or_default()
            .as_bytes(),
    );
    let idempotency_key = idempotency_key_header
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("push:{payload_sha256}"));

    let metadata = request.metadata.unwrap_or_default();
    let metadata_json = serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".into());
    if let Some(existing) = accepted_batches
        .lookup(&idempotency_key)
        .await
        .map_err(IngestError::Internal)?
    {
        if existing.payload_sha256 != payload_sha256 {
            return Err(IngestError::IdempotencyConflict(format!(
                "idempotency key already used for different payload: {idempotency_key}"
            )));
        }
        return Ok(IntakeAccepted {
            batch_id: existing.batch_id,
            tenant_id: existing.tenant_id,
            source: existing.source,
            integration_id: existing.integration_id,
            record_count: existing.record_count,
            dead_letter_count: existing.dead_letter_count,
            manifest_key: existing.manifest_key,
            object_key: existing.object_key,
            idempotency_key: existing.idempotency_key,
            duplicate: true,
        });
    }

    let intake_kind = request.intake_kind;
    let request_source = request.source;
    let mut envelopes = Vec::with_capacity(request.records.len() + 1);
    for record in &request.records {
        let mut event_metadata = EventMetadata::new(&batch_id, &request_source, &creds.tenant_id);
        event_metadata.integration_id = Some(integration_normalized.clone());
        event_metadata.intake_ts = received_at.into();
        envelopes.push(Envelope::new(
            serde_json::to_vec(record)
                .map_err(|e| IngestError::BadRequest(format!("failed to serialize record: {e}")))?,
            event_metadata,
        ));
    }
    let mut control_metadata = EventMetadata::new(&batch_id, &request_source, &creds.tenant_id);
    control_metadata.integration_id = Some(integration_normalized.clone());
    control_metadata.intake_ts = received_at.into();
    envelopes.push(Envelope::control(ControlSignal::Flush, control_metadata));

    let mut chain = Chain::builder()
        .transform(validate::ValidateTransform::new(
            allowed_sources,
            request_source.clone(),
        ))
        .transform(Buffer::new(BufferConfig {
            max_count: max_records,
            max_bytes: buffer_max_bytes,
        }))
        .build()
        .map_err(|error| IngestError::Internal(error.into()))?;
    let processed = chain
        .process(envelopes, &Context::new(&creds.tenant_id))
        .await
        .map_err(|error| IngestError::Internal(error.into()))?;

    let mut valid_envelopes = Vec::new();
    let mut dead_letter_envelopes = Vec::new();
    for envelope in processed {
        if envelope.is_control() {
            continue;
        }
        if envelope.is_dead_letter() {
            dead_letter_envelopes.push(envelope);
        } else {
            valid_envelopes.push(envelope);
        }
    }

    let descriptor = BatchDescriptor {
        batch_id: batch_id.clone(),
        tenant_id: creds.tenant_id.clone(),
        source: request_source.clone(),
        integration_id: Some(integration_id.into()),
        received_at,
    };
    let dead_letter_object_key = write_dead_letter_batch(
        store.as_ref(),
        &descriptor,
        &intake_kind,
        &metadata_json,
        &dead_letter_envelopes,
    )
    .await
    .map_err(IngestError::Internal)?;
    let write_result = write_raw_batch(
        store.as_ref(),
        queue.as_ref(),
        RawBatchWriteInput {
            descriptor,
            intake_kind: intake_kind.clone(),
            producer_run_id: producer_run_id.into(),
            metadata_json,
            idempotency_key: idempotency_key.clone(),
            payload_sha256: payload_sha256.clone(),
            checkpoint_extra: serde_json::Map::new(),
            envelopes: valid_envelopes,
        },
    )
    .await
    .map_err(IngestError::Internal)?;

    accepted_batches
        .register(&AcceptedBatch {
            batch_id: batch_id.clone(),
            tenant_id: creds.tenant_id.clone(),
            source: request_source.clone(),
            integration_id: integration_normalized.clone(),
            record_count: write_result.record_count,
            dead_letter_count: dead_letter_envelopes.len(),
            manifest_key: write_result.manifest_key.clone(),
            object_key: write_result.object_key.clone(),
            idempotency_key: idempotency_key.clone(),
            payload_sha256,
            dead_letter_object_key: dead_letter_object_key.clone(),
        })
        .await
        .map_err(IngestError::Internal)?;

    tracing::info!(
        batch_id = %batch_id,
        tenant_id = %creds.tenant_id,
        source = %request_source,
        record_count = write_result.record_count,
        dead_letter_count = dead_letter_envelopes.len(),
        "intake batch accepted"
    );

    Ok(IntakeAccepted {
        batch_id,
        tenant_id: creds.tenant_id.clone(),
        source: request_source,
        integration_id: integration_normalized,
        record_count: write_result.record_count,
        dead_letter_count: dead_letter_envelopes.len(),
        manifest_key: write_result.manifest_key,
        object_key: write_result.object_key,
        idempotency_key,
        duplicate: false,
    })
}
