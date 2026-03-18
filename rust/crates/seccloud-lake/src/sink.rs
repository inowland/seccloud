use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use seccloud_pipeline::context::Context;
use seccloud_pipeline::envelope::Envelope;
use seccloud_pipeline::error::TransformError;
use seccloud_pipeline::transform::Transform;

use crate::batch_writer::{
    BatchDescriptor, RawBatchWriteInput, write_dead_letter_batch, write_raw_batch,
};
use crate::intake_queue::IntakeQueue;
use crate::object_store::ObjectStore;
/// Configuration for the S3 Parquet sink.
pub struct S3ParquetSinkConfig {
    pub intake_kind: String,
    pub producer_run_id: String,
}

/// Sink transform that writes envelopes as Parquet to S3 and records manifests.
///
/// Processes a batch of envelopes (typically from a Buffer flush), serializes
/// them to Parquet, uploads to S3, writes a manifest, and submits to the intake queue.
///
/// Non-dead-lettered envelopes are written to the raw prefix.
/// Dead-lettered envelopes pass through unchanged (for [`DeadLetterSink`] downstream).
pub struct S3ParquetSink {
    store: Arc<dyn ObjectStore>,
    queue: Arc<dyn IntakeQueue>,
    config: S3ParquetSinkConfig,
}

impl S3ParquetSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        queue: Arc<dyn IntakeQueue>,
        config: S3ParquetSinkConfig,
    ) -> Self {
        Self {
            store,
            queue,
            config,
        }
    }
}

#[async_trait]
impl Transform for S3ParquetSink {
    fn name(&self) -> &str {
        "S3ParquetSink"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        // Dead-lettered or control envelopes pass through
        if envelope.is_dead_letter() || envelope.is_control() {
            return Ok(vec![envelope]);
        }
        let received_at: DateTime<Utc> = envelope.metadata.intake_ts.into();
        let metadata_json =
            serde_json::to_string(&envelope.metadata.attributes).unwrap_or_else(|_| "{}".into());
        let result = write_raw_batch(
            self.store.as_ref(),
            self.queue.as_ref(),
            RawBatchWriteInput {
                descriptor: BatchDescriptor {
                    batch_id: envelope.metadata.batch_id.clone(),
                    tenant_id: envelope.metadata.tenant_id.clone(),
                    source: envelope.metadata.source.clone(),
                    integration_id: envelope.metadata.integration_id.clone(),
                    received_at,
                },
                intake_kind: self.config.intake_kind.clone(),
                producer_run_id: self.config.producer_run_id.clone(),
                metadata_json,
                idempotency_key: envelope
                    .metadata
                    .attributes
                    .get("idempotency_key")
                    .cloned()
                    .unwrap_or_default(),
                payload_sha256: envelope
                    .metadata
                    .attributes
                    .get("payload_sha256")
                    .cloned()
                    .unwrap_or_default(),
                checkpoint_extra: serde_json::Map::new(),
                envelopes: vec![envelope],
            },
        )
        .await
        .map_err(TransformError::transient)?;

        tracing::debug!(
            batch_id = %result.manifest_key,
            object_key = %result.object_key,
            record_count = result.record_count,
            "wrote raw batch to S3"
        );

        // Sink consumes the envelope (does not pass it downstream)
        Ok(vec![])
    }
}

/// Sink transform that writes dead-lettered envelopes as Parquet to the dead-letter prefix.
pub struct DeadLetterSink {
    store: Arc<dyn ObjectStore>,
    intake_kind: String,
}

impl DeadLetterSink {
    pub fn new(store: Arc<dyn ObjectStore>, intake_kind: &str) -> Self {
        Self {
            store,
            intake_kind: intake_kind.into(),
        }
    }
}

#[async_trait]
impl Transform for DeadLetterSink {
    fn name(&self) -> &str {
        "DeadLetterSink"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        // Only process dead-lettered envelopes
        if !envelope.is_dead_letter() {
            return Ok(vec![envelope]);
        }

        // Control envelopes pass through
        if envelope.is_control() {
            return Ok(vec![envelope]);
        }

        let received_at: DateTime<Utc> = envelope.metadata.intake_ts.into();
        let metadata_json =
            serde_json::to_string(&envelope.metadata.attributes).unwrap_or_else(|_| "{}".into());
        let object_key = write_dead_letter_batch(
            self.store.as_ref(),
            &BatchDescriptor {
                batch_id: envelope.metadata.batch_id.clone(),
                tenant_id: envelope.metadata.tenant_id.clone(),
                source: envelope.metadata.source.clone(),
                integration_id: envelope.metadata.integration_id.clone(),
                received_at,
            },
            &self.intake_kind,
            &metadata_json,
            std::slice::from_ref(&envelope),
        )
        .await
        .map_err(TransformError::transient)?
        .unwrap_or_default();

        tracing::debug!(
            batch_id = %envelope.metadata.batch_id,
            reason = envelope.metadata.dead_letter_reason.as_deref().unwrap_or("unknown"),
            object_key = %object_key,
            "wrote dead-letter batch to S3"
        );

        // Consumed — remove from stream
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intake_queue::InMemoryIntakeQueue;
    use crate::object_store::InMemoryObjectStore;
    use crate::schema::{read_parquet_bytes, record_batch_to_rows};
    use seccloud_pipeline::testing::{test_context, test_envelope};

    fn make_sink() -> (
        S3ParquetSink,
        Arc<InMemoryObjectStore>,
        Arc<InMemoryIntakeQueue>,
    ) {
        let store = Arc::new(InMemoryObjectStore::new());
        let queue = Arc::new(InMemoryIntakeQueue::new());
        let sink = S3ParquetSink::new(
            store.clone(),
            queue.clone(),
            S3ParquetSinkConfig {
                intake_kind: "push_gateway".into(),
                producer_run_id: "test-run".into(),
            },
        );
        (sink, store, queue)
    }

    #[tokio::test]
    async fn writes_parquet_and_manifest() {
        let (mut sink, store, queue) = make_sink();
        let ctx = test_context();

        let env =
            test_envelope(br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z"}"#);

        let result = sink.process(env, &ctx).await.unwrap();
        // Sink consumes the envelope
        assert!(result.is_empty());

        let keys = store.keys();
        // Should have a parquet file and a manifest
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k.ends_with(".parquet")));
        assert!(keys.iter().any(|k| k.ends_with(".json")));

        // Queue should have an entry
        assert_eq!(queue.pending_count("test-tenant"), 1);
    }

    #[tokio::test]
    async fn parquet_content_is_valid() {
        let (mut sink, store, _queue) = make_sink();
        let ctx = test_context();

        let env = test_envelope(
            br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z","event_type":"login"}"#,
        );

        sink.process(env, &ctx).await.unwrap();

        // Find and read the parquet file
        let parquet_key = store
            .keys()
            .into_iter()
            .find(|k| k.ends_with(".parquet"))
            .unwrap();
        let parquet_bytes = store.get_bytes(&parquet_key).await.unwrap().unwrap();

        let batches = read_parquet_bytes(&parquet_bytes).unwrap();
        let rows = record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source, "test-source");
        assert_eq!(rows[0].intake_kind, "push_gateway");

        let record: serde_json::Value = serde_json::from_str(&rows[0].record_json).unwrap();
        assert_eq!(record["source_event_id"], "e1");
        assert_eq!(record["event_type"], "login");
    }

    #[tokio::test]
    async fn manifest_content_is_valid() {
        let (mut sink, store, _queue) = make_sink();
        let ctx = test_context();

        let env = test_envelope(b"{}");
        sink.process(env, &ctx).await.unwrap();

        let manifest_key = store
            .keys()
            .into_iter()
            .find(|k| k.ends_with(".json"))
            .unwrap();
        let manifest_json = store.get_json(&manifest_key).await.unwrap().unwrap();

        assert_eq!(manifest_json["manifest_version"], 1);
        assert_eq!(manifest_json["manifest_type"], "raw");
        assert_eq!(manifest_json["layout_version"], 1);
        assert_eq!(manifest_json["record_count"], 1);
        assert_eq!(manifest_json["objects"][0]["object_format"], "parquet");
    }

    #[tokio::test]
    async fn dead_letter_passes_through_raw_sink() {
        let (mut sink, store, _queue) = make_sink();
        let ctx = test_context();

        let mut env = test_envelope(b"bad");
        env.mark_dead_letter("invalid");

        let result = sink.process(env, &ctx).await.unwrap();
        // Dead-lettered envelope passes through
        assert_eq!(result.len(), 1);
        assert!(result[0].is_dead_letter());
        // Nothing written to store
        assert!(store.keys().is_empty());
    }

    #[tokio::test]
    async fn dead_letter_sink_writes_dead_letters() {
        let store = Arc::new(InMemoryObjectStore::new());
        let mut dl_sink = DeadLetterSink::new(store.clone(), "push_gateway");
        let ctx = test_context();

        let mut env =
            test_envelope(br#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z"}"#);
        env.mark_dead_letter("bad schema");

        let result = dl_sink.process(env, &ctx).await.unwrap();
        // Dead-letter sink consumes the envelope
        assert!(result.is_empty());

        let keys = store.keys();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains("dead-letter"));
        assert!(keys[0].ends_with(".parquet"));

        // Verify dead_letter_reason column
        let bytes = store.get_bytes(&keys[0]).await.unwrap().unwrap();
        let batches = crate::schema::read_parquet_bytes(&bytes).unwrap();
        let reason_col = batches[0]
            .column_by_name("dead_letter_reason")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(reason_col.value(0), "bad schema");
    }

    #[tokio::test]
    async fn dead_letter_sink_passes_healthy_envelopes() {
        let store = Arc::new(InMemoryObjectStore::new());
        let mut dl_sink = DeadLetterSink::new(store.clone(), "push_gateway");
        let ctx = test_context();

        let env = test_envelope(b"good");
        let result = dl_sink.process(env, &ctx).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0].is_dead_letter());
        assert!(store.keys().is_empty());
    }
}
