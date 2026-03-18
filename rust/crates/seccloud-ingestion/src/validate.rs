use async_trait::async_trait;
use seccloud_pipeline::context::Context;
use seccloud_pipeline::envelope::Envelope;
use seccloud_pipeline::error::TransformError;
use seccloud_pipeline::transform::Transform;

/// Validate a single raw event record.
///
/// Required fields: `source_event_id` (non-empty string), `observed_at` (non-empty string).
pub fn validate_record(
    record: &serde_json::Value,
    allowed_sources: &[String],
    request_source: &str,
) -> Result<(), RecordValidationError> {
    // Check source_event_id
    match record.get("source_event_id") {
        Some(serde_json::Value::String(s)) if !s.is_empty() => {}
        _ => return Err(RecordValidationError::MissingField("source_event_id")),
    }

    // Check observed_at
    match record.get("observed_at") {
        Some(serde_json::Value::String(s)) if !s.is_empty() => {}
        _ => return Err(RecordValidationError::MissingField("observed_at")),
    }

    // Check nested source doesn't conflict
    if let Some(serde_json::Value::String(s)) = record.get("source") {
        if s != request_source {
            return Err(RecordValidationError::SourceMismatch {
                record_source: s.clone(),
                request_source: request_source.into(),
            });
        }
    }

    // Validate request source is in the allowlist
    if !allowed_sources.iter().any(|s| s == request_source) {
        return Err(RecordValidationError::UnsupportedSource(
            request_source.into(),
        ));
    }

    Ok(())
}

pub struct ValidateTransform {
    allowed_sources: Vec<String>,
    request_source: String,
}

impl ValidateTransform {
    pub fn new(allowed_sources: &[String], request_source: impl Into<String>) -> Self {
        Self {
            allowed_sources: allowed_sources.to_vec(),
            request_source: request_source.into(),
        }
    }
}

#[async_trait]
impl Transform for ValidateTransform {
    fn name(&self) -> &str {
        "ValidateTransform"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        if envelope.is_control() {
            return Ok(vec![envelope]);
        }

        let record: serde_json::Value = serde_json::from_slice(&envelope.payload)
            .map_err(|e| TransformError::validation(format!("invalid record JSON: {e}")))?;
        validate_record(&record, &self.allowed_sources, &self.request_source)
            .map_err(|e| TransformError::validation(e.to_string()))?;
        Ok(vec![envelope])
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordValidationError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("record source mismatch: record={record_source}, request={request_source}")]
    SourceMismatch {
        record_source: String,
        request_source: String,
    },
    #[error("unsupported source: {0}")]
    UnsupportedSource(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sources() -> Vec<String> {
        vec!["okta".into(), "github".into()]
    }

    #[test]
    fn valid_record() {
        let record = json!({
            "source_event_id": "e1",
            "observed_at": "2026-03-15T14:00:00Z",
            "event_type": "login"
        });
        validate_record(&record, &sources(), "okta").unwrap();
    }

    #[test]
    fn missing_source_event_id() {
        let record = json!({"observed_at": "2026-03-15T14:00:00Z"});
        let err = validate_record(&record, &sources(), "okta").unwrap_err();
        assert!(matches!(
            err,
            RecordValidationError::MissingField("source_event_id")
        ));
    }

    #[test]
    fn empty_source_event_id() {
        let record = json!({"source_event_id": "", "observed_at": "2026-03-15T14:00:00Z"});
        let err = validate_record(&record, &sources(), "okta").unwrap_err();
        assert!(matches!(
            err,
            RecordValidationError::MissingField("source_event_id")
        ));
    }

    #[test]
    fn missing_observed_at() {
        let record = json!({"source_event_id": "e1"});
        let err = validate_record(&record, &sources(), "okta").unwrap_err();
        assert!(matches!(
            err,
            RecordValidationError::MissingField("observed_at")
        ));
    }

    #[test]
    fn source_mismatch_in_record() {
        let record = json!({
            "source_event_id": "e1",
            "observed_at": "2026-03-15T14:00:00Z",
            "source": "github"
        });
        let err = validate_record(&record, &sources(), "okta").unwrap_err();
        assert!(matches!(err, RecordValidationError::SourceMismatch { .. }));
    }

    #[test]
    fn unsupported_source() {
        let record = json!({"source_event_id": "e1", "observed_at": "2026-03-15T14:00:00Z"});
        let err = validate_record(&record, &sources(), "slack").unwrap_err();
        assert!(matches!(err, RecordValidationError::UnsupportedSource(_)));
    }
}
