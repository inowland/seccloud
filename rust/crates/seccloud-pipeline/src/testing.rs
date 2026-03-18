use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;

use crate::context::{Context, ResourceBag};
use crate::envelope::{Envelope, EventMetadata};
use crate::error::TransformError;
use crate::transform::Transform;

/// Create a test envelope with minimal metadata.
pub fn test_envelope(payload: &[u8]) -> Envelope {
    Envelope::new(
        Bytes::copy_from_slice(payload),
        EventMetadata::new("test-batch", "test-source", "test-tenant"),
    )
}

/// Create a test context with default values.
pub fn test_context() -> Context {
    Context::new("test-tenant")
}

/// A transform that records every envelope it sees (passes them through unchanged).
#[derive(Clone, Default)]
pub struct RecordingTransform {
    pub seen: Arc<Mutex<Vec<Bytes>>>,
}

impl RecordingTransform {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn payloads(&self) -> Vec<Bytes> {
        self.seen.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

#[async_trait]
impl Transform for RecordingTransform {
    fn name(&self) -> &str {
        "RecordingTransform"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        self.seen.lock().unwrap().push(envelope.payload.clone());
        Ok(vec![envelope])
    }
}

/// A transform that always fails with the specified error kind.
pub struct FailingTransform {
    error_fn: Box<dyn Fn() -> TransformError + Send + Sync>,
}

impl FailingTransform {
    pub fn validation(reason: &str) -> Self {
        let reason = reason.to_string();
        Self {
            error_fn: Box::new(move || TransformError::validation(&reason)),
        }
    }

    pub fn transient(msg: &str) -> Self {
        let msg = msg.to_string();
        Self {
            error_fn: Box::new(move || TransformError::transient(anyhow::anyhow!(msg.clone()))),
        }
    }

    pub fn fatal(msg: &str) -> Self {
        let msg = msg.to_string();
        Self {
            error_fn: Box::new(move || TransformError::fatal(anyhow::anyhow!(msg.clone()))),
        }
    }
}

#[async_trait]
impl Transform for FailingTransform {
    fn name(&self) -> &str {
        "FailingTransform"
    }

    async fn process(
        &mut self,
        _envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        Err((self.error_fn)())
    }
}

/// A transform that fans out each envelope into N copies with modified payloads.
pub struct FanOutTestTransform {
    pub copies: usize,
}

#[async_trait]
impl Transform for FanOutTestTransform {
    fn name(&self) -> &str {
        "FanOutTestTransform"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        let mut out = Vec::with_capacity(self.copies);
        for i in 0..self.copies {
            let mut e = envelope.clone();
            let new_payload = format!("{}-{}", String::from_utf8_lossy(&envelope.payload), i);
            e.payload = Bytes::from(new_payload);
            out.push(e);
        }
        Ok(out)
    }
}

/// Create a test context with the given resources.
pub fn test_context_with_resources(bag: ResourceBag) -> Context {
    Context {
        tenant_id: "test-tenant".into(),
        config: Default::default(),
        resources: Arc::new(bag),
    }
}
