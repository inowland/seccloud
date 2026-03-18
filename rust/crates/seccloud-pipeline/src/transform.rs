use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;

/// A composable, async processing step in a pipeline.
///
/// Transforms receive a single envelope and return zero or more envelopes.
/// Returning an empty vec drops the envelope. Returning multiple envelopes
/// creates a fan-out — downstream transforms see each one.
#[async_trait]
pub trait Transform: Send + Sync {
    /// Human-readable name used for lineage tracking and diagnostics.
    fn name(&self) -> &str;

    /// Process a single envelope, returning zero or more output envelopes.
    async fn process(
        &mut self,
        envelope: Envelope,
        ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError>;
}
