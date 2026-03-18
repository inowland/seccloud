use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;
use crate::transform::Transform;

/// Identity transform — passes envelopes through unchanged.
pub struct PassThrough;

#[async_trait]
impl Transform for PassThrough {
    fn name(&self) -> &str {
        "PassThrough"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        Ok(vec![envelope])
    }
}
