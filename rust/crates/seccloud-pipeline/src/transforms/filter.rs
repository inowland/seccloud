use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;
use crate::transform::Transform;

/// A predicate-based filter transform.
///
/// The predicate receives the envelope and context and returns:
/// - `Ok(true)` — keep the envelope
/// - `Ok(false)` — drop the envelope
/// - `Err(TransformError)` — propagate the error (e.g. validation failure → dead-letter)
pub struct Filter<F>
where
    F: Fn(&Envelope, &Context) -> Result<bool, TransformError> + Send + Sync,
{
    predicate: F,
}

impl<F> Filter<F>
where
    F: Fn(&Envelope, &Context) -> Result<bool, TransformError> + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

#[async_trait]
impl<F> Transform for Filter<F>
where
    F: Fn(&Envelope, &Context) -> Result<bool, TransformError> + Send + Sync,
{
    fn name(&self) -> &str {
        "Filter"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        if (self.predicate)(&envelope, ctx)? {
            Ok(vec![envelope])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::testing::*;

    #[tokio::test]
    async fn keeps_matching_envelopes() {
        let mut f = Filter::new(|env: &Envelope, _| Ok(env.payload.len() > 2));
        let ctx = test_context();

        let kept = f.process(test_envelope(b"hello"), &ctx).await.unwrap();
        assert_eq!(kept.len(), 1);

        let dropped = f.process(test_envelope(b"hi"), &ctx).await.unwrap();
        assert!(dropped.is_empty());
    }

    #[tokio::test]
    async fn predicate_error_propagates() {
        let mut f =
            Filter::new(|_env: &Envelope, _ctx: &Context| Err(TransformError::validation("nope")));
        let ctx = test_context();

        let result = f.process(test_envelope(b"x"), &ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn filter_by_metadata() {
        let mut f = Filter::new(|env: &Envelope, _| Ok(env.metadata.source == "okta"));
        let ctx = test_context();

        // test_envelope has source "test-source", should be dropped
        let dropped = f.process(test_envelope(b"x"), &ctx).await.unwrap();
        assert!(dropped.is_empty());

        // Custom envelope with source "okta"
        let meta = crate::envelope::EventMetadata::new("b", "okta", "t");
        let env = Envelope::new(Bytes::from_static(b"y"), meta);
        let kept = f.process(env, &ctx).await.unwrap();
        assert_eq!(kept.len(), 1);
    }
}
