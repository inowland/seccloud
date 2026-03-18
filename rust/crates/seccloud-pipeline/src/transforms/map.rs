use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;
use crate::transform::Transform;

/// A mapping transform that applies a function to each envelope.
///
/// The map function receives the envelope and context and returns a
/// transformed envelope or an error.
pub struct Map<F>
where
    F: Fn(Envelope, &Context) -> Result<Envelope, TransformError> + Send + Sync,
{
    map_fn: F,
}

impl<F> Map<F>
where
    F: Fn(Envelope, &Context) -> Result<Envelope, TransformError> + Send + Sync,
{
    pub fn new(map_fn: F) -> Self {
        Self { map_fn }
    }
}

#[async_trait]
impl<F> Transform for Map<F>
where
    F: Fn(Envelope, &Context) -> Result<Envelope, TransformError> + Send + Sync,
{
    fn name(&self) -> &str {
        "Map"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        let result = (self.map_fn)(envelope, ctx)?;
        Ok(vec![result])
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::testing::*;

    #[tokio::test]
    async fn transforms_payload() {
        let mut m = Map::new(|mut env: Envelope, _ctx: &Context| {
            let upper = String::from_utf8_lossy(&env.payload).to_uppercase();
            env.payload = Bytes::from(upper);
            Ok(env)
        });

        let ctx = test_context();
        let result = m.process(test_envelope(b"hello"), &ctx).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload, Bytes::from("HELLO"));
    }

    #[tokio::test]
    async fn adds_metadata_attribute() {
        let mut m = Map::new(|mut env: Envelope, _ctx: &Context| {
            env.metadata
                .attributes
                .insert("processed".into(), "true".into());
            Ok(env)
        });

        let ctx = test_context();
        let result = m.process(test_envelope(b"x"), &ctx).await.unwrap();

        assert_eq!(
            result[0].metadata.attributes.get("processed"),
            Some(&"true".to_string())
        );
    }

    #[tokio::test]
    async fn map_error_propagates() {
        let mut m = Map::new(
            |_env: Envelope, _ctx: &Context| -> Result<Envelope, TransformError> {
                Err(TransformError::validation("bad format"))
            },
        );

        let ctx = test_context();
        let result = m.process(test_envelope(b"x"), &ctx).await;
        assert!(result.is_err());
    }
}
