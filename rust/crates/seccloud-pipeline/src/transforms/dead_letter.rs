use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;
use crate::transform::Transform;

/// Terminal transform that captures dead-lettered envelopes and removes them
/// from the output stream. Non-dead-lettered envelopes pass through.
pub struct DeadLetterRouter {
    collected: Arc<Mutex<Vec<Envelope>>>,
}

impl DeadLetterRouter {
    pub fn new() -> Self {
        Self {
            collected: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a handle to the collected dead-letter envelopes.
    pub fn collected(&self) -> Arc<Mutex<Vec<Envelope>>> {
        Arc::clone(&self.collected)
    }
}

impl Default for DeadLetterRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transform for DeadLetterRouter {
    fn name(&self) -> &str {
        "DeadLetterRouter"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        if envelope.is_dead_letter() {
            tracing::debug!(
                reason = envelope
                    .metadata
                    .dead_letter_reason
                    .as_deref()
                    .unwrap_or("unknown"),
                "captured dead-letter envelope"
            );
            self.collected.lock().unwrap().push(envelope);
            Ok(vec![])
        } else {
            Ok(vec![envelope])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;

    #[tokio::test]
    async fn collects_dead_letters() {
        let mut dlr = DeadLetterRouter::new();
        let collected = dlr.collected();
        let ctx = test_context();

        let mut env = test_envelope(b"bad");
        env.mark_dead_letter("schema error");

        let result = dlr.process(env, &ctx).await.unwrap();
        assert!(result.is_empty());
        assert_eq!(collected.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn passes_through_healthy_envelopes() {
        let mut dlr = DeadLetterRouter::new();
        let collected = dlr.collected();
        let ctx = test_context();

        let env = test_envelope(b"good");
        let result = dlr.process(env, &ctx).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(collected.lock().unwrap().len(), 0);
    }
}
