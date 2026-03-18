use tracing::{debug, warn};

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::{PipelineError, TransformError};
use crate::transform::Transform;

/// Default maximum retry attempts for transient errors before dead-lettering.
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// A linear chain of transforms that processes envelopes sequentially.
///
/// Fan-out: if transform N returns 3 envelopes, transform N+1 processes all 3.
/// Dead-lettered envelopes continue through the chain so a terminal
/// [`DeadLetterRouter`](crate::transforms::dead_letter::DeadLetterRouter) can capture them.
/// Control envelopes flow through all transforms.
pub struct Chain {
    transforms: Vec<Box<dyn Transform>>,
    max_attempts: u32,
}

impl Chain {
    pub fn builder() -> ChainBuilder {
        ChainBuilder::new()
    }

    /// Process a batch of envelopes through the chain.
    pub async fn process(
        &mut self,
        envelopes: Vec<Envelope>,
        ctx: &Context,
    ) -> Result<Vec<Envelope>, PipelineError> {
        let mut current = envelopes;

        for transform in &mut self.transforms {
            let mut next = Vec::with_capacity(current.len());

            for envelope in current {
                match transform.process(envelope.clone(), ctx).await {
                    Ok(mut results) => {
                        for r in &mut results {
                            r.record_lineage(transform.name());
                        }
                        next.extend(results);
                    }
                    Err(TransformError::ValidationFailed { reason }) => {
                        debug!(
                            transform = transform.name(),
                            reason = %reason,
                            "validation failed — dead-lettering envelope"
                        );
                        let mut env = envelope;
                        env.mark_dead_letter(reason);
                        env.record_lineage(transform.name());
                        next.push(env);
                    }
                    Err(TransformError::Transient { source }) => {
                        let mut env = envelope;
                        env.metadata.attempt_count += 1;
                        if env.metadata.attempt_count >= self.max_attempts {
                            warn!(
                                transform = transform.name(),
                                attempts = env.metadata.attempt_count,
                                "max attempts reached — dead-lettering envelope"
                            );
                            env.mark_dead_letter(format!(
                                "transient error after {} attempts: {source}",
                                env.metadata.attempt_count
                            ));
                            env.record_lineage(transform.name());
                            next.push(env);
                        } else {
                            debug!(
                                transform = transform.name(),
                                attempt = env.metadata.attempt_count,
                                "transient error — dead-lettering (no in-chain retry)"
                            );
                            env.mark_dead_letter(format!("transient error: {source}"));
                            env.record_lineage(transform.name());
                            next.push(env);
                        }
                    }
                    Err(e @ TransformError::Fatal { .. }) => {
                        return Err(PipelineError::FatalHalt { source: e });
                    }
                }
            }

            current = next;
        }

        Ok(current)
    }
}

/// Builder for constructing a [`Chain`].
pub struct ChainBuilder {
    transforms: Vec<Box<dyn Transform>>,
    max_attempts: u32,
}

impl ChainBuilder {
    fn new() -> Self {
        Self {
            transforms: Vec::new(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Append a transform to the chain.
    pub fn transform(mut self, t: impl Transform + 'static) -> Self {
        self.transforms.push(Box::new(t));
        self
    }

    /// Set the maximum number of attempts before a transient error causes dead-lettering.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Build the chain.
    pub fn build(self) -> Result<Chain, PipelineError> {
        if self.transforms.is_empty() {
            return Err(PipelineError::BuildError {
                reason: "chain must have at least one transform".into(),
            });
        }
        Ok(Chain {
            transforms: self.transforms,
            max_attempts: self.max_attempts,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::envelope::ControlSignal;
    use crate::testing::*;
    use crate::transforms::dead_letter::DeadLetterRouter;
    use crate::transforms::passthrough::PassThrough;

    #[tokio::test]
    async fn passthrough_chain() {
        let mut chain = Chain::builder().transform(PassThrough).build().unwrap();

        let env = test_envelope(b"hello");
        let result = chain.process(vec![env], &test_context()).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload, Bytes::from_static(b"hello"));
        assert_eq!(result[0].metadata.lineage, vec!["PassThrough"]);
    }

    #[tokio::test]
    async fn fan_out_multiplies_downstream() {
        let recorder = RecordingTransform::new();
        let recorder_clone = recorder.clone();

        let mut chain = Chain::builder()
            .transform(FanOutTestTransform { copies: 3 })
            .transform(recorder)
            .build()
            .unwrap();

        let env = test_envelope(b"x");
        let result = chain.process(vec![env], &test_context()).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(recorder_clone.count(), 3);
        // Each copy got a suffix
        let payloads = recorder_clone.payloads();
        assert_eq!(payloads[0], Bytes::from("x-0"));
        assert_eq!(payloads[1], Bytes::from("x-1"));
        assert_eq!(payloads[2], Bytes::from("x-2"));
    }

    #[tokio::test]
    async fn validation_error_dead_letters_and_continues() {
        let recorder = RecordingTransform::new();
        let recorder_clone = recorder.clone();

        let mut chain = Chain::builder()
            .transform(FailingTransform::validation("bad data"))
            .transform(recorder) // should still see the envelope (dead-lettered)
            .build()
            .unwrap();

        let env = test_envelope(b"data");
        let result = chain.process(vec![env], &test_context()).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_dead_letter());
        assert_eq!(
            result[0].metadata.dead_letter_reason.as_deref(),
            Some("bad data")
        );
        // Dead-lettered envelope still flows through remaining transforms
        assert_eq!(recorder_clone.count(), 1);
    }

    #[tokio::test]
    async fn transient_error_dead_letters_after_max_attempts() {
        let mut chain = Chain::builder()
            .transform(FailingTransform::transient("timeout"))
            .max_attempts(2)
            .build()
            .unwrap();

        // Simulate an envelope that has already been attempted once
        let mut env = test_envelope(b"retry-me");
        env.metadata.attempt_count = 1;

        let result = chain.process(vec![env], &test_context()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_dead_letter());
        assert_eq!(result[0].metadata.attempt_count, 2);
    }

    #[tokio::test]
    async fn fatal_error_halts_chain() {
        let mut chain = Chain::builder()
            .transform(FailingTransform::fatal("disk full"))
            .transform(PassThrough)
            .build()
            .unwrap();

        let env = test_envelope(b"data");
        let result = chain.process(vec![env], &test_context()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            PipelineError::FatalHalt { source } => {
                assert!(source.to_string().contains("disk full"));
            }
            other => panic!("expected FatalHalt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn control_envelope_flows_through() {
        let recorder = RecordingTransform::new();
        let recorder_clone = recorder.clone();

        let mut chain = Chain::builder().transform(recorder).build().unwrap();

        let meta = crate::envelope::EventMetadata::new("b", "s", "t");
        let env = Envelope::control(ControlSignal::Flush, meta);

        let result = chain.process(vec![env], &test_context()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_control());
        assert_eq!(recorder_clone.count(), 1);
    }

    #[tokio::test]
    async fn dead_letter_router_collects() {
        let dlr = DeadLetterRouter::new();
        let collected = dlr.collected();

        let mut chain = Chain::builder()
            .transform(FailingTransform::validation("nope"))
            .transform(dlr)
            .build()
            .unwrap();

        let env = test_envelope(b"bad");
        let result = chain.process(vec![env], &test_context()).await.unwrap();

        // DeadLetterRouter removes dead-lettered envelopes from the output
        assert!(result.is_empty());
        assert_eq!(collected.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn mixed_batch_some_fail_some_pass() {
        let dlr = DeadLetterRouter::new();
        let collected = dlr.collected();

        // Transform that fails envelopes with payload "bad"
        let mut chain = Chain::builder()
            .transform(crate::transforms::filter::Filter::new(
                |env: &Envelope, _ctx: &Context| {
                    if env.payload == Bytes::from_static(b"bad") {
                        Err(TransformError::validation("is bad"))
                    } else {
                        Ok(true)
                    }
                },
            ))
            .transform(dlr)
            .build()
            .unwrap();

        let envs = vec![
            test_envelope(b"good"),
            test_envelope(b"bad"),
            test_envelope(b"also-good"),
        ];

        let result = chain.process(envs, &test_context()).await.unwrap();

        // 2 good envelopes pass through, 1 bad one collected by DLR
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].payload, Bytes::from_static(b"good"));
        assert_eq!(result[1].payload, Bytes::from_static(b"also-good"));
        assert_eq!(collected.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_chain_fails_to_build() {
        let result = Chain::builder().build();
        assert!(result.is_err());
    }
}
