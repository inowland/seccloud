use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::{ControlSignal, Envelope};
use crate::error::TransformError;
use crate::transform::Transform;

/// Configuration for when the buffer should flush.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Flush when envelope count reaches this threshold.
    pub max_count: usize,
    /// Flush when total buffered bytes reaches this threshold.
    pub max_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_count: 1000,
            max_bytes: 5 * 1024 * 1024, // 5 MB
        }
    }
}

/// Buffers envelopes and flushes when a threshold is met or a control signal arrives.
///
/// Flush triggers:
/// - Envelope count reaches `max_count`
/// - Total buffered payload bytes reaches `max_bytes`
/// - `ControlSignal::Flush` or `ControlSignal::Shutdown` received
///
/// Non-flush control envelopes (e.g. `BatchComplete`) are passed through without
/// triggering a drain.
pub struct Buffer {
    config: BufferConfig,
    pending: Vec<Envelope>,
    pending_bytes: usize,
}

impl Buffer {
    pub fn new(config: BufferConfig) -> Self {
        Self {
            config,
            pending: Vec::new(),
            pending_bytes: 0,
        }
    }

    /// Returns the number of currently buffered envelopes.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn should_flush(&self) -> bool {
        self.pending.len() >= self.config.max_count || self.pending_bytes >= self.config.max_bytes
    }

    fn drain(&mut self) -> Vec<Envelope> {
        self.pending_bytes = 0;
        std::mem::take(&mut self.pending)
    }
}

#[async_trait]
impl Transform for Buffer {
    fn name(&self) -> &str {
        "Buffer"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        _ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        // Control signals that trigger flush
        if let Some(ref signal) = envelope.control {
            match signal {
                ControlSignal::Flush | ControlSignal::Shutdown => {
                    let mut flushed = self.drain();
                    // Pass the control envelope through as well
                    flushed.push(envelope);
                    return Ok(flushed);
                }
                ControlSignal::BatchComplete { .. } => {
                    // Pass through without flushing
                    return Ok(vec![envelope]);
                }
            }
        }

        // Buffer the data envelope
        self.pending_bytes += envelope.payload.len();
        self.pending.push(envelope);

        if self.should_flush() {
            Ok(self.drain())
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::envelope::EventMetadata;
    use crate::testing::*;

    fn small_buffer() -> Buffer {
        Buffer::new(BufferConfig {
            max_count: 3,
            max_bytes: 1024,
        })
    }

    #[tokio::test]
    async fn buffers_until_count_threshold() {
        let mut buf = small_buffer();
        let ctx = test_context();

        // First two envelopes are buffered
        let r1 = buf.process(test_envelope(b"a"), &ctx).await.unwrap();
        assert!(r1.is_empty());
        let r2 = buf.process(test_envelope(b"b"), &ctx).await.unwrap();
        assert!(r2.is_empty());

        // Third envelope triggers flush
        let r3 = buf.process(test_envelope(b"c"), &ctx).await.unwrap();
        assert_eq!(r3.len(), 3);
        assert_eq!(r3[0].payload, Bytes::from_static(b"a"));
        assert_eq!(r3[1].payload, Bytes::from_static(b"b"));
        assert_eq!(r3[2].payload, Bytes::from_static(b"c"));
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn flushes_on_bytes_threshold() {
        let mut buf = Buffer::new(BufferConfig {
            max_count: 1000,
            max_bytes: 10,
        });
        let ctx = test_context();

        let r1 = buf.process(test_envelope(b"12345"), &ctx).await.unwrap();
        assert!(r1.is_empty());

        // This pushes us to 10 bytes
        let r2 = buf.process(test_envelope(b"67890"), &ctx).await.unwrap();
        assert_eq!(r2.len(), 2);
    }

    #[tokio::test]
    async fn flush_signal_drains_buffer() {
        let mut buf = small_buffer();
        let ctx = test_context();

        buf.process(test_envelope(b"a"), &ctx).await.unwrap();
        buf.process(test_envelope(b"b"), &ctx).await.unwrap();
        assert_eq!(buf.len(), 2);

        // Send flush signal
        let meta = EventMetadata::new("b", "s", "t");
        let flush = Envelope::control(ControlSignal::Flush, meta);
        let result = buf.process(flush, &ctx).await.unwrap();

        // 2 buffered + 1 control envelope
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].payload, Bytes::from_static(b"a"));
        assert_eq!(result[1].payload, Bytes::from_static(b"b"));
        assert!(result[2].is_control());
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn shutdown_signal_drains_buffer() {
        let mut buf = small_buffer();
        let ctx = test_context();

        buf.process(test_envelope(b"x"), &ctx).await.unwrap();

        let meta = EventMetadata::new("b", "s", "t");
        let shutdown = Envelope::control(ControlSignal::Shutdown, meta);
        let result = buf.process(shutdown, &ctx).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(!result[0].is_control());
        assert!(result[1].is_control());
    }

    #[tokio::test]
    async fn batch_complete_passes_through_without_flush() {
        let mut buf = small_buffer();
        let ctx = test_context();

        buf.process(test_envelope(b"a"), &ctx).await.unwrap();

        let meta = EventMetadata::new("b", "s", "t");
        let bc = Envelope::control(
            ControlSignal::BatchComplete {
                batch_id: "b1".into(),
            },
            meta,
        );
        let result = buf.process(bc, &ctx).await.unwrap();

        // BatchComplete passes through, buffer still has 1 envelope
        assert_eq!(result.len(), 1);
        assert!(result[0].is_control());
        assert_eq!(buf.len(), 1);
    }

    #[tokio::test]
    async fn empty_buffer_flush_returns_only_control() {
        let mut buf = small_buffer();
        let ctx = test_context();

        let meta = EventMetadata::new("b", "s", "t");
        let flush = Envelope::control(ControlSignal::Flush, meta);
        let result = buf.process(flush, &ctx).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_control());
    }
}
