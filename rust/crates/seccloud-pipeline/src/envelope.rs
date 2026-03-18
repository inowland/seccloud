use std::collections::HashMap;
use std::time::SystemTime;

use bytes::Bytes;

/// A signal injected into the pipeline to trigger out-of-band behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlSignal {
    /// Drain any buffered envelopes.
    Flush,
    /// A batch has been fully ingested.
    BatchComplete { batch_id: String },
    /// Graceful shutdown — flush and stop.
    Shutdown,
}

/// Metadata carried alongside every envelope through the pipeline.
#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub batch_id: String,
    pub source: String,
    pub tenant_id: String,
    pub integration_id: Option<String>,
    pub intake_ts: SystemTime,
    pub attempt_count: u32,
    pub dead_letter_reason: Option<String>,
    pub lineage: Vec<String>,
    pub attributes: HashMap<String, String>,
}

impl EventMetadata {
    pub fn new(
        batch_id: impl Into<String>,
        source: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Self {
        Self {
            batch_id: batch_id.into(),
            source: source.into(),
            tenant_id: tenant_id.into(),
            integration_id: None,
            intake_ts: SystemTime::now(),
            attempt_count: 0,
            dead_letter_reason: None,
            lineage: Vec::new(),
            attributes: HashMap::new(),
        }
    }

    /// Returns `true` if this envelope has been marked as dead-lettered.
    pub fn is_dead_letter(&self) -> bool {
        self.dead_letter_reason.is_some()
    }
}

/// The unit of work flowing through a pipeline.
///
/// An envelope carries an immutable payload together with mutable metadata.
/// It may optionally carry a [`ControlSignal`] instead of (or in addition to) payload data.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub payload: Bytes,
    pub metadata: EventMetadata,
    pub control: Option<ControlSignal>,
}

impl Envelope {
    /// Create a data envelope.
    pub fn new(payload: impl Into<Bytes>, metadata: EventMetadata) -> Self {
        Self {
            payload: payload.into(),
            metadata,
            control: None,
        }
    }

    /// Create a control-only envelope (empty payload).
    pub fn control(signal: ControlSignal, metadata: EventMetadata) -> Self {
        Self {
            payload: Bytes::new(),
            metadata,
            control: Some(signal),
        }
    }

    /// Returns `true` if this envelope carries a control signal.
    pub fn is_control(&self) -> bool {
        self.control.is_some()
    }

    /// Returns `true` if this envelope has been dead-lettered.
    pub fn is_dead_letter(&self) -> bool {
        self.metadata.is_dead_letter()
    }

    /// Mark this envelope as dead-lettered with the given reason.
    pub fn mark_dead_letter(&mut self, reason: impl Into<String>) {
        self.metadata.dead_letter_reason = Some(reason.into());
    }

    /// Append a lineage entry (e.g. transform name that processed this envelope).
    pub fn record_lineage(&mut self, step: impl Into<String>) {
        self.metadata.lineage.push(step.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_envelope_basics() {
        let meta = EventMetadata::new("batch-1", "okta", "tenant-abc");
        let env = Envelope::new(b"hello".as_slice(), meta);

        assert_eq!(env.payload, Bytes::from_static(b"hello"));
        assert!(!env.is_control());
        assert!(!env.is_dead_letter());
        assert_eq!(env.metadata.batch_id, "batch-1");
        assert_eq!(env.metadata.source, "okta");
        assert_eq!(env.metadata.tenant_id, "tenant-abc");
    }

    #[test]
    fn control_envelope() {
        let meta = EventMetadata::new("b", "s", "t");
        let env = Envelope::control(ControlSignal::Flush, meta);

        assert!(env.is_control());
        assert_eq!(env.control, Some(ControlSignal::Flush));
        assert!(env.payload.is_empty());
    }

    #[test]
    fn dead_letter_marking() {
        let meta = EventMetadata::new("b", "s", "t");
        let mut env = Envelope::new(b"data".as_slice(), meta);

        assert!(!env.is_dead_letter());
        env.mark_dead_letter("bad schema");
        assert!(env.is_dead_letter());
        assert_eq!(
            env.metadata.dead_letter_reason.as_deref(),
            Some("bad schema")
        );
    }

    #[test]
    fn lineage_tracking() {
        let meta = EventMetadata::new("b", "s", "t");
        let mut env = Envelope::new(b"x".as_slice(), meta);

        env.record_lineage("filter");
        env.record_lineage("map");
        assert_eq!(env.metadata.lineage, vec!["filter", "map"]);
    }

    #[test]
    fn batch_complete_signal() {
        let signal = ControlSignal::BatchComplete {
            batch_id: "b-99".into(),
        };
        let meta = EventMetadata::new("b-99", "src", "t");
        let env = Envelope::control(signal.clone(), meta);

        assert_eq!(
            env.control,
            Some(ControlSignal::BatchComplete {
                batch_id: "b-99".into()
            })
        );
    }
}
