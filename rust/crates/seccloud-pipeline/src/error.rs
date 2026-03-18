use std::fmt;

/// Errors returned by [`Transform::process`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    /// Envelope failed validation — mark as dead-letter, continue chain.
    #[error("validation failed: {reason}")]
    ValidationFailed { reason: String },

    /// Transient failure (network, timeout) — retry up to max_attempts.
    #[error("transient error: {source}")]
    Transient { source: anyhow::Error },

    /// Unrecoverable — halt the chain immediately.
    #[error("fatal error: {source}")]
    Fatal { source: anyhow::Error },
}

/// Top-level pipeline error (chain execution, builder, etc.).
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("chain halted by fatal transform error: {source}")]
    FatalHalt { source: TransformError },

    #[error("chain build error: {reason}")]
    BuildError { reason: String },
}

impl TransformError {
    pub fn validation(reason: impl fmt::Display) -> Self {
        Self::ValidationFailed {
            reason: reason.to_string(),
        }
    }

    pub fn transient(source: impl Into<anyhow::Error>) -> Self {
        Self::Transient {
            source: source.into(),
        }
    }

    pub fn fatal(source: impl Into<anyhow::Error>) -> Self {
        Self::Fatal {
            source: source.into(),
        }
    }
}
