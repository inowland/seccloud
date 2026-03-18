use async_trait::async_trait;

/// A page of records returned by a collector poll.
#[derive(Debug, Clone)]
pub struct CollectorPage {
    /// Raw event records (JSON values).
    pub records: Vec<serde_json::Value>,
    /// Updated checkpoint after this page.
    pub next_checkpoint: Checkpoint,
    /// Whether more pages are available.
    pub has_more: bool,
    /// Collector-specific metadata for the manifest.
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// Opaque cursor for tracking collection position.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    /// Cursor value — interpretation depends on the collector.
    pub cursor: serde_json::Value,
    /// Optimistic concurrency version.
    pub version: u64,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            cursor: serde_json::Value::Null,
            version: 0,
        }
    }
}

/// Trait implemented by all pull-based collectors.
#[async_trait]
pub trait Collector: Send + Sync {
    /// Collector kind identifier (e.g. "okta_system_log_fixture").
    fn kind(&self) -> &str;

    /// Source system (e.g. "okta", "github").
    fn source(&self) -> &str;

    /// Integration instance ID (e.g. "okta-primary").
    fn integration_id(&self) -> &str;

    /// Poll the source for a page of events starting from the given checkpoint.
    async fn poll(
        &self,
        checkpoint: &Checkpoint,
        limit: Option<usize>,
    ) -> anyhow::Result<CollectorPage>;
}
