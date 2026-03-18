use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::manifest::IntakeQueueEntry;

/// Async intake queue trait — abstracts DynamoDB or local filesystem queue.
#[async_trait]
pub trait IntakeQueue: Send + Sync {
    /// Submit a new batch to the queue.
    async fn submit(&self, entry: &IntakeQueueEntry) -> anyhow::Result<()>;

    /// List all pending (unprocessed) batches for a tenant.
    async fn list_pending(&self, tenant_id: &str) -> anyhow::Result<Vec<IntakeQueueEntry>>;

    /// Mark a batch as processed.
    async fn mark_processed(&self, tenant_id: &str, batch_id: &str) -> anyhow::Result<()>;
}

/// In-memory intake queue for testing.
#[derive(Default)]
pub struct InMemoryIntakeQueue {
    /// (tenant_id, batch_id) → (entry, processed)
    entries: Mutex<HashMap<(String, String), (IntakeQueueEntry, bool)>>,
}

impl InMemoryIntakeQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_count(&self, tenant_id: &str) -> usize {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|((tid, _), (_, processed))| tid == tenant_id && !processed)
            .count()
    }
}

#[async_trait]
impl IntakeQueue for InMemoryIntakeQueue {
    async fn submit(&self, entry: &IntakeQueueEntry) -> anyhow::Result<()> {
        let key = (entry.tenant_id.clone(), entry.batch_id.clone());
        self.entries
            .lock()
            .unwrap()
            .insert(key, (entry.clone(), false));
        Ok(())
    }

    async fn list_pending(&self, tenant_id: &str) -> anyhow::Result<Vec<IntakeQueueEntry>> {
        let entries = self.entries.lock().unwrap();
        let mut pending: Vec<_> = entries
            .iter()
            .filter(|((tid, _), (_, processed))| tid == tenant_id && !processed)
            .map(|(_, (entry, _))| entry.clone())
            .collect();
        pending.sort_by(|a, b| {
            a.received_at
                .cmp(&b.received_at)
                .then(a.batch_id.cmp(&b.batch_id))
        });
        Ok(pending)
    }

    async fn mark_processed(&self, tenant_id: &str, batch_id: &str) -> anyhow::Result<()> {
        let key = (tenant_id.to_string(), batch_id.to_string());
        let mut entries = self.entries.lock().unwrap();
        if let Some((_, processed)) = entries.get_mut(&key) {
            *processed = true;
        }
        Ok(())
    }
}

/// Local filesystem intake queue — compatible with the Python `Workspace`.
///
/// Writes entries to `{root}/intake/pending/{batch_id}.json`, matching the path
/// that the Python normalization worker polls via `list_pending_intake_batches()`.
pub struct LocalIntakeQueue {
    root: std::path::PathBuf,
}

impl LocalIntakeQueue {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn pending_dir(&self) -> std::path::PathBuf {
        self.root.join("intake").join("pending")
    }

    fn processed_dir(&self) -> std::path::PathBuf {
        self.root.join("intake").join("processed")
    }
}

#[async_trait]
impl IntakeQueue for LocalIntakeQueue {
    async fn submit(&self, entry: &IntakeQueueEntry) -> anyhow::Result<()> {
        let dir = self.pending_dir();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.json", entry.batch_id));
        let json = serde_json::to_string_pretty(entry)?;
        std::fs::write(path, json + "\n")?;
        Ok(())
    }

    async fn list_pending(&self, tenant_id: &str) -> anyhow::Result<Vec<IntakeQueueEntry>> {
        let dir = self.pending_dir();
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        for path in std::fs::read_dir(&dir)? {
            let path = path?.path();
            if path.extension().is_some_and(|e| e == "json") {
                let content = std::fs::read_to_string(&path)?;
                let entry: IntakeQueueEntry = serde_json::from_str(&content)?;
                if entry.tenant_id == tenant_id {
                    entries.push(entry);
                }
            }
        }
        entries.sort_by(|a, b| {
            a.received_at
                .cmp(&b.received_at)
                .then(a.batch_id.cmp(&b.batch_id))
        });
        Ok(entries)
    }

    async fn mark_processed(&self, _tenant_id: &str, batch_id: &str) -> anyhow::Result<()> {
        let src = self.pending_dir().join(format!("{batch_id}.json"));
        let dst_dir = self.processed_dir();
        std::fs::create_dir_all(&dst_dir)?;
        let dst = dst_dir.join(format!("{batch_id}.json"));
        if src.exists() {
            std::fs::rename(src, dst)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Producer;

    fn make_entry(batch_id: &str, tenant_id: &str) -> IntakeQueueEntry {
        IntakeQueueEntry {
            batch_id: batch_id.into(),
            tenant_id: tenant_id.into(),
            source: "okta".into(),
            integration_id: "default".into(),
            received_at: "2026-03-15T14:30:00Z".into(),
            record_count: 10,
            idempotency_key: format!("key-{batch_id}"),
            payload_sha256: "sha".into(),
            manifest_key: "m".into(),
            object_key: "o".into(),
            producer: Producer {
                kind: "push_gateway".into(),
                run_id: "r".into(),
            },
        }
    }

    #[tokio::test]
    async fn submit_and_list_pending() {
        let queue = InMemoryIntakeQueue::new();

        queue.submit(&make_entry("b1", "t1")).await.unwrap();
        queue.submit(&make_entry("b2", "t1")).await.unwrap();
        queue.submit(&make_entry("b3", "t2")).await.unwrap();

        let pending = queue.list_pending("t1").await.unwrap();
        assert_eq!(pending.len(), 2);

        let pending_t2 = queue.list_pending("t2").await.unwrap();
        assert_eq!(pending_t2.len(), 1);
    }

    #[tokio::test]
    async fn mark_processed_removes_from_pending() {
        let queue = InMemoryIntakeQueue::new();

        queue.submit(&make_entry("b1", "t1")).await.unwrap();
        queue.submit(&make_entry("b2", "t1")).await.unwrap();

        queue.mark_processed("t1", "b1").await.unwrap();

        let pending = queue.list_pending("t1").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].batch_id, "b2");
    }

    #[tokio::test]
    async fn empty_queue() {
        let queue = InMemoryIntakeQueue::new();
        let pending = queue.list_pending("t1").await.unwrap();
        assert!(pending.is_empty());
    }
}
