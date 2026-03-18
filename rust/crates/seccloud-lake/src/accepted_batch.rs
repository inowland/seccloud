use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedBatch {
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub record_count: usize,
    pub dead_letter_count: usize,
    pub manifest_key: String,
    pub object_key: String,
    pub idempotency_key: String,
    pub payload_sha256: String,
    pub dead_letter_object_key: Option<String>,
}

#[async_trait]
pub trait AcceptedBatchStore: Send + Sync {
    async fn lookup(&self, idempotency_key: &str) -> anyhow::Result<Option<AcceptedBatch>>;
    async fn register(&self, accepted: &AcceptedBatch) -> anyhow::Result<()>;
}

#[derive(Default)]
pub struct InMemoryAcceptedBatchStore {
    entries: Mutex<HashMap<String, AcceptedBatch>>,
}

impl InMemoryAcceptedBatchStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AcceptedBatchStore for InMemoryAcceptedBatchStore {
    async fn lookup(&self, idempotency_key: &str) -> anyhow::Result<Option<AcceptedBatch>> {
        Ok(self.entries.lock().unwrap().get(idempotency_key).cloned())
    }

    async fn register(&self, accepted: &AcceptedBatch) -> anyhow::Result<()> {
        self.entries
            .lock()
            .unwrap()
            .insert(accepted.idempotency_key.clone(), accepted.clone());
        Ok(())
    }
}

pub struct LocalAcceptedBatchStore {
    root: PathBuf,
}

impl LocalAcceptedBatchStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("manifests").join("intake_manifest.json")
    }

    fn read_manifest(&self) -> anyhow::Result<serde_json::Value> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(serde_json::json!({
                "submitted_batch_ids": [],
                "processed_batch_ids": [],
                "accepted_batches_by_idempotency_key": {},
            }));
        }
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }

    fn write_manifest(&self, value: &serde_json::Value) -> anyhow::Result<()> {
        let path = self.manifest_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
        Ok(())
    }
}

#[async_trait]
impl AcceptedBatchStore for LocalAcceptedBatchStore {
    async fn lookup(&self, idempotency_key: &str) -> anyhow::Result<Option<AcceptedBatch>> {
        let manifest = self.read_manifest()?;
        let accepted = manifest
            .get("accepted_batches_by_idempotency_key")
            .and_then(|v| v.get(idempotency_key))
            .cloned();
        match accepted {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }

    async fn register(&self, accepted: &AcceptedBatch) -> anyhow::Result<()> {
        let mut manifest = self.read_manifest()?;
        let object = manifest
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("intake manifest is not a JSON object"))?;
        let accepted_map = object
            .entry("accepted_batches_by_idempotency_key")
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let accepted_map = accepted_map
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("accepted batch index is not a JSON object"))?;
        accepted_map.insert(
            accepted.idempotency_key.clone(),
            serde_json::to_value(accepted)?,
        );
        self.write_manifest(&manifest)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_store_round_trips() {
        let store = InMemoryAcceptedBatchStore::new();
        let accepted = AcceptedBatch {
            batch_id: "raw_1".into(),
            tenant_id: "tenant".into(),
            source: "okta".into(),
            integration_id: "default".into(),
            record_count: 2,
            dead_letter_count: 0,
            manifest_key: "m".into(),
            object_key: "o".into(),
            idempotency_key: "k".into(),
            payload_sha256: "sha256:1".into(),
            dead_letter_object_key: None,
        };

        store.register(&accepted).await.unwrap();
        let loaded = store.lookup("k").await.unwrap().unwrap();
        assert_eq!(loaded.batch_id, "raw_1");
    }

    #[tokio::test]
    async fn local_store_round_trips_through_manifest_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let store = LocalAcceptedBatchStore::new(tempdir.path());
        let accepted = AcceptedBatch {
            batch_id: "raw_2".into(),
            tenant_id: "tenant".into(),
            source: "github".into(),
            integration_id: "github-primary".into(),
            record_count: 1,
            dead_letter_count: 0,
            manifest_key: "manifest".into(),
            object_key: "object".into(),
            idempotency_key: "key-2".into(),
            payload_sha256: "sha256:2".into(),
            dead_letter_object_key: None,
        };

        store.register(&accepted).await.unwrap();

        let reloaded = LocalAcceptedBatchStore::new(tempdir.path())
            .lookup("key-2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.object_key, "object");
        assert_eq!(reloaded.integration_id, "github-primary");
    }
}
