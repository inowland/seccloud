use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

/// Async object store trait — abstracts S3, local filesystem, or in-memory storage.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Store raw bytes at the given key.
    async fn put_bytes(&self, key: &str, data: &[u8], content_type: &str) -> anyhow::Result<()>;

    /// Retrieve raw bytes from the given key. Returns `None` if the key does not exist.
    async fn get_bytes(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;

    /// Downcast support for testing.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Store a JSON value at the given key.
    async fn put_json(&self, key: &str, value: &serde_json::Value) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        self.put_bytes(key, &bytes, "application/json").await
    }

    /// Retrieve and parse a JSON value from the given key.
    async fn get_json(&self, key: &str) -> anyhow::Result<Option<serde_json::Value>> {
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }
}

/// In-memory object store for testing.
#[derive(Default)]
pub struct InMemoryObjectStore {
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all stored keys (for test assertions).
    pub fn keys(&self) -> Vec<String> {
        let data = self.data.lock().unwrap();
        let mut keys: Vec<_> = data.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Get the size of a stored object (for test assertions).
    pub fn size(&self, key: &str) -> Option<usize> {
        self.data.lock().unwrap().get(key).map(|v| v.len())
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put_bytes(&self, key: &str, data: &[u8], _content_type: &str) -> anyhow::Result<()> {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.data.lock().unwrap().get(key).cloned())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Local filesystem object store — compatible with the Python `LocalObjectStore`.
///
/// Writes files under `root/{key}`, creating parent directories as needed.
pub struct LocalObjectStore {
    root: std::path::PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn put_bytes(&self, key: &str, data: &[u8], _content_type: &str) -> anyhow::Result<()> {
        let path = self.root.join(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let path = self.root.join(key);
        if path.exists() {
            Ok(Some(std::fs::read(&path)?))
        } else {
            Ok(None)
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_put_get() {
        let store = InMemoryObjectStore::new();

        store
            .put_bytes("test/file.bin", b"hello", "application/octet-stream")
            .await
            .unwrap();

        let data = store.get_bytes("test/file.bin").await.unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn in_memory_missing_key() {
        let store = InMemoryObjectStore::new();
        let data = store.get_bytes("nope").await.unwrap();
        assert!(data.is_none());
    }

    #[tokio::test]
    async fn in_memory_json_round_trip() {
        let store = InMemoryObjectStore::new();
        let val = serde_json::json!({"batch_id": "raw_1", "count": 42});

        store.put_json("manifest.json", &val).await.unwrap();

        let read = store.get_json("manifest.json").await.unwrap().unwrap();
        assert_eq!(read["batch_id"], "raw_1");
        assert_eq!(read["count"], 42);
    }

    #[tokio::test]
    async fn in_memory_keys() {
        let store = InMemoryObjectStore::new();
        store
            .put_bytes("b", b"", "application/octet-stream")
            .await
            .unwrap();
        store
            .put_bytes("a", b"", "application/octet-stream")
            .await
            .unwrap();

        assert_eq!(store.keys(), vec!["a", "b"]);
    }
}
