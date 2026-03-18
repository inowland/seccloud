use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;

use crate::collector::Checkpoint;

/// Persisted state for a single collector instance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointState {
    pub tenant_id: String,
    pub collector_kind: String,
    pub source: String,
    pub integration_id: String,
    pub checkpoint: Checkpoint,
    pub last_run_status: String,
    pub last_run_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error: Option<String>,
    pub last_batch_id: Option<String>,
    pub last_idempotency_key: Option<String>,
    pub attempt_count: u64,
    pub success_count: u64,
}

impl CheckpointState {
    pub fn new(tenant_id: &str, kind: &str, source: &str, integration_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            collector_kind: kind.into(),
            source: source.into(),
            integration_id: integration_id.into(),
            checkpoint: Checkpoint::default(),
            last_run_status: "never".into(),
            last_run_at: None,
            last_success_at: None,
            last_error: None,
            last_batch_id: None,
            last_idempotency_key: None,
            attempt_count: 0,
            success_count: 0,
        }
    }

    /// Record a successful run.
    pub fn record_success(
        &mut self,
        next_checkpoint: Checkpoint,
        batch_id: &str,
        idempotency_key: &str,
    ) {
        self.checkpoint = next_checkpoint;
        self.last_run_status = "succeeded".into();
        self.last_success_at = Some(Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
        self.last_error = None;
        self.last_batch_id = Some(batch_id.into());
        self.last_idempotency_key = Some(idempotency_key.into());
        self.success_count += 1;
    }

    /// Record a failed run (checkpoint NOT advanced).
    pub fn record_failure(&mut self, error: &str) {
        self.last_run_status = "failed".into();
        self.last_error = Some(error.into());
    }

    /// Record an idle run (no records).
    pub fn record_idle(&mut self) {
        self.last_run_status = "idle".into();
        self.last_error = None;
    }

    /// Mark the start of a run attempt.
    pub fn start_attempt(&mut self) {
        self.attempt_count += 1;
        self.last_run_at = Some(Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
    }
}

fn checkpoint_key(tenant_id: &str, kind: &str, source: &str, integration_id: &str) -> String {
    format!("{tenant_id}::{kind}::{source}::{integration_id}")
}

/// Async checkpoint store trait.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(
        &self,
        tenant_id: &str,
        kind: &str,
        source: &str,
        integration_id: &str,
    ) -> anyhow::Result<CheckpointState>;
    async fn save(&self, state: &CheckpointState) -> anyhow::Result<()>;
}

/// In-memory checkpoint store for testing.
#[derive(Default)]
pub struct InMemoryCheckpointStore {
    data: Mutex<HashMap<String, CheckpointState>>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct FileCheckpointStore {
    root: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn manifest_path(&self) -> PathBuf {
        self.root
            .join("manifests")
            .join("collector_checkpoints.json")
    }

    fn read_manifest(&self) -> anyhow::Result<serde_json::Value> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(serde_json::json!({
                "checkpoint_version": 1,
                "collectors": {},
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
impl CheckpointStore for FileCheckpointStore {
    async fn load(
        &self,
        tenant_id: &str,
        kind: &str,
        source: &str,
        integration_id: &str,
    ) -> anyhow::Result<CheckpointState> {
        let key = checkpoint_key(tenant_id, kind, source, integration_id);
        let manifest = self.read_manifest()?;
        let state = manifest
            .get("collectors")
            .and_then(|v| v.get(&key))
            .cloned();
        match state {
            Some(value) => Ok(serde_json::from_value(value)?),
            None => Ok(CheckpointState::new(
                tenant_id,
                kind,
                source,
                integration_id,
            )),
        }
    }

    async fn save(&self, state: &CheckpointState) -> anyhow::Result<()> {
        let key = checkpoint_key(
            &state.tenant_id,
            &state.collector_kind,
            &state.source,
            &state.integration_id,
        );
        let mut manifest = self.read_manifest()?;
        let object = manifest
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("collector checkpoint manifest is not a JSON object"))?;
        let collectors = object
            .entry("collectors")
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let collectors = collectors
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("collector checkpoint map is not a JSON object"))?;
        collectors.insert(key, serde_json::to_value(state)?);
        self.write_manifest(&manifest)?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(
        &self,
        tenant_id: &str,
        kind: &str,
        source: &str,
        integration_id: &str,
    ) -> anyhow::Result<CheckpointState> {
        let key = checkpoint_key(tenant_id, kind, source, integration_id);
        let data = self.data.lock().unwrap();
        Ok(data
            .get(&key)
            .cloned()
            .unwrap_or_else(|| CheckpointState::new(tenant_id, kind, source, integration_id)))
    }

    async fn save(&self, state: &CheckpointState) -> anyhow::Result<()> {
        let key = checkpoint_key(
            &state.tenant_id,
            &state.collector_kind,
            &state.source,
            &state.integration_id,
        );
        self.data.lock().unwrap().insert(key, state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn load_returns_default_when_missing() {
        let store = InMemoryCheckpointStore::new();
        let state = store
            .load("t1", "okta_fixture", "okta", "primary")
            .await
            .unwrap();
        assert_eq!(state.last_run_status, "never");
        assert_eq!(state.attempt_count, 0);
        assert_eq!(state.checkpoint.version, 0);
    }

    #[tokio::test]
    async fn save_and_load_round_trip() {
        let store = InMemoryCheckpointStore::new();

        let mut state = CheckpointState::new("t1", "okta_fixture", "okta", "primary");
        state.start_attempt();
        state.record_success(
            Checkpoint {
                cursor: serde_json::json!(42),
                version: 1,
            },
            "raw_abc",
            "key-1",
        );

        store.save(&state).await.unwrap();

        let loaded = store
            .load("t1", "okta_fixture", "okta", "primary")
            .await
            .unwrap();
        assert_eq!(loaded.last_run_status, "succeeded");
        assert_eq!(loaded.checkpoint.cursor, serde_json::json!(42));
        assert_eq!(loaded.checkpoint.version, 1);
        assert_eq!(loaded.success_count, 1);
        assert_eq!(loaded.attempt_count, 1);
    }

    #[tokio::test]
    async fn failure_does_not_advance_checkpoint() {
        let mut state = CheckpointState::new("t1", "k", "s", "i");
        state.checkpoint = Checkpoint {
            cursor: serde_json::json!(10),
            version: 1,
        };

        state.start_attempt();
        state.record_failure("network error");

        assert_eq!(state.checkpoint.cursor, serde_json::json!(10));
        assert_eq!(state.last_run_status, "failed");
        assert_eq!(state.last_error.as_deref(), Some("network error"));
    }

    #[tokio::test]
    async fn file_store_round_trips_state() {
        let tempdir = tempfile::tempdir().unwrap();
        let store = FileCheckpointStore::new(tempdir.path());
        let mut state = CheckpointState::new("t1", "okta_fixture", "okta", "primary");
        state.start_attempt();
        state.record_success(
            Checkpoint {
                cursor: serde_json::json!(7),
                version: 2,
            },
            "raw_7",
            "key-7",
        );

        store.save(&state).await.unwrap();

        let reloaded = FileCheckpointStore::new(tempdir.path())
            .load("t1", "okta_fixture", "okta", "primary")
            .await
            .unwrap();
        assert_eq!(reloaded.checkpoint.cursor, serde_json::json!(7));
        assert_eq!(reloaded.last_batch_id.as_deref(), Some("raw_7"));
        assert_eq!(reloaded.last_idempotency_key.as_deref(), Some("key-7"));
    }
}
