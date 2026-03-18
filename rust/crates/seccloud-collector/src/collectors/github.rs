use std::path::PathBuf;

use async_trait::async_trait;

use crate::collector::{Checkpoint, Collector, CollectorPage};
use crate::collectors::{build_fixture_page, read_jsonl_fixture};

/// Maps a GitHub Audit Log vendor event to the raw event schema.
fn map_github_event(event: &serde_json::Value) -> serde_json::Value {
    let id = event.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let created_at = event
        .get("created_at")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let received_at = event
        .get("ingested_at")
        .and_then(|v| v.as_str())
        .unwrap_or(created_at);

    let action = event.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let event_type = match action {
        "repo.archive_download" => "archive_download",
        _ => "view",
    };

    let actor = event.get("actor");
    let actor_email = actor
        .and_then(|a| a.get("email"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let actor_name = actor
        .and_then(|a| a.get("display_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let department = actor
        .and_then(|a| a.get("department"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let role = actor
        .and_then(|a| a.get("role"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let repo = event.get("repo");
    let repo_name = repo
        .and_then(|r| r.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sensitivity = repo
        .and_then(|r| r.get("sensitivity"))
        .and_then(|v| v.as_str())
        .unwrap_or("internal");
    let bytes_mb = event
        .get("transfer")
        .and_then(|t| t.get("mb"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    serde_json::json!({
        "source": "github",
        "source_event_id": id,
        "observed_at": created_at,
        "received_at": received_at,
        "actor_email": actor_email,
        "actor_name": actor_name,
        "department": department,
        "role": role,
        "event_type": event_type,
        "resource_id": format!("github:repo/{repo_name}"),
        "resource_name": repo_name,
        "resource_kind": "repo",
        "sensitivity": sensitivity,
        "bytes_transferred_mb": bytes_mb,
        "vendor_event_type": action,
    })
}

/// GitHub Audit Log collector that reads from a JSONL fixture file.
pub struct GitHubFixtureCollector {
    fixture_path: PathBuf,
    integration_id: String,
    batch_size: usize,
}

impl GitHubFixtureCollector {
    pub fn new(fixture_path: impl Into<PathBuf>) -> Self {
        Self {
            fixture_path: fixture_path.into(),
            integration_id: "github-primary".into(),
            batch_size: 100,
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[async_trait]
impl Collector for GitHubFixtureCollector {
    fn kind(&self) -> &str {
        "github_audit_log_fixture"
    }
    fn source(&self) -> &str {
        "github"
    }
    fn integration_id(&self) -> &str {
        &self.integration_id
    }

    async fn poll(
        &self,
        checkpoint: &Checkpoint,
        limit: Option<usize>,
    ) -> anyhow::Result<CollectorPage> {
        let all = read_jsonl_fixture(&self.fixture_path)?;
        Ok(build_fixture_page(
            &all,
            checkpoint,
            limit,
            self.batch_size,
            map_github_event,
            serde_json::Map::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_github_event() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/poc/vendor-fixtures/fixed-source-pack/github_audit_log.jsonl");
        if !fixture.exists() {
            return;
        }
        let c = GitHubFixtureCollector::new(&fixture);
        let page = c.poll(&Checkpoint::default(), None).await.unwrap();
        assert!(!page.records.is_empty());
        let r = &page.records[0];
        assert_eq!(r["source"], "github");
        assert_eq!(r["source_event_id"], "github-vendor-0001");
        assert_eq!(r["event_type"], "view");
        assert_eq!(r["resource_kind"], "repo");
    }
}
