use std::path::PathBuf;

use async_trait::async_trait;

use crate::collector::{Checkpoint, Collector, CollectorPage};
use crate::collectors::{build_fixture_page, read_jsonl_fixture};

/// Maps a Google Workspace Drive audit event to the raw event schema.
fn map_gworkspace_event(event: &serde_json::Value) -> serde_json::Value {
    let id_obj = event.get("id");
    let source_event_id = id_obj
        .and_then(|i| i.get("uniqueQualifier"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let observed_at = id_obj
        .and_then(|i| i.get("time"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let received_at = event
        .get("ingested_at")
        .and_then(|v| v.as_str())
        .unwrap_or(observed_at);

    let activity = event.get("activity").and_then(|v| v.as_str()).unwrap_or("");
    let visibility = event
        .get("visibility")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let event_type = if activity == "share" && visibility == "external" {
        "share_external"
    } else {
        "view"
    };

    let actor = event.get("actor");
    let actor_email = actor
        .and_then(|a| a.get("email"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let actor_name = actor
        .and_then(|a| a.get("profileName"))
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

    let item = event.get("item");
    let item_id = item
        .and_then(|i| i.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let item_title = item
        .and_then(|i| i.get("title"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sensitivity = item
        .and_then(|i| i.get("sensitivity"))
        .and_then(|v| v.as_str())
        .unwrap_or("internal");
    let external = visibility == "external";

    serde_json::json!({
        "source": "gworkspace",
        "source_event_id": source_event_id,
        "observed_at": observed_at,
        "received_at": received_at,
        "actor_email": actor_email,
        "actor_name": actor_name,
        "department": department,
        "role": role,
        "event_type": event_type,
        "resource_id": format!("gworkspace:doc/{item_id}"),
        "resource_name": item_title,
        "resource_kind": "document",
        "sensitivity": sensitivity,
        "external": external,
        "vendor_event_type": activity,
    })
}

/// Google Workspace fixture collector.
pub struct GWorkspaceFixtureCollector {
    fixture_path: PathBuf,
    integration_id: String,
    batch_size: usize,
}

impl GWorkspaceFixtureCollector {
    pub fn new(fixture_path: impl Into<PathBuf>) -> Self {
        Self {
            fixture_path: fixture_path.into(),
            integration_id: "gworkspace-primary".into(),
            batch_size: 100,
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[async_trait]
impl Collector for GWorkspaceFixtureCollector {
    fn kind(&self) -> &str {
        "gworkspace_drive_audit_fixture"
    }
    fn source(&self) -> &str {
        "gworkspace"
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
            map_gworkspace_event,
            serde_json::Map::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_gworkspace_event() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../examples/poc/vendor-fixtures/fixed-source-pack/gworkspace_drive_audit.jsonl",
        );
        if !fixture.exists() {
            return;
        }
        let c = GWorkspaceFixtureCollector::new(&fixture);
        let page = c.poll(&Checkpoint::default(), None).await.unwrap();
        assert!(!page.records.is_empty());
        let r = &page.records[0];
        assert_eq!(r["source"], "gworkspace");
        assert_eq!(r["source_event_id"], "gws-vendor-0001");
        assert_eq!(r["resource_kind"], "document");
    }
}
