use std::path::PathBuf;

use async_trait::async_trait;

use crate::collector::{Checkpoint, Collector, CollectorPage};
use crate::collectors::{build_fixture_page, read_jsonl_fixture};

/// Maps an Okta System Log vendor event to the raw event schema.
fn map_okta_event(event: &serde_json::Value) -> serde_json::Value {
    let uuid = event.get("uuid").and_then(|v| v.as_str()).unwrap_or("");
    let published = event
        .get("published")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let received_at = event
        .get("ingested_at")
        .and_then(|v| v.as_str())
        .unwrap_or(published);
    let event_type_raw = event
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let event_type = match event_type_raw {
        "user.session.start" => "login",
        "user.session.end" => "logout",
        other => other,
    };

    let actor = event.get("actor");
    let actor_email = actor
        .and_then(|a| a.get("alternateId"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let actor_name = actor
        .and_then(|a| a.get("displayName"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let department = actor
        .and_then(|a| a.get("department"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let role = actor
        .and_then(|a| a.get("role"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let client = event.get("client");
    let ip = client
        .and_then(|c| c.get("ipAddress"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let geo_ctx = client.and_then(|c| c.get("geographicalContext"));
    let country = geo_ctx
        .and_then(|g| g.get("country"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let state = geo_ctx
        .and_then(|g| g.get("state"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let geo = if !country.is_empty() && !state.is_empty() {
        format!("{country}-{state}")
    } else {
        String::new()
    };

    let target = event
        .get("target")
        .and_then(|t| t.as_array())
        .and_then(|arr| arr.first());
    let resource_id = target
        .and_then(|t| t.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resource_name = target
        .and_then(|t| t.get("displayName"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resource_kind = target
        .and_then(|t| t.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let privileged = event
        .get("securityContext")
        .and_then(|s| s.get("isPrivileged"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let sensitivity = event
        .get("debugContext")
        .and_then(|d| d.get("sensitivity"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    serde_json::json!({
        "source": "okta",
        "source_event_id": uuid,
        "observed_at": published,
        "received_at": received_at,
        "actor_email": actor_email,
        "actor_name": actor_name,
        "department": department,
        "role": role,
        "event_type": event_type,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "resource_kind": resource_kind,
        "sensitivity": sensitivity,
        "geo": geo,
        "ip": ip,
        "privileged": privileged,
    })
}

/// Okta System Log collector that reads from a JSONL fixture file.
pub struct OktaFixtureCollector {
    fixture_path: PathBuf,
    integration_id: String,
    batch_size: usize,
}

impl OktaFixtureCollector {
    pub fn new(fixture_path: impl Into<PathBuf>) -> Self {
        Self {
            fixture_path: fixture_path.into(),
            integration_id: "okta-primary".into(),
            batch_size: 100,
        }
    }

    pub fn with_integration_id(mut self, id: impl Into<String>) -> Self {
        self.integration_id = id.into();
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[async_trait]
impl Collector for OktaFixtureCollector {
    fn kind(&self) -> &str {
        "okta_system_log_fixture"
    }

    fn source(&self) -> &str {
        "okta"
    }

    fn integration_id(&self) -> &str {
        &self.integration_id
    }

    async fn poll(
        &self,
        checkpoint: &Checkpoint,
        limit: Option<usize>,
    ) -> anyhow::Result<CollectorPage> {
        let all_events = read_jsonl_fixture(&self.fixture_path)?;

        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "fixture_path".into(),
            serde_json::json!(self.fixture_path.display().to_string()),
        );
        let page = build_fixture_page(
            &all_events,
            checkpoint,
            limit,
            self.batch_size,
            map_okta_event,
            metadata,
        );
        let mut page = page;
        page.metadata
            .insert("record_count".into(), serde_json::json!(page.records.len()));
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn fixture_file(events: &[serde_json::Value]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for event in events {
            writeln!(f, "{}", serde_json::to_string(event).unwrap()).unwrap();
        }
        f
    }

    fn okta_vendor_event(uuid: &str, event_type: &str) -> serde_json::Value {
        serde_json::json!({
            "uuid": uuid,
            "published": "2026-03-15T14:00:00Z",
            "ingested_at": "2026-03-15T14:01:00Z",
            "eventType": event_type,
            "actor": {
                "alternateId": "alice@example.com",
                "displayName": "Alice",
                "department": "security",
                "role": "admin"
            },
            "client": {
                "ipAddress": "10.0.0.1",
                "geographicalContext": {"country": "US", "state": "CA"}
            },
            "target": [{"id": "app-1", "displayName": "Dashboard", "type": "app"}],
            "securityContext": {"isPrivileged": true},
            "debugContext": {"sensitivity": "high"}
        })
    }

    #[tokio::test]
    async fn maps_vendor_event_correctly() {
        let events = vec![okta_vendor_event("e1", "user.session.start")];
        let f = fixture_file(&events);

        let collector = OktaFixtureCollector::new(f.path());
        let page = collector.poll(&Checkpoint::default(), None).await.unwrap();

        assert_eq!(page.records.len(), 1);
        let r = &page.records[0];
        assert_eq!(r["source"], "okta");
        assert_eq!(r["source_event_id"], "e1");
        assert_eq!(r["event_type"], "login"); // mapped from user.session.start
        assert_eq!(r["actor_email"], "alice@example.com");
        assert_eq!(r["geo"], "US-CA");
        assert_eq!(r["ip"], "10.0.0.1");
        assert_eq!(r["privileged"], true);
        assert_eq!(r["sensitivity"], "high");
    }

    #[tokio::test]
    async fn paginates_across_pages() {
        let events = vec![
            okta_vendor_event("e1", "user.session.start"),
            okta_vendor_event("e2", "user.session.end"),
            okta_vendor_event("e3", "user.session.start"),
        ];
        let f = fixture_file(&events);

        let collector = OktaFixtureCollector::new(f.path()).with_batch_size(2);

        // Page 1
        let page1 = collector.poll(&Checkpoint::default(), None).await.unwrap();
        assert_eq!(page1.records.len(), 2);
        assert!(page1.has_more);
        assert_eq!(page1.next_checkpoint.cursor, serde_json::json!(2));

        // Page 2
        let page2 = collector.poll(&page1.next_checkpoint, None).await.unwrap();
        assert_eq!(page2.records.len(), 1);
        assert!(!page2.has_more);
        assert_eq!(page2.next_checkpoint.cursor, serde_json::json!(3));

        // Page 3 — empty
        let page3 = collector.poll(&page2.next_checkpoint, None).await.unwrap();
        assert!(page3.records.is_empty());
        assert!(!page3.has_more);
    }

    #[tokio::test]
    async fn limit_overrides_batch_size() {
        let events = vec![
            okta_vendor_event("e1", "user.session.start"),
            okta_vendor_event("e2", "user.session.end"),
        ];
        let f = fixture_file(&events);

        let collector = OktaFixtureCollector::new(f.path()).with_batch_size(100);
        let page = collector
            .poll(&Checkpoint::default(), Some(1))
            .await
            .unwrap();

        assert_eq!(page.records.len(), 1);
        assert!(page.has_more);
    }

    #[tokio::test]
    async fn reads_real_fixture() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/poc/vendor-fixtures/fixed-source-pack/okta_system_log.jsonl");
        if !fixture.exists() {
            return; // Skip if fixture not available
        }

        let collector = OktaFixtureCollector::new(&fixture);
        let page = collector.poll(&Checkpoint::default(), None).await.unwrap();

        assert!(!page.records.is_empty());
        for r in &page.records {
            assert_eq!(r["source"], "okta");
            assert!(
                r["source_event_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("okta-vendor-")
            );
        }
    }
}
