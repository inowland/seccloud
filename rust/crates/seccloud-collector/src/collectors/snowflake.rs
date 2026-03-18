use std::path::PathBuf;

use async_trait::async_trait;

use crate::collector::{Checkpoint, Collector, CollectorPage};
use crate::collectors::{build_fixture_page, read_jsonl_fixture};

/// Maps a Snowflake query history event to the raw event schema.
fn map_snowflake_event(event: &serde_json::Value) -> serde_json::Value {
    let query_id = event.get("QUERY_ID").and_then(|v| v.as_str()).unwrap_or("");
    let start_time = event
        .get("START_TIME")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let received_at = event
        .get("INGESTED_AT")
        .and_then(|v| v.as_str())
        .unwrap_or(start_time);
    let query_type = event
        .get("QUERY_TYPE")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let event_type = match query_type {
        "COPY" => "export",
        _ => "query",
    };

    let user_name = event
        .get("USER_NAME")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let display_name = event
        .get("DISPLAY_NAME")
        .and_then(|v| v.as_str())
        .unwrap_or(user_name);
    let department = event
        .get("DEPARTMENT")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let role_name = event
        .get("ROLE_NAME")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let object_name = event
        .get("OBJECT_NAME")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sensitivity = event
        .get("SENSITIVITY")
        .and_then(|v| v.as_str())
        .unwrap_or("internal");
    let rows_produced = event
        .get("ROWS_PRODUCED")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let warehouse = event
        .get("WAREHOUSE_NAME")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    serde_json::json!({
        "source": "snowflake",
        "source_event_id": query_id,
        "observed_at": start_time,
        "received_at": received_at,
        "actor_email": user_name,
        "actor_name": display_name,
        "department": department,
        "role": role_name,
        "event_type": event_type,
        "resource_id": format!("snowflake:dataset/{object_name}"),
        "resource_name": object_name,
        "resource_kind": "dataset",
        "sensitivity": sensitivity,
        "rows_read": rows_produced,
        "warehouse": warehouse,
        "vendor_event_type": query_type,
    })
}

/// Snowflake query history fixture collector.
pub struct SnowflakeFixtureCollector {
    fixture_path: PathBuf,
    integration_id: String,
    batch_size: usize,
}

impl SnowflakeFixtureCollector {
    pub fn new(fixture_path: impl Into<PathBuf>) -> Self {
        Self {
            fixture_path: fixture_path.into(),
            integration_id: "snowflake-primary".into(),
            batch_size: 100,
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[async_trait]
impl Collector for SnowflakeFixtureCollector {
    fn kind(&self) -> &str {
        "snowflake_query_history_fixture"
    }
    fn source(&self) -> &str {
        "snowflake"
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
            map_snowflake_event,
            serde_json::Map::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_snowflake_event() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../examples/poc/vendor-fixtures/fixed-source-pack/snowflake_query_history.jsonl",
        );
        if !fixture.exists() {
            return;
        }
        let c = SnowflakeFixtureCollector::new(&fixture);
        let page = c.poll(&Checkpoint::default(), None).await.unwrap();
        assert!(!page.records.is_empty());
        let r = &page.records[0];
        assert_eq!(r["source"], "snowflake");
        assert_eq!(r["source_event_id"], "snowflake-vendor-0001");
        assert_eq!(r["event_type"], "query");
        assert_eq!(r["resource_kind"], "dataset");
        assert_eq!(r["warehouse"], "analytics_wh");
    }
}
