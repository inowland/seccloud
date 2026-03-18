use seccloud_lake::accepted_batch::{AcceptedBatchStore, InMemoryAcceptedBatchStore};
use std::io::Write;
use std::sync::Arc;

use seccloud_collector::checkpoint::{CheckpointStore, InMemoryCheckpointStore};
use seccloud_collector::collectors::okta::OktaFixtureCollector;
use seccloud_collector::runner::{self, JobResult};
use seccloud_lake::intake_queue::InMemoryIntakeQueue;
use seccloud_lake::object_store::{InMemoryObjectStore, ObjectStore};

fn okta_event(uuid: &str) -> serde_json::Value {
    serde_json::json!({
        "uuid": uuid,
        "published": "2026-03-15T14:00:00Z",
        "eventType": "user.session.start",
        "actor": {"alternateId": "a@b.com", "displayName": "A", "department": "d", "role": "r"},
        "client": {"ipAddress": "1.2.3.4", "geographicalContext": {"country": "US", "state": "NY"}},
        "target": [{"id": "app", "displayName": "App", "type": "app"}],
        "securityContext": {"isPrivileged": false},
        "debugContext": {"sensitivity": "low"}
    })
}

fn fixture_file(events: &[serde_json::Value]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    for e in events {
        writeln!(f, "{}", serde_json::to_string(e).unwrap()).unwrap();
    }
    f
}

#[tokio::test]
async fn full_collector_run_writes_parquet_and_advances_checkpoint() {
    let events = vec![okta_event("e1"), okta_event("e2"), okta_event("e3")];
    let f = fixture_file(&events);

    let collector = OktaFixtureCollector::new(f.path()).with_batch_size(2);
    let checkpoint_store = InMemoryCheckpointStore::new();
    let accepted_batches: Arc<dyn AcceptedBatchStore> = Arc::new(InMemoryAcceptedBatchStore::new());
    let object_store: Arc<dyn seccloud_lake::object_store::ObjectStore> =
        Arc::new(InMemoryObjectStore::new());
    let intake_queue: Arc<dyn seccloud_lake::intake_queue::IntakeQueue> =
        Arc::new(InMemoryIntakeQueue::new());

    // First run: 2 records
    let result = runner::run_collector_job(
        &collector,
        "test-tenant",
        &checkpoint_store,
        &object_store,
        &intake_queue,
        &accepted_batches,
        "test-run",
        None,
    )
    .await
    .unwrap();

    match &result {
        JobResult::Submitted {
            record_count,
            has_more,
            ..
        } => {
            assert_eq!(*record_count, 2);
            assert!(*has_more);
        }
        JobResult::Idle => panic!("expected Submitted, got Idle"),
    }

    // Verify checkpoint advanced
    let state = checkpoint_store
        .load(
            "test-tenant",
            "okta_system_log_fixture",
            "okta",
            "okta-primary",
        )
        .await
        .unwrap();
    assert_eq!(state.checkpoint.cursor, serde_json::json!(2));
    assert_eq!(state.success_count, 1);
    assert_eq!(state.last_run_status, "succeeded");

    // Second run: 1 record (last page)
    let result2 = runner::run_collector_job(
        &collector,
        "test-tenant",
        &checkpoint_store,
        &object_store,
        &intake_queue,
        &accepted_batches,
        "test-run",
        None,
    )
    .await
    .unwrap();

    match &result2 {
        JobResult::Submitted {
            record_count,
            has_more,
            ..
        } => {
            assert_eq!(*record_count, 1);
            assert!(!*has_more);
        }
        _ => panic!("expected Submitted"),
    }

    // Third run: idle (no more records)
    let result3 = runner::run_collector_job(
        &collector,
        "test-tenant",
        &checkpoint_store,
        &object_store,
        &intake_queue,
        &accepted_batches,
        "test-run",
        None,
    )
    .await
    .unwrap();
    assert!(matches!(result3, JobResult::Idle));

    // Verify final checkpoint
    let state = checkpoint_store
        .load(
            "test-tenant",
            "okta_system_log_fixture",
            "okta",
            "okta-primary",
        )
        .await
        .unwrap();
    assert_eq!(state.checkpoint.cursor, serde_json::json!(3));
    assert_eq!(state.success_count, 2);
    assert_eq!(state.attempt_count, 3); // 2 submitted + 1 idle

    // Verify Parquet files in object store
    let mem_store = object_store
        .as_any()
        .downcast_ref::<InMemoryObjectStore>()
        .unwrap();
    let keys = mem_store.keys();
    let parquet_count = keys.iter().filter(|k| k.ends_with(".parquet")).count();
    let manifest_count = keys.iter().filter(|k| k.ends_with(".json")).count();
    assert_eq!(parquet_count, 2); // Two batches
    assert_eq!(manifest_count, 2);

    // Read first parquet file and verify content
    let parquet_key = keys.iter().find(|k| k.ends_with(".parquet")).unwrap();
    let parquet_bytes = mem_store.get_bytes(parquet_key).await.unwrap().unwrap();
    let batches = seccloud_lake::schema::read_parquet_bytes(&parquet_bytes).unwrap();
    let rows = seccloud_lake::schema::record_batch_to_rows(&batches[0]).unwrap();
    assert_eq!(rows[0].source, "okta");
    assert_eq!(rows[0].intake_kind, "collector_pull");
}
