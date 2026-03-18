#[cfg(test)]
mod integration {
    use std::io::Write;
    use std::sync::Arc;

    use axum::http::{HeaderName, HeaderValue, StatusCode};
    use axum_test::TestServer;
    use flate2::Compression;
    use flate2::write::GzEncoder;

    use seccloud_lake::accepted_batch::{AcceptedBatchStore, InMemoryAcceptedBatchStore};
    use seccloud_lake::intake_queue::InMemoryIntakeQueue;
    use seccloud_lake::object_store::{InMemoryObjectStore, ObjectStore};

    use crate::config::GatewayConfig;
    use crate::service::{AppState, build_router};

    fn test_state() -> (AppState, Arc<InMemoryObjectStore>, Arc<InMemoryIntakeQueue>) {
        let config = GatewayConfig::for_test();
        let store = Arc::new(InMemoryObjectStore::new());
        let queue = Arc::new(InMemoryIntakeQueue::new());
        let accepted_batches: Arc<dyn AcceptedBatchStore> =
            Arc::new(InMemoryAcceptedBatchStore::new());
        let state = AppState {
            config: Arc::new(config),
            store: store.clone(),
            queue: queue.clone(),
            accepted_batches,
            load: Arc::new(crate::service::LoadTracker::default()),
        };
        (state, store, queue)
    }

    fn test_server(state: AppState) -> TestServer {
        let app = build_router(state);
        TestServer::builder().build(app).unwrap()
    }

    fn auth_header() -> (HeaderName, HeaderValue) {
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer test-token"),
        )
    }

    fn valid_body() -> serde_json::Value {
        serde_json::json!({
            "source": "okta",
            "records": [
                {
                    "source_event_id": "e1",
                    "observed_at": "2026-03-15T14:00:00Z",
                    "event_type": "login",
                    "actor_email": "alice@example.com"
                },
                {
                    "source_event_id": "e2",
                    "observed_at": "2026-03-15T14:01:00Z",
                    "event_type": "logout",
                    "actor_email": "bob@example.com"
                }
            ]
        })
    }

    #[tokio::test]
    async fn health_check() {
        let (state, _, _) = test_state();
        let server = test_server(state);

        let response = server.get("/health").await;
        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        assert_eq!(body["status"], "ok");
        assert!(body["in_flight"].is_number());
        assert!(body["in_flight_bytes"].is_number());
        assert!(body["max_concurrent"].is_number());
        assert!(body["max_in_flight_bytes"].is_number());
    }

    #[tokio::test]
    async fn successful_ingest() {
        let (state, store, queue) = test_state();
        let server = test_server(state);

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .json(&valid_body())
            .await;

        response.assert_status(StatusCode::ACCEPTED);

        let body: serde_json::Value = response.json();
        assert_eq!(body["tenant_id"], "test-tenant");
        assert_eq!(body["source"], "okta");
        assert_eq!(body["record_count"], 2);
        assert_eq!(body["dead_letter_count"], 0);
        assert_eq!(body["duplicate"], false);
        assert!(body["batch_id"].as_str().unwrap().starts_with("raw_"));
        assert!(body["object_key"].as_str().unwrap().ends_with(".parquet"));
        assert!(body["manifest_key"].as_str().unwrap().ends_with(".json"));

        // Verify S3 objects written
        let keys = store.keys();
        assert!(keys.iter().any(|k| k.ends_with(".parquet")));
        assert!(keys.iter().any(|k| k.ends_with(".json")));

        // Verify queue entry
        assert_eq!(queue.pending_count("test-tenant"), 1);
    }

    #[tokio::test]
    async fn parquet_content_valid() {
        let (state, store, _) = test_state();
        let server = test_server(state);

        let (hk, hv) = auth_header();
        server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .json(&valid_body())
            .await;

        let parquet_key = store
            .keys()
            .into_iter()
            .find(|k| k.ends_with(".parquet"))
            .unwrap();
        let parquet_bytes = store.get_bytes(&parquet_key).await.unwrap().unwrap();

        let batches = seccloud_lake::schema::read_parquet_bytes(&parquet_bytes).unwrap();
        let rows = seccloud_lake::schema::record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].source, "okta");
        assert_eq!(rows[0].tenant_id, "test-tenant");

        let r0: serde_json::Value = serde_json::from_str(&rows[0].record_json).unwrap();
        assert_eq!(r0["source_event_id"], "e1");
        assert_eq!(r0["event_type"], "login");
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let (state, _, _) = test_state();
        let server = test_server(state);

        let response = server
            .post("/intake/v1/raw-events")
            .json(&valid_body())
            .await;

        response.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let (state, _, _) = test_state();
        let server = test_server(state);

        let response = server
            .post("/intake/v1/raw-events")
            .add_header(
                HeaderName::from_static("authorization"),
                HeaderValue::from_static("Bearer bad-token"),
            )
            .json(&valid_body())
            .await;

        response.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn source_mismatch_returns_403() {
        let (state, _, _) = test_state();
        let server = test_server(state);

        let body = serde_json::json!({
            "source": "github",
            "records": [{"source_event_id": "e1", "observed_at": "2026-03-15T14:00:00Z"}]
        });

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .json(&body)
            .await;

        response.assert_status(StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn empty_records_returns_400() {
        let (state, _, _) = test_state();
        let server = test_server(state);

        let body = serde_json::json!({"source": "okta", "records": []});

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .json(&body)
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn gzip_body_accepted() {
        let (state, store, _) = test_state();
        let server = test_server(state);

        let json_bytes = serde_json::to_vec(&valid_body()).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&json_bytes).unwrap();
        let compressed = encoder.finish().unwrap();

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .add_header(
                HeaderName::from_static("content-encoding"),
                HeaderValue::from_static("gzip"),
            )
            .bytes(compressed.into())
            .await;

        response.assert_status(StatusCode::ACCEPTED);

        let body: serde_json::Value = response.json();
        assert_eq!(body["record_count"], 2);

        assert!(store.keys().iter().any(|k| k.ends_with(".parquet")));
    }

    #[tokio::test]
    async fn dead_letter_invalid_records() {
        let (state, store, _) = test_state();
        let server = test_server(state);

        let body = serde_json::json!({
            "source": "okta",
            "records": [
                {"source_event_id": "e1", "observed_at": "2026-03-15T14:00:00Z"},
                {"observed_at": "2026-03-15T14:01:00Z"},
                {"source_event_id": "e3", "observed_at": "2026-03-15T14:02:00Z"}
            ]
        });

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .json(&body)
            .await;

        response.assert_status(StatusCode::ACCEPTED);
        let resp: serde_json::Value = response.json();
        assert_eq!(resp["record_count"], 2);
        assert_eq!(resp["dead_letter_count"], 1);

        // Should have both raw and dead-letter parquet files
        let keys = store.keys();
        let parquet_keys: Vec<_> = keys.iter().filter(|k| k.ends_with(".parquet")).collect();
        assert_eq!(parquet_keys.len(), 2);
        assert!(parquet_keys.iter().any(|k| k.contains("/raw/")));
        assert!(parquet_keys.iter().any(|k| k.contains("/dead-letter/")));
    }

    #[tokio::test]
    async fn duplicate_batch_returns_existing_acceptance() {
        let (state, _store, queue) = test_state();
        let server = test_server(state);
        let body = valid_body();

        let (hk, hv) = auth_header();
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk.clone(), hv.clone())
            .add_header(
                HeaderName::from_static("idempotency-key"),
                HeaderValue::from_static("dup-1"),
            )
            .json(&body)
            .await;
        response.assert_status(StatusCode::ACCEPTED);
        let first: serde_json::Value = response.json();

        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .add_header(
                HeaderName::from_static("idempotency-key"),
                HeaderValue::from_static("dup-1"),
            )
            .json(&body)
            .await;
        response.assert_status(StatusCode::ACCEPTED);
        let second: serde_json::Value = response.json();

        assert_eq!(first["batch_id"], second["batch_id"]);
        assert_eq!(first["manifest_key"], second["manifest_key"]);
        assert_eq!(first["object_key"], second["object_key"]);
        assert_eq!(second["duplicate"], true);
        assert_eq!(queue.pending_count("test-tenant"), 1);
    }

    #[tokio::test]
    async fn idempotency_conflict_returns_409() {
        let (state, _store, queue) = test_state();
        let server = test_server(state);

        let (hk, hv) = auth_header();
        server
            .post("/intake/v1/raw-events")
            .add_header(hk.clone(), hv.clone())
            .add_header(
                HeaderName::from_static("idempotency-key"),
                HeaderValue::from_static("dup-2"),
            )
            .json(&valid_body())
            .await
            .assert_status(StatusCode::ACCEPTED);

        let conflicting = serde_json::json!({
            "source": "okta",
            "records": [{
                "source_event_id": "different",
                "observed_at": "2026-03-15T14:00:00Z",
                "event_type": "login",
                "actor_email": "alice@example.com"
            }]
        });
        let response = server
            .post("/intake/v1/raw-events")
            .add_header(hk, hv)
            .add_header(
                HeaderName::from_static("idempotency-key"),
                HeaderValue::from_static("dup-2"),
            )
            .json(&conflicting)
            .await;
        response.assert_status(StatusCode::CONFLICT);
        assert_eq!(queue.pending_count("test-tenant"), 1);
    }
}
