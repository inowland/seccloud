use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};

use crate::auth;
use crate::ingest::{self, IngestError, IntakeRequest};
use crate::service::AppState;

/// GET /health — readiness probe, reports in-flight request count.
pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "in_flight": state.load.current_requests(),
        "in_flight_bytes": state.load.current_bytes(),
        "max_concurrent": state.config.max_concurrent_requests,
        "max_in_flight_bytes": state.config.max_in_flight_bytes,
    }))
}

/// POST /intake/v1/raw-events — main ingestion endpoint.
pub async fn ingest_raw_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Backpressure: reject if at capacity
    let _guard = match state.load.acquire(
        state.config.max_concurrent_requests,
        state.config.max_in_flight_bytes,
        body.len(),
    ) {
        Some(g) => g,
        None => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "error": "server at capacity, try again later",
                    "in_flight": state.load.current_requests(),
                    "in_flight_bytes": state.load.current_bytes(),
                })),
            )
                .into_response();
        }
    };

    // Authenticate
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let creds = match auth::authenticate(auth_header, &state.config.auth_tokens) {
        Ok(c) => c,
        Err(e) => {
            return (
                e.status_code(),
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Check body size
    if body.len() > state.config.max_body_bytes {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": format!("request body {} bytes exceeds limit {}", body.len(), state.config.max_body_bytes)
            })),
        )
            .into_response();
    }

    // Decompress if needed
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok());
    let decompressed = match ingest::maybe_decompress(&body, content_encoding) {
        Ok(d) => d,
        Err(e) => {
            return (
                e.status_code(),
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Parse request
    let request: IntakeRequest = match serde_json::from_slice(&decompressed) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid JSON: {e}")})),
            )
                .into_response();
        }
    };

    // Verify source matches token
    if let Err(e) = auth::verify_source(&creds, &request.source) {
        return (
            e.status_code(),
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    let idempotency_key = headers.get("idempotency-key").and_then(|v| v.to_str().ok());

    // Process
    let params = ingest::IntakeParams {
        creds: &creds,
        store: &state.store,
        queue: &state.queue,
        accepted_batches: &state.accepted_batches,
        allowed_sources: &state.config.allowed_sources,
        max_records: state.config.max_records,
        buffer_max_bytes: state.config.buffer_max_bytes,
        producer_run_id: &state.config.producer_run_id,
        idempotency_key_header: idempotency_key,
    };
    match ingest::process_intake(params, request).await {
        Ok(accepted) => (StatusCode::ACCEPTED, Json(serde_json::json!(accepted))).into_response(),
        Err(e) => {
            if matches!(e, IngestError::Internal(_)) {
                tracing::error!(error = %e, "internal error during intake");
            }
            (
                e.status_code(),
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}
