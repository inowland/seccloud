use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::routing::{get, post};
use tower_http::trace::TraceLayer;

use seccloud_lake::accepted_batch::AcceptedBatchStore;
use seccloud_lake::intake_queue::IntakeQueue;
use seccloud_lake::object_store::ObjectStore;

use crate::config::GatewayConfig;
use crate::routes;

/// Tracks in-flight requests for backpressure.
#[derive(Default)]
pub struct LoadTracker {
    pub in_flight_requests: AtomicUsize,
    pub in_flight_bytes: AtomicUsize,
}

impl LoadTracker {
    pub fn acquire(
        &self,
        max_requests: usize,
        max_bytes: usize,
        request_bytes: usize,
    ) -> Option<LoadGuard<'_>> {
        let prev_requests = self.in_flight_requests.fetch_add(1, Ordering::Relaxed);
        let prev_bytes = self
            .in_flight_bytes
            .fetch_add(request_bytes, Ordering::Relaxed);
        if prev_requests >= max_requests || prev_bytes.saturating_add(request_bytes) > max_bytes {
            self.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
            self.in_flight_bytes
                .fetch_sub(request_bytes, Ordering::Relaxed);
            return None;
        }
        Some(LoadGuard {
            tracker: self,
            request_bytes,
        })
    }

    pub fn current_requests(&self) -> usize {
        self.in_flight_requests.load(Ordering::Relaxed)
    }

    pub fn current_bytes(&self) -> usize {
        self.in_flight_bytes.load(Ordering::Relaxed)
    }
}

pub struct LoadGuard<'a> {
    tracker: &'a LoadTracker,
    request_bytes: usize,
}

impl Drop for LoadGuard<'_> {
    fn drop(&mut self) {
        self.tracker
            .in_flight_requests
            .fetch_sub(1, Ordering::Relaxed);
        self.tracker
            .in_flight_bytes
            .fetch_sub(self.request_bytes, Ordering::Relaxed);
    }
}

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<GatewayConfig>,
    pub store: Arc<dyn ObjectStore>,
    pub queue: Arc<dyn IntakeQueue>,
    pub accepted_batches: Arc<dyn AcceptedBatchStore>,
    pub load: Arc<LoadTracker>,
}

/// Build the axum router with all routes and middleware.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(routes::health))
        .route("/intake/v1/raw-events", post(routes::ingest_raw_events))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::LoadTracker;

    #[test]
    fn load_tracker_rejects_when_request_budget_exceeded() {
        let tracker = LoadTracker::default();
        let _guard = tracker.acquire(2, 10, 6).unwrap();

        assert!(tracker.acquire(2, 10, 5).is_none());
        assert_eq!(tracker.current_requests(), 1);
        assert_eq!(tracker.current_bytes(), 6);
    }

    #[test]
    fn load_tracker_releases_bytes_on_drop() {
        let tracker = LoadTracker::default();
        {
            let _guard = tracker.acquire(1, 10, 4).unwrap();
            assert_eq!(tracker.current_requests(), 1);
            assert_eq!(tracker.current_bytes(), 4);
        }

        assert_eq!(tracker.current_requests(), 0);
        assert_eq!(tracker.current_bytes(), 0);
    }
}
