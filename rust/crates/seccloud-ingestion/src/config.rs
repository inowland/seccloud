use std::collections::HashMap;
use std::net::SocketAddr;

use serde::Deserialize;

/// Per-token credentials extracted from SECCLOUD_PUSH_AUTH_TOKENS.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenCredentials {
    pub tenant_id: String,
    pub source: String,
    pub integration_id: Option<String>,
}

/// Service configuration, loaded from environment variables.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub listen_addr: SocketAddr,
    pub auth_tokens: HashMap<String, TokenCredentials>,
    pub max_body_bytes: usize,
    pub max_records: usize,
    pub allowed_sources: Vec<String>,
    pub producer_run_id: String,
    pub max_concurrent_requests: usize,
    pub max_in_flight_bytes: usize,
    pub buffer_max_bytes: usize,
}

impl GatewayConfig {
    /// Load configuration from environment variables, matching PoC env var names.
    pub fn from_env() -> anyhow::Result<Self> {
        let listen_addr: SocketAddr = std::env::var("SECCLOUD_GATEWAY_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:8080".into())
            .parse()?;

        let tokens_json =
            std::env::var("SECCLOUD_PUSH_AUTH_TOKENS").unwrap_or_else(|_| "{}".into());
        let auth_tokens: HashMap<String, TokenCredentials> = serde_json::from_str(&tokens_json)?;

        let max_body_bytes: usize = std::env::var("SECCLOUD_PUSH_MAX_BODY_BYTES")
            .unwrap_or_else(|_| "5242880".into())
            .parse()?;

        let max_records: usize = std::env::var("SECCLOUD_PUSH_MAX_RECORDS")
            .unwrap_or_else(|_| "5000".into())
            .parse()?;

        let allowed_sources = vec![
            "okta".into(),
            "github".into(),
            "gworkspace".into(),
            "snowflake".into(),
        ];

        let producer_run_id = format!("gateway_{}", uuid::Uuid::now_v7());

        let max_concurrent_requests: usize = std::env::var("SECCLOUD_GATEWAY_MAX_CONCURRENT")
            .unwrap_or_else(|_| "128".into())
            .parse()?;
        let max_in_flight_bytes: usize = std::env::var("SECCLOUD_GATEWAY_MAX_IN_FLIGHT_BYTES")
            .unwrap_or_else(|_| (64 * 1024 * 1024).to_string())
            .parse()?;
        let buffer_max_bytes: usize = std::env::var("SECCLOUD_GATEWAY_BUFFER_MAX_BYTES")
            .unwrap_or_else(|_| (64 * 1024 * 1024).to_string())
            .parse()?;

        Ok(Self {
            listen_addr,
            auth_tokens,
            max_body_bytes,
            max_records,
            allowed_sources,
            producer_run_id,
            max_concurrent_requests,
            max_in_flight_bytes,
            buffer_max_bytes,
        })
    }

    /// Create a config for testing with a pre-configured token.
    #[cfg(test)]
    pub fn for_test() -> Self {
        let mut auth_tokens = HashMap::new();
        auth_tokens.insert(
            "test-token".into(),
            TokenCredentials {
                tenant_id: "test-tenant".into(),
                source: "okta".into(),
                integration_id: Some("okta-primary".into()),
            },
        );

        Self {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            auth_tokens,
            max_body_bytes: 5 * 1024 * 1024,
            max_records: 5000,
            allowed_sources: vec![
                "okta".into(),
                "github".into(),
                "gworkspace".into(),
                "snowflake".into(),
            ],
            producer_run_id: "test-run".into(),
            max_concurrent_requests: 128,
            max_in_flight_bytes: 64 * 1024 * 1024,
            buffer_max_bytes: 64 * 1024 * 1024,
        }
    }
}
