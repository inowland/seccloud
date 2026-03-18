use std::collections::HashMap;

use crate::config::TokenCredentials;

/// Validated credentials extracted from a bearer token.
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
}

/// Validate a bearer token and extract credentials.
///
/// Returns `None` if the token is invalid. If the token is valid but the
/// request source doesn't match, returns an appropriate error.
pub fn authenticate(
    auth_header: Option<&str>,
    tokens: &HashMap<String, TokenCredentials>,
) -> Result<AuthCredentials, AuthError> {
    let header = auth_header.ok_or(AuthError::MissingHeader)?;

    let token = header
        .strip_prefix("Bearer ")
        .ok_or(AuthError::MalformedHeader)?;

    let creds = tokens.get(token).ok_or(AuthError::InvalidToken)?;

    Ok(AuthCredentials {
        tenant_id: creds.tenant_id.clone(),
        source: creds.source.clone(),
        integration_id: creds
            .integration_id
            .clone()
            .unwrap_or_else(|| "default".into()),
    })
}

/// Verify that the request source matches the token's authorized source.
pub fn verify_source(creds: &AuthCredentials, request_source: &str) -> Result<(), AuthError> {
    if creds.source != request_source {
        return Err(AuthError::SourceMismatch {
            token_source: creds.source.clone(),
            request_source: request_source.into(),
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingHeader,
    #[error("expected Bearer token")]
    MalformedHeader,
    #[error("invalid push ingestion token")]
    InvalidToken,
    #[error("push token source mismatch: token={token_source}, request={request_source}")]
    SourceMismatch {
        token_source: String,
        request_source: String,
    },
}

impl AuthError {
    pub fn status_code(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::MissingHeader | Self::MalformedHeader | Self::InvalidToken => {
                StatusCode::UNAUTHORIZED
            }
            Self::SourceMismatch { .. } => StatusCode::FORBIDDEN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tokens() -> HashMap<String, TokenCredentials> {
        let mut m = HashMap::new();
        m.insert(
            "tok-okta".into(),
            TokenCredentials {
                tenant_id: "t1".into(),
                source: "okta".into(),
                integration_id: Some("okta-primary".into()),
            },
        );
        m.insert(
            "tok-github".into(),
            TokenCredentials {
                tenant_id: "t1".into(),
                source: "github".into(),
                integration_id: None,
            },
        );
        m
    }

    #[test]
    fn valid_token() {
        let creds = authenticate(Some("Bearer tok-okta"), &test_tokens()).unwrap();
        assert_eq!(creds.tenant_id, "t1");
        assert_eq!(creds.source, "okta");
        assert_eq!(creds.integration_id, "okta-primary");
    }

    #[test]
    fn missing_header() {
        let err = authenticate(None, &test_tokens()).unwrap_err();
        assert!(matches!(err, AuthError::MissingHeader));
    }

    #[test]
    fn malformed_header() {
        let err = authenticate(Some("Basic abc"), &test_tokens()).unwrap_err();
        assert!(matches!(err, AuthError::MalformedHeader));
    }

    #[test]
    fn invalid_token() {
        let err = authenticate(Some("Bearer bad"), &test_tokens()).unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn source_mismatch() {
        let creds = authenticate(Some("Bearer tok-okta"), &test_tokens()).unwrap();
        let err = verify_source(&creds, "github").unwrap_err();
        assert!(matches!(err, AuthError::SourceMismatch { .. }));
    }

    #[test]
    fn source_match() {
        let creds = authenticate(Some("Bearer tok-okta"), &test_tokens()).unwrap();
        verify_source(&creds, "okta").unwrap();
    }

    #[test]
    fn default_integration_id() {
        let creds = authenticate(Some("Bearer tok-github"), &test_tokens()).unwrap();
        assert_eq!(creds.integration_id, "default");
    }
}
