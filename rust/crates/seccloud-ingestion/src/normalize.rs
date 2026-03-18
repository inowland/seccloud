use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Deserialize)]
struct SourceMeta {
    provider: String,
    kind: String,
    action_map: BTreeMap<String, ActionMeta>,
}

#[derive(Debug, Clone, Deserialize)]
struct ActionMeta {
    verb: String,
    category: String,
}

const REQUIRED_RAW_FIELDS: &[&str] = &[
    "source",
    "source_event_id",
    "observed_at",
    "actor_email",
    "actor_name",
    "department",
    "role",
    "event_type",
    "resource_id",
    "resource_name",
    "resource_kind",
    "sensitivity",
];

const EXCLUDED_ATTRIBUTE_FIELDS: &[&str] = &[
    "source",
    "source_event_id",
    "observed_at",
    "actor_email",
    "actor_name",
    "department",
    "role",
    "event_type",
    "resource_id",
    "resource_name",
    "resource_kind",
    "sensitivity",
    "integration_id",
];

fn source_registry() -> &'static BTreeMap<String, SourceMeta> {
    static REGISTRY: OnceLock<BTreeMap<String, SourceMeta>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        [
            ("okta", include_str!("../sources/okta.toml")),
            ("gworkspace", include_str!("../sources/gworkspace.toml")),
            ("github", include_str!("../sources/github.toml")),
            ("snowflake", include_str!("../sources/snowflake.toml")),
        ]
        .into_iter()
        .map(|(source, config)| {
            let parsed: SourceMeta = toml::from_str(config)
                .unwrap_or_else(|error| panic!("invalid source config for {source}: {error}"));
            (source.to_string(), parsed)
        })
        .collect()
    })
}

fn source_meta(source: &str) -> Option<&'static SourceMeta> {
    source_registry().get(source)
}

fn action_mapping(source: &str, event_type: &str) -> Option<&'static ActionMeta> {
    source_meta(source).and_then(|meta| meta.action_map.get(event_type))
}

fn stable_digest_key(prefix: &str, parts: &[&str]) -> String {
    let serialized = parts.join("|");
    let digest = Sha256::digest(serialized.as_bytes());
    format!("{prefix}_{:x}", digest)[..prefix.len() + 1 + 24].to_string()
}

pub fn event_key(source: &str, source_event_id: &str, integration_id: Option<&str>) -> String {
    stable_digest_key(
        "evk",
        &[source, integration_id.unwrap_or(""), source_event_id],
    )
}

fn entity_key(entity_kind: &str, source: &str, native_id: &str, provider: Option<&str>) -> String {
    stable_digest_key(
        "enk",
        &[entity_kind, source, provider.unwrap_or(""), native_id],
    )
}

fn canonical_entity_id(entity_key: &str) -> String {
    format!("entity::{entity_key}")
}

fn canonical_event_id(event_key: &str) -> String {
    format!("event::{event_key}")
}

fn required_string<'a>(
    record: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, NormalizeError> {
    record
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NormalizeError::MissingField(key.to_string()))
}

fn build_attributes(record: &Map<String, Value>) -> Map<String, Value> {
    let mut attributes = BTreeMap::new();
    for (key, value) in record {
        if !EXCLUDED_ATTRIBUTE_FIELDS.contains(&key.as_str()) {
            attributes.insert(key.clone(), value.clone());
        }
    }
    attributes.into_iter().collect()
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NormalizeError {
    #[error("missing_required_fields:{0}")]
    MissingField(String),
    #[error("unsupported_source")]
    UnsupportedSource,
    #[error("unsupported_event_type")]
    UnsupportedEventType,
    #[error("invalid_record:{0}")]
    InvalidRecord(String),
}

pub fn validate_raw_event(record: &Value) -> Result<(), NormalizeError> {
    let object = record
        .as_object()
        .ok_or_else(|| NormalizeError::InvalidRecord("expected object".into()))?;
    let mut missing = Vec::new();
    for field in REQUIRED_RAW_FIELDS {
        match object.get(*field) {
            Some(Value::String(value)) if !value.is_empty() => {}
            Some(_) if *field == "source" => missing.push((*field).to_string()),
            Some(_) => missing.push((*field).to_string()),
            None => missing.push((*field).to_string()),
        }
    }
    if !missing.is_empty() {
        missing.sort();
        return Err(NormalizeError::MissingField(missing.join(",")));
    }

    let source = required_string(object, "source")?;
    let event_type = required_string(object, "event_type")?;
    if source_meta(source).is_none() {
        return Err(NormalizeError::UnsupportedSource);
    }
    if action_mapping(source, event_type).is_none() {
        return Err(NormalizeError::UnsupportedEventType);
    }
    Ok(())
}

pub fn normalize_raw_event(
    record: &Value,
    integration_id: Option<&str>,
    object_key: &str,
) -> Result<Value, NormalizeError> {
    validate_raw_event(record)?;
    let object = record
        .as_object()
        .ok_or_else(|| NormalizeError::InvalidRecord("expected object".into()))?;

    let source = required_string(object, "source")?;
    let source_event_id = required_string(object, "source_event_id")?;
    let observed_at = required_string(object, "observed_at")?;
    let actor_email = required_string(object, "actor_email")?;
    let actor_name = required_string(object, "actor_name")?;
    let department = required_string(object, "department")?;
    let role = required_string(object, "role")?;
    let resource_id = required_string(object, "resource_id")?;
    let resource_name = required_string(object, "resource_name")?;
    let resource_kind = required_string(object, "resource_kind")?;
    let sensitivity = required_string(object, "sensitivity")?;
    let event_type = required_string(object, "event_type")?;
    let source_meta = source_meta(source).ok_or(NormalizeError::UnsupportedSource)?;
    let action = action_mapping(source, event_type).ok_or(NormalizeError::UnsupportedEventType)?;

    let integration_value = object
        .get("integration_id")
        .and_then(Value::as_str)
        .or(integration_id);
    let stable_event_key = event_key(source, source_event_id, integration_value);
    let principal_entity_key = entity_key("principal", source, actor_email, Some(source));
    let resource_entity_key = entity_key(
        "resource",
        source,
        resource_id,
        Some(source_meta.provider.as_str()),
    );

    Ok(json!({
        "event_id": canonical_event_id(&stable_event_key),
        "event_key": stable_event_key,
        "integration_id": integration_value,
        "source": source,
        "source_event_id": source_event_id,
        "principal": {
            "id": actor_email,
            "entity_id": canonical_entity_id(&principal_entity_key),
            "entity_key": principal_entity_key,
            "kind": "human",
            "provider": source,
            "email": actor_email,
            "display_name": actor_name,
            "department": department,
            "attributes": {
                "role": role,
            },
        },
        "resource": {
            "id": resource_id,
            "entity_id": canonical_entity_id(&resource_entity_key),
            "entity_key": resource_entity_key,
            "kind": resource_kind,
            "provider": source_meta.provider,
            "name": resource_name,
            "sensitivity": sensitivity,
            "attributes": {},
        },
        "action": {
            "source": source,
            "verb": action.verb,
            "category": action.category,
        },
        "observed_at": observed_at,
        "environment": {
            "source_kind": source_meta.kind,
        },
        "attributes": Value::Object(build_attributes(object)),
        "evidence": {
            "source": source,
            "object_key": object_key,
            "raw_event_id": source_event_id,
            "observed_at": observed_at,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn fixtures_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../seccloud-lake/tests/fixtures/normalization")
            .canonicalize()
            .expect("normalization fixtures should resolve")
    }

    fn fixture_paths() -> Vec<PathBuf> {
        let mut fixtures: Vec<_> = fs::read_dir(fixtures_dir())
            .expect("fixture dir should be readable")
            .map(|entry| entry.expect("fixture entry should load").path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect();
        fixtures.sort();
        fixtures
    }

    #[test]
    fn normalization_matches_fixture_expectations() {
        for fixture_path in fixture_paths() {
            let fixture: Value = serde_json::from_slice(
                &fs::read(&fixture_path).expect("fixture should be readable"),
            )
            .expect("fixture should parse");
            let raw_event = fixture
                .get("raw_event")
                .expect("fixture should include raw_event");
            let expected = fixture
                .get("expected")
                .expect("fixture should include expected");

            match expected.get("kind").and_then(Value::as_str) {
                Some("normalized") => {
                    let source = raw_event
                        .get("source")
                        .and_then(Value::as_str)
                        .expect("normalized fixture should include source");
                    let source_event_id = raw_event
                        .get("source_event_id")
                        .and_then(Value::as_str)
                        .expect("normalized fixture should include source_event_id");
                    let observed_at = raw_event
                        .get("observed_at")
                        .and_then(Value::as_str)
                        .expect("normalized fixture should include observed_at");
                    let event_key = event_key(source, source_event_id, None);
                    let object_key = format!(
                        "raw/tenant=local/source={source}/integration=default/dt={}/hour={}/{}.json",
                        &observed_at[..10],
                        &observed_at[11..13],
                        event_key
                    );
                    let actual = normalize_raw_event(raw_event, None, &object_key).unwrap();
                    assert_eq!(
                        actual,
                        expected["event"],
                        "normalized fixture mismatch for {}",
                        fixture_path.display()
                    );
                }
                Some("dead_letter") => {
                    let actual = validate_raw_event(raw_event).unwrap_err();
                    assert_eq!(
                        actual.to_string(),
                        expected["reason"].as_str().unwrap(),
                        "dead-letter fixture mismatch for {}",
                        fixture_path.display()
                    );
                }
                other => panic!("unexpected fixture kind: {other:?}"),
            }
        }
    }

    #[test]
    fn source_registry_includes_all_initial_sources() {
        let sources: Vec<_> = source_registry().keys().cloned().collect();
        assert_eq!(sources, vec!["github", "gworkspace", "okta", "snowflake"]);
    }
}
