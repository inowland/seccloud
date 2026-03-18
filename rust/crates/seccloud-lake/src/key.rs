use chrono::{DateTime, Utc};

/// Normalize a partition value to match the PoC `_partition_value` behavior:
/// strip, lowercase, replace `/` and spaces with `_`.
pub fn partition_value(value: Option<&str>) -> String {
    let raw = value.unwrap_or("default").trim().to_lowercase();
    raw.replace(['/', ' '], "_")
}

/// S3 object key for a raw batch Parquet file.
///
/// Layout: `lake/raw/layout=v1/tenant={t}/source={s}/integration={i}/dt={d}/hour={h}/batch={b}/part-00000.parquet`
pub fn raw_batch_object_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    batch_id: &str,
    received_at: &DateTime<Utc>,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "lake/raw/layout=v1/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/batch={batch_id}/part-00000.parquet",
        received_at.format("%Y-%m-%d"),
        received_at.format("%H"),
    )
}

/// S3 object key for a dead-letter batch Parquet file.
pub fn dead_letter_object_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    batch_id: &str,
    received_at: &DateTime<Utc>,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "lake/dead-letter/layout=v1/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/batch={batch_id}/part-00000.parquet",
        received_at.format("%Y-%m-%d"),
        received_at.format("%H"),
    )
}

/// S3 key for a raw intake manifest JSON file.
pub fn raw_manifest_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    batch_id: &str,
    received_at: &DateTime<Utc>,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "lake/manifests/layout=v1/type=raw/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/batch={batch_id}.json",
        received_at.format("%Y-%m-%d"),
        received_at.format("%H"),
    )
}

/// S3 object key for a normalized batch Parquet file.
pub fn normalized_batch_object_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    observed_at: &DateTime<Utc>,
    batch_id: &str,
    schema_version: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "lake/normalized/layout=v1/schema={schema_version}/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/batch={batch_id}/part-00000.parquet",
        observed_at.format("%Y-%m-%d"),
        observed_at.format("%H"),
    )
}

/// S3 key for a normalized manifest JSON file.
pub fn normalized_manifest_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    observed_at: &DateTime<Utc>,
    batch_id: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "lake/manifests/layout=v1/type=normalized/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/batch={batch_id}.json",
        observed_at.format("%Y-%m-%d"),
        observed_at.format("%H"),
    )
}

/// S3 object key for a feature batch Parquet file.
pub fn feature_batch_object_key(
    tenant_id: &str,
    table_name: &str,
    source: Option<&str>,
    generated_at: &DateTime<Utc>,
    batch_id: &str,
    schema_version: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let source = partition_value(source);
    format!(
        "lake/features/layout=v1/table={table_name}/schema={schema_version}/tenant={tenant}/source={source}/dt={}/hour={}/batch={batch_id}/part-00000.parquet",
        generated_at.format("%Y-%m-%d"),
        generated_at.format("%H"),
    )
}

/// S3 key for a feature manifest JSON file.
pub fn feature_manifest_key(
    tenant_id: &str,
    table_name: &str,
    source: Option<&str>,
    generated_at: &DateTime<Utc>,
    batch_id: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let source = partition_value(source);
    format!(
        "lake/manifests/layout=v1/type=feature/table={table_name}/tenant={tenant}/source={source}/dt={}/hour={}/batch={batch_id}.json",
        generated_at.format("%Y-%m-%d"),
        generated_at.format("%H"),
    )
}

/// S3 object key for a detection batch Parquet file.
pub fn detection_batch_object_key(
    tenant_id: &str,
    detected_at: &DateTime<Utc>,
    batch_id: &str,
    schema_version: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    format!(
        "lake/detections/layout=v1/schema={schema_version}/tenant={tenant}/dt={}/hour={}/batch={batch_id}/part-00000.parquet",
        detected_at.format("%Y-%m-%d"),
        detected_at.format("%H"),
    )
}

/// S3 key for a detection manifest JSON file.
pub fn detection_manifest_key(
    tenant_id: &str,
    detected_at: &DateTime<Utc>,
    batch_id: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    format!(
        "lake/manifests/layout=v1/type=detection/tenant={tenant}/dt={}/hour={}/batch={batch_id}.json",
        detected_at.format("%Y-%m-%d"),
        detected_at.format("%H"),
    )
}

/// Object key for a landed raw event JSON document.
pub fn raw_event_json_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    received_at: &DateTime<Utc>,
    event_key: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "raw/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/{}.json",
        received_at.format("%Y-%m-%d"),
        received_at.format("%H"),
        event_key
    )
}

/// Object key for a normalized event JSON document.
pub fn normalized_event_json_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    observed_at: &DateTime<Utc>,
    event_id: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "normalized/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/{}.json",
        observed_at.format("%Y-%m-%d"),
        observed_at.format("%H"),
        event_id
    )
}

/// Object key for a dead-letter JSON document.
pub fn dead_letter_json_key(
    tenant_id: &str,
    source: &str,
    integration_id: Option<&str>,
    received_at: &DateTime<Utc>,
    dead_letter_id: &str,
) -> String {
    let tenant = partition_value(Some(tenant_id));
    let integration = partition_value(integration_id);
    format!(
        "dead_letters/tenant={tenant}/source={source}/integration={integration}/dt={}/hour={}/{}.json",
        received_at.format("%Y-%m-%d"),
        received_at.format("%H"),
        dead_letter_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 15, 14, 30, 0).unwrap()
    }

    #[test]
    fn partition_value_normalizes() {
        assert_eq!(partition_value(Some("Tenant-One")), "tenant-one");
        assert_eq!(partition_value(Some("a/b c")), "a_b_c");
        assert_eq!(partition_value(None), "default");
        assert_eq!(partition_value(Some("  X  ")), "x");
    }

    #[test]
    fn raw_batch_key() {
        let key = raw_batch_object_key("tenant-1", "okta", Some("okta-primary"), "raw_abc", &ts());
        assert_eq!(
            key,
            "lake/raw/layout=v1/tenant=tenant-1/source=okta/integration=okta-primary/dt=2026-03-15/hour=14/batch=raw_abc/part-00000.parquet"
        );
    }

    #[test]
    fn raw_batch_key_default_integration() {
        let key = raw_batch_object_key("t", "github", None, "raw_1", &ts());
        assert!(key.contains("integration=default"));
    }

    #[test]
    fn dead_letter_key() {
        let key = dead_letter_object_key("t", "okta", None, "raw_dl1", &ts());
        assert!(key.starts_with("lake/dead-letter/layout=v1/"));
        assert!(key.ends_with("part-00000.parquet"));
    }

    #[test]
    fn manifest_key() {
        let key = raw_manifest_key("t", "okta", None, "raw_m1", &ts());
        assert!(key.starts_with("lake/manifests/layout=v1/type=raw/"));
        assert!(key.ends_with("batch=raw_m1.json"));
    }

    #[test]
    fn normalized_batch_key() {
        let key = normalized_batch_object_key("t", "okta", None, &ts(), "norm_1", "event.v1");
        assert!(key.starts_with("lake/normalized/layout=v1/schema=event.v1/"));
        assert!(key.ends_with("batch=norm_1/part-00000.parquet"));
    }

    #[test]
    fn normalized_manifest_key_path() {
        let key = normalized_manifest_key("t", "okta", None, &ts(), "norm_1");
        assert!(key.starts_with("lake/manifests/layout=v1/type=normalized/"));
        assert!(key.ends_with("batch=norm_1.json"));
    }

    #[test]
    fn feature_batch_key_path() {
        let key = feature_batch_object_key(
            "t",
            "action-accessor-set",
            Some("gworkspace"),
            &ts(),
            "feat_1",
            "feature.v1",
        );
        assert!(
            key.starts_with("lake/features/layout=v1/table=action-accessor-set/schema=feature.v1/")
        );
        assert!(key.ends_with("batch=feat_1/part-00000.parquet"));
    }

    #[test]
    fn feature_manifest_key_path() {
        let key = feature_manifest_key("t", "history-window", None, &ts(), "feat_2");
        assert!(key.starts_with("lake/manifests/layout=v1/type=feature/table=history-window/"));
        assert!(key.ends_with("batch=feat_2.json"));
    }

    #[test]
    fn detection_batch_key_path() {
        let key = detection_batch_object_key("t", &ts(), "det_1", "detection.v1");
        assert!(key.starts_with("lake/detections/layout=v1/schema=detection.v1/"));
        assert!(key.ends_with("batch=det_1/part-00000.parquet"));
    }

    #[test]
    fn detection_manifest_key_path() {
        let key = detection_manifest_key("t", &ts(), "det_1");
        assert!(key.starts_with("lake/manifests/layout=v1/type=detection/"));
        assert!(key.ends_with("batch=det_1.json"));
    }

    #[test]
    fn raw_event_json_path() {
        let key = raw_event_json_key("t", "okta", None, &ts(), "evk_1");
        assert_eq!(
            key,
            "raw/tenant=t/source=okta/integration=default/dt=2026-03-15/hour=14/evk_1.json"
        );
    }

    #[test]
    fn normalized_event_json_path() {
        let key = normalized_event_json_key("t", "okta", None, &ts(), "evt_1");
        assert_eq!(
            key,
            "normalized/tenant=t/source=okta/integration=default/dt=2026-03-15/hour=14/evt_1.json"
        );
    }

    #[test]
    fn dead_letter_json_path() {
        let key = dead_letter_json_key("t", "okta", None, &ts(), "okta-e1");
        assert_eq!(
            key,
            "dead_letters/tenant=t/source=okta/integration=default/dt=2026-03-15/hour=14/okta-e1.json"
        );
    }
}
