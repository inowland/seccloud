use serde::{Deserialize, Serialize};

/// An object descriptor within a manifest — describes one Parquet (or other) file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    pub object_key: String,
    pub object_format: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub record_count: usize,
    pub first_record_ordinal: usize,
    pub last_record_ordinal: usize,
}

/// Time partition identifiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub dt: String,
    pub hour: String,
}

/// Time bounds for records in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBounds {
    pub min: String,
    pub max: String,
}

/// Producer metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Producer {
    pub kind: String,
    pub run_id: String,
}

/// Checkpoint metadata stored in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub request_payload_sha256: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Checkpoint metadata stored in a normalized manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedCheckpointInfo {
    pub event_id: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Raw intake manifest — written to S3 as JSON alongside the Parquet data files.
///
/// Matches the PoC manifest format from `storage.py:654-689`, with
/// `object_format` changed from `jsonl.gz` to `parquet`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntakeManifest {
    pub manifest_version: u32,
    pub manifest_type: String,
    pub layout_version: u32,
    pub manifest_id: String,
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub produced_at: String,
    pub partition: Partition,
    pub time_bounds: TimeBounds,
    pub received_at_bounds: TimeBounds,
    pub record_count: usize,
    pub idempotency_key: String,
    pub source_event_id_hash: String,
    pub raw_envelope_version: u32,
    pub producer: Producer,
    pub checkpoint: CheckpointInfo,
    pub objects: Vec<ObjectDescriptor>,
}

/// An entry in the intake queue (DynamoDB or filesystem).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntakeQueueEntry {
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub received_at: String,
    pub record_count: usize,
    pub idempotency_key: String,
    pub payload_sha256: String,
    pub manifest_key: String,
    pub object_key: String,
    pub producer: Producer,
}

/// Normalized-event manifest written alongside normalized Parquet output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedManifest {
    pub manifest_version: u32,
    pub manifest_type: String,
    pub layout_version: u32,
    pub manifest_id: String,
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub produced_at: String,
    pub partition: Partition,
    pub time_bounds: TimeBounds,
    pub observed_at_bounds: TimeBounds,
    pub record_count: usize,
    pub idempotency_key: String,
    pub normalized_schema_version: String,
    pub event_id_min: String,
    pub event_id_max: String,
    pub upstream_raw_batches: Vec<String>,
    pub checkpoint: NormalizedCheckpointInfo,
    pub objects: Vec<ObjectDescriptor>,
}

/// Feature-table manifest written alongside feature Parquet output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureManifest {
    pub manifest_version: u32,
    pub manifest_type: String,
    pub layout_version: u32,
    pub manifest_id: String,
    pub batch_id: String,
    pub tenant_id: String,
    pub source: String,
    pub produced_at: String,
    pub partition: Partition,
    pub time_bounds: TimeBounds,
    pub record_count: usize,
    pub feature_table: String,
    pub feature_schema_version: String,
    pub upstream_normalized_batches: Vec<String>,
    pub checkpoint: NormalizedCheckpointInfo,
    pub objects: Vec<ObjectDescriptor>,
}

/// Detection manifest written alongside detection Parquet output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionManifest {
    pub manifest_version: u32,
    pub manifest_type: String,
    pub layout_version: u32,
    pub manifest_id: String,
    pub batch_id: String,
    pub tenant_id: String,
    pub produced_at: String,
    pub partition: Partition,
    pub time_bounds: TimeBounds,
    pub record_count: usize,
    pub detection_schema_version: String,
    pub upstream_detection_context_signature: String,
    pub scoring_runtime_mode: String,
    pub model_version: String,
    pub objects: Vec<ObjectDescriptor>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrips_json() {
        let manifest = IntakeManifest {
            manifest_version: 1,
            manifest_type: "raw".into(),
            layout_version: 1,
            manifest_id: "man_test".into(),
            batch_id: "raw_test".into(),
            tenant_id: "tenant-1".into(),
            source: "okta".into(),
            integration_id: "default".into(),
            produced_at: "2026-03-15T14:30:00Z".into(),
            partition: Partition {
                dt: "2026-03-15".into(),
                hour: "14".into(),
            },
            time_bounds: TimeBounds {
                min: "2026-03-15T14:00:00Z".into(),
                max: "2026-03-15T14:59:59Z".into(),
            },
            received_at_bounds: TimeBounds {
                min: "2026-03-15T14:30:00Z".into(),
                max: "2026-03-15T14:30:00Z".into(),
            },
            record_count: 42,
            idempotency_key: "push:sha256:abc".into(),
            source_event_id_hash: "sha256:def".into(),
            raw_envelope_version: 1,
            producer: Producer {
                kind: "push_gateway".into(),
                run_id: "run-1".into(),
            },
            checkpoint: CheckpointInfo {
                request_payload_sha256: "sha256:ghi".into(),
                extra: serde_json::Map::new(),
            },
            objects: vec![ObjectDescriptor {
                object_key: "lake/raw/layout=v1/...parquet".into(),
                object_format: "parquet".into(),
                sha256: "sha256:jkl".into(),
                size_bytes: 1024,
                record_count: 42,
                first_record_ordinal: 0,
                last_record_ordinal: 41,
            }],
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: IntakeManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.batch_id, "raw_test");
        assert_eq!(parsed.objects.len(), 1);
        assert_eq!(parsed.objects[0].object_format, "parquet");
    }

    #[test]
    fn queue_entry_roundtrips() {
        let entry = IntakeQueueEntry {
            batch_id: "raw_1".into(),
            tenant_id: "t".into(),
            source: "okta".into(),
            integration_id: "default".into(),
            received_at: "2026-03-15T14:30:00Z".into(),
            record_count: 10,
            idempotency_key: "key".into(),
            payload_sha256: "sha".into(),
            manifest_key: "m".into(),
            object_key: "o".into(),
            producer: Producer {
                kind: "push_gateway".into(),
                run_id: "r".into(),
            },
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: IntakeQueueEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.batch_id, "raw_1");
    }

    #[test]
    fn normalized_manifest_roundtrips_json() {
        let manifest = NormalizedManifest {
            manifest_version: 1,
            manifest_type: "normalized".into(),
            layout_version: 1,
            manifest_id: "man_norm".into(),
            batch_id: "norm_1".into(),
            tenant_id: "tenant-1".into(),
            source: "okta".into(),
            integration_id: "default".into(),
            produced_at: "2026-03-15T14:30:00Z".into(),
            partition: Partition {
                dt: "2026-03-15".into(),
                hour: "14".into(),
            },
            time_bounds: TimeBounds {
                min: "2026-03-15T14:00:00Z".into(),
                max: "2026-03-15T14:59:59Z".into(),
            },
            observed_at_bounds: TimeBounds {
                min: "2026-03-15T14:05:00Z".into(),
                max: "2026-03-15T14:25:00Z".into(),
            },
            record_count: 2,
            idempotency_key: "norm:key".into(),
            normalized_schema_version: "event.v1".into(),
            event_id_min: "evt_1".into(),
            event_id_max: "evt_2".into(),
            upstream_raw_batches: vec!["raw_1".into()],
            checkpoint: NormalizedCheckpointInfo {
                event_id: "evt_2".into(),
                extra: serde_json::Map::new(),
            },
            objects: vec![ObjectDescriptor {
                object_key: "lake/normalized/layout=v1/...parquet".into(),
                object_format: "parquet".into(),
                sha256: "sha256:jkl".into(),
                size_bytes: 1024,
                record_count: 2,
                first_record_ordinal: 0,
                last_record_ordinal: 1,
            }],
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: NormalizedManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.manifest_type, "normalized");
        assert_eq!(parsed.normalized_schema_version, "event.v1");
        assert_eq!(parsed.upstream_raw_batches, vec!["raw_1"]);
    }

    #[test]
    fn feature_manifest_roundtrips_json() {
        let manifest = FeatureManifest {
            manifest_version: 1,
            manifest_type: "feature".into(),
            layout_version: 1,
            manifest_id: "man_feat".into(),
            batch_id: "feat_1".into(),
            tenant_id: "tenant-1".into(),
            source: "gworkspace".into(),
            produced_at: "2026-03-15T14:30:00Z".into(),
            partition: Partition {
                dt: "2026-03-15".into(),
                hour: "14".into(),
            },
            time_bounds: TimeBounds {
                min: "2026-03-15T14:00:00Z".into(),
                max: "2026-03-15T14:59:59Z".into(),
            },
            record_count: 2,
            feature_table: "action-accessor-set".into(),
            feature_schema_version: "feature.v1".into(),
            upstream_normalized_batches: vec!["norm_1".into()],
            checkpoint: NormalizedCheckpointInfo {
                event_id: "evt_2".into(),
                extra: serde_json::Map::new(),
            },
            objects: vec![ObjectDescriptor {
                object_key: "lake/features/layout=v1/...parquet".into(),
                object_format: "parquet".into(),
                sha256: "sha256:jkl".into(),
                size_bytes: 1024,
                record_count: 2,
                first_record_ordinal: 0,
                last_record_ordinal: 1,
            }],
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: FeatureManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.manifest_type, "feature");
        assert_eq!(parsed.feature_table, "action-accessor-set");
        assert_eq!(parsed.feature_schema_version, "feature.v1");
    }

    #[test]
    fn detection_manifest_roundtrips_json() {
        let manifest = DetectionManifest {
            manifest_version: 1,
            manifest_type: "detection".into(),
            layout_version: 1,
            manifest_id: "man_det".into(),
            batch_id: "det_1".into(),
            tenant_id: "tenant-1".into(),
            produced_at: "2026-03-15T14:30:00Z".into(),
            partition: Partition {
                dt: "2026-03-15".into(),
                hour: "14".into(),
            },
            time_bounds: TimeBounds {
                min: "2026-03-15T14:05:00Z".into(),
                max: "2026-03-15T14:25:00Z".into(),
            },
            record_count: 1,
            detection_schema_version: "detection.v1".into(),
            upstream_detection_context_signature: "sig-1".into(),
            scoring_runtime_mode: "onnx_native".into(),
            model_version: "demo-v1".into(),
            objects: vec![ObjectDescriptor {
                object_key: "lake/detections/layout=v1/...parquet".into(),
                object_format: "parquet".into(),
                sha256: "sha256:jkl".into(),
                size_bytes: 1024,
                record_count: 1,
                first_record_ordinal: 0,
                last_record_ordinal: 0,
            }],
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: DetectionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.batch_id, "det_1");
        assert_eq!(parsed.model_version, "demo-v1");
        assert_eq!(parsed.scoring_runtime_mode, "onnx_native");
    }
}
