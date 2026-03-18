use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub const NORMALIZED_SCHEMA_VERSION: &str = "event.v1";
pub const FEATURE_SCHEMA_VERSION: &str = "feature.v1";
pub const DETECTION_SCHEMA_VERSION: &str = "detection.v1";

/// A single raw event row in the Parquet file.
///
/// Matches the PoC `raw_envelope` structure from `storage.py:627-641`.
#[derive(Debug, Clone)]
pub struct RawEventRow {
    pub raw_envelope_version: i32,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub intake_kind: String,
    pub batch_id: String,
    pub received_at: String,
    pub record_ordinal: i32,
    pub metadata_json: String,
    pub record_json: String,
}

/// A dead-lettered event row — same as [`RawEventRow`] plus a reason.
#[derive(Debug, Clone)]
pub struct DeadLetterRow {
    pub raw: RawEventRow,
    pub dead_letter_reason: String,
}

/// A single normalized event row in the Parquet file.
#[derive(Debug, Clone)]
pub struct NormalizedEventRow {
    pub normalized_schema_version: String,
    pub tenant_id: String,
    pub source: String,
    pub integration_id: String,
    pub observed_at: String,
    pub event_id: String,
    pub event_key: String,
    pub source_event_id: String,
    pub principal_entity_key: String,
    pub resource_entity_key: String,
    pub action_source: String,
    pub action_verb: String,
    pub action_category: String,
    pub payload_json: String,
}

/// A single action accessor-set feature row.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionFeatureRow {
    pub feature_schema_version: String,
    pub tenant_id: String,
    pub source: String,
    pub resource_entity_key: String,
    pub principal_entity_key: String,
    pub access_count: i64,
    pub accessor_weight: f64,
}

/// A single history-window feature row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFeatureRow {
    pub feature_schema_version: String,
    pub tenant_id: String,
    pub principal_entity_key: String,
    pub window_start: String,
    pub resource_entity_key: String,
}

/// A single collaboration feature row.
#[derive(Debug, Clone, PartialEq)]
pub struct CollaborationFeatureRow {
    pub feature_schema_version: String,
    pub tenant_id: String,
    pub principal_entity_key: String,
    pub collaborator_entity_key: String,
    pub co_access_weight: f64,
}

/// A single principal static feature row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticPrincipalFeatureRow {
    pub feature_schema_version: String,
    pub tenant_id: String,
    pub principal_entity_key: String,
    pub principal_id: String,
    pub department: String,
    pub role: String,
    pub location: String,
    pub employment_duration_bucket: String,
    pub privilege_level: String,
}

/// A single peer-group feature row.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerGroupFeatureRow {
    pub feature_schema_version: String,
    pub tenant_id: String,
    pub principal_entity_key: String,
    pub peer_entity_key: String,
    pub peer_type: String,
    pub peer_weight: f64,
}

/// A single detection row.
#[derive(Debug, Clone, PartialEq)]
pub struct DetectionRow {
    pub detection_schema_version: String,
    pub tenant_id: String,
    pub detection_id: String,
    pub observed_at: String,
    pub scenario: String,
    pub title: String,
    pub score: f64,
    pub confidence: f64,
    pub severity: String,
    pub status: String,
    pub model_version: String,
    pub primary_event_id: String,
    pub payload_json: String,
}

/// Arrow schema for raw event Parquet files.
pub fn raw_event_schema() -> Schema {
    Schema::new(vec![
        Field::new("raw_envelope_version", DataType::Int32, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("integration_id", DataType::Utf8, false),
        Field::new("intake_kind", DataType::Utf8, false),
        Field::new("batch_id", DataType::Utf8, false),
        Field::new("received_at", DataType::Utf8, false),
        Field::new("record_ordinal", DataType::Int32, false),
        Field::new("metadata_json", DataType::Utf8, false),
        Field::new("record_json", DataType::Utf8, false),
    ])
}

/// Arrow schema for dead-letter Parquet files (raw schema + reason column).
pub fn dead_letter_schema() -> Schema {
    let mut fields: Vec<Arc<Field>> = raw_event_schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "dead_letter_reason",
        DataType::Utf8,
        false,
    )));
    Schema::new(fields)
}

/// Arrow schema for normalized event Parquet files.
pub fn normalized_event_schema() -> Schema {
    Schema::new(vec![
        Field::new("normalized_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("integration_id", DataType::Utf8, false),
        Field::new("observed_at", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("event_key", DataType::Utf8, false),
        Field::new("source_event_id", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("resource_entity_key", DataType::Utf8, false),
        Field::new("action_source", DataType::Utf8, false),
        Field::new("action_verb", DataType::Utf8, false),
        Field::new("action_category", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
    ])
}

/// Arrow schema for action accessor-set feature files.
pub fn action_feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("resource_entity_key", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("access_count", DataType::Int64, false),
        Field::new("accessor_weight", DataType::Float64, false),
    ])
}

/// Arrow schema for history-window feature files.
pub fn history_feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("window_start", DataType::Utf8, false),
        Field::new("resource_entity_key", DataType::Utf8, false),
    ])
}

/// Arrow schema for collaboration feature files.
pub fn collaboration_feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("collaborator_entity_key", DataType::Utf8, false),
        Field::new("co_access_weight", DataType::Float64, false),
    ])
}

/// Arrow schema for principal static feature files.
pub fn static_principal_feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("principal_id", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("employment_duration_bucket", DataType::Utf8, false),
        Field::new("privilege_level", DataType::Utf8, false),
    ])
}

/// Arrow schema for peer-group feature files.
pub fn peer_group_feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("feature_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("principal_entity_key", DataType::Utf8, false),
        Field::new("peer_entity_key", DataType::Utf8, false),
        Field::new("peer_type", DataType::Utf8, false),
        Field::new("peer_weight", DataType::Float64, false),
    ])
}

/// Arrow schema for detection files.
pub fn detection_schema() -> Schema {
    Schema::new(vec![
        Field::new("detection_schema_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("detection_id", DataType::Utf8, false),
        Field::new("observed_at", DataType::Utf8, false),
        Field::new("scenario", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
        Field::new("confidence", DataType::Float64, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("model_version", DataType::Utf8, false),
        Field::new("primary_event_id", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
    ])
}

/// Build an Arrow [`RecordBatch`] from raw event rows.
pub fn raw_event_record_batch(rows: &[RawEventRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(raw_event_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.raw_envelope_version),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.source.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.integration_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.intake_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.batch_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.received_at.as_str()),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.record_ordinal),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.metadata_json.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.record_json.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from dead-letter rows.
pub fn dead_letter_record_batch(rows: &[DeadLetterRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(dead_letter_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.raw.raw_envelope_version),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.source.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.integration_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.intake_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.batch_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.received_at.as_str()),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.raw.record_ordinal),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.metadata_json.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.raw.record_json.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.dead_letter_reason.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from normalized event rows.
pub fn normalized_event_record_batch(rows: &[NormalizedEventRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(normalized_event_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.normalized_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.source.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.integration_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.observed_at.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.event_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.event_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.source_event_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.resource_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.action_source.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.action_verb.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.action_category.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.payload_json.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from action feature rows.
pub fn action_feature_record_batch(rows: &[ActionFeatureRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(action_feature_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.feature_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.source.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.resource_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.access_count),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.accessor_weight),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from history feature rows.
pub fn history_feature_record_batch(rows: &[HistoryFeatureRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(history_feature_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.feature_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.window_start.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.resource_entity_key.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from collaboration feature rows.
pub fn collaboration_feature_record_batch(
    rows: &[CollaborationFeatureRow],
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(collaboration_feature_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.feature_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.collaborator_entity_key.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.co_access_weight),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from principal static feature rows.
pub fn static_principal_feature_record_batch(
    rows: &[StaticPrincipalFeatureRow],
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(static_principal_feature_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.feature_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.department.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.role.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.location.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.employment_duration_bucket.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.privilege_level.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from peer-group feature rows.
pub fn peer_group_feature_record_batch(
    rows: &[PeerGroupFeatureRow],
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(peer_group_feature_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.feature_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.principal_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.peer_entity_key.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.peer_type.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.peer_weight),
            )),
        ],
    )?;
    Ok(batch)
}

/// Build an Arrow [`RecordBatch`] from detection rows.
pub fn detection_record_batch(rows: &[DetectionRow]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(detection_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.detection_schema_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.tenant_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.detection_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.observed_at.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.scenario.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.title.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.score))),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.confidence),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.severity.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.status.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.model_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.primary_event_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.payload_json.as_str()),
            )),
        ],
    )?;
    Ok(batch)
}

/// Serialize a [`RecordBatch`] to Parquet bytes (ZSTD compressed).
pub fn write_parquet_bytes(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

/// Read Parquet bytes back into [`RecordBatch`]es.
pub fn read_parquet_bytes(data: &[u8]) -> anyhow::Result<Vec<RecordBatch>> {
    let data: bytes::Bytes = data.to_vec().into();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(data, 1024)?;
    let batches: Result<Vec<_>, _> = reader.collect();
    Ok(batches?)
}

/// Extract [`RawEventRow`]s from a [`RecordBatch`] that has the raw event schema.
pub fn record_batch_to_rows(batch: &RecordBatch) -> anyhow::Result<Vec<RawEventRow>> {
    let n = batch.num_rows();
    let version_col = batch
        .column_by_name("raw_envelope_version")
        .ok_or_else(|| anyhow::anyhow!("missing raw_envelope_version column"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("raw_envelope_version is not Int32"))?;

    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let tenant_id = str_col!("tenant_id");
    let source = str_col!("source");
    let integration_id = str_col!("integration_id");
    let intake_kind = str_col!("intake_kind");
    let batch_id = str_col!("batch_id");
    let received_at = str_col!("received_at");
    let record_ordinal = batch
        .column_by_name("record_ordinal")
        .ok_or_else(|| anyhow::anyhow!("missing record_ordinal"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("record_ordinal is not Int32"))?;
    let metadata_json = str_col!("metadata_json");
    let record_json = str_col!("record_json");

    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        rows.push(RawEventRow {
            raw_envelope_version: version_col.value(i),
            tenant_id: tenant_id.value(i).to_string(),
            source: source.value(i).to_string(),
            integration_id: integration_id.value(i).to_string(),
            intake_kind: intake_kind.value(i).to_string(),
            batch_id: batch_id.value(i).to_string(),
            received_at: received_at.value(i).to_string(),
            record_ordinal: record_ordinal.value(i),
            metadata_json: metadata_json.value(i).to_string(),
            record_json: record_json.value(i).to_string(),
        });
    }
    Ok(rows)
}

fn value_at_path<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> anyhow::Result<&'a serde_json::Value> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .ok_or_else(|| anyhow::anyhow!("missing field: {}", path.join(".")))?;
    }
    Ok(current)
}

fn string_at_path(value: &serde_json::Value, path: &[&str]) -> anyhow::Result<String> {
    value_at_path(value, path)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("field is not a string: {}", path.join(".")))
}

/// Convert a normalized event payload into a row-oriented representation for Parquet.
pub fn normalized_event_row_from_value(
    event: &serde_json::Value,
    tenant_id: &str,
    integration_id: &str,
    schema_version: &str,
) -> anyhow::Result<NormalizedEventRow> {
    Ok(NormalizedEventRow {
        normalized_schema_version: schema_version.to_string(),
        tenant_id: tenant_id.to_string(),
        source: string_at_path(event, &["source"])?,
        integration_id: integration_id.to_string(),
        observed_at: string_at_path(event, &["observed_at"])?,
        event_id: string_at_path(event, &["event_id"])?,
        event_key: string_at_path(event, &["event_key"])?,
        source_event_id: string_at_path(event, &["source_event_id"])?,
        principal_entity_key: string_at_path(event, &["principal", "entity_key"])?,
        resource_entity_key: string_at_path(event, &["resource", "entity_key"])?,
        action_source: string_at_path(event, &["action", "source"])?,
        action_verb: string_at_path(event, &["action", "verb"])?,
        action_category: string_at_path(event, &["action", "category"])?,
        payload_json: serde_json::to_string(event)?,
    })
}

/// Extract [`NormalizedEventRow`]s from a [`RecordBatch`] that has the normalized schema.
pub fn normalized_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<NormalizedEventRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let normalized_schema_version = str_col!("normalized_schema_version");
    let tenant_id = str_col!("tenant_id");
    let source = str_col!("source");
    let integration_id = str_col!("integration_id");
    let observed_at = str_col!("observed_at");
    let event_id = str_col!("event_id");
    let event_key = str_col!("event_key");
    let source_event_id = str_col!("source_event_id");
    let principal_entity_key = str_col!("principal_entity_key");
    let resource_entity_key = str_col!("resource_entity_key");
    let action_source = str_col!("action_source");
    let action_verb = str_col!("action_verb");
    let action_category = str_col!("action_category");
    let payload_json = str_col!("payload_json");

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(NormalizedEventRow {
            normalized_schema_version: normalized_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            source: source.value(i).to_string(),
            integration_id: integration_id.value(i).to_string(),
            observed_at: observed_at.value(i).to_string(),
            event_id: event_id.value(i).to_string(),
            event_key: event_key.value(i).to_string(),
            source_event_id: source_event_id.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            resource_entity_key: resource_entity_key.value(i).to_string(),
            action_source: action_source.value(i).to_string(),
            action_verb: action_verb.value(i).to_string(),
            action_category: action_category.value(i).to_string(),
            payload_json: payload_json.value(i).to_string(),
        });
    }
    Ok(rows)
}

/// Extract [`ActionFeatureRow`]s from a feature action [`RecordBatch`].
pub fn action_feature_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<ActionFeatureRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let feature_schema_version = str_col!("feature_schema_version");
    let tenant_id = str_col!("tenant_id");
    let source = str_col!("source");
    let resource_entity_key = str_col!("resource_entity_key");
    let principal_entity_key = str_col!("principal_entity_key");
    let access_count = batch
        .column_by_name("access_count")
        .ok_or_else(|| anyhow::anyhow!("missing access_count"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("access_count is not Int64"))?;
    let accessor_weight = batch
        .column_by_name("accessor_weight")
        .ok_or_else(|| anyhow::anyhow!("missing accessor_weight"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("accessor_weight is not Float64"))?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(ActionFeatureRow {
            feature_schema_version: feature_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            source: source.value(i).to_string(),
            resource_entity_key: resource_entity_key.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            access_count: access_count.value(i),
            accessor_weight: accessor_weight.value(i),
        });
    }
    Ok(rows)
}

/// Extract [`HistoryFeatureRow`]s from a feature history [`RecordBatch`].
pub fn history_feature_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<HistoryFeatureRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let feature_schema_version = str_col!("feature_schema_version");
    let tenant_id = str_col!("tenant_id");
    let principal_entity_key = str_col!("principal_entity_key");
    let window_start = str_col!("window_start");
    let resource_entity_key = str_col!("resource_entity_key");

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(HistoryFeatureRow {
            feature_schema_version: feature_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            window_start: window_start.value(i).to_string(),
            resource_entity_key: resource_entity_key.value(i).to_string(),
        });
    }
    Ok(rows)
}

/// Extract [`CollaborationFeatureRow`]s from a feature collaboration [`RecordBatch`].
pub fn collaboration_feature_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<CollaborationFeatureRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let feature_schema_version = str_col!("feature_schema_version");
    let tenant_id = str_col!("tenant_id");
    let principal_entity_key = str_col!("principal_entity_key");
    let collaborator_entity_key = str_col!("collaborator_entity_key");
    let co_access_weight = batch
        .column_by_name("co_access_weight")
        .ok_or_else(|| anyhow::anyhow!("missing co_access_weight"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("co_access_weight is not Float64"))?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(CollaborationFeatureRow {
            feature_schema_version: feature_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            collaborator_entity_key: collaborator_entity_key.value(i).to_string(),
            co_access_weight: co_access_weight.value(i),
        });
    }
    Ok(rows)
}

/// Extract [`StaticPrincipalFeatureRow`]s from a feature principal-static [`RecordBatch`].
pub fn static_principal_feature_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<StaticPrincipalFeatureRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let feature_schema_version = str_col!("feature_schema_version");
    let tenant_id = str_col!("tenant_id");
    let principal_entity_key = str_col!("principal_entity_key");
    let principal_id = str_col!("principal_id");
    let department = str_col!("department");
    let role = str_col!("role");
    let location = str_col!("location");
    let employment_duration_bucket = str_col!("employment_duration_bucket");
    let privilege_level = str_col!("privilege_level");

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(StaticPrincipalFeatureRow {
            feature_schema_version: feature_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            principal_id: principal_id.value(i).to_string(),
            department: department.value(i).to_string(),
            role: role.value(i).to_string(),
            location: location.value(i).to_string(),
            employment_duration_bucket: employment_duration_bucket.value(i).to_string(),
            privilege_level: privilege_level.value(i).to_string(),
        });
    }
    Ok(rows)
}

/// Extract [`PeerGroupFeatureRow`]s from a feature peer-group [`RecordBatch`].
pub fn peer_group_feature_record_batch_to_rows(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<PeerGroupFeatureRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let feature_schema_version = str_col!("feature_schema_version");
    let tenant_id = str_col!("tenant_id");
    let principal_entity_key = str_col!("principal_entity_key");
    let peer_entity_key = str_col!("peer_entity_key");
    let peer_type = str_col!("peer_type");
    let peer_weight = batch
        .column_by_name("peer_weight")
        .ok_or_else(|| anyhow::anyhow!("missing peer_weight"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("peer_weight is not Float64"))?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(PeerGroupFeatureRow {
            feature_schema_version: feature_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            principal_entity_key: principal_entity_key.value(i).to_string(),
            peer_entity_key: peer_entity_key.value(i).to_string(),
            peer_type: peer_type.value(i).to_string(),
            peer_weight: peer_weight.value(i),
        });
    }
    Ok(rows)
}

/// Extract [`DetectionRow`]s from a detection [`RecordBatch`].
pub fn detection_record_batch_to_rows(batch: &RecordBatch) -> anyhow::Result<Vec<DetectionRow>> {
    macro_rules! str_col {
        ($name:expr) => {
            batch
                .column_by_name($name)
                .ok_or_else(|| anyhow::anyhow!(concat!("missing column: ", $name)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!(concat!($name, " is not Utf8")))?
        };
    }

    let detection_schema_version = str_col!("detection_schema_version");
    let tenant_id = str_col!("tenant_id");
    let detection_id = str_col!("detection_id");
    let observed_at = str_col!("observed_at");
    let scenario = str_col!("scenario");
    let title = str_col!("title");
    let score = batch
        .column_by_name("score")
        .ok_or_else(|| anyhow::anyhow!("missing score"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("score is not Float64"))?;
    let confidence = batch
        .column_by_name("confidence")
        .ok_or_else(|| anyhow::anyhow!("missing confidence"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("confidence is not Float64"))?;
    let severity = str_col!("severity");
    let status = str_col!("status");
    let model_version = str_col!("model_version");
    let primary_event_id = str_col!("primary_event_id");
    let payload_json = str_col!("payload_json");

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(DetectionRow {
            detection_schema_version: detection_schema_version.value(i).to_string(),
            tenant_id: tenant_id.value(i).to_string(),
            detection_id: detection_id.value(i).to_string(),
            observed_at: observed_at.value(i).to_string(),
            scenario: scenario.value(i).to_string(),
            title: title.value(i).to_string(),
            score: score.value(i),
            confidence: confidence.value(i),
            severity: severity.value(i).to_string(),
            status: status.value(i).to_string(),
            model_version: model_version.value(i).to_string(),
            primary_event_id: primary_event_id.value(i).to_string(),
            payload_json: payload_json.value(i).to_string(),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rows() -> Vec<RawEventRow> {
        vec![
            RawEventRow {
                raw_envelope_version: 1,
                tenant_id: "tenant-1".into(),
                source: "okta".into(),
                integration_id: "okta-primary".into(),
                intake_kind: "push_gateway".into(),
                batch_id: "raw_abc".into(),
                received_at: "2026-03-15T14:30:00Z".into(),
                record_ordinal: 0,
                metadata_json: "{}".into(),
                record_json: r#"{"source_event_id":"e1","observed_at":"2026-03-15T14:00:00Z","event_type":"login"}"#.into(),
            },
            RawEventRow {
                raw_envelope_version: 1,
                tenant_id: "tenant-1".into(),
                source: "okta".into(),
                integration_id: "okta-primary".into(),
                intake_kind: "push_gateway".into(),
                batch_id: "raw_abc".into(),
                received_at: "2026-03-15T14:30:00Z".into(),
                record_ordinal: 1,
                metadata_json: "{}".into(),
                record_json: r#"{"source_event_id":"e2","observed_at":"2026-03-15T14:01:00Z","event_type":"logout"}"#.into(),
            },
        ]
    }

    #[test]
    fn schema_has_expected_columns() {
        let schema = raw_event_schema();
        assert_eq!(schema.fields().len(), 10);
        assert!(schema.field_with_name("raw_envelope_version").is_ok());
        assert!(schema.field_with_name("record_json").is_ok());
    }

    #[test]
    fn dead_letter_schema_has_reason() {
        let schema = dead_letter_schema();
        assert_eq!(schema.fields().len(), 11);
        assert!(schema.field_with_name("dead_letter_reason").is_ok());
    }

    #[test]
    fn normalized_schema_has_expected_columns() {
        let schema = normalized_event_schema();
        assert_eq!(schema.fields().len(), 14);
        assert!(schema.field_with_name("event_key").is_ok());
        assert!(schema.field_with_name("payload_json").is_ok());
    }

    #[test]
    fn feature_schemas_have_expected_columns() {
        let action = action_feature_schema();
        assert_eq!(action.fields().len(), 7);
        assert!(action.field_with_name("accessor_weight").is_ok());

        let history = history_feature_schema();
        assert_eq!(history.fields().len(), 5);
        assert!(history.field_with_name("window_start").is_ok());

        let collaboration = collaboration_feature_schema();
        assert_eq!(collaboration.fields().len(), 5);
        assert!(collaboration.field_with_name("co_access_weight").is_ok());

        let static_principal = static_principal_feature_schema();
        assert_eq!(static_principal.fields().len(), 9);
        assert!(static_principal.field_with_name("privilege_level").is_ok());

        let peer_group = peer_group_feature_schema();
        assert_eq!(peer_group.fields().len(), 6);
        assert!(peer_group.field_with_name("peer_type").is_ok());
    }

    #[test]
    fn record_batch_creation() {
        let rows = sample_rows();
        let batch = raw_event_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn parquet_round_trip() {
        let rows = sample_rows();
        let batch = raw_event_record_batch(&rows).unwrap();

        let parquet_bytes = write_parquet_bytes(&batch).unwrap();
        assert!(!parquet_bytes.is_empty());

        let batches = read_parquet_bytes(&parquet_bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let read_rows = record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows.len(), 2);
        assert_eq!(read_rows[0].tenant_id, "tenant-1");
        assert_eq!(read_rows[0].source, "okta");
        assert_eq!(read_rows[0].record_ordinal, 0);
        assert_eq!(read_rows[1].record_ordinal, 1);

        // Verify record_json roundtrips correctly
        let r0: serde_json::Value = serde_json::from_str(&read_rows[0].record_json).unwrap();
        assert_eq!(r0["source_event_id"], "e1");
        assert_eq!(r0["event_type"], "login");
    }

    #[test]
    fn dead_letter_record_batch_creation() {
        let rows = vec![DeadLetterRow {
            raw: sample_rows().remove(0),
            dead_letter_reason: "invalid schema".into(),
        }];
        let batch = dead_letter_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn dead_letter_parquet_round_trip() {
        let rows = vec![DeadLetterRow {
            raw: sample_rows().remove(0),
            dead_letter_reason: "bad field".into(),
        }];
        let batch = dead_letter_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let reason_col = batches[0]
            .column_by_name("dead_letter_reason")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(reason_col.value(0), "bad field");
    }

    #[test]
    fn normalized_parquet_round_trip() {
        let event = serde_json::json!({
            "event_id": "evt_1",
            "event_key": "evk_1",
            "integration_id": null,
            "source": "okta",
            "source_event_id": "okta_evt_1",
            "principal": {"entity_key": "enk_principal"},
            "resource": {"entity_key": "enk_resource"},
            "action": {"source": "okta", "verb": "login", "category": "authentication"},
            "observed_at": "2026-03-15T14:00:00Z",
            "environment": {"source_kind": "identity"},
            "attributes": {"received_at": "2026-03-15T14:02:00Z"},
            "evidence": {"source": "okta", "object_key": "raw/key", "raw_event_id": "okta_evt_1", "observed_at": "2026-03-15T14:00:00Z"}
        });
        let row = normalized_event_row_from_value(
            &event,
            "tenant-1",
            "default",
            NORMALIZED_SCHEMA_VERSION,
        )
        .unwrap();
        let batch = normalized_event_record_batch(std::slice::from_ref(&row)).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let rows = normalized_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "evt_1");
        assert_eq!(rows[0].action_verb, "login");

        let payload: serde_json::Value = serde_json::from_str(&rows[0].payload_json).unwrap();
        assert_eq!(payload["event_key"], "evk_1");
    }

    #[test]
    fn action_feature_parquet_round_trip() {
        let rows = vec![
            ActionFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                source: "gworkspace".into(),
                resource_entity_key: "enk_resource_a".into(),
                principal_entity_key: "enk_principal_a".into(),
                access_count: 2,
                accessor_weight: 2.0 / 3.0,
            },
            ActionFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                source: "gworkspace".into(),
                resource_entity_key: "enk_resource_a".into(),
                principal_entity_key: "enk_principal_b".into(),
                access_count: 1,
                accessor_weight: 1.0 / 3.0,
            },
        ];

        let batch = action_feature_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let read_rows = action_feature_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows, rows);
    }

    #[test]
    fn history_feature_parquet_round_trip() {
        let rows = vec![
            HistoryFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                window_start: "2026-03-15T14:00:00Z".into(),
                resource_entity_key: "enk_resource_a".into(),
            },
            HistoryFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                window_start: "2026-03-15T14:00:00Z".into(),
                resource_entity_key: "enk_resource_b".into(),
            },
        ];

        let batch = history_feature_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let read_rows = history_feature_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows, rows);
    }

    #[test]
    fn collaboration_feature_parquet_round_trip() {
        let rows = vec![
            CollaborationFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                collaborator_entity_key: "enk_principal_b".into(),
                co_access_weight: 1.5,
            },
            CollaborationFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_b".into(),
                collaborator_entity_key: "enk_principal_a".into(),
                co_access_weight: 1.5,
            },
        ];

        let batch = collaboration_feature_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let read_rows = collaboration_feature_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows, rows);
    }

    #[test]
    fn static_principal_feature_parquet_round_trip() {
        let rows = vec![
            StaticPrincipalFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                principal_id: "alice@example.com".into(),
                department: "security".into(),
                role: "security-admin".into(),
                location: "US-NY".into(),
                employment_duration_bucket: "3-12mo".into(),
                privilege_level: "admin".into(),
            },
            StaticPrincipalFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_b".into(),
                principal_id: "bob@example.com".into(),
                department: "engineering".into(),
                role: "engineer".into(),
                location: "US-CA".into(),
                employment_duration_bucket: "1-3yr".into(),
                privilege_level: "regular".into(),
            },
        ];

        let batch = static_principal_feature_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let read_rows = static_principal_feature_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows, rows);
    }

    #[test]
    fn peer_group_feature_parquet_round_trip() {
        let rows = vec![
            PeerGroupFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                peer_entity_key: "enk_principal_b".into(),
                peer_type: "department".into(),
                peer_weight: 1.0,
            },
            PeerGroupFeatureRow {
                feature_schema_version: FEATURE_SCHEMA_VERSION.into(),
                tenant_id: "tenant-1".into(),
                principal_entity_key: "enk_principal_a".into(),
                peer_entity_key: "enk_principal_c".into(),
                peer_type: "manager".into(),
                peer_weight: 0.5,
            },
        ];

        let batch = peer_group_feature_record_batch(&rows).unwrap();
        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let read_rows = peer_group_feature_record_batch_to_rows(&batches[0]).unwrap();
        assert_eq!(read_rows, rows);
    }

    #[test]
    fn empty_batch() {
        let rows: Vec<RawEventRow> = vec![];
        let batch = raw_event_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 0);

        let bytes = write_parquet_bytes(&batch).unwrap();
        let batches = read_parquet_bytes(&bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
