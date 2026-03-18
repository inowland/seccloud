use std::path::Path;

use crate::collector::{Checkpoint, CollectorPage};

pub mod github;
pub mod gworkspace;
pub mod okta;
pub mod snowflake;

pub(crate) fn read_jsonl_fixture(path: &Path) -> anyhow::Result<Vec<serde_json::Value>> {
    let content = std::fs::read_to_string(path)?;
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

pub(crate) fn build_fixture_page(
    all_records: &[serde_json::Value],
    checkpoint: &Checkpoint,
    limit: Option<usize>,
    default_batch_size: usize,
    map_record: impl Fn(&serde_json::Value) -> serde_json::Value,
    metadata: serde_json::Map<String, serde_json::Value>,
) -> CollectorPage {
    let offset = checkpoint.cursor.as_u64().unwrap_or(0) as usize;
    let page_size = limit.unwrap_or(default_batch_size);
    let end = (offset + page_size).min(all_records.len());

    CollectorPage {
        records: all_records[offset..end].iter().map(map_record).collect(),
        next_checkpoint: Checkpoint {
            cursor: serde_json::json!(end),
            version: checkpoint.version + 1,
        },
        has_more: end < all_records.len(),
        metadata,
    }
}
