use std::path::{Path, PathBuf};
use std::sync::Arc;

use seccloud_lake::intake_queue::{IntakeQueue, LocalIntakeQueue};
use seccloud_lake::object_store::{LocalObjectStore, ObjectStore};

#[derive(Debug, Clone)]
pub struct LocalRuntimeContext {
    workspace: PathBuf,
    tenant_id: String,
    dsn: Option<String>,
}

impl LocalRuntimeContext {
    pub fn new(
        workspace: impl AsRef<Path>,
        tenant_id: impl Into<String>,
        dsn: Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            workspace: resolve_workspace_path(workspace.as_ref())?,
            tenant_id: tenant_id.into(),
            dsn,
        })
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn dsn(&self) -> Option<&str> {
        self.dsn.as_deref()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::new(LocalObjectStore::new(&self.workspace))
    }

    pub fn intake_queue(&self) -> Arc<dyn IntakeQueue> {
        Arc::new(LocalIntakeQueue::new(&self.workspace))
    }

    pub fn count_processed_batches(&self) -> anyhow::Result<usize> {
        let dir = self.workspace.join("intake").join("processed");
        if !dir.exists() {
            return Ok(0);
        }
        Ok(std::fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().and_then(|value| value.to_str()) == Some("json")
            })
            .count())
    }
}

pub fn resolve_workspace_path(workspace: &Path) -> anyhow::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    Ok(resolve_workspace_path_from_cwd(&cwd, workspace))
}

pub fn resolve_workspace_path_from_cwd(cwd: &Path, workspace: &Path) -> PathBuf {
    if workspace.is_absolute() {
        return workspace.to_path_buf();
    }
    cwd.join(workspace)
}

pub fn dsn_summary(dsn: Option<&str>) -> String {
    let Some(dsn) = dsn else {
        return "none".to_string();
    };
    let parts: Vec<String> = dsn
        .split_whitespace()
        .filter_map(|token| {
            let (key, value) = token.split_once('=')?;
            let value = if key.eq_ignore_ascii_case("password") {
                "<redacted>"
            } else {
                value
            };
            Some(format!("{key}={value}"))
        })
        .collect();
    if parts.is_empty() {
        "<opaque-dsn>".to_string()
    } else {
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{LocalRuntimeContext, dsn_summary, resolve_workspace_path_from_cwd};

    #[test]
    fn resolves_relative_workspace_from_cwd() {
        let cwd = Path::new("/tmp/seccloud/rust");
        let resolved = resolve_workspace_path_from_cwd(cwd, Path::new("../.seccloud"));
        assert_eq!(
            resolved,
            Path::new("/tmp/seccloud/rust").join("../.seccloud")
        );
    }

    #[test]
    fn context_keeps_absolute_workspace_path() {
        let context =
            LocalRuntimeContext::new("/tmp/seccloud/.seccloud", "tenant-1", None).unwrap();
        assert!(context.workspace().is_absolute());
        assert_eq!(context.tenant_id(), "tenant-1");
    }

    #[test]
    fn dsn_summary_redacts_passwords() {
        let summary = dsn_summary(Some(
            "dbname=seccloud user=alice password=secret host=/tmp port=55432",
        ));
        assert!(summary.contains("dbname=seccloud"));
        assert!(summary.contains("user=alice"));
        assert!(summary.contains("password=<redacted>"));
        assert!(summary.contains("host=/tmp"));
    }
}
