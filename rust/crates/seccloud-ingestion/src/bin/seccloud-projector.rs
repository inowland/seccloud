use clap::Parser;
use seccloud_ingestion::local_runtime;
use seccloud_ingestion::runtime::{LocalRuntimeContext, dsn_summary};

#[derive(Debug, Parser)]
#[command(name = "seccloud-projector")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    dsn: String,
}

fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-projector");
    let args = Args::parse();
    let runtime = LocalRuntimeContext::new(args.workspace, args.tenant_id, Some(args.dsn))?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        dsn = %dsn_summary(runtime.dsn()),
        "starting projection sync"
    );
    let result = seccloud_ingestion::projector::sync_workspace_projection(
        runtime.workspace(),
        runtime.tenant_id(),
        runtime.require_dsn()?,
    )?;
    local_runtime::clear_projection_refresh_request(runtime.workspace())?;
    local_runtime::record_projection_run(runtime.workspace())?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
