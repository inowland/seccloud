use clap::Parser;
use seccloud_ingestion::local_runtime;
use seccloud_ingestion::runtime::LocalRuntimeContext;

#[derive(Debug, Parser)]
#[command(name = "seccloud-features")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
    #[arg(long)]
    tenant_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-features");
    let args = Args::parse();
    let runtime = LocalRuntimeContext::new(args.workspace, args.tenant_id, None)?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        "starting feature build"
    );
    let result = seccloud_ingestion::feature_runtime::build_workspace_feature_tables(
        runtime.workspace(),
        runtime.tenant_id(),
    )
    .await?;
    local_runtime::record_feature_run(runtime.workspace())?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
