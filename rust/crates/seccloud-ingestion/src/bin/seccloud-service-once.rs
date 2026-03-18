use clap::Parser;
use seccloud_ingestion::runtime::{LocalRuntimeContext, dsn_summary};

#[derive(Debug, Parser)]
#[command(name = "seccloud-service-once")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    dsn: String,
    #[arg(long)]
    max_batches: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-service-once");
    let args = Args::parse();
    let runtime = LocalRuntimeContext::new(args.workspace, args.tenant_id, Some(args.dsn))?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        dsn = %dsn_summary(runtime.dsn()),
        max_batches = ?args.max_batches,
        "starting worker service once"
    );
    let result =
        seccloud_ingestion::service_runtime::run_service_once(&runtime, args.max_batches).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
