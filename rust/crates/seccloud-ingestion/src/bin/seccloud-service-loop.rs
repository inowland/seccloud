use clap::Parser;
use seccloud_ingestion::runtime::{LocalRuntimeContext, dsn_summary};

#[derive(Debug, Parser)]
#[command(name = "seccloud-service-loop")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    dsn: String,
    #[arg(long, default_value_t = 1.0)]
    poll_interval_seconds: f64,
    #[arg(long)]
    max_batches: Option<usize>,
    #[arg(long)]
    iterations: Option<usize>,
    #[arg(long, default_value_t = false)]
    exit_when_idle: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-service-loop");
    let args = Args::parse();
    let runtime = LocalRuntimeContext::new(args.workspace, args.tenant_id, Some(args.dsn))?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        dsn = %dsn_summary(runtime.dsn()),
        poll_interval_seconds = args.poll_interval_seconds,
        max_batches = ?args.max_batches,
        iterations = ?args.iterations,
        exit_when_idle = args.exit_when_idle,
        "starting worker service loop"
    );
    let result = seccloud_ingestion::service_runtime::run_service_loop(
        &runtime,
        args.poll_interval_seconds,
        args.max_batches,
        args.iterations,
        args.exit_when_idle,
    )
    .await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
