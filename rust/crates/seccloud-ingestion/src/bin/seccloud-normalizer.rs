use clap::Parser;
use seccloud_ingestion::local_runtime;
use seccloud_ingestion::runtime::LocalRuntimeContext;

#[derive(Debug, Parser)]
#[command(name = "seccloud-normalizer")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    max_batches: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-normalizer");
    let args = Args::parse();
    let runtime = LocalRuntimeContext::new(args.workspace, args.tenant_id, None)?;
    tracing::info!(
        workspace = %runtime.workspace().display(),
        tenant_id = runtime.tenant_id(),
        max_batches = ?args.max_batches,
        "starting normalization worker"
    );
    let store = runtime.object_store();
    let queue = runtime.intake_queue();
    let manifest = local_runtime::load_ingest_manifest(runtime.workspace())?;
    let state = local_runtime::state_from_manifest(manifest);
    let (result, next_state) = seccloud_ingestion::worker::process_pending_batches_with_state(
        runtime.tenant_id(),
        &store,
        &queue,
        state,
        args.max_batches,
    )
    .await?;
    local_runtime::save_ingest_manifest(
        runtime.workspace(),
        &local_runtime::manifest_from_state(next_state),
    )?;
    local_runtime::save_source_stats(
        runtime.workspace(),
        &local_runtime::rebuild_source_stats(runtime.workspace())?,
    )?;
    local_runtime::record_normalization_run(
        runtime.workspace(),
        result.processed_batch_ids.last().cloned(),
    )?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
