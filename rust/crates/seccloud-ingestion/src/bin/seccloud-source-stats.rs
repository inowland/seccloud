use clap::Parser;
use seccloud_ingestion::local_runtime;
use seccloud_ingestion::runtime::resolve_workspace_path;

#[derive(Debug, Parser)]
#[command(name = "seccloud-source-stats")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-source-stats");
    let args = Args::parse();
    let workspace = resolve_workspace_path(&args.workspace)?;
    tracing::info!(workspace = %workspace.display(), "starting source stats refresh");
    let stats = local_runtime::rebuild_source_stats(&workspace)?;
    local_runtime::save_source_stats(&workspace, &stats)?;
    local_runtime::clear_source_stats_refresh_request(&workspace)?;
    local_runtime::record_source_stats_run(&workspace)?;
    println!(
        "{}",
        serde_json::to_string(&local_runtime::source_stats_summary(&stats))?
    );
    Ok(())
}
