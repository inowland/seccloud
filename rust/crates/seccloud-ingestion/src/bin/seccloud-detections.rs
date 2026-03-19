use clap::Parser;
use seccloud_ingestion::local_runtime;
use seccloud_ingestion::runtime::resolve_workspace_path;

#[derive(Debug, Parser)]
#[command(name = "seccloud-detections")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
}

fn ensure_workspace_layout(workspace: &std::path::Path) -> anyhow::Result<()> {
    for path in [
        workspace.join("detections"),
        workspace.join("derived"),
        workspace.join("ops"),
        workspace.join("manifests"),
    ] {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-detections");
    let args = Args::parse();
    let workspace = resolve_workspace_path(&args.workspace)?;
    tracing::info!(workspace = %workspace.display(), "starting detection worker");
    ensure_workspace_layout(&workspace)?;
    let result = seccloud_ingestion::detector::run_detection_worker_async(&workspace).await?;
    local_runtime::record_detection_run(&workspace)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
