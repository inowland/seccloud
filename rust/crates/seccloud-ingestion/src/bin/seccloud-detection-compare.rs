use clap::Parser;
use seccloud_ingestion::runtime::resolve_workspace_path;

#[derive(Debug, Parser)]
#[command(name = "seccloud-detection-compare")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-detection-compare");
    let args = Args::parse();
    let workspace = resolve_workspace_path(&args.workspace)?;
    tracing::info!(
        workspace = %workspace.display(),
        "starting detection mode comparison"
    );
    let result = seccloud_ingestion::detector::compare_detection_modes(&workspace)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
