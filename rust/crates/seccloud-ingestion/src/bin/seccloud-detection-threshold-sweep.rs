use clap::Parser;
use seccloud_ingestion::runtime::resolve_workspace_path;

#[derive(Debug, Parser)]
#[command(name = "seccloud-detection-threshold-sweep")]
struct Args {
    #[arg(long)]
    workspace: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    seccloud_ingestion::init_runtime_logging("seccloud-detection-threshold-sweep");
    let args = Args::parse();
    let workspace = resolve_workspace_path(&args.workspace)?;
    tracing::info!(
        workspace = %workspace.display(),
        "starting model threshold sweep"
    );
    let result = seccloud_ingestion::detector::sweep_model_thresholds(&workspace)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
