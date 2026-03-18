#[tokio::main]
async fn main() -> anyhow::Result<()> {
    seccloud_ingestion::run_gateway_from_env().await
}
