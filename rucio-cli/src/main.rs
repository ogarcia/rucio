#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rucio_cli::run().await
}
