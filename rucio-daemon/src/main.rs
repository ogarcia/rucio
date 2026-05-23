#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rucio_daemon::run().await
}
