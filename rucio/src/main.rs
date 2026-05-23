/// Fat binary entry point.
///
/// Behaves as the daemon (`ruciod`) or as the CLI (`rucio`) depending on
/// the name it is invoked with — same approach as BusyBox.
///
/// Distributing a single binary:
///   - `rucio`  → CLI mode
///   - `ruciod` → daemon mode  (symlink or hardlink to the same binary)
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let argv0 = std::env::args().next().unwrap_or_default();

    if argv0.contains("ruciod") {
        rucio_daemon::run().await
    } else {
        rucio_cli::run().await
    }
}
