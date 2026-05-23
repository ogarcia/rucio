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
        // Parse --config for the daemon sub-path.
        // We do minimal parsing here to avoid pulling clap into the fat binary
        // just for this; rucio-daemon/main.rs has the full clap setup.
        let args: Vec<String> = std::env::args().collect();
        let config = args
            .windows(2)
            .find(|w| w[0] == "--config" || w[0] == "-c")
            .map(|w| std::path::PathBuf::from(&w[1]));
        rucio_daemon::run(config.as_deref()).await
    } else {
        rucio_cli::run().await
    }
}
