/// Fat binary entry point.
///
/// Behaves as the daemon (`ruciod`) or as the CLI (`rucio`) depending on
/// the name it is invoked with — same approach as BusyBox.
///
/// Distributing a single binary:
///   - `rucio`  → CLI mode
///   - `ruciod` → daemon mode  (symlink or hardlink to the same binary)
fn main() -> anyhow::Result<()> {
    let argv0 = std::env::args().next().unwrap_or_default();

    if argv0.contains("ruciod") {
        use clap::Parser;

        #[derive(Parser)]
        #[command(name = "ruciod", about = "Rucio P2P daemon", version)]
        struct Cli {
            /// Path to the TOML configuration file
            #[arg(long, short, env = "RUCIOD_CONFIG")]
            config: Option<std::path::PathBuf>,

            /// Portable mode: keep all data (config, database, identity,
            /// downloads, temp) next to the executable.
            #[arg(long, conflicts_with = "base_dir")]
            portable: bool,

            /// Root all storage under DIR.
            #[arg(long, value_name = "DIR", env = "RUCIOD_BASE_DIR")]
            base_dir: Option<std::path::PathBuf>,
        }

        let cli = Cli::parse();
        // Export RUCIOD_BASE_DIR before the runtime starts (set_var must not
        // race Tokio's worker threads).
        rucio_daemon::apply_base_dir_env(cli.portable, cli.base_dir.as_deref());
        run_daemon(cli.config)
    } else {
        run_cli()
    }
}

#[tokio::main]
async fn run_daemon(config: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    rucio_daemon::run(config.as_deref()).await
}

#[tokio::main]
async fn run_cli() -> anyhow::Result<()> {
    rucio_cli::run().await
}
