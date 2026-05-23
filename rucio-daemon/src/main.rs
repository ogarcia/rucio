#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;

    #[derive(Parser)]
    #[command(name = "rucio-daemon", about = "Rucio P2P daemon", version)]
    struct Cli {
        /// Path to the TOML configuration file
        #[arg(long, short, env = "RUCIOD_CONFIG")]
        config: Option<std::path::PathBuf>,
    }

    let cli = Cli::parse();
    rucio_daemon::run(cli.config.as_deref()).await
}
