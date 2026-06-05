use clap::Parser;

#[derive(Parser)]
#[command(name = "rucio-daemon", about = "Rucio P2P daemon", version)]
struct Cli {
    /// Path to the TOML configuration file
    #[arg(long, short, env = "RUCIOD_CONFIG")]
    config: Option<std::path::PathBuf>,

    /// Portable mode: keep all data (config, database, identity, downloads,
    /// temp) next to the executable instead of the platform's config/data dirs.
    #[arg(long, conflicts_with = "base_dir")]
    portable: bool,

    /// Root all storage under DIR (config, identity, database, temp, downloads).
    #[arg(long, value_name = "DIR", env = "RUCIOD_BASE_DIR")]
    base_dir: Option<std::path::PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Resolve portable/base-dir and export RUCIOD_BASE_DIR before the async
    // runtime starts (set_var must not race Tokio's worker threads).
    rucio_daemon::apply_base_dir_env(cli.portable, cli.base_dir.as_deref());
    run(cli.config)
}

#[tokio::main]
async fn run(config: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    rucio_daemon::run(config.as_deref()).await
}
