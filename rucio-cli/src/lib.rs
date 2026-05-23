pub mod client;
pub mod cmd;

use anyhow::Result;
use clap::{Parser, Subcommand};

use client::ApiClient;

#[derive(Parser, Debug)]
#[command(name = "rucio", about = "Rucio P2P file sharing client", version)]
pub struct Cli {
    /// Daemon API address
    #[arg(long, default_value = "http://127.0.0.1:7070")]
    pub api: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Show daemon status
    Status,
    /// List connected peers
    Peers,
    /// Share a file
    Add {
        /// Path to the file to share
        path: String,
    },
    /// Stop sharing a file
    Remove {
        /// Root hash of the file (hex)
        hash: String,
    },
    /// List shared files
    Shares,
    /// Start downloading a file
    Get {
        /// Magnet link (rucio:<hash>...)
        magnet: String,
    },
    /// List active and completed downloads
    Downloads,
    /// Cancel a download
    Cancel {
        /// Root hash of the download (hex)
        hash: String,
    },
    /// Search for files on the network
    Search {
        /// Keywords to search for
        keywords: Vec<String>,
    },
    /// Show or update configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigAction {
    /// Show current configuration
    Show,
}

/// Entry point for the CLI logic.
/// Called both from the CLI's own `main.rs` and from the fat binary.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let client = ApiClient::new(&cli.api);

    match cli.command {
        Commands::Status => cmd::status::status(&client).await,
        Commands::Peers => cmd::status::peers(&client).await,
        Commands::Shares => cmd::shares::list(&client).await,
        Commands::Add { path } => cmd::shares::add(&client, &path).await,
        Commands::Remove { hash } => cmd::shares::remove(&client, &hash).await,
        Commands::Downloads => cmd::downloads::list(&client).await,
        Commands::Get { magnet } => cmd::downloads::start(&client, &magnet).await,
        Commands::Cancel { hash } => cmd::downloads::cancel(&client, &hash).await,
        Commands::Search { keywords } => cmd::search::search(&client, keywords).await,
        Commands::Config { action } => match action {
            ConfigAction::Show => cmd::config::show(&client).await,
        },
    }
}
