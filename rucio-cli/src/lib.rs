pub mod client;
pub mod cmd;
pub mod state;

use anyhow::Result;
use clap::{Parser, Subcommand};

use client::ApiClient;

#[derive(Parser, Debug)]
#[command(name = "rucio", about = "Rucio P2P file sharing client", version)]
pub struct Cli {
    /// Daemon API address
    #[arg(long, default_value = "http://127.0.0.1:7070", env = "RUCIO_API")]
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
    /// Share a directory
    Add {
        /// Path to the directory to share (individual files are not accepted)
        path: String,
    },
    /// Stop sharing a file or directory
    Remove {
        /// Root hash (hex) of a single file, or filesystem path (file or directory)
        target: String,
    },
    /// List shared files
    Shares {
        /// Optional filter — only show files whose name contains this string (case-insensitive)
        filter: Option<String>,
    },
    /// Get the magnet link for a file — shared or not
    Magnet {
        /// Row number from `rucio shares`, file name (unique), or hash (full or prefix).
        /// Omit when using --file.
        target: Option<String>,
        /// Compute the magnet link for a local file without sharing it or contacting the daemon
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },
    /// Download a file (by search result index or magnet link)
    Get {
        /// Search result index (e.g. 1) or a full magnet link (rucio:<hash>...)
        target: String,
        /// PeerId of the provider — only needed when target is a magnet link
        #[arg(long)]
        provider: Option<String>,
    },
    /// List active and completed downloads
    Downloads {
        /// Refresh the table every second until all downloads finish
        #[arg(short, long)]
        watch: bool,
    },
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
        Commands::Shares { filter } => cmd::shares::list(&client, filter.as_deref()).await,
        Commands::Magnet { target, file } => {
            cmd::shares::magnet(&client, target.as_deref(), file.as_deref()).await
        }
        Commands::Add { path } => cmd::shares::add(&client, &path).await,
        Commands::Remove { target } => cmd::shares::remove(&client, &target).await,
        Commands::Downloads { watch } => cmd::downloads::list(&client, watch).await,
        Commands::Get { target, provider } => {
            cmd::downloads::start(&client, &target, provider.as_deref()).await
        }
        Commands::Cancel { hash } => cmd::downloads::cancel(&client, &hash).await,
        Commands::Search { keywords } => cmd::search::search(&client, keywords).await,
        Commands::Config { action } => match action {
            ConfigAction::Show => cmd::config::show(&client).await,
        },
    }
}
