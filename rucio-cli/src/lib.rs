use anyhow::Result;
use clap::{Parser, Subcommand};

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
        /// Root hash of the file
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
        /// Root hash of the download
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
    // TODO: implement command dispatch
    println!("API endpoint: {}", cli.api);
    println!("Command: {:?}", cli.command);
    Ok(())
}
