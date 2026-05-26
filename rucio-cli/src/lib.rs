pub mod client;
pub mod cmd;
pub mod color;
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
    /// Download a file (by search result index, magnet link, or ed2k link)
    Get {
        /// Search result index (e.g. 1), a full magnet link (rucio:<hash>…), or an
        /// ed2k link (ed2k://|file|…|…|…|/) to download from the eMule network.
        target: String,
        /// PeerId of the provider — only needed when target is a rucio: magnet link
        #[arg(long)]
        provider: Option<String>,
    },
    /// List active and completed downloads
    Downloads {
        /// Refresh the table every second until all downloads finish
        #[arg(short, long)]
        watch: bool,
        /// Show only in-progress downloads
        #[arg(long, conflicts_with = "done")]
        active: bool,
        /// Show only completed, failed, and cancelled downloads
        #[arg(long, conflicts_with = "active")]
        done: bool,
    },
    /// Cancel an in-progress download
    Cancel {
        /// Row number from `rucio downloads` (e.g. 1) or root hash (full or prefix)
        hash: String,
    },
    /// Remove completed/failed/cancelled downloads from the history
    Clean {
        /// Row number from `rucio downloads` (e.g. 1) or root hash prefix (omit to remove all finished downloads)
        hash: Option<String>,
    },
    /// Search for files on the network
    Search {
        /// Keywords to search for
        keywords: Vec<String>,
    },
    /// Show how many files are currently being indexed
    Indexing {
        /// Keep watching until indexing finishes
        #[arg(short, long)]
        watch: bool,
    },
    /// Show or update configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Show transfer metrics (session and lifetime totals)
    Metrics,
    /// eMule Kad compatibility commands
    Emule {
        #[command(subcommand)]
        action: cmd::emule::EmuleCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigAction {
    /// Show current configuration
    Show,
    /// Set a configuration value (restarts may be required for some changes)
    ///
    /// Settable keys:
    ///   storage.download_dir            <path>
    ///   storage.temp_dir                <path>
    ///   network.bootstrap_peers         <multiaddr>  (appends to the list)
    ///   node.listen_addrs               <multiaddr>  (appends to the list)
    ///   network.upload_limit_kbps       <integer>    (0 = unlimited, applied immediately)
    ///   network.download_limit_kbps     <integer>    (0 = unlimited, applied immediately)
    ///   emule.enabled                   <bool>
    ///   emule.temp_dir                  <path>
    ///   emule.udp_port                  <integer>    (1-65535)
    ///   emule.tcp_port                  <integer>    (1-65535)
    ///   emule.external_ip               <ipv4>
    ///   emule.download_slots_per_file   <integer>    (1-50)
    ///   emule.max_upload_slots          <integer>    (1-50)
    ///   emule.max_concurrent_downloads  <integer>    (1-50)
    Set {
        /// Configuration key (e.g. storage.download_dir)
        key: String,
        /// New value
        value: String,
    },
    /// Remove a value from a configuration key
    ///
    /// List keys remove the given entry; scalar keys revert to their default.
    ///   network.bootstrap_peers <multiaddr>   (removes one entry)
    ///   node.listen_addrs       <multiaddr>   (removes one entry)
    ///   emule.external_ip                     (reverts to auto-detect)
    Unset {
        /// Configuration key
        key: String,
        /// Value to remove (required for list keys, ignored for scalar keys)
        value: Option<String>,
    },
}

/// Entry point for the CLI logic.
/// Called both from the CLI's own `main.rs` and from the fat binary.
pub async fn run() -> Result<()> {
    rucio_core::logging::init("RUCIO");
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
        Commands::Downloads {
            watch,
            active,
            done,
        } => cmd::downloads::list(&client, watch, active, done).await,
        Commands::Get { target, provider } => {
            cmd::downloads::start(&client, &target, provider.as_deref()).await
        }
        Commands::Cancel { hash } => cmd::downloads::cancel(&client, &hash).await,
        Commands::Clean { hash } => cmd::downloads::clean(&client, hash.as_deref()).await,
        Commands::Search { keywords } => cmd::search::search(&client, keywords).await,
        Commands::Indexing { watch } => cmd::shares::indexing(&client, watch).await,
        Commands::Config { action } => match action {
            ConfigAction::Show => cmd::config::show(&client).await,
            ConfigAction::Set { key, value } => cmd::config::set(&client, &key, &value).await,
            ConfigAction::Unset { key, value } => {
                cmd::config::unset(&client, &key, value.as_deref()).await
            }
        },
        Commands::Metrics => cmd::status::metrics_cmd(&client).await,
        Commands::Emule { action } => cmd::emule::run(&client, action).await,
    }
}
