pub mod client;
pub mod cmd;
pub mod color;
pub mod state;
pub mod table_util;

use anyhow::Result;
use clap::{Parser, Subcommand};

use client::ApiClient;

#[derive(Parser, Debug)]
#[command(name = "rucio", about = "Rucio P2P file sharing client", version)]
pub struct Cli {
    /// Daemon API address
    #[arg(long, default_value = "http://127.0.0.1:3003", env = "RUCIO_API")]
    pub api: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manage downloads
    Download {
        #[command(subcommand)]
        action: DownloadAction,
    },
    /// Search for files on the network
    Search {
        #[command(subcommand)]
        action: SearchAction,
    },
    /// Manage shared files
    Share {
        #[command(subcommand)]
        action: ShareAction,
    },
    /// Node and daemon information
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },
    /// Show or update configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

/// `rucio share …` — manage shared files.
#[derive(Subcommand, Debug)]
pub enum ShareAction {
    /// List shared files
    List {
        /// Only show files whose name contains this string (case-insensitive)
        #[arg(long)]
        filter: Option<String>,
    },
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
    /// Show how many files are currently being indexed
    Indexing {
        /// Keep watching until indexing finishes
        #[arg(short, long)]
        watch: bool,
    },
    /// Get the magnet link for a file — shared or not
    Magnet {
        /// Row number from `rucio share list`, file name (unique), or hash (full or prefix).
        /// Omit when using --file.
        target: Option<String>,
        /// Compute the magnet link for a local file without sharing it or contacting the daemon
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
    },
}

/// `rucio download …` — manage downloads.
#[derive(Subcommand, Debug)]
pub enum DownloadAction {
    /// List active and completed downloads
    List {
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
    /// Show full details for a single download
    Show {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        target: String,
    },
    /// Queue a download (by search result index, magnet link, or ed2k link)
    Add {
        /// Search result index (e.g. 1), a full magnet link (rucio:<hash>…), or an
        /// ed2k link (ed2k://|file|…|…|…|/) to download from the eMule network.
        target: String,
        /// PeerId of the provider — only needed when target is a rucio: magnet link
        #[arg(long)]
        provider: Option<String>,
    },
    /// Cancel an in-progress download
    Cancel {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        hash: String,
    },
    /// Remove completed/failed/cancelled downloads from the history
    Clean {
        /// Row number from `rucio download list` (e.g. 1) or root hash prefix (omit to remove all finished downloads)
        hash: Option<String>,
    },
}

/// `rucio node …` — node and daemon information.
#[derive(Subcommand, Debug)]
pub enum NodeAction {
    /// Show daemon status
    Status,
    /// List connected peers
    Peers,
    /// Show transfer metrics (session and lifetime totals)
    Metrics,
    /// eMule / Kad2 compatibility subsystem
    Emule {
        #[command(subcommand)]
        action: cmd::emule::EmuleCmd,
    },
}

/// `rucio search …` — search for files on the Rucio and eMule networks.
#[derive(Subcommand, Debug)]
pub enum SearchAction {
    /// List all searches currently held in daemon memory
    List,
    /// Show results for a search, waiting if it is still running
    Show {
        /// Search ID returned by `rucio search add`
        id: u64,
    },
    /// Start a new search (prints search ID and returns immediately)
    Add {
        /// Keywords to search for
        keywords: Vec<String>,
        /// Poll until the search finishes and print results
        #[arg(short, long)]
        wait: bool,
    },
    /// Relaunch a search, keeping the same ID and preserving existing results
    Relaunch {
        /// Search ID
        id: u64,
    },
    /// Cancel a running search
    Cancel {
        /// Search ID
        id: u64,
    },
    /// Remove done or cancelled searches from daemon memory
    Clean {
        /// Search ID to remove (omit to remove all done/cancelled searches)
        id: Option<u64>,
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
    ///   network.max_upload_tasks        <integer>    (≥1, requires restart)
    ///   emule.enabled                   <bool>
    ///   emule.temp_dir                  <path>
    ///   emule.tcp_port                  <integer>    (1-65535)
    ///   emule.udp_port                  <integer>    (1-65535)
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
    rucio_core::logging::init("RUCIO", "off");
    let cli = Cli::parse();
    let client = ApiClient::new(&cli.api);

    match cli.command {
        Commands::Share { action } => match action {
            ShareAction::Add { path } => cmd::shares::add(&client, &path).await,
            ShareAction::List { filter } => cmd::shares::list(&client, filter.as_deref()).await,
            ShareAction::Remove { target } => cmd::shares::remove(&client, &target).await,
            ShareAction::Magnet { target, file } => {
                cmd::shares::magnet(&client, target.as_deref(), file.as_deref()).await
            }
            ShareAction::Indexing { watch } => cmd::shares::indexing(&client, watch).await,
        },
        Commands::Download { action } => match action {
            DownloadAction::Add { target, provider } => {
                cmd::downloads::start(&client, &target, provider.as_deref()).await
            }
            DownloadAction::List {
                watch,
                active,
                done,
            } => cmd::downloads::list(&client, watch, active, done).await,
            DownloadAction::Show { target } => cmd::downloads::show(&client, &target).await,
            DownloadAction::Cancel { hash } => cmd::downloads::cancel(&client, &hash).await,
            DownloadAction::Clean { hash } => cmd::downloads::clean(&client, hash.as_deref()).await,
        },
        Commands::Node { action } => match action {
            NodeAction::Status => cmd::status::status(&client).await,
            NodeAction::Peers => cmd::status::peers(&client).await,
            NodeAction::Metrics => cmd::status::metrics_cmd(&client).await,
            NodeAction::Emule { action } => cmd::emule::run(&client, action).await,
        },
        Commands::Search { action } => match action {
            SearchAction::Add { keywords, wait } => cmd::search::add(&client, keywords, wait).await,
            SearchAction::List => cmd::search::list(&client).await,
            SearchAction::Show { id } => cmd::search::show(&client, id).await,
            SearchAction::Cancel { id } => cmd::search::cancel(&client, id).await,
            SearchAction::Clean { id } => cmd::search::clean(&client, id).await,
            SearchAction::Relaunch { id } => cmd::search::relaunch(&client, id).await,
        },
        Commands::Config { action } => match action {
            ConfigAction::Show => cmd::config::show(&client).await,
            ConfigAction::Set { key, value } => cmd::config::set(&client, &key, &value).await,
            ConfigAction::Unset { key, value } => {
                cmd::config::unset(&client, &key, value.as_deref()).await
            }
        },
    }
}
