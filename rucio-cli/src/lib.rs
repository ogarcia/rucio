pub mod client;
pub mod cmd;
pub mod color;
pub mod help;
pub mod state;
pub mod table_util;

use anyhow::Result;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};

use client::ApiClient;

// Load the translation catalogues under `locales/`. English is the source
// locale and the fallback when a key is missing in the active language.
rust_i18n::i18n!("locales", fallback = "en");

#[derive(Parser, Debug)]
#[command(name = "rucio", about = "Rucio P2P file sharing client", version)]
pub struct Cli {
    /// Daemon API address
    #[arg(long, default_value = "http://127.0.0.1:3003", env = "RUCIO_API")]
    pub api: String,

    /// Interface language (e.g. en, es). Defaults to the system locale.
    #[arg(long, env = "RUCIO_LANG", global = true)]
    pub lang: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

/// Pick the UI language: the `--lang`/`RUCIO_LANG` override if given, otherwise
/// the system locale, falling back to English. A locale tag such as
/// `es_ES.UTF-8` or `en-US` is reduced to its base language (`es`, `en`).
fn resolve_locale(flag: Option<&str>) -> String {
    let raw = flag
        .map(str::to_string)
        .or_else(sys_locale::get_locale)
        .unwrap_or_else(|| "en".to_string());
    raw.split(['-', '_', '.'])
        .next()
        .unwrap_or("en")
        .to_lowercase()
}

/// Read `--lang`/`RUCIO_LANG` straight from the environment, before clap parses.
/// The active locale must be known *before* we build the (localized) `Command`,
/// so we can't wait for clap to populate `Cli.lang`.
fn preparse_lang() -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--lang" {
            return args.next();
        }
        if let Some(value) = arg.strip_prefix("--lang=") {
            return Some(value.to_string());
        }
    }
    std::env::var("RUCIO_LANG").ok()
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manage downloads
    Download {
        #[command(subcommand)]
        action: DownloadAction,
    },
    /// Manage download categories
    Category {
        #[command(subcommand)]
        action: CategoryAction,
    },
    /// Keep content available on this node (pin: fetch-and-retain)
    Pin {
        #[command(subcommand)]
        action: PinAction,
    },
    /// Mirror other nodes' pinned content (cooperative pinning)
    Subscription {
        #[command(subcommand)]
        action: SubscriptionAction,
    },
    /// Inspect active uploads (peers downloading from us)
    Upload {
        #[command(subcommand)]
        action: UploadAction,
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
    /// List shared files (first page by default; see --page/--all)
    List {
        /// Optional name filter: only files whose name contains this string
        /// (case-insensitive). Omit to list everything.
        #[arg(value_name = "FILTER")]
        filter: Option<String>,
        /// Fetch every file, no paging (can be a lot on a large library)
        #[arg(long)]
        all: bool,
        /// Page number, 1-based (ignored with --all)
        #[arg(long)]
        page: Option<usize>,
        /// Page size (default 50, max 1000; ignored with --all)
        #[arg(long)]
        limit: Option<i64>,
    },
    /// List the directories being shared (with file count and size)
    Dirs,
    /// Share a directory
    Add {
        /// Path to the directory to share
        path: String,
        /// Share only files directly in the directory, not its subdirectories
        #[arg(long)]
        no_recursive: bool,
        /// Share only files with these extensions ('|'-separated, e.g. mp3|mkv)
        #[arg(long, value_name = "EXTS", conflicts_with = "except")]
        only: Option<String>,
        /// Share every file except those with these extensions ('|'-separated)
        #[arg(long, value_name = "EXTS", conflicts_with = "only")]
        except: Option<String>,
    },
    /// Edit a shared directory's file filter (recursion + extensions). Only the
    /// options you pass are changed; the rest keep their current value.
    Edit {
        /// A directory number from `rucio share dirs`, or its filesystem path
        target: String,
        /// Recurse into subdirectories
        #[arg(long, conflicts_with = "no_recursive")]
        recursive: bool,
        /// Share only files directly in the directory
        #[arg(long, conflicts_with = "recursive")]
        no_recursive: bool,
        /// Clear the extension filter (share every file)
        #[arg(long, conflicts_with_all = ["only", "except"])]
        all: bool,
        /// Share only these extensions ('|'-separated)
        #[arg(long, value_name = "EXTS", conflicts_with_all = ["all", "except"])]
        only: Option<String>,
        /// Share every file except these extensions ('|'-separated)
        #[arg(long, value_name = "EXTS", conflicts_with_all = ["all", "only"])]
        except: Option<String>,
    },
    /// Stop sharing a directory
    Remove {
        /// A directory number from `rucio share dirs`, or its filesystem path
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
    /// Get the eMule (ed2k://) link for a shared file
    Ed2k {
        /// Row number from `rucio share list`, file name (unique), or hash (full or prefix)
        target: Option<String>,
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
        /// Category id to file the download under (see `rucio category list`).
        /// Its directory becomes the destination; omit for the global download dir.
        #[arg(long)]
        category: Option<i64>,
    },
    /// Move a download to a category (or clear it)
    Category {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        target: String,
        /// Category id (from `rucio category list`); omit to clear the category
        category: Option<i64>,
    },
    /// Set a download's priority (low, medium, high)
    Priority {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        target: String,
        /// New priority level
        #[arg(value_enum)]
        level: PriorityLevel,
    },
    /// Cancel an in-progress download
    Cancel {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        hash: String,
    },
    /// Pause an in-progress download (keeps progress; resume later)
    Pause {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        hash: String,
    },
    /// Resume a paused download
    Resume {
        /// Row number from `rucio download list` (e.g. 1) or root hash (full or prefix)
        hash: String,
    },
    /// Remove completed/failed/cancelled downloads from the history
    Clean {
        /// Row number from `rucio download list` (e.g. 1) or root hash prefix (omit to remove all finished downloads)
        hash: Option<String>,
    },
}

/// Download priority level accepted by `rucio download priority`.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum PriorityLevel {
    Low,
    Medium,
    High,
}

impl PriorityLevel {
    fn to_core(self) -> rucio_core::api::downloads::DownloadPriority {
        use rucio_core::api::downloads::DownloadPriority as P;
        match self {
            PriorityLevel::Low => P::Low,
            PriorityLevel::Medium => P::Medium,
            PriorityLevel::High => P::High,
        }
    }
}

/// `rucio category …` — manage download categories.
#[derive(Subcommand, Debug)]
pub enum CategoryAction {
    /// List categories
    List,
    /// Create a category
    Add {
        /// Unique category name
        name: String,
        /// Directory where this category's downloads are saved (absolute path).
        /// Omit to use the global download directory.
        #[arg(long, value_name = "PATH")]
        dir: Option<String>,
        /// Badge colour as a hex string, e.g. #3b82f6
        #[arg(long, value_name = "HEX")]
        color: Option<String>,
        /// Auto-assign new downloads whose name contains one of these
        /// '|'-separated substrings, e.g. "1080p|bluray"
        #[arg(long, value_name = "A|B|C")]
        r#match: Option<String>,
    },
    /// Update a category's name, directory, colour and match keywords
    Set {
        /// Category id (from `rucio category list`)
        id: i64,
        /// New name
        name: String,
        /// New directory (absolute path); omit to use the global download directory
        #[arg(long, value_name = "PATH")]
        dir: Option<String>,
        /// Badge colour as a hex string, e.g. #3b82f6 (omit to clear)
        #[arg(long, value_name = "HEX")]
        color: Option<String>,
        /// '|'-separated auto-assign substrings (omit to clear)
        #[arg(long, value_name = "A|B|C")]
        r#match: Option<String>,
    },
    /// Delete a category (its downloads fall back to the global download dir)
    Remove {
        /// Category id (from `rucio category list`)
        id: i64,
    },
}

/// `rucio pin …` — keep content available on this node (fetch-and-retain).
#[derive(Subcommand, Debug)]
pub enum PinAction {
    /// List pinned content
    List,
    /// Pin content: a magnet (fetched if missing), or a numeric download id /
    /// 64-char root hash for something you already have.
    Add {
        /// A `rucio:` magnet, a download id (from `rucio download list`), or a
        /// full root hash (hex).
        target: String,
        /// Provider PeerId hint to seed the fetch (repeatable)
        #[arg(long, value_name = "PEER_ID")]
        provider: Vec<String>,
        /// Publishing collection to file this pin under (subscribers can follow
        /// specific collections of yours)
        #[arg(long, value_name = "NAME")]
        collection: Option<String>,
    },
    /// Unpin a root hash (removes the intent; content stays on disk)
    Remove {
        /// Root hash (hex), from `rucio pin list`
        hash: String,
    },
}

/// `rucio subscription …` — mirror other nodes' pinned content.
#[derive(Subcommand, Debug)]
pub enum SubscriptionAction {
    /// List subscriptions and their mirror progress
    List,
    /// Subscribe to a peer's pin-set, mirroring it within a disk quota
    Add {
        /// The peer to mirror: a PeerId or a `rucio-peer:` link (from the
        /// peer's `rucio subscription link`)
        peer: String,
        /// Disk quota to devote to this peer, e.g. 10G, 500M, 1.5T
        quota: String,
    },
    /// Unsubscribe from a peer. By default frees the space by evicting
    /// mirror-only content; --keep retains it as permanent shares you own.
    Remove {
        /// The peer's PeerId, from `rucio subscription list`
        peer_id: String,
        /// Keep the content mirrored from this peer instead of evicting it
        #[arg(long)]
        keep: bool,
    },
    /// Print this node's shareable link, so others can mirror you
    Link,
}

/// `rucio upload …` — inspect peers downloading from us.
#[derive(Subcommand, Debug)]
pub enum UploadAction {
    /// List peers currently downloading a file from us
    List {
        /// Refresh the table live until interrupted with Ctrl-C
        #[arg(short, long)]
        watch: bool,
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

/// Which network(s) `rucio search add` should query.
#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum NetworkArg {
    /// Search only the Rucio P2P network
    Rucio,
    /// Search only the eMule/Kad2 network
    Emule,
    /// Search both networks
    Both,
}

impl From<NetworkArg> for rucio_core::api::searches::SearchNetwork {
    fn from(n: NetworkArg) -> Self {
        use rucio_core::api::searches::SearchNetwork;
        match n {
            NetworkArg::Rucio => SearchNetwork::Rucio,
            NetworkArg::Emule => SearchNetwork::Emule,
            NetworkArg::Both => SearchNetwork::Both,
        }
    }
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
        /// Which network(s) to query (default: both)
        #[arg(long, value_enum, default_value_t = NetworkArg::Both)]
        network: NetworkArg,
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
    ///   storage.outboard_dir            <path>
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
    // Resolve the locale before building the command so `--help` is localized.
    rust_i18n::set_locale(&resolve_locale(preparse_lang().as_deref()));
    let cmd = help::localize(Cli::command(), "help");
    let cli = Cli::from_arg_matches(&cmd.get_matches())?;
    let client = ApiClient::new(&cli.api);

    match cli.command {
        Commands::Share { action } => match action {
            ShareAction::Add {
                path,
                no_recursive,
                only,
                except,
            } => cmd::shares::add(&client, &path, !no_recursive, only, except).await,
            ShareAction::Edit {
                target,
                recursive,
                no_recursive,
                all,
                only,
                except,
            } => {
                cmd::shares::edit(&client, &target, recursive, no_recursive, all, only, except)
                    .await
            }
            ShareAction::List {
                filter,
                all,
                page,
                limit,
            } => cmd::shares::list(&client, filter.as_deref(), all, page, limit).await,
            ShareAction::Dirs => cmd::shares::dirs(&client).await,
            ShareAction::Remove { target } => cmd::shares::remove(&client, &target).await,
            ShareAction::Magnet { target, file } => {
                cmd::shares::magnet(&client, target.as_deref(), file.as_deref()).await
            }
            ShareAction::Ed2k { target } => cmd::shares::ed2k(&client, target.as_deref()).await,
            ShareAction::Indexing { watch } => cmd::shares::indexing(&client, watch).await,
        },
        Commands::Download { action } => match action {
            DownloadAction::Add {
                target,
                provider,
                category,
            } => cmd::downloads::start(&client, &target, provider.as_deref(), category).await,
            DownloadAction::List {
                watch,
                active,
                done,
            } => cmd::downloads::list(&client, watch, active, done).await,
            DownloadAction::Show { target } => cmd::downloads::show(&client, &target).await,
            DownloadAction::Category { target, category } => {
                cmd::downloads::set_category(&client, &target, category).await
            }
            DownloadAction::Priority { target, level } => {
                cmd::downloads::set_priority(&client, &target, level.to_core()).await
            }
            DownloadAction::Cancel { hash } => cmd::downloads::cancel(&client, &hash).await,
            DownloadAction::Pause { hash } => cmd::downloads::pause(&client, &hash).await,
            DownloadAction::Resume { hash } => cmd::downloads::resume(&client, &hash).await,
            DownloadAction::Clean { hash } => cmd::downloads::clean(&client, hash.as_deref()).await,
        },
        Commands::Category { action } => match action {
            CategoryAction::List => cmd::categories::list(&client).await,
            CategoryAction::Add {
                name,
                dir,
                color,
                r#match,
            } => {
                cmd::categories::add(
                    &client,
                    &name,
                    dir.as_deref(),
                    color.as_deref(),
                    r#match.as_deref(),
                )
                .await
            }
            CategoryAction::Set {
                id,
                name,
                dir,
                color,
                r#match,
            } => {
                cmd::categories::set(
                    &client,
                    id,
                    &name,
                    dir.as_deref(),
                    color.as_deref(),
                    r#match.as_deref(),
                )
                .await
            }
            CategoryAction::Remove { id } => cmd::categories::remove(&client, id).await,
        },
        Commands::Pin { action } => match action {
            PinAction::List => cmd::pins::list(&client).await,
            PinAction::Add {
                target,
                provider,
                collection,
            } => cmd::pins::add(&client, &target, provider, collection).await,
            PinAction::Remove { hash } => cmd::pins::remove(&client, &hash).await,
        },
        Commands::Subscription { action } => match action {
            SubscriptionAction::List => cmd::subscriptions::list(&client).await,
            SubscriptionAction::Add { peer, quota } => {
                cmd::subscriptions::add(&client, &peer, &quota).await
            }
            SubscriptionAction::Remove { peer_id, keep } => {
                cmd::subscriptions::remove(&client, &peer_id, keep).await
            }
            SubscriptionAction::Link => cmd::subscriptions::link(&client).await,
        },
        Commands::Upload { action } => match action {
            UploadAction::List { watch } => cmd::uploads::list(&client, watch).await,
        },
        Commands::Node { action } => match action {
            NodeAction::Status => cmd::status::status(&client).await,
            NodeAction::Peers => cmd::status::peers(&client).await,
            NodeAction::Metrics => cmd::status::metrics_cmd(&client).await,
            NodeAction::Emule { action } => cmd::emule::run(&client, action).await,
        },
        Commands::Search { action } => match action {
            SearchAction::Add {
                keywords,
                wait,
                network,
            } => cmd::search::add(&client, keywords, wait, network.into()).await,
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

#[cfg(test)]
mod tests {
    use super::resolve_locale;
    use rust_i18n::t;

    #[test]
    fn resolve_locale_prefers_flag_over_system() {
        assert_eq!(resolve_locale(Some("es")), "es");
    }

    #[test]
    fn resolve_locale_strips_region_and_encoding() {
        assert_eq!(resolve_locale(Some("es_ES.UTF-8")), "es");
        assert_eq!(resolve_locale(Some("en-US")), "en");
        assert_eq!(resolve_locale(Some("PT-BR")), "pt");
    }

    #[test]
    fn catalogues_load_for_both_languages() {
        // English source and Spanish translation both resolve and differ.
        assert_eq!(t!("category.none", locale = "en"), "No categories.");
        assert_eq!(t!("category.none", locale = "es"), "No hay categorías.");
    }

    #[test]
    fn missing_translation_falls_back_to_english() {
        // An unknown locale falls back to the English source string.
        assert_eq!(t!("category.none", locale = "xx"), "No categories.");
    }

    #[test]
    fn interpolation_fills_placeholders() {
        assert_eq!(
            t!("category.updated", locale = "es", id = 7),
            "Categoría 7 actualizada."
        );
    }

    #[test]
    fn t_accepts_a_runtime_key() {
        // SPIKE: clap-help localization needs keys built at runtime.
        let key = format!("category.{}", "none");
        assert_eq!(t!(key, locale = "en"), "No categories.");
        // A missing key returns the key itself — lets us detect "not translated".
        let missing = "help.does.not.exist".to_string();
        assert_eq!(t!(&missing, locale = "en"), missing);
    }

    #[test]
    fn clap_help_is_localized() {
        // The root command's `about` and a nested subcommand's `about` come from
        // the catalogue once the command tree is localized.
        use super::Cli;
        use clap::CommandFactory;

        rust_i18n::set_locale("es");
        let cmd = super::help::localize(Cli::command(), "help");
        assert_eq!(
            cmd.get_about().map(|s| s.to_string()),
            Some(t!("help.about", locale = "es").to_string())
        );
        let download = cmd
            .get_subcommands()
            .find(|s| s.get_name() == "download")
            .expect("download subcommand exists");
        assert_eq!(
            download.get_about().map(|s| s.to_string()),
            Some(t!("help.download.about", locale = "es").to_string())
        );
        rust_i18n::set_locale("en");
    }
}
