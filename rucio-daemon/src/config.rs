use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Full daemon configuration.
/// All fields have defaults — the user can run without a config file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub identity_path: PathBuf,
    pub listen_addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub listen: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkConfig {
    pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory where completed downloads are stored and automatically shared.
    /// This directory is always shared and cannot be removed from the share list.
    pub download_dir: PathBuf,
    /// Directory where in-progress downloads are stored (.part files).
    /// Chunks that are already downloaded are shared from here.
    /// Files are moved to `download_dir` once fully downloaded.
    pub temp_dir: PathBuf,
    pub database_path: PathBuf,
}

// --- Defaults ----------------------------------------------------------------

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            identity_path: default_config_dir().join("identity.key"),
            listen_addrs: vec![
                "/ip4/0.0.0.0/tcp/4321".to_string(),
                "/ip6/::/tcp/4321".to_string(),
            ],
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7070".to_string(),
            token: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            download_dir: default_download_dir(),
            temp_dir: dirs::cache_dir()
                .unwrap_or_else(|| home_dir().join(".cache"))
                .join("rucio")
                .join("tmp"),
            database_path: default_data_dir().join("rucio.db"),
        }
    }
}

/// Resolve the default download directory with a three-step fallback:
///
/// 1. `$XDG_DOWNLOAD_DIR` from `~/.config/user-dirs.dirs` (if set and non-empty)
/// 2. `$HOME/Downloads` (if the directory already exists)
/// 3. `$HOME` (always exists)
///
/// A `rucio/` subdirectory is appended in every case.
fn default_download_dir() -> PathBuf {
    let home = home_dir();

    // 1. Parse ~/.config/user-dirs.dirs for XDG_DOWNLOAD_DIR.
    if let Some(xdg_dl) = xdg_download_dir(&home) {
        return xdg_dl.join("rucio");
    }

    // 2. $HOME/Downloads — only if it already exists (common on desktop systems).
    let home_downloads = home.join("Downloads");
    if home_downloads.is_dir() {
        return home_downloads.join("rucio");
    }

    // 3. Fall back to $HOME/rucio — always resolvable.
    home.join("rucio")
}

/// Return the user's home directory from `$HOME`, falling back to whatever
/// `dirs::home_dir()` reports.  Never returns a literal `~`.
fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(dirs::home_dir)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// Parse `~/.config/user-dirs.dirs` and return the value of `XDG_DOWNLOAD_DIR`
/// if present and not empty.  The file uses shell-style `$HOME` expansion.
fn xdg_download_dir(home: &std::path::Path) -> Option<PathBuf> {
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));

    let user_dirs = config_home.join("user-dirs.dirs");
    let content = std::fs::read_to_string(&user_dirs).ok()?;

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("XDG_DOWNLOAD_DIR=") {
            // Strip surrounding quotes if present.
            let val = rest.trim_matches('"');
            if val.is_empty() {
                return None;
            }
            // Expand $HOME prefix.
            let expanded = if let Some(rel) = val.strip_prefix("$HOME/") {
                home.join(rel)
            } else if val == "$HOME" {
                home.to_path_buf()
            } else if val.starts_with('/') {
                PathBuf::from(val)
            } else {
                return None;
            };
            return Some(expanded);
        }
    }
    None
}

// --- Well-known bootstrap nodes ----------------------------------------------
//
// These are the hardcoded fallback bootstrap peers used when the user has not
// configured any in [network] bootstrap_peers.
//
// TODO: populate this list once we have funded infrastructure.
//
// Format:
//   IPv4:  "/ip4/1.2.3.4/tcp/4321/p2p/12D3KooWXXXXXXXX..."
//   IPv6:  "/ip6/2001:db8::1/tcp/4321/p2p/12D3KooWXXXXXXXX..."
//
// How to obtain the PeerId of a node:
//   Run `ruciod` once to generate a persistent identity key, then run
//   `rucio status` — it prints the PeerId and the full multiaddrs ready
//   to paste here or into a client's config.toml bootstrap_peers list.
//
// Example entries (not real nodes):
//   "/ip4/203.0.113.10/tcp/4321/p2p/12D3KooWXXXXXXXX...",
//   "/ip6/2001:db8:cafe::1/tcp/4321/p2p/12D3KooWXXXXXXXX...",
//
const BUILTIN_BOOTSTRAP_PEERS: &[&str] = &[
    // (none yet — add here when infrastructure is available)
];

// --- Loading -----------------------------------------------------------------

impl Config {
    /// Load configuration from `path`, or from the default location if `None`,
    /// falling back to built-in defaults if no file exists.
    pub fn load(path: Option<&std::path::Path>) -> Result<Self> {
        let resolved = path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| default_config_dir().join("config.toml"));

        if resolved.exists() {
            let contents = std::fs::read_to_string(&resolved)?;
            let config: Config = toml::from_str(&contents)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    /// Returns the bootstrap peers to use at startup.
    ///
    /// If the user has configured peers in `[network] bootstrap_peers` those
    /// are used exclusively.  Otherwise the built-in fallback list is returned.
    /// This lets operators run a fully private network by setting at least one
    /// peer in the config, while giving out-of-the-box users automatic access
    /// to the public network once `BUILTIN_BOOTSTRAP_PEERS` is populated.
    pub fn effective_bootstrap_peers(&self) -> Vec<&str> {
        if !self.network.bootstrap_peers.is_empty() {
            self.network
                .bootstrap_peers
                .iter()
                .map(String::as_str)
                .collect()
        } else {
            BUILTIN_BOOTSTRAP_PEERS.to_vec()
        }
    }

    /// Persist the current configuration to disk.
    pub fn save(&self) -> Result<()> {
        let path = default_config_dir().join("config.toml");
        std::fs::create_dir_all(path.parent().unwrap())?;
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
}

// --- Helpers -----------------------------------------------------------------

fn default_config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("rucio")
}

fn default_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("~/.local/share"))
        .join("rucio")
}
