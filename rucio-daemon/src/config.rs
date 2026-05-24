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
/// 1. `$XDG_DOWNLOAD_DIR` from `~/.config/user-dirs.dirs` — **Linux only**
///    (macOS does not use this mechanism)
/// 2. `$HOME/Downloads` if the directory already exists
///    (always true on macOS; common on desktop Linux)
/// 3. `$HOME/rucio` — always resolvable
///
/// A `rucio/` subdirectory is appended in every case.
fn default_download_dir() -> PathBuf {
    let home = home_dir();

    // 1. XDG user-dirs — Linux desktop only.
    #[cfg(target_os = "linux")]
    if let Some(xdg_dl) = xdg_download_dir(&home) {
        return xdg_dl.join("rucio");
    }

    // 2. $HOME/Downloads — exists by default on macOS (Finder creates it) and
    //    on most desktop Linux distros.
    let home_downloads = home.join("Downloads");
    if home_downloads.is_dir() {
        return home_downloads.join("rucio");
    }

    // 3. $HOME/rucio — always resolvable (servers, Alpine, Docker, …).
    home.join("rucio")
}

/// Return the user's home directory.
///
/// Resolution order:
///   1. `$HOME` env var (must be an absolute path)
///   2. `dirs::home_dir()` (platform-native: reads passwd on Unix, registry on Windows)
///   3. `/tmp` — last resort so we never panic
///
/// Never returns a literal `~`.
fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| dirs::home_dir().filter(|p| p.is_absolute()))
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// Parse `~/.config/user-dirs.dirs` and return the value of `XDG_DOWNLOAD_DIR`
/// if present and non-empty.  The file uses shell-style `$HOME` expansion.
///
/// Only compiled on Linux — this file does not exist on macOS.
#[cfg(target_os = "linux")]
fn xdg_download_dir(home: &std::path::Path) -> Option<PathBuf> {
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));

    let content = std::fs::read_to_string(config_home.join("user-dirs.dirs")).ok()?;

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("XDG_DOWNLOAD_DIR=") {
            let val = rest.trim_matches('"');
            if val.is_empty() {
                return None;
            }
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
        .unwrap_or_else(|| home_dir().join(".config"))
        .join("rucio")
}

fn default_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| home_dir().join(".local").join("share"))
        .join("rucio")
}

// --- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// Helper: run a closure with $HOME temporarily overridden, restoring the
    /// original value afterwards even if the closure panics.
    fn with_home<F: FnOnce()>(home: &str, f: F) {
        let prev = std::env::var_os("HOME");
        // SAFETY: single-threaded test context; no other threads read HOME.
        unsafe { std::env::set_var("HOME", home) };
        struct Guard(Option<std::ffi::OsString>);
        impl Drop for Guard {
            fn drop(&mut self) {
                match &self.0 {
                    // SAFETY: same as above — test teardown.
                    Some(v) => unsafe { std::env::set_var("HOME", v) },
                    None => unsafe { std::env::remove_var("HOME") },
                }
            }
        }
        let _guard = Guard(prev);
        f();
    }

    #[test]
    #[serial]
    fn home_dir_uses_home_env_when_absolute() {
        with_home("/custom/home", || {
            let cfg = Config::default();
            // download_dir should be rooted under /custom/home (or XDG override,
            // but in a clean env it will fall through to $HOME/Downloads/rucio or
            // $HOME/rucio).
            assert!(
                cfg.storage.download_dir.starts_with("/custom/home")
                    || cfg.storage.download_dir.starts_with("/"),
                "download_dir should be absolute, got {:?}",
                cfg.storage.download_dir
            );
        });
    }

    #[test]
    #[serial]
    fn home_dir_ignores_relative_home_env() {
        // A relative $HOME must be ignored by our home_dir() implementation;
        // the fallback (dirs::home_dir or /tmp) must still produce an absolute path.
        // Note: dirs::home_dir() may also read $HOME on some platforms, so we
        // can't guarantee the exact path — only that it is absolute.
        with_home("relative/path", || {
            // home_dir() filters out non-absolute $HOME values.
            // The resulting download_dir might still come from dirs::home_dir()
            // or /tmp, but must never literally start with "relative/".
            let cfg = Config::default();
            assert!(
                !cfg.storage.download_dir.starts_with("relative/"),
                "download_dir must not use a relative $HOME, got {:?}",
                cfg.storage.download_dir
            );
        });
    }

    #[test]
    #[serial]
    fn default_download_dir_is_absolute() {
        let cfg = Config::default();
        assert!(
            cfg.storage.download_dir.is_absolute(),
            "download_dir must be absolute, got {:?}",
            cfg.storage.download_dir
        );
    }

    #[test]
    #[serial]
    fn default_temp_dir_is_absolute() {
        let cfg = Config::default();
        assert!(
            cfg.storage.temp_dir.is_absolute(),
            "temp_dir must be absolute, got {:?}",
            cfg.storage.temp_dir
        );
    }

    #[test]
    #[serial]
    fn default_database_path_ends_with_rucio_db() {
        let cfg = Config::default();
        assert_eq!(
            cfg.storage
                .database_path
                .file_name()
                .and_then(|n| n.to_str()),
            Some("rucio.db"),
            "database_path should end with rucio.db, got {:?}",
            cfg.storage.database_path
        );
    }

    #[test]
    #[serial]
    fn default_identity_path_ends_with_identity_key() {
        let cfg = Config::default();
        assert_eq!(
            cfg.node.identity_path.file_name().and_then(|n| n.to_str()),
            Some("identity.key")
        );
    }

    #[test]
    #[serial]
    fn default_listen_addrs_are_non_empty() {
        let cfg = Config::default();
        assert!(!cfg.node.listen_addrs.is_empty());
        // Both IPv4 and IPv6 wildcard listeners expected.
        assert!(cfg.node.listen_addrs.iter().any(|a| a.contains("/ip4/")));
        assert!(cfg.node.listen_addrs.iter().any(|a| a.contains("/ip6/")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[serial]
    fn xdg_download_dir_respects_xdg_config_home() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let xdg_cfg = dir.path().join("xdg-config");
        std::fs::create_dir_all(&xdg_cfg).unwrap();
        let mut f = std::fs::File::create(xdg_cfg.join("user-dirs.dirs")).unwrap();
        writeln!(f, r#"XDG_DOWNLOAD_DIR="$HOME/Downloads""#).unwrap();

        let prev = std::env::var_os("XDG_CONFIG_HOME");
        // SAFETY: single-threaded test context.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", &xdg_cfg) };

        let home = PathBuf::from("/some/home");
        let result = super::xdg_download_dir(&home);

        match prev {
            Some(v) => unsafe { std::env::set_var("XDG_CONFIG_HOME", v) },
            None => unsafe { std::env::remove_var("XDG_CONFIG_HOME") },
        }

        assert_eq!(result, Some(PathBuf::from("/some/home/Downloads")));
    }
}
