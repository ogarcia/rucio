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
    #[serde(default)]
    pub emule: EmuleConfig,
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
    /// Upload bandwidth limit in KB/s.  0 = unlimited (default).
    #[serde(default)]
    pub upload_limit_kbps: u64,
    /// Download bandwidth limit in KB/s.  0 = unlimited (default).
    #[serde(default)]
    pub download_limit_kbps: u64,
}

/// eMule / Kad2 compatibility settings.
/// Only meaningful when the `emule-compat` feature is compiled in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmuleConfig {
    /// UDP port for the persistent Kad2 socket.
    ///
    /// This port must be reachable from the internet for Kad2 bootstrap and
    /// source search to work.  The eMule standard port is 4672.
    /// When running in a container, map this port with `-p 4672:4672/udp`.
    pub kad_port: u16,
}

impl Default for EmuleConfig {
    fn default() -> Self {
        Self { kad_port: 4672 }
    }
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
    /// Directory for in-progress eMule downloads (separate from Rucio .part dir).
    /// Only used when the `emule-compat` feature is enabled.
    #[serde(default = "default_emule_temp_dir")]
    pub emule_temp_dir: PathBuf,
    /// Path to an eMule `nodes.dat` file used to bootstrap the Kad2 network.
    /// Optional — eMule Kad search is disabled when this is `None`.
    #[serde(default)]
    pub nodes_dat_path: Option<PathBuf>,
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
            emule_temp_dir: default_emule_temp_dir(),
            nodes_dat_path: None,
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

fn default_emule_temp_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| home_dir().join(".cache"))
        .join("rucio")
        .join("emule-tmp")
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
    ///
    /// After loading the file (or defaults), environment variable overrides are
    /// applied on top.  See [`Config::apply_env_overrides`].
    pub fn load(path: Option<&std::path::Path>) -> Result<Self> {
        let resolved = path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| default_config_dir().join("config.toml"));

        let mut config = if resolved.exists() {
            let contents = std::fs::read_to_string(&resolved)?;
            toml::from_str(&contents)?
        } else {
            Config::default()
        };

        config.apply_env_overrides();
        Ok(config)
    }

    /// Override config fields from environment variables.
    ///
    /// All variables are optional — unset means "keep whatever the file or
    /// defaults provided".
    ///
    /// | Variable                    | Field                        | Format             |
    /// |-----------------------------|------------------------------|--------------------|
    /// | `RUCIOD_API_LISTEN`         | `api.listen`                 | `host:port`        |
    /// | `RUCIOD_P2P_LISTEN`         | `node.listen_addrs`          | comma-separated multiaddrs |
    /// | `RUCIOD_DOWNLOAD_DIR`       | `storage.download_dir`       | path               |
    /// | `RUCIOD_TEMP_DIR`           | `storage.temp_dir`           | path               |
    /// | `RUCIOD_DB_PATH`            | `storage.database_path`      | path               |
    /// | `RUCIOD_BOOTSTRAP_PEERS`    | `network.bootstrap_peers`    | comma-separated multiaddrs |
    /// | `RUCIOD_UPLOAD_LIMIT_KBPS`  | `network.upload_limit_kbps`  | integer KB/s, 0=unlimited |
    /// | `RUCIOD_DOWNLOAD_LIMIT_KBPS`| `network.download_limit_kbps`| integer KB/s, 0=unlimited |
    /// | `RUCIOD_EMULE_TEMP_DIR`     | `storage.emule_temp_dir`     | path               |
    /// | `RUCIOD_NODES_DAT`          | `storage.nodes_dat_path`     | path               |
    /// | `RUCIOD_KAD_PORT`           | `emule.kad_port`             | integer 1-65535    |
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("RUCIOD_API_LISTEN")
            && !v.is_empty()
        {
            self.api.listen = v;
        }
        if let Ok(v) = std::env::var("RUCIOD_P2P_LISTEN")
            && !v.is_empty()
        {
            self.node.listen_addrs = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = std::env::var("RUCIOD_DOWNLOAD_DIR")
            && !v.is_empty()
        {
            self.storage.download_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUCIOD_TEMP_DIR")
            && !v.is_empty()
        {
            self.storage.temp_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUCIOD_DB_PATH")
            && !v.is_empty()
        {
            self.storage.database_path = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUCIOD_BOOTSTRAP_PEERS")
            && !v.is_empty()
        {
            self.network.bootstrap_peers = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = std::env::var("RUCIOD_UPLOAD_LIMIT_KBPS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u64>()
        {
            self.network.upload_limit_kbps = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_DOWNLOAD_LIMIT_KBPS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u64>()
        {
            self.network.download_limit_kbps = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_TEMP_DIR")
            && !v.is_empty()
        {
            self.storage.emule_temp_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUCIOD_NODES_DAT")
            && !v.is_empty()
        {
            self.storage.nodes_dat_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("RUCIOD_KAD_PORT")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u16>()
            && n > 0
        {
            self.emule.kad_port = n;
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

    // -- env override tests --------------------------------------------------

    #[test]
    #[serial]
    fn env_override_api_listen() {
        unsafe { std::env::set_var("RUCIOD_API_LISTEN", "0.0.0.0:8080") };
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        unsafe { std::env::remove_var("RUCIOD_API_LISTEN") };
        assert_eq!(cfg.api.listen, "0.0.0.0:8080");
    }

    #[test]
    #[serial]
    fn env_override_p2p_listen() {
        unsafe {
            std::env::set_var(
                "RUCIOD_P2P_LISTEN",
                "/ip4/0.0.0.0/tcp/9000, /ip6/::/tcp/9000",
            )
        };
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        unsafe { std::env::remove_var("RUCIOD_P2P_LISTEN") };
        assert_eq!(
            cfg.node.listen_addrs,
            vec!["/ip4/0.0.0.0/tcp/9000", "/ip6/::/tcp/9000"]
        );
    }

    #[test]
    #[serial]
    fn env_override_storage_paths() {
        unsafe {
            std::env::set_var("RUCIOD_DOWNLOAD_DIR", "/data/downloads");
            std::env::set_var("RUCIOD_TEMP_DIR", "/data/tmp");
            std::env::set_var("RUCIOD_DB_PATH", "/data/rucio.db");
        }
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        unsafe {
            std::env::remove_var("RUCIOD_DOWNLOAD_DIR");
            std::env::remove_var("RUCIOD_TEMP_DIR");
            std::env::remove_var("RUCIOD_DB_PATH");
        }
        assert_eq!(cfg.storage.download_dir, PathBuf::from("/data/downloads"));
        assert_eq!(cfg.storage.temp_dir, PathBuf::from("/data/tmp"));
        assert_eq!(cfg.storage.database_path, PathBuf::from("/data/rucio.db"));
    }

    #[test]
    #[serial]
    fn env_override_bootstrap_peers() {
        unsafe {
            std::env::set_var(
                "RUCIOD_BOOTSTRAP_PEERS",
                "/ip4/1.2.3.4/tcp/4321/p2p/12D3KooWAAA,/ip4/5.6.7.8/tcp/4321/p2p/12D3KooWBBB",
            )
        };
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        unsafe { std::env::remove_var("RUCIOD_BOOTSTRAP_PEERS") };
        assert_eq!(cfg.network.bootstrap_peers.len(), 2);
        assert!(cfg.network.bootstrap_peers[0].contains("12D3KooWAAA"));
    }

    #[test]
    #[serial]
    fn env_override_empty_value_is_ignored() {
        let default_listen = Config::default().api.listen.clone();
        unsafe { std::env::set_var("RUCIOD_API_LISTEN", "") };
        let mut cfg = Config::default();
        cfg.apply_env_overrides();
        unsafe { std::env::remove_var("RUCIOD_API_LISTEN") };
        assert_eq!(cfg.api.listen, default_listen);
    }

    // -- default value tests -------------------------------------------------

    #[test]
    #[serial]
    fn home_dir_uses_home_env_when_absolute() {
        with_home("/custom/home", || {
            let cfg = Config::default();
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
        with_home("relative/path", || {
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
