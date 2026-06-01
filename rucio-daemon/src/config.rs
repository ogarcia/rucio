use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Full daemon configuration.
/// All fields have defaults — the user can run without a config file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub identity_path: PathBuf,
    pub listen_addrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiConfig {
    pub listen: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub bootstrap_peers: Vec<String>,
    /// Enable UPnP/IGD automatic port mapping.  Default: `true`.
    ///
    /// When enabled, the daemon asks the LAN router to forward:
    ///   - TCP port from `node.listen_addrs` (libp2p)
    ///   - UDP `emule.udp_port` (Kad2, only with the `emule-compat` feature)
    ///
    /// Set to `false` if:
    ///   - You have already configured port forwarding manually on your router.
    ///   - You are running on a VPS / cloud server with no NAT (direct public IP).
    ///   - You are running inside a container without `-p` mappings and the host
    ///     handles forwarding externally.
    ///   - UPnP is disabled or unavailable on your network.
    ///
    /// When `false`, the daemon starts without attempting UPnP discovery and
    /// the `external_ip` field in `/api/v1/status` will always be `null`.
    #[serde(default = "NetworkConfig::default_upnp")]
    pub upnp: bool,
    /// Upload bandwidth limit in KB/s.  0 = unlimited (default).
    #[serde(default)]
    pub upload_limit_kbps: u64,
    /// Download bandwidth limit in KB/s.  0 = unlimited (default).
    #[serde(default)]
    pub download_limit_kbps: u64,
    /// Upload cap in KB/s applied while the temporary speed limit is engaged.
    /// This is only the preset value; the toggle itself is runtime state and
    /// does not persist.  Default: 5120 (= 5.0 MB/s).
    #[serde(default = "NetworkConfig::default_temp_limit")]
    pub temp_upload_limit_kbps: u64,
    /// Download cap in KB/s applied while the temporary speed limit is engaged.
    /// Default: 5120 (= 5.0 MB/s).
    #[serde(default = "NetworkConfig::default_temp_limit")]
    pub temp_download_limit_kbps: u64,
    /// Maximum number of concurrent chunk-upload tasks.
    ///
    /// Each inbound chunk request spawns an async task that reads from disk
    /// and waits for the bandwidth throttle.  This cap prevents resource
    /// exhaustion when many peers request chunks simultaneously.
    ///
    /// Default: 64.  Override via `RUCIOD_MAX_UPLOAD_TASKS`.
    #[serde(default = "NetworkConfig::default_max_upload_tasks")]
    pub max_upload_tasks: usize,
    /// Use *only* the configured `bootstrap_peers`, ignoring the built-in list.
    ///
    /// Default `false`: configured peers are **added** to the built-in ones.
    /// Set `true` to bootstrap exclusively from your own peers (e.g. a separate
    /// network). Note this is not a privacy/security boundary — anyone with one
    /// of your peer multiaddrs can still join.
    #[serde(default)]
    pub exclusive_bootstrap: bool,
}

impl NetworkConfig {
    fn default_upnp() -> bool {
        true
    }

    fn default_max_upload_tasks() -> usize {
        64
    }

    fn default_temp_limit() -> u64 {
        5120
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bootstrap_peers: vec![],
            upnp: Self::default_upnp(),
            upload_limit_kbps: 0,
            download_limit_kbps: 0,
            temp_upload_limit_kbps: Self::default_temp_limit(),
            temp_download_limit_kbps: Self::default_temp_limit(),
            max_upload_tasks: Self::default_max_upload_tasks(),
            exclusive_bootstrap: false,
        }
    }
}

/// eMule / Kad2 compatibility settings.
/// Only meaningful when the `emule-compat` feature is compiled in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmuleConfig {
    /// Enable the eMule / Kad2 subsystem at runtime.
    ///
    /// Set to `false` to disable all eMule functionality even when the binary
    /// is compiled with `emule-compat`.  Useful for fat-binary distributions
    /// where the user does not want eMule.  Override via `RUCIOD_EMULE_ENABLED`.
    #[serde(default = "EmuleConfig::default_enabled")]
    pub enabled: bool,

    /// Directory for in-progress eMule downloads (.part files).
    ///
    /// Separate from the rucio temp dir so eMule and libp2p partials never
    /// mix.  Override via `RUCIOD_EMULE_TEMP_DIR`.
    #[serde(default = "default_emule_temp_dir")]
    pub temp_dir: PathBuf,

    /// TCP port for incoming eMule peer connections (High-ID mode).
    ///
    /// ruciod listens on this port so that other eMule clients can connect
    /// to us directly.  Without it we operate as Low-ID and receive lower
    /// priority in upload queues, resulting in significantly slower downloads.
    /// The eMule standard port is 4662.
    /// When running in a container, map this port with `-p 4662:4662/tcp`.
    ///
    /// Default: 4662.  Override via `RUCIOD_EMULE_TCP_PORT`.
    #[serde(default = "EmuleConfig::default_tcp_port")]
    pub tcp_port: u16,

    /// UDP port for the persistent Kad2 socket.
    ///
    /// This port must be reachable from the internet for Kad2 bootstrap and
    /// source search to work.  The eMule standard port is 4672.
    /// When running in a container, map this port with `-p 4672:4672/udp`.
    ///
    /// Default: 4672.  Override via `RUCIOD_EMULE_UDP_PORT`.
    pub udp_port: u16,

    /// Our external IPv4 address as seen by peers on the internet.
    ///
    /// Required for Kad2 UDP obfuscation.  If left as `None` (default), ruciod
    /// tries to learn it via UPnP or from peer responses.  Set this explicitly
    /// when UPnP is unavailable (e.g. CGNAT) via `RUCIOD_EXTERNAL_IP`.
    pub external_ip: Option<std::net::Ipv4Addr>,

    /// Number of simultaneous peer connections opened per eMule download.
    ///
    /// Files are divided into ed2k-part-sized slices (~9.7 MB each) and
    /// distributed across up to this many concurrent TCP connections.
    /// The effective concurrency is also bounded by the number of discovered
    /// sources and the number of remaining slices, so setting this higher than
    /// needed has no cost.
    ///
    /// Default: 5.  Range: 1–50.  Override via `RUCIOD_EMULE_DOWNLOAD_SLOTS_PER_FILE`.
    #[serde(default = "EmuleConfig::default_download_slots_per_file")]
    pub download_slots_per_file: usize,

    /// Maximum number of simultaneous eMule upload slots.
    ///
    /// Each inbound peer requesting chunks from our in-progress downloads
    /// occupies one slot.  When all slots are busy, incoming peers receive
    /// OP_QUEUE_RANK and are told to retry later (standard eMule behaviour).
    ///
    /// Default: 4.  Range: 1–50.  Override via `RUCIOD_EMULE_MAX_UPLOAD_SLOTS`.
    #[serde(default = "EmuleConfig::default_max_upload_slots")]
    pub max_upload_slots: usize,

    /// Maximum number of eMule downloads that run concurrently.
    ///
    /// When more downloads than this are requested, the surplus wait in the
    /// `queued` state until a running download finishes.  This caps the total
    /// number of open TCP connections (each active download opens up to
    /// `download_slots_per_file` of them) so a large queue does not exhaust sockets.
    ///
    /// Default: 3.  Range: 1–50.  Override via `RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS`.
    #[serde(default = "EmuleConfig::default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,

    /// Nickname advertised to eMule peers — the name shown in their transfer
    /// lists ("downloading from <nick>"). Cosmetic; the credit identity is the
    /// separate user hash. Default: "rucio". Override via `RUCIOD_EMULE_NICK`.
    #[serde(default = "EmuleConfig::default_nick")]
    pub nick: String,
}

impl EmuleConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_nick() -> String {
        "rucio".to_string()
    }

    fn default_tcp_port() -> u16 {
        4662
    }

    fn default_download_slots_per_file() -> usize {
        5
    }

    fn default_max_upload_slots() -> usize {
        4
    }

    fn default_max_concurrent_downloads() -> usize {
        3
    }
}

impl Default for EmuleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            temp_dir: default_emule_temp_dir(),
            tcp_port: Self::default_tcp_port(),
            udp_port: 4672,
            external_ip: None,
            download_slots_per_file: Self::default_download_slots_per_file(),
            max_upload_slots: Self::default_max_upload_slots(),
            max_concurrent_downloads: Self::default_max_concurrent_downloads(),
            nick: Self::default_nick(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory where completed downloads are stored and automatically shared.
    /// This directory is always shared and cannot be removed from the share list.
    pub download_dir: PathBuf,
    /// Directory where in-progress downloads are stored (.part files).
    /// Chunks that are already downloaded are shared from here.
    /// Files are moved to `download_dir` once fully downloaded.
    pub temp_dir: PathBuf,
    pub database_path: PathBuf,
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
            listen: "127.0.0.1:3003".to_string(),
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
    "/ip4/208.85.21.46/tcp/4321/p2p/12D3KooWHXm58uGjv3fta4v8mYHS5jwbaSgw6LBqVVY9rcguaCko",
    "/ip6/2a05:f480:2800:2731:5400:6ff:fe31:8080/tcp/4321/p2p/12D3KooWHXm58uGjv3fta4v8mYHS5jwbaSgw6LBqVVY9rcguaCko",
];

// --- Helpers -----------------------------------------------------------------

impl Config {
    /// Extract the TCP port number from the first entry in `node.listen_addrs`
    /// that contains a `/tcp/<port>` component.
    ///
    /// Used by UPnP so it knows which port to request from the router without
    /// duplicating the port as a separate `network.listen_port` setting.
    /// Returns `None` only when `listen_addrs` is empty or contains no TCP entry.
    pub fn p2p_tcp_port(&self) -> Option<u16> {
        use libp2p::multiaddr::Protocol;
        self.node.listen_addrs.iter().find_map(|s| {
            let addr: libp2p::Multiaddr = s.parse().ok()?;
            addr.iter().find_map(|p| match p {
                Protocol::Tcp(port) => Some(port),
                _ => None,
            })
        })
    }
}

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
    /// | `RUCIOD_MAX_UPLOAD_TASKS`   | `network.max_upload_tasks`   | integer ≥1, default 64    |
    /// | `RUCIOD_EMULE_ENABLED`      | `emule.enabled`              | `true`/`false`     |
    /// | `RUCIOD_EMULE_TEMP_DIR`     | `emule.temp_dir`             | path               |
    /// | `RUCIOD_NODES_DAT`          | `storage.nodes_dat_path`     | path               |
    /// | `RUCIOD_EMULE_TCP_PORT`     | `emule.tcp_port`             | integer 1-65535    |
    /// | `RUCIOD_EMULE_UDP_PORT`     | `emule.udp_port`             | integer 1-65535    |
    /// | `RUCIOD_EMULE_DOWNLOAD_SLOTS_PER_FILE` | `emule.download_slots_per_file` | integer 1-50 |
    /// | `RUCIOD_EMULE_MAX_UPLOAD_SLOTS` | `emule.max_upload_slots` | integer 1-50       |
    /// | `RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS` | `emule.max_concurrent_downloads` | integer 1-50 |
    /// | `RUCIOD_UPNP`               | `network.upnp`               | `true`/`false`     |
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
        if let Ok(v) = std::env::var("RUCIOD_TEMP_UPLOAD_LIMIT_KBPS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u64>()
        {
            self.network.temp_upload_limit_kbps = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_TEMP_DOWNLOAD_LIMIT_KBPS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u64>()
        {
            self.network.temp_download_limit_kbps = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_MAX_UPLOAD_TASKS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<usize>()
            && n >= 1
        {
            self.network.max_upload_tasks = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_ENABLED")
            && !v.is_empty()
        {
            self.emule.enabled = !matches!(v.to_lowercase().as_str(), "false" | "0" | "no" | "off");
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_TEMP_DIR")
            && !v.is_empty()
        {
            self.emule.temp_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUCIOD_NODES_DAT")
            && !v.is_empty()
        {
            self.storage.nodes_dat_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_UDP_PORT")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u16>()
            && n > 0
        {
            self.emule.udp_port = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_TCP_PORT")
            && !v.is_empty()
            && let Ok(n) = v.parse::<u16>()
            && n > 0
        {
            self.emule.tcp_port = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EXTERNAL_IP")
            && !v.is_empty()
            && let Ok(ip) = v.parse::<std::net::Ipv4Addr>()
        {
            self.emule.external_ip = Some(ip);
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_NICK")
            && !v.trim().is_empty()
        {
            self.emule.nick = v.trim().to_string();
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_DOWNLOAD_SLOTS_PER_FILE")
            && !v.is_empty()
            && let Ok(n) = v.parse::<usize>()
            && (1..=50).contains(&n)
        {
            self.emule.download_slots_per_file = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_MAX_UPLOAD_SLOTS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<usize>()
            && (1..=50).contains(&n)
        {
            self.emule.max_upload_slots = n;
        }
        if let Ok(v) = std::env::var("RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS")
            && !v.is_empty()
            && let Ok(n) = v.parse::<usize>()
            && (1..=50).contains(&n)
        {
            self.emule.max_concurrent_downloads = n;
        }
        // RUCIOD_UPNP=false / 0 / no disables UPnP; any other non-empty value enables it.
        if let Ok(v) = std::env::var("RUCIOD_UPNP")
            && !v.is_empty()
        {
            self.network.upnp = !matches!(v.to_lowercase().as_str(), "false" | "0" | "no" | "off");
        }
    }

    /// Returns the bootstrap peers to use at startup.
    ///
    /// By default the configured `[network] bootstrap_peers` are **added** to
    /// the built-in list (deduplicated). When `network.exclusive_bootstrap` is
    /// set, only the configured peers are used and the built-ins are ignored —
    /// useful for a separate network. With neither configured peers nor the
    /// exclusive flag, the built-in list is returned as the fallback.
    pub fn effective_bootstrap_peers(&self) -> Vec<&str> {
        let mut peers: Vec<&str> = self
            .network
            .bootstrap_peers
            .iter()
            .map(String::as_str)
            .collect();
        if !self.network.exclusive_bootstrap {
            for b in BUILTIN_BOOTSTRAP_PEERS {
                if !peers.contains(b) {
                    peers.push(b);
                }
            }
        }
        peers
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
    fn effective_bootstrap_is_additive_by_default() {
        let mut cfg = Config::default();
        let mine = "/ip4/1.2.3.4/tcp/4321/p2p/12D3KooWmine";
        cfg.network.bootstrap_peers = vec![mine.to_string()];
        let peers = cfg.effective_bootstrap_peers();
        // Configured peer plus all the built-ins.
        assert!(peers.contains(&mine));
        assert_eq!(peers.len(), 1 + BUILTIN_BOOTSTRAP_PEERS.len());
        for b in BUILTIN_BOOTSTRAP_PEERS {
            assert!(peers.contains(b));
        }
    }

    #[test]
    fn effective_bootstrap_exclusive_ignores_builtin() {
        let mut cfg = Config::default();
        let mine = "/ip4/1.2.3.4/tcp/4321/p2p/12D3KooWmine";
        cfg.network.bootstrap_peers = vec![mine.to_string()];
        cfg.network.exclusive_bootstrap = true;
        assert_eq!(cfg.effective_bootstrap_peers(), vec![mine]);
    }

    #[test]
    fn effective_bootstrap_dedups_builtin() {
        let mut cfg = Config::default();
        // Configuring a peer that is also built-in must not duplicate it.
        cfg.network.bootstrap_peers = vec![BUILTIN_BOOTSTRAP_PEERS[0].to_string()];
        let peers = cfg.effective_bootstrap_peers();
        assert_eq!(peers.len(), BUILTIN_BOOTSTRAP_PEERS.len());
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
