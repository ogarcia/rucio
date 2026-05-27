//! TOML configuration file for `rucio-bootstrap`.
//!
//! On first run, [`load_or_init`] writes a documented template to
//! `~/.config/rucio-bootstrap/config.toml` (XDG config dir). CLI flags always
//! override the file; the file is the persistent default.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Top-level configuration loaded from the TOML file.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub indexer: IndexerConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Path to the persistent Ed25519 identity key file.
    pub identity: Option<PathBuf>,
    /// Multiaddrs to listen on.
    #[serde(default = "default_listen")]
    pub listen: Vec<String>,
    /// Multiaddrs of nodes to bootstrap from. Empty = seed node.
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerConfig {
    /// Enable the indexer role at startup (equivalent to `--index`).
    #[serde(default)]
    pub enabled: bool,
    /// SQLite database path.
    pub db: Option<PathBuf>,
    /// REST API bind address.
    #[serde(default = "default_api_listen")]
    pub api_listen: SocketAddr,
    /// Bearer token for admin endpoints. `None` disables them.
    pub api_token: Option<String>,
    /// Drop records not refreshed within this many days.
    #[serde(default = "default_retention_days")]
    pub retention_days: i64,
    /// Resolve file name/size from announcing peers.
    #[serde(default = "default_enrich")]
    pub enrich: bool,
    /// Number of additional Kademlia identities to spawn alongside the
    /// primary, spreading DHT coverage across the keyspace. Extra keys are
    /// stored as `identity-1.key`, `identity-2.key`, … next to the primary.
    /// 0 = single identity (default).
    #[serde(default)]
    pub identity_count: u8,
}

// ── serde defaults ────────────────────────────────────────────────────────────

fn default_listen() -> Vec<String> {
    vec!["/ip4/0.0.0.0/tcp/4321".into(), "/ip6/::/tcp/4321".into()]
}

fn default_api_listen() -> SocketAddr {
    "127.0.0.1:8090".parse().expect("hardcoded addr is valid")
}

fn default_retention_days() -> i64 {
    30
}

fn default_enrich() -> bool {
    true
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            identity: None,
            listen: default_listen(),
            bootstrap_peers: vec![],
        }
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            db: None,
            api_listen: default_api_listen(),
            api_token: None,
            retention_days: default_retention_days(),
            enrich: default_enrich(),
            identity_count: 0,
        }
    }
}

// ── default paths ─────────────────────────────────────────────────────────────

pub fn default_config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rucio-bootstrap")
        .join("config.toml")
}

pub fn default_identity_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rucio-bootstrap")
        .join("identity.key")
}

pub fn default_index_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rucio-bootstrap")
        .join("index.db")
}

#[cfg(feature = "indexer")]
/// Path for the i-th extra indexer identity, derived from the primary.
///
/// `identity.key` → `identity-1.key`, `identity-2.key`, …
pub fn extra_identity_path(primary: &Path, i: usize) -> PathBuf {
    let dir = primary.parent().unwrap_or_else(|| Path::new("."));
    let stem = primary
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("identity");
    match primary.extension().and_then(|s| s.to_str()) {
        Some(ext) => dir.join(format!("{stem}-{i}.{ext}")),
        None => dir.join(format!("{stem}-{i}")),
    }
}

// ── load / init ───────────────────────────────────────────────────────────────

/// Load the config from `path`, or write a documented default template there
/// on first run.
///
/// Returns `(config, first_run)`.  On first run the identity and database paths
/// in the config are pre-populated with the platform default locations.
pub fn load_or_init(path: &Path) -> Result<(Config, bool)> {
    if path.exists() {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        return Ok((cfg, false));
    }

    let identity = default_identity_path();
    let db = default_index_db_path();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config dir {}", parent.display()))?;
    }
    std::fs::write(path, render_template(&identity, &db))
        .with_context(|| format!("writing default config to {}", path.display()))?;

    let cfg = Config {
        node: NodeConfig {
            identity: Some(identity),
            ..NodeConfig::default()
        },
        indexer: IndexerConfig {
            db: Some(db),
            ..IndexerConfig::default()
        },
    };
    Ok((cfg, true))
}

fn render_template(identity: &Path, db: &Path) -> String {
    let id = identity.to_string_lossy();
    let db = db.to_string_lossy();
    format!(
        r#"# rucio-bootstrap configuration
# Written on first run — edit freely.
# CLI flags always override these values for a single invocation.

[node]
# Persistent Ed25519 identity (keeps the same PeerId across restarts).
identity = "{id}"

# Multiaddrs to listen on (env: RUCIO_BOOTSTRAP_LISTEN, comma-separated).
listen = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

# Existing nodes to bootstrap from. Leave empty to run as a seed node.
# Example: bootstrap_peers = ["/ip4/1.2.3.4/tcp/4321/p2p/12D3Koo..."]
bootstrap_peers = []

[indexer]
# Enable the passive DHT indexer role.
# Requires: compiled with --features indexer.  CLI override: --index
enabled = false

# SQLite database path.
db = "{db}"

# REST API bind address (env: RUCIO_BOOTSTRAP_API_LISTEN).
api_listen = "127.0.0.1:8090"

# Bearer token for /api/v1/admin/* endpoints. Unset = admin disabled.
# api_token = "change-me"

# Prune records not refreshed within this many days.
retention_days = 30

# Resolve file name/size from announcing peers (recommended).
enrich = true

# Additional Kademlia identities to spread DHT coverage across the keyspace.
# Each extra identity listens on an ephemeral port and bootstraps from the
# same peers as the primary.  Keys are stored as identity-1.key, identity-2.key, …
# next to the primary identity key.  0 = single identity (default).
identity_count = 0
"#
    )
}
