//! TOML configuration file for `rucio-bootstrap`.
//!
//! Every field has a default, so the node runs with no config file at all —
//! handy for server deployments driven entirely by env vars / flags. [`load`]
//! reads the file if present and otherwise returns the built-in defaults
//! without writing anything. [`write_template`] writes a documented example to
//! the config path on demand (the `--init-config` flag). CLI flags always
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
    /// Run the indexer role at startup. Defaults to `true` when built with the
    /// `indexer` feature — the whole point of that build — so a plain bootstrap
    /// node needs `enabled = false` (or the `--no-index` flag).
    #[serde(default = "default_indexer_enabled")]
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
    "127.0.0.1:3003".parse().expect("hardcoded addr is valid")
}

fn default_retention_days() -> i64 {
    30
}

fn default_enrich() -> bool {
    true
}

fn default_indexer_enabled() -> bool {
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
            enabled: default_indexer_enabled(),
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

/// Load the config from `path`, or return the built-in defaults if the file
/// does not exist.
///
/// Nothing is written: every field has a default (see the module docs), so a
/// missing file is a valid "all defaults" configuration. Unresolved paths
/// (identity, database) fall back to their platform defaults at the call site.
pub fn load(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
}

/// Write a documented example config template to `path`, with the identity and
/// database fields pre-filled with the platform default locations.
///
/// Refuses to overwrite an existing file. Used by the `--init-config` flag so
/// operators can opt into a config file instead of having one written silently
/// on first run.
pub fn write_template(path: &Path) -> Result<()> {
    if path.exists() {
        anyhow::bail!(
            "config file already exists at {} — refusing to overwrite",
            path.display()
        );
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config dir {}", parent.display()))?;
    }
    let identity = default_identity_path();
    let db = default_index_db_path();
    std::fs::write(path, render_template(&identity, &db))
        .with_context(|| format!("writing config template to {}", path.display()))
}

fn render_template(identity: &Path, db: &Path) -> String {
    let id = identity.to_string_lossy();
    let db = db.to_string_lossy();
    format!(
        r#"# rucio-bootstrap configuration
# Example written by `rucio-bootstrap --init-config` — edit freely.
# Every value shown is also the built-in default, so you only need to keep the
# lines you actually want to change. CLI flags and env vars override the file.

[node]
# Persistent Ed25519 identity (keeps the same PeerId across restarts).
identity = "{id}"

# Multiaddrs to listen on (env: RUCIO_BOOTSTRAP_LISTEN, comma-separated).
listen = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

# Existing nodes to bootstrap from. Leave empty to run as a seed node.
# Example: bootstrap_peers = ["/ip4/1.2.3.4/tcp/4321/p2p/12D3Koo..."]
bootstrap_peers = []

[indexer]
# Run the passive DHT indexer role. With an `indexer`-feature build it runs by
# default; set this to false (or pass --no-index) for a plain bootstrap node.
enabled = true

# SQLite database path.
db = "{db}"

# REST API bind address (env: RUCIO_BOOTSTRAP_API_LISTEN).
api_listen = "127.0.0.1:3003"

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
