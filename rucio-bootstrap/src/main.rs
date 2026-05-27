//! `rucio-bootstrap` — a stable Rucio DHT bootstrap node.
//!
//! Meant to run on a server with a fixed public IP and a persistent identity.
//! It joins the rucio Kademlia DHT with only `identify` + `kademlia` (via
//! [`BehaviourConfig::dht_only`]) and keeps the routing table alive so that new
//! nodes always have a known entry point. It does not discover, search, serve
//! or transfer files.
//!
//! Functionally any full `ruciod` is also a DHT participant others can bootstrap
//! from; this binary exists to be a *dedicated, stable* entry point without the
//! overhead of serving content.
//!
//! On first run a documented `config.toml` is written to the XDG config dir
//! (and an Ed25519 identity key is generated alongside it) so that subsequent
//! restarts are fully reproducible without flags.
//!
//! This is role 1 of SPEC phase 5. Role 2 (the passive DHT indexer) is an
//! opt-in extension compiled in with the `indexer` feature and enabled at
//! runtime with `--index` or `indexer.enabled = true` in the config; see the
//! [`indexer`] module.

mod config;

#[cfg(feature = "indexer")]
mod indexer;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::mpsc;
use tracing::{info, warn};

use rucio_net::{BehaviourConfig, NetConfig, NodeCmd, NodeEvent};

#[derive(Parser, Debug)]
#[command(
    name = "rucio-bootstrap",
    version,
    about = "Stable Rucio DHT bootstrap node"
)]
struct Args {
    /// Path to the configuration file.  Defaults to
    /// `$XDG_CONFIG_HOME/rucio-bootstrap/config.toml`.  On first run the file
    /// is created automatically with documented defaults.
    #[arg(long, env = "RUCIO_BOOTSTRAP_CONFIG")]
    config: Option<PathBuf>,

    /// Path to the persistent Ed25519 identity key.  Overrides `node.identity`
    /// in the config file.
    #[arg(long, env = "RUCIO_BOOTSTRAP_IDENTITY")]
    identity: Option<PathBuf>,

    /// Multiaddr to listen on. Repeatable or comma-separated. Overrides
    /// `node.listen` in the config file.
    #[arg(long = "listen", env = "RUCIO_BOOTSTRAP_LISTEN", value_delimiter = ',')]
    listen: Vec<String>,

    /// Multiaddr of an existing node to bootstrap into the DHT. Repeatable or
    /// comma-separated. Overrides `node.bootstrap_peers` in the config file.
    #[arg(
        long = "bootstrap-peer",
        env = "RUCIO_BOOTSTRAP_PEERS",
        value_delimiter = ','
    )]
    bootstrap_peer: Vec<String>,

    /// Enable the passive DHT indexer role. Overrides `indexer.enabled`.
    /// Requires the `indexer` build feature.
    #[cfg(feature = "indexer")]
    #[arg(long)]
    index: bool,

    /// SQLite path for the indexer database. Overrides `indexer.db`.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_INDEX_DB")]
    index_db: Option<PathBuf>,

    /// Address the indexer REST API binds to. Overrides `indexer.api_listen`.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_API_LISTEN")]
    api_listen: Option<std::net::SocketAddr>,

    /// Bearer token guarding the indexer admin endpoints. Overrides
    /// `indexer.api_token`.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_API_TOKEN")]
    api_token: Option<String>,

    /// Drop indexed announcements not refreshed within this many days.
    /// Overrides `indexer.retention_days`.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_RETENTION_DAYS")]
    retention_days: Option<i64>,

    /// Do not resolve file name/size from announcing peers (index hashes only).
    /// When set, overrides `indexer.enrich = true` in the config file.
    #[cfg(feature = "indexer")]
    #[arg(long)]
    no_enrich: bool,

    /// Number of additional Kademlia identities to spawn (beyond the primary).
    /// Overrides `indexer.identity_count`.  Each extra identity listens on an
    /// ephemeral port and bootstraps from the same peers as the primary.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_IDENTITY_COUNT")]
    identity_count: Option<u8>,
}

#[tokio::main]
async fn main() -> Result<()> {
    rucio_core::logging::init("RUCIO_BOOTSTRAP");
    let args = Args::parse();

    // ── Load / initialise config ──────────────────────────────────────────────
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(config::default_config_path);
    let (mut cfg, first_run) = config::load_or_init(&config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;

    if first_run {
        info!(
            path = %config_path.display(),
            "First run — config file created. Edit it to customise the node."
        );
    } else {
        info!(path = %config_path.display(), "Loaded config");
    }

    // ── Merge CLI flags (CLI wins over config file) ───────────────────────────
    if let Some(id) = args.identity {
        cfg.node.identity = Some(id);
    }
    if !args.listen.is_empty() {
        cfg.node.listen = args.listen;
    }
    if !args.bootstrap_peer.is_empty() {
        cfg.node.bootstrap_peers = args.bootstrap_peer;
    }

    #[cfg(feature = "indexer")]
    {
        if args.index {
            cfg.indexer.enabled = true;
        }
        if let Some(db) = args.index_db {
            cfg.indexer.db = Some(db);
        }
        if let Some(al) = args.api_listen {
            cfg.indexer.api_listen = al;
        }
        if args.api_token.is_some() {
            cfg.indexer.api_token = args.api_token;
        }
        if let Some(days) = args.retention_days {
            cfg.indexer.retention_days = days;
        }
        if args.no_enrich {
            cfg.indexer.enrich = false;
        }
        if let Some(n) = args.identity_count {
            cfg.indexer.identity_count = n;
        }
    }

    // ── Resolve effective values ──────────────────────────────────────────────
    let identity_path = cfg
        .node
        .identity
        .unwrap_or_else(config::default_identity_path);
    let listen_addrs = if cfg.node.listen.is_empty() {
        config::NodeConfig::default().listen
    } else {
        cfg.node.listen
    };

    // Pre-compute extra identity paths before identity_path is moved.
    #[cfg(feature = "indexer")]
    let extra_identity_paths: Vec<PathBuf> = (1..=(cfg.indexer.identity_count as usize))
        .map(|i| config::extra_identity_path(&identity_path, i))
        .collect();

    // Resolve the PeerId up front so we can print dialable addresses (spawn
    // loads the same key again internally — load_or_create is idempotent).
    let keypair = rucio_net::identity::load_or_create(&identity_path)?;
    let peer_id = keypair.public().to_peer_id();
    info!(%peer_id, identity = %identity_path.display(), "Starting rucio-bootstrap");

    #[cfg(feature = "indexer")]
    let enrich = cfg.indexer.enrich;
    #[cfg(feature = "indexer")]
    let behaviour = if cfg.indexer.enabled {
        BehaviourConfig::indexer(enrich)
    } else {
        BehaviourConfig::dht_only()
    };
    #[cfg(not(feature = "indexer"))]
    let behaviour = BehaviourConfig::dht_only();

    let net_cfg = NetConfig {
        identity_path,
        listen_addrs,
        behaviour,
    };
    let handle = rucio_net::spawn(&net_cfg)
        .await
        .context("starting the bootstrap node")?;

    // ── Indexer ───────────────────────────────────────────────────────────────
    #[cfg(feature = "indexer")]
    let indexer = if cfg.indexer.enabled {
        let db_path = cfg.indexer.db.unwrap_or_else(config::default_index_db_path);
        Some(
            indexer::Indexer::start(indexer::IndexerOpts {
                db_path,
                api_listen: cfg.indexer.api_listen,
                token: cfg.indexer.api_token,
                retention_days: cfg.indexer.retention_days,
                enrich,
                // Only the primary swarm carries the manifest protocol.
                node_cmd: handle.cmd_tx.clone(),
            })
            .await?,
        )
    } else {
        None
    };

    // ── Fan-in: merge events from all swarms into one channel ─────────────────
    //
    // Each event is tagged with its swarm index (0 = primary) so the event
    // loop can apply swarm-specific logic (e.g. suppress bootstrap announcements
    // for ephemeral extra identities).
    let (fan_tx, mut fan_rx) = mpsc::channel::<(usize, NodeEvent)>(256);
    let mut all_cmd_txs: Vec<mpsc::Sender<NodeCmd>> = Vec::new();

    // Primary swarm forwarder.
    {
        let tx = fan_tx.clone();
        let mut rx = handle.event_rx;
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if tx.send((0, ev)).await.is_err() {
                    break;
                }
            }
        });
    }
    all_cmd_txs.push(handle.cmd_tx);

    // Extra indexer identities (each on an ephemeral port).
    #[cfg(feature = "indexer")]
    if cfg.indexer.enabled {
        for (i, id_path) in extra_identity_paths.into_iter().enumerate() {
            let swarm_idx = i + 1;
            info!(
                identity = %id_path.display(),
                swarm = swarm_idx,
                "Starting extra indexer identity"
            );
            let extra_cfg = NetConfig {
                identity_path: id_path,
                listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".into(), "/ip6/::/tcp/0".into()],
                behaviour: BehaviourConfig::indexer(enrich),
            };
            let extra_handle = rucio_net::spawn(&extra_cfg)
                .await
                .with_context(|| format!("starting indexer identity {swarm_idx}"))?;
            let tx = fan_tx.clone();
            let mut rx = extra_handle.event_rx;
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if tx.send((swarm_idx, ev)).await.is_err() {
                        break;
                    }
                }
            });
            all_cmd_txs.push(extra_handle.cmd_tx);
        }
    }
    // Drop the original sender so the channel closes when all forwarders finish.
    drop(fan_tx);

    // ── Bootstrap all swarms from the configured peers ────────────────────────
    let mut joined = false;
    for raw in &cfg.node.bootstrap_peers {
        match raw.parse::<Multiaddr>() {
            Ok(addr) => {
                for tx in &all_cmd_txs {
                    tx.send(NodeCmd::AddBootstrapPeer(addr.clone())).await.ok();
                }
                joined = true;
            }
            Err(e) => warn!("Ignoring invalid bootstrap_peer {raw:?}: {e}"),
        }
    }
    if joined {
        for tx in &all_cmd_txs {
            tx.send(NodeCmd::KadBootstrapPeersReady).await.ok();
        }
    } else {
        info!("No bootstrap peers configured — running as a seed node (listen only)");
    }

    // ── Main event loop ───────────────────────────────────────────────────────
    let mut connected: usize = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C — shutting down");
                for tx in &all_cmd_txs {
                    tx.send(NodeCmd::Shutdown).await.ok();
                }
                break;
            }
            _ = heartbeat.tick() => {
                info!(connected_peers = connected, "Bootstrap node alive");
            }
            ev = fan_rx.recv() => {
                let Some((swarm_idx, ev)) = ev else {
                    warn!("All node tasks ended unexpectedly");
                    break;
                };
                match ev {
                    NodeEvent::Ready { peer_id: ev_peer_id, ref listen_addrs } => {
                        if swarm_idx == 0 {
                            info!(%ev_peer_id, "Ready — add one of these to a node's bootstrap_peers:");
                            for addr in listen_addrs {
                                announce(addr, &ev_peer_id);
                            }
                        } else {
                            info!(%ev_peer_id, swarm = swarm_idx, "Indexer identity ready");
                        }
                    }
                    NodeEvent::ListenAddrAdded(ref addr) if swarm_idx == 0 => {
                        announce(addr, &peer_id);
                    }
                    NodeEvent::ObservedAddr { ref addr, .. } if swarm_idx == 0 => {
                        info!("Observed public address: {addr}/p2p/{peer_id}");
                    }
                    NodeEvent::PeerConnected { .. } => connected += 1,
                    NodeEvent::PeerDisconnected { .. } => {
                        connected = connected.saturating_sub(1)
                    }
                    NodeEvent::FatalError(ref e) => {
                        warn!(swarm = swarm_idx, "Fatal node error: {e}");
                        break;
                    }
                    #[cfg(feature = "indexer")]
                    NodeEvent::ProviderRecord { ref key, ref provider, .. } => {
                        if let Some(ix) = indexer.as_ref() {
                            ix.record(key, provider).await;
                        }
                    }
                    #[cfg(feature = "indexer")]
                    NodeEvent::ManifestReceived {
                        request_id,
                        ref response,
                        ..
                    } => {
                        if let Some(ix) = indexer.as_ref() {
                            ix.on_manifest(request_id, response.clone()).await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

/// Log a `bootstrap_peers` entry for one listen address.
fn announce(addr: &Multiaddr, peer_id: &PeerId) {
    let dialable = format!("{addr}/p2p/{peer_id}");
    if is_unspecified(addr) {
        info!("  {dialable}   (replace 0.0.0.0 / :: with the server's public IP)");
    } else {
        info!("  {dialable}");
    }
}

/// Whether the multiaddr contains an unspecified (`0.0.0.0` / `::`) IP.
fn is_unspecified(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_unspecified(),
        Protocol::Ip6(ip) => ip.is_unspecified(),
        _ => false,
    })
}
