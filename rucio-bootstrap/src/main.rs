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
//! This is role 1 of SPEC phase 5. Role 2 (the passive DHT indexer) is an
//! opt-in extension compiled in with the `indexer` feature and enabled at
//! runtime with `--index`; see the [`indexer`] module.

#[cfg(feature = "indexer")]
mod indexer;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use libp2p::{Multiaddr, PeerId};
use tracing::{info, warn};

use rucio_net::{BehaviourConfig, NetConfig, NodeCmd, NodeEvent};

#[derive(Parser, Debug)]
#[command(
    name = "rucio-bootstrap",
    version,
    about = "Stable Rucio DHT bootstrap node"
)]
struct Args {
    /// Path to the persistent Ed25519 identity key. A stable key keeps the
    /// node's PeerId — and therefore its bootstrap multiaddr — constant across
    /// restarts.
    #[arg(long, env = "RUCIO_BOOTSTRAP_IDENTITY")]
    identity: Option<PathBuf>,

    /// Multiaddr to listen on. Repeatable or comma-separated. Defaults to TCP
    /// 4321 on all IPv4 and IPv6 interfaces (the rucio network port).
    #[arg(long = "listen", env = "RUCIO_BOOTSTRAP_LISTEN", value_delimiter = ',')]
    listen: Vec<String>,

    /// Multiaddr of an existing node to bootstrap into the DHT. Repeatable or
    /// comma-separated. Leave empty to run as a seed node (listen only).
    #[arg(
        long = "bootstrap-peer",
        env = "RUCIO_BOOTSTRAP_PEERS",
        value_delimiter = ','
    )]
    bootstrap_peer: Vec<String>,

    /// Enable the passive DHT indexer role: record provider announcements and
    /// expose a search/admin REST API. Requires the `indexer` build feature.
    #[cfg(feature = "indexer")]
    #[arg(long)]
    index: bool,

    /// SQLite path for the indexer database.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_INDEX_DB")]
    index_db: Option<PathBuf>,

    /// Address the indexer REST API binds to.
    #[cfg(feature = "indexer")]
    #[arg(
        long,
        env = "RUCIO_BOOTSTRAP_API_LISTEN",
        default_value = "127.0.0.1:8090"
    )]
    api_listen: std::net::SocketAddr,

    /// Bearer token guarding the indexer admin endpoints. When unset, the admin
    /// endpoints are disabled.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_API_TOKEN")]
    api_token: Option<String>,

    /// Drop indexed announcements not refreshed within this many days.
    #[cfg(feature = "indexer")]
    #[arg(long, env = "RUCIO_BOOTSTRAP_RETENTION_DAYS", default_value_t = 30)]
    retention_days: i64,

    /// Do not resolve file name/size from announcing peers (index hashes only).
    /// By default the indexer enriches each hash via the manifest protocol so
    /// the search API can match on names.
    #[cfg(feature = "indexer")]
    #[arg(long)]
    no_enrich: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    rucio_core::logging::init("RUCIO_BOOTSTRAP");
    let args = Args::parse();

    let identity_path = args.identity.unwrap_or_else(default_identity_path);
    let listen_addrs = if args.listen.is_empty() {
        default_listen_addrs()
    } else {
        args.listen
    };

    // Resolve the PeerId up front so we can print dialable addresses (spawn
    // loads the same key again internally — load_or_create is idempotent).
    let keypair = rucio_net::identity::load_or_create(&identity_path)?;
    let peer_id = keypair.public().to_peer_id();
    info!(%peer_id, identity = %identity_path.display(), "Starting rucio-bootstrap");

    #[cfg(feature = "indexer")]
    let enrich = !args.no_enrich;
    #[cfg(feature = "indexer")]
    let behaviour = if args.index {
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
    let mut handle = rucio_net::spawn(&net_cfg)
        .await
        .context("starting the bootstrap node")?;

    #[cfg(feature = "indexer")]
    let indexer = if args.index {
        let db_path = args.index_db.clone().unwrap_or_else(default_index_db_path);
        Some(
            indexer::Indexer::start(indexer::IndexerOpts {
                db_path,
                api_listen: args.api_listen,
                token: args.api_token.clone(),
                retention_days: args.retention_days,
                enrich,
                node_cmd: handle.cmd_tx.clone(),
            })
            .await?,
        )
    } else {
        None
    };

    // Join the DHT through the configured peers, if any.
    let mut joined = false;
    for raw in &args.bootstrap_peer {
        match raw.parse::<Multiaddr>() {
            Ok(addr) => {
                handle
                    .cmd_tx
                    .send(NodeCmd::AddBootstrapPeer(addr))
                    .await
                    .ok();
                joined = true;
            }
            Err(e) => warn!("Ignoring invalid --bootstrap-peer {raw:?}: {e}"),
        }
    }
    if joined {
        handle
            .cmd_tx
            .send(NodeCmd::KadBootstrapPeersReady)
            .await
            .ok();
    } else {
        info!("No bootstrap peers configured — running as a seed node (listen only)");
    }

    let mut connected: usize = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C — shutting down");
                handle.cmd_tx.send(NodeCmd::Shutdown).await.ok();
                break;
            }
            _ = heartbeat.tick() => {
                info!(connected_peers = connected, "Bootstrap node alive");
            }
            ev = handle.event_rx.recv() => {
                let Some(ev) = ev else {
                    warn!("Node task ended unexpectedly");
                    break;
                };
                match ev {
                    NodeEvent::Ready { peer_id, listen_addrs } => {
                        info!(%peer_id, "Ready — add one of these to a node's bootstrap_peers:");
                        for addr in &listen_addrs {
                            announce(addr, &peer_id);
                        }
                    }
                    NodeEvent::ListenAddrAdded(addr) => announce(&addr, &peer_id),
                    NodeEvent::ObservedAddr { addr, .. } => {
                        info!("Observed public address: {addr}/p2p/{peer_id}");
                    }
                    NodeEvent::PeerConnected { .. } => connected += 1,
                    NodeEvent::PeerDisconnected { .. } => {
                        connected = connected.saturating_sub(1)
                    }
                    NodeEvent::FatalError(e) => {
                        warn!("Fatal node error: {e}");
                        break;
                    }
                    #[cfg(feature = "indexer")]
                    NodeEvent::ProviderRecord { key, provider, .. } => {
                        if let Some(ix) = indexer.as_ref() {
                            ix.record(&key, &provider).await;
                        }
                    }
                    #[cfg(feature = "indexer")]
                    NodeEvent::ManifestReceived {
                        request_id,
                        response,
                        ..
                    } => {
                        if let Some(ix) = indexer.as_ref() {
                            ix.on_manifest(request_id, response).await;
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

/// Whether the multiaddr contains an unspecified (`0.0.0.0` / `::`) IP, which a
/// remote peer cannot dial — the operator must substitute the public IP.
fn is_unspecified(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_unspecified(),
        Protocol::Ip6(ip) => ip.is_unspecified(),
        _ => false,
    })
}

fn default_identity_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rucio-bootstrap")
        .join("identity.key")
}

fn default_listen_addrs() -> Vec<String> {
    vec![
        "/ip4/0.0.0.0/tcp/4321".to_string(),
        "/ip6/::/tcp/4321".to_string(),
    ]
}

#[cfg(feature = "indexer")]
fn default_index_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rucio-bootstrap")
        .join("index.db")
}
