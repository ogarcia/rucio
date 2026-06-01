//! UPnP / IGD port mapping for transparent NAT traversal.
//!
//! Spawns a background task that:
//!
//! 1. Discovers the UPnP gateway on the LAN.
//! 2. Adds port mappings for:
//!    - TCP 4321 (libp2p)
//!    - UDP 4672 (Kad2, only when `emule-compat` feature is enabled)
//!    - TCP 4662 (eMule peer connections, only when `emule-compat` feature is enabled)
//! 3. Logs the external IP address.
//! 4. Renews the leases every [`RENEW_INTERVAL`] seconds (before expiry).
//! 5. Re-discovers the gateway if it becomes unreachable.
//!
//! ## Design
//!
//! The task is fire-and-forget: it is spawned at daemon startup and never
//! joined.  Failures are logged as WARN (no gateway on LAN is normal in
//! many environments) and retried automatically.
//!
//! The external IP is published via an `Arc<RwLock<Option<String>>>` so that
//! the status API can include it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use igd_next::aio::tokio::search_gateway;
use igd_next::{PortMappingProtocol, SearchOptions};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Lease duration requested from the gateway (seconds).
const LEASE_SECS: u32 = 3600;

/// How often to renew leases (seconds).  Must be < LEASE_SECS.
const RENEW_INTERVAL: Duration = Duration::from_secs(3000);

/// How long to wait before retrying after a failure.
const RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Description advertised to the gateway for each mapping.
const MAPPING_DESC: &str = "rucio";

// ── Public API ────────────────────────────────────────────────────────────────

/// Shared external-IP state.  `None` until UPnP discovery succeeds.
pub type ExternalIp = Arc<RwLock<Option<String>>>;

/// Configuration for the UPnP task.
#[derive(Debug, Clone)]
pub struct UpnpConfig {
    /// libp2p TCP port to map (external == internal).
    pub tcp_port: u16,
    /// Kad2 UDP port to map (external == internal).  `None` disables UDP mapping.
    pub udp_port: Option<u16>,
    /// eMule TCP port for incoming peer connections.  `None` disables TCP mapping.
    pub emule_tcp_port: Option<u16>,
}

/// Handle to the running UPnP task.
pub struct UpnpHandle {
    /// Shared external-IP state, updated once the gateway responds.
    pub external_ip: ExternalIp,
    /// Signals the task to remove its mappings and exit.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// The background task, awaited (bounded) during graceful shutdown.
    task: tokio::task::JoinHandle<()>,
}

impl UpnpHandle {
    /// Tell the task to unmap its ports, then wait — bounded — for it to finish.
    /// Best-effort: the leases also expire on their own, so a timeout is benign.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        if tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .is_err()
        {
            warn!("Timed out removing UPnP mappings on shutdown");
        }
    }
}

/// Spawn the UPnP background task and return a handle to it.
///
/// The external IP starts as `None` and is updated once the gateway responds.
pub fn spawn(cfg: UpnpConfig) -> UpnpHandle {
    let external_ip: ExternalIp = Arc::new(RwLock::new(None));
    let ip_handle = Arc::clone(&external_ip);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let task = tokio::spawn(async move {
        run_upnp_task(cfg, ip_handle, shutdown_rx).await;
    });

    UpnpHandle {
        external_ip,
        shutdown_tx,
        task,
    }
}

// ── Task implementation ───────────────────────────────────────────────────────

/// The set of `(external_port, protocol)` mappings this task manages — used for
/// both adding/renewing and removing them on shutdown so the two stay in sync.
fn mappings(cfg: &UpnpConfig) -> Vec<(u16, PortMappingProtocol)> {
    let mut v = vec![(cfg.tcp_port, PortMappingProtocol::TCP)];
    if let Some(udp_port) = cfg.udp_port {
        v.push((udp_port, PortMappingProtocol::UDP));
    }
    if let Some(emule_tcp) = cfg.emule_tcp_port {
        v.push((emule_tcp, PortMappingProtocol::TCP));
    }
    v
}

async fn run_upnp_task(
    cfg: UpnpConfig,
    external_ip: ExternalIp,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        match try_upnp_cycle(&cfg, &external_ip, &mut shutdown_rx).await {
            // Shutdown requested: mappings removed, stop the supervisor.
            Ok(Cycle::Shutdown) => return,
            Err(e) => {
                warn!(
                    "UPnP cycle failed: {e} — retrying in {}s",
                    RETRY_INTERVAL.as_secs()
                );
                *external_ip.write().await = None;
                // Pause before retrying, but wake immediately on shutdown.
                tokio::select! {
                    _ = sleep(RETRY_INTERVAL) => {}
                    _ = shutdown_rx.changed() => {}
                }
            }
        }
    }
}

/// How a UPnP cycle ended. Only shutdown ends it successfully — otherwise the
/// renew loop runs forever or the cycle returns `Err`.
enum Cycle {
    Shutdown,
}

/// One full UPnP cycle: discover → map → renew loop (until shutdown).
async fn try_upnp_cycle(
    cfg: &UpnpConfig,
    external_ip: &ExternalIp,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<Cycle> {
    // ── 1. Discover gateway ───────────────────────────────────────────────────
    debug!("UPnP: searching for gateway");
    let gateway = search_gateway(SearchOptions {
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("gateway search failed: {e}"))?;

    // ── 2. Determine OUR LAN IP (the internal client for the mapping) ─────────
    // `gateway.addr` is the ROUTER's address, not ours. A secure IGD rejects
    // AddPortMapping with UPnP error 606 ("not authorized") unless the request's
    // NewInternalClient is the requester's own IP, so we must map to our local
    // interface address — the one the OS would use to reach the gateway.
    let local_ipv4 = local_ip_towards(gateway.addr)
        .ok_or_else(|| anyhow::anyhow!("could not determine local IP towards gateway"))?;
    debug!(local_ip = %local_ipv4, "UPnP: mapping to local interface");

    // ── 3. Get external IP ────────────────────────────────────────────────────
    let ext_ip = gateway
        .get_external_ip()
        .await
        .map_err(|e| anyhow::anyhow!("get_external_ip failed: {e}"))?;
    info!(external_ip = %ext_ip, "UPnP gateway found");
    *external_ip.write().await = Some(ext_ip.to_string());

    // ── 4. Add port mappings ──────────────────────────────────────────────────
    for (port, proto) in mappings(cfg) {
        add_mapping(&gateway, local_ipv4, port, proto).await;
    }

    // ── 5. Renew loop ─────────────────────────────────────────────────────────
    loop {
        tokio::select! {
            _ = sleep(RENEW_INTERVAL) => {
                debug!("UPnP: renewing port mappings");

                // Re-check external IP in case it changed (dynamic ISP).
                if let Ok(new_ext) = gateway.get_external_ip().await {
                    let changed = external_ip
                        .read()
                        .await
                        .as_deref()
                        .map(|old| old != new_ext.to_string().as_str())
                        .unwrap_or(true);
                    if changed {
                        info!(external_ip = %new_ext, "UPnP external IP changed");
                        *external_ip.write().await = Some(new_ext.to_string());
                    }
                }

                for (port, proto) in mappings(cfg) {
                    add_mapping(&gateway, local_ipv4, port, proto).await;
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    // Etiquette: drop our mappings before exiting (they would
                    // expire on their own, but leaving stale entries is rude).
                    info!("UPnP: removing port mappings on shutdown");
                    for (port, proto) in mappings(cfg) {
                        remove_mapping(&gateway, port, proto).await;
                    }
                    *external_ip.write().await = None;
                    return Ok(Cycle::Shutdown);
                }
            }
        }
    }
}

/// Determine the local IPv4 the OS would use to reach `gateway` — our LAN
/// address, used as the internal client of port mappings. Connecting a UDP
/// socket sends no packets; it just resolves the routed source address.
fn local_ip_towards(gateway: SocketAddr) -> Option<Ipv4Addr> {
    let socket = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect(gateway).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(v4) if !v4.is_unspecified() => Some(v4),
        _ => None,
    }
}

/// Add (or refresh) a single port mapping, logging the result.
async fn add_mapping(
    gateway: &igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>,
    local_ip: Ipv4Addr,
    port: u16,
    proto: PortMappingProtocol,
) {
    let local_addr = SocketAddr::from((local_ip, port));
    let proto_str = match proto {
        PortMappingProtocol::TCP => "TCP",
        PortMappingProtocol::UDP => "UDP",
    };

    match gateway
        .add_port(proto, port, local_addr, LEASE_SECS, MAPPING_DESC)
        .await
    {
        Ok(()) => info!(port, proto = proto_str, "UPnP port mapped"),
        Err(e) => warn!(port, proto = proto_str, "UPnP port mapping failed: {e}"),
    }
}

/// Remove a single port mapping, logging the result.
async fn remove_mapping(
    gateway: &igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>,
    port: u16,
    proto: PortMappingProtocol,
) {
    let proto_str = match proto {
        PortMappingProtocol::TCP => "TCP",
        PortMappingProtocol::UDP => "UDP",
    };

    match gateway.remove_port(proto, port).await {
        Ok(()) => info!(port, proto = proto_str, "UPnP port unmapped"),
        Err(e) => warn!(port, proto = proto_str, "UPnP port unmap failed: {e}"),
    }
}
