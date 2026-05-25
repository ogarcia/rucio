//! UPnP / IGD port mapping for transparent NAT traversal.
//!
//! Spawns a background task that:
//!
//! 1. Discovers the UPnP gateway on the LAN.
//! 2. Adds port mappings for:
//!    - TCP 4321 (libp2p)
//!    - UDP 4672 (Kad2, only when `emule-compat` feature is enabled)
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
}

/// Spawn the UPnP background task and return a handle to the external IP.
///
/// The handle starts as `None` and is updated once the gateway responds.
pub fn spawn(cfg: UpnpConfig) -> ExternalIp {
    let external_ip: ExternalIp = Arc::new(RwLock::new(None));
    let ip_handle = Arc::clone(&external_ip);

    tokio::spawn(async move {
        run_upnp_task(cfg, ip_handle).await;
    });

    external_ip
}

// ── Task implementation ───────────────────────────────────────────────────────

async fn run_upnp_task(cfg: UpnpConfig, external_ip: ExternalIp) {
    loop {
        match try_upnp_cycle(&cfg, &external_ip).await {
            Ok(()) => {
                // Renewals finished (shouldn't happen — loop internally).
                // If we get here, pause then retry.
                sleep(RETRY_INTERVAL).await;
            }
            Err(e) => {
                warn!(
                    "UPnP cycle failed: {e} — retrying in {}s",
                    RETRY_INTERVAL.as_secs()
                );
                *external_ip.write().await = None;
                sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

/// One full UPnP cycle: discover → map → renew loop.
async fn try_upnp_cycle(cfg: &UpnpConfig, external_ip: &ExternalIp) -> anyhow::Result<()> {
    // ── 1. Discover gateway ───────────────────────────────────────────────────
    debug!("UPnP: searching for gateway");
    let gateway = search_gateway(SearchOptions {
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("gateway search failed: {e}"))?;

    // ── 2. Get our LAN IP from the gateway's socket ───────────────────────────
    let local_ip = gateway.addr.ip();
    let local_ipv4 = match local_ip {
        IpAddr::V4(v4) => v4,
        IpAddr::V6(_) => {
            anyhow::bail!("UPnP gateway returned IPv6 local address — skipping");
        }
    };

    // ── 3. Get external IP ────────────────────────────────────────────────────
    let ext_ip = gateway
        .get_external_ip()
        .await
        .map_err(|e| anyhow::anyhow!("get_external_ip failed: {e}"))?;
    info!(external_ip = %ext_ip, "UPnP gateway found");
    *external_ip.write().await = Some(ext_ip.to_string());

    // ── 4. Add port mappings ──────────────────────────────────────────────────
    add_mapping(&gateway, local_ipv4, cfg.tcp_port, PortMappingProtocol::TCP).await;

    if let Some(udp_port) = cfg.udp_port {
        add_mapping(&gateway, local_ipv4, udp_port, PortMappingProtocol::UDP).await;
    }

    // ── 5. Renew loop ─────────────────────────────────────────────────────────
    loop {
        sleep(RENEW_INTERVAL).await;

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

        add_mapping(&gateway, local_ipv4, cfg.tcp_port, PortMappingProtocol::TCP).await;

        if let Some(udp_port) = cfg.udp_port {
            add_mapping(&gateway, local_ipv4, udp_port, PortMappingProtocol::UDP).await;
        }
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
