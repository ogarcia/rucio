use crate::protocol::node::{NodeClass, Reachability};

/// GET /api/v1/status
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StatusResponse {
    pub peer_id: String,
    pub class: NodeClass,
    /// State of the AutoNAT reachability check, explaining why the node has or
    /// has not reached `HighId` (confirmed / verifying / no servers to verify).
    #[serde(default)]
    pub reachability: Reachability,
    pub connected_peers: usize,
    pub listen_addrs: Vec<String>,
    /// External addresses observed by remote peers via the Identify protocol.
    /// These are the addresses other nodes on the internet see us from.
    /// May be empty until at least one peer has connected and reported back.
    pub observed_addrs: Vec<String>,
    pub uptime_secs: u64,
    pub version: String,
    /// Short git commit hash the daemon was built from, or empty when git was
    /// unavailable at build time. Displayed with `version` (e.g. the web About
    /// panel shows `v0.36.0-dev (49e59a1)`).
    #[serde(default)]
    pub commit: String,
    /// Number of rucio/libp2p downloads currently active (finding providers,
    /// queued, transferring, or stalled). The eMule equivalent lives in the
    /// eMule status response.
    #[serde(default)]
    pub active_downloads: usize,
    /// Number of peers currently downloading a file from us over rucio/libp2p
    /// right now (distinct active uploads).
    #[serde(default)]
    pub active_uploads: usize,
    /// External IP address as reported by UPnP gateway.
    /// `null` when no UPnP gateway is available on the LAN.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_ip: Option<String>,
}

/// GET /api/v1/peers
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PeersResponse {
    pub peers: Vec<PeerResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PeerResponse {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub class: NodeClass,
    /// HTTP User-Agent-style identifier the peer reported via the Identify
    /// protocol (e.g. `Rucio/0.28.0 (Linux x86_64) libp2p/0.56.0`). Absent for a
    /// peer that has not completed an Identify exchange yet (e.g. just seen via
    /// mDNS), or one running software that advertises no agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<String>,
}
