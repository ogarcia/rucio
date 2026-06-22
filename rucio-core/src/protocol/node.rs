/// Connectivity class of a node, inspired by eMule's LowID/HighID.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
pub enum NodeClass {
    /// Node has a publicly reachable address. Can serve chunks to any peer.
    HighId,
    /// Node is behind NAT/CGNAT and cannot receive inbound connections.
    /// Can search and download but is not announced as a provider in the DHT.
    LowId,
    /// Class not yet determined (startup phase).
    #[default]
    Unknown,
}

/// State of the AutoNAT reachability check, surfaced alongside [`NodeClass`]
/// for diagnostics: it explains *why* a node has not reached `HighId` yet.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
pub enum Reachability {
    /// An external address has been confirmed — the node is `HighId`.
    Confirmed,
    /// Not `HighId` yet, but an AutoNAT server is connected and can verify us;
    /// a reachability probe is in progress or pending.
    Verifying,
    /// Not `HighId` yet and no AutoNAT server is connected to verify us. The
    /// node will dial a bootstrap on the next external-address candidate.
    #[default]
    NoServers,
}

/// Basic information about a connected peer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct PeerInfo {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub class: NodeClass,
}

/// Status of the local node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NodeStatus {
    pub peer_id: String,
    pub class: NodeClass,
    pub connected_peers: usize,
    pub uptime_secs: u64,
}
