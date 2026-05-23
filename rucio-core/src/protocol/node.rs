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
