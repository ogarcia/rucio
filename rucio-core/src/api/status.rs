use crate::protocol::node::NodeClass;

/// GET /api/v1/status
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StatusResponse {
    pub peer_id: String,
    pub class: NodeClass,
    pub connected_peers: usize,
    pub listen_addrs: Vec<String>,
    /// External addresses observed by remote peers via the Identify protocol.
    /// These are the addresses other nodes on the internet see us from.
    /// May be empty until at least one peer has connected and reported back.
    pub observed_addrs: Vec<String>,
    pub uptime_secs: u64,
    pub version: String,
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
}
