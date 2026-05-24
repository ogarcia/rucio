//! WebSocket event types pushed from the daemon to connected clients.
//!
//! Every message on the `/api/ws` channel is a JSON-serialized [`WsEvent`].
//! Clients can discriminate on the `type` field.

use crate::api::downloads::DownloadResponse;
use crate::protocol::node::NodeClass;

/// A single event emitted by the daemon over the WebSocket bus.
///
/// Serialized as tagged JSON:
/// ```json
/// { "type": "download_progress", "data": { ... } }
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WsEvent {
    /// One or more downloads changed state or made progress.
    /// Emitted whenever the download engine ticks and at least one active
    /// download exists.
    DownloadProgress(Vec<DownloadResponse>),

    /// The number of files currently being indexed changed.
    IndexingCount { pending: usize },

    /// A new search result arrived for an open query.
    SearchResult(crate::api::search::SearchResultResponse),

    /// A peer connected to this node.
    PeerConnected { peer_id: String },

    /// A peer disconnected from this node.
    PeerDisconnected { peer_id: String },

    /// The node's connectivity class changed (e.g. Unknown → HighId).
    NodeClassChanged { class: NodeClass },
}
