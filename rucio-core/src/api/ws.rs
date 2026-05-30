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
    /// Liveness keepalive. Sent once immediately when a client connects (so the
    /// connection indicator turns green right away, without waiting for any
    /// download/indexing activity) and periodically thereafter.
    Ping,

    /// Aggregate transfer speeds for the current session (5-second moving
    /// average).  Emitted every second in the ws_tick so the client can show
    /// live up/down rates without polling the metrics endpoint.
    SessionStats {
        download_speed: u64,
        upload_speed: u64,
    },

    /// One or more downloads changed state or made progress.
    /// Emitted whenever the download engine ticks and at least one active
    /// download exists.
    DownloadProgress(Vec<DownloadResponse>),

    /// The number of files currently being indexed changed.
    IndexingCount { pending: usize },

    /// A new search result arrived for an open query. Carries the owning
    /// `search_id` so the client can route it to the right search (several may
    /// run in parallel), and the unified result shape (Rucio or eMule).
    SearchResult {
        search_id: u64,
        result: crate::api::searches::SearchResult,
    },

    /// A search's lifecycle state and/or result count changed (e.g. its window
    /// closed → `done`). Lets the client keep the search list live without polling.
    SearchStateChanged {
        id: u64,
        state: crate::api::searches::SearchState,
        result_count: usize,
        /// Whether the eMule/Kad2 leg is currently queued waiting for its turn.
        #[serde(default)]
        emule_queued: bool,
    },

    /// A peer connected to this node.
    PeerConnected { peer_id: String },

    /// A peer disconnected from this node.
    PeerDisconnected { peer_id: String },

    /// The node's connectivity class changed (e.g. Unknown → HighId).
    NodeClassChanged { class: NodeClass },
}
