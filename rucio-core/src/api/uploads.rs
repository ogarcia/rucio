//! Active-upload DTOs: peers currently pulling data *from* this node.
//!
//! Mirrors the download-side view (`downloads.rs`) for the other direction.
//! An upload entry exists only while a remote peer is actively transferring a
//! file from us; the daemon prunes it when the transfer ends (eMule sessions
//! deregister explicitly, rucio chunk serving expires on inactivity).

/// Which network a given upload is served over.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UploadNetwork {
    /// Native rucio/libp2p chunk transfer.
    Rucio,
    /// eMule/ed2k partial-file upload.
    Emule,
}

/// A single peer actively downloading a file from us right now.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ActiveUpload {
    pub network: UploadNetwork,
    /// Remote peer identity: a base58 PeerId (rucio) or `ip:port` (eMule).
    pub peer: String,
    /// File hash in hex — BLAKE3 root hash (rucio) or MD4 ed2k hash (eMule).
    pub file_hash: String,
    /// File display name, when known.
    pub file_name: Option<String>,
    /// Bytes sent to this peer for this file during the current session.
    pub bytes_sent: u64,
    /// Smoothed upload rate to this peer, in bytes per second.
    pub rate_bps: u64,
    /// Unix timestamp (seconds) when this upload started.
    pub started_at: u64,
}

/// GET /api/v1/uploads
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct UploadsResponse {
    pub uploads: Vec<ActiveUpload>,
}
