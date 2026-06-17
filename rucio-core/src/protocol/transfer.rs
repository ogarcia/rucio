//! Wire types for the `/rucio/transfer/1.0.0` request-response protocol.
//!
//! Serialised with postcard for compact binary framing.
//!
//! The chunk `data` field is a self-verifying BLAKE3 *slice* (proof nodes + data)
//! that the requester checks against the file's `root_hash` — no per-chunk hash
//! from the manifest is needed or trusted. Every `Ok` response also carries a
//! short list of other known providers for the same file (PEX).

/// A request for a single chunk of a file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkRequest {
    /// BLAKE3 root hash of the file (identifies the file uniquely).
    pub root_hash: [u8; 32],
    /// Zero-based chunk index within the file.
    pub chunk_idx: u32,
}

/// The response to a [`ChunkRequest`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChunkResponse {
    /// The chunk data was found and is included.
    Ok {
        /// Self-verifying bao slice for the requested chunk (proof nodes + the
        /// chunk's bytes), verifiable against the file's `root_hash`.
        data: Vec<u8>,
        /// PEX — other known providers for this file (base58-encoded PeerIds).
        /// May be empty. Capped at 8 entries by the sender to limit message size.
        peers: Vec<String>,
    },
    /// The provider does not have the requested file or chunk.
    NotFound,
    /// The provider encountered an internal error.
    Error(String),
}
