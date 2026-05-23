//! Wire types for the `/rucio/transfer/1.0.0` request-response protocol.
//!
//! Serialised with postcard for compact binary framing.
//! The chunk `data` field is the raw bytes of the chunk; the requester
//! verifies the BLAKE3 hash against the expected value from the file manifest
//! before writing to disk.

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
        /// Raw bytes of the requested chunk.
        data: Vec<u8>,
    },
    /// The provider does not have the requested file or chunk.
    NotFound,
    /// The provider encountered an internal error.
    Error(String),
}
