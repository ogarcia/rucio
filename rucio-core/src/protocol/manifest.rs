//! Wire types for the `/rucio/manifest/1.0.0` request-response protocol.
//!
//! A manifest contains the full chunk list for a file — root hash, total
//! size, and the hash + size of every individual chunk.  The downloader
//! fetches the manifest first, stores it in the DB, then requests chunks.

/// Request the manifest for a file identified by its root hash.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManifestRequest {
    pub root_hash: [u8; 32],
}

/// A single chunk descriptor returned in a manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkInfo {
    pub idx: u32,
    pub hash: [u8; 32],
    pub size: u32,
}

/// The response to a [`ManifestRequest`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ManifestResponse {
    /// The manifest was found.
    Ok {
        root_hash: [u8; 32],
        name: String,
        total_size: u64,
        chunk_size: u32,
        chunks: Vec<ChunkInfo>,
    },
    /// The provider does not have this file.
    NotFound,
    /// Internal error on the provider side.
    Error(String),
}
