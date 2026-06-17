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

/// The response to a [`ManifestRequest`].
///
/// With bao verified streaming the manifest no longer carries per-chunk hashes:
/// each chunk is fetched as a self-verifying slice checked against `root_hash`.
/// The manifest is now just the metadata needed to plan the download.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ManifestResponse {
    /// The manifest was found.
    Ok {
        root_hash: [u8; 32],
        name: String,
        total_size: u64,
        /// Transfer chunk size in bytes (the unit of a `ChunkRequest`).
        chunk_size: u32,
        /// Number of transfer chunks: `ceil(total_size / chunk_size)`.
        chunk_count: u32,
    },
    /// The provider does not have this file.
    NotFound,
    /// Internal error on the provider side.
    Error(String),
}
