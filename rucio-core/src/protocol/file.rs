use crate::protocol::chunk::Hash;

/// Full descriptor of a file shared on the network.
/// This is what gets exchanged during search and announced in the DHT.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileDescriptor {
    /// Suggested file name.
    pub name: String,
    /// Total size in bytes.
    pub size: u64,
    /// BLAKE3 root hash of the Merkle tree — canonical identifier of the file.
    pub root_hash: Hash,
    /// Number of transfer chunks (`ceil(size / CHUNK_SIZE)`).
    pub chunk_count: u32,
    /// Optional MIME type.
    pub mime_type: Option<String>,
    /// Unix timestamp (seconds) when the descriptor was created.
    pub created_at: u64,
}

impl FileDescriptor {
    /// Number of chunks in this file.
    pub fn chunk_count(&self) -> usize {
        self.chunk_count as usize
    }
}
