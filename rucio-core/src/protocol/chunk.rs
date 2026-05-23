/// A BLAKE3 hash identifying a chunk or file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        Self(*hash.as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A single chunk of a file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
    /// Position of this chunk within the file (0-indexed).
    pub index: u32,
    /// BLAKE3 hash of this chunk's raw bytes.
    pub hash: Hash,
    /// Size in bytes of this chunk.
    pub size: u32,
}

/// Default chunk size: 4 MiB.
pub const CHUNK_SIZE: u32 = 4 * 1024 * 1024;
