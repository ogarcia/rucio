use crate::protocol::file::FileDescriptor;

/// POST /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct AddShareRequest {
    /// Absolute path to a directory to watch and share.
    /// Individual files are not accepted; wrap them in a directory first.
    pub path: String,
}

/// Response to POST /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct AddShareResponse {
    /// Number of files queued for indexing.
    pub queued: usize,
    /// Paths that could not be read (permission errors, broken symlinks…).
    pub errors: Vec<String>,
}

/// Response for a single shared file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ShareResponse {
    pub root_hash: String,
    pub name: String,
    pub size: u64,
    pub chunk_count: usize,
    pub mime_type: Option<String>,
    /// Absolute path on the host filesystem.
    pub path: String,
    /// Magnet link for this file — can be shared directly with other peers.
    /// Format: `rucio:<hash>?name=<name>&size=<bytes>`
    pub magnet: String,
}

/// GET /api/v1/shares/files
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharesResponse {
    pub shares: Vec<ShareResponse>,
}

/// A shared directory (watched folder) with aggregate counts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharedDirResponse {
    /// Absolute path of the watched directory.
    pub path: String,
    /// True for the download directory, which is always shared and cannot be
    /// removed.
    pub protected: bool,
    /// Number of indexed files under this directory.
    pub file_count: u64,
    /// Total size of indexed files under this directory, in bytes.
    pub total_size: u64,
}

/// GET /api/v1/shares — the watched directories (the unit of add/remove).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharedDirsResponse {
    pub dirs: Vec<SharedDirResponse>,
}

impl From<&FileDescriptor> for ShareResponse {
    fn from(fd: &FileDescriptor) -> Self {
        let magnet = crate::protocol::magnet::MagnetLink {
            root_hash: fd.root_hash.clone(),
            name: Some(fd.name.clone()),
            size: Some(fd.size),
            providers: vec![],
        }
        .to_string();
        Self {
            root_hash: fd.root_hash.to_hex(),
            name: fd.name.clone(),
            size: fd.size,
            chunk_count: fd.chunk_count(),
            mime_type: fd.mime_type.clone(),
            path: String::new(), // not available from FileDescriptor alone
            magnet,
        }
    }
}
