use crate::protocol::file::FileDescriptor;

/// POST /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct AddShareRequest {
    /// Absolute path to a file or directory.
    /// If a directory is given the daemon will recurse into it.
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
}

/// GET /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharesResponse {
    pub shares: Vec<ShareResponse>,
}

impl From<&FileDescriptor> for ShareResponse {
    fn from(fd: &FileDescriptor) -> Self {
        Self {
            root_hash: fd.root_hash.to_hex(),
            name: fd.name.clone(),
            size: fd.size,
            chunk_count: fd.chunk_count(),
            mime_type: fd.mime_type.clone(),
            path: String::new(), // not available from FileDescriptor alone
        }
    }
}
