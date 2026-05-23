use crate::protocol::file::FileDescriptor;

/// POST /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AddShareRequest {
    pub path: String,
}

/// Response for a single shared file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShareResponse {
    pub root_hash: String,
    pub name: String,
    pub size: u64,
    pub chunk_count: usize,
    pub mime_type: Option<String>,
}

/// GET /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        }
    }
}
