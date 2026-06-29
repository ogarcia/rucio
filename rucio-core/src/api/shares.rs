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
    /// eMule `ed2k://` link for this file, when its MD4 hash is known (the eMule
    /// backfill has hashed it so we already seed it to Kad). `None` until then,
    /// or when eMule integration is off — the file is on the Rucio network
    /// regardless. Format: `ed2k://|file|<name>|<size>|<md4>|/`
    pub ed2k: Option<String>,
}

/// GET /api/v1/shares/files — one page of shared files plus the total count
/// matching the (optional) `q`/`dir` filter, so the client can show progress
/// ("N of TOTAL") and know whether more pages remain.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharesResponse {
    pub shares: Vec<ShareResponse>,
    /// Total files matching the filter (not just this page).
    pub total: u64,
}

/// Why a watched directory exists — drives how the UI labels it and whether it
/// can be removed through the API. Every kind except `User` is protected
/// (declared by configuration or managed by the daemon) and cannot be removed
/// via `DELETE /api/v1/shares`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SharedDirKind {
    /// A folder the user added through the app; removable.
    #[default]
    User,
    /// The global download directory.
    Downloads,
    /// The pin directory, where pinned content is kept.
    Pins,
    /// A category's destination directory.
    Category,
    /// Declared in `[storage].shared_dirs` in the config file.
    Config,
}

/// A shared directory (watched folder) with aggregate counts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SharedDirResponse {
    /// Absolute path of the watched directory.
    pub path: String,
    /// True for directories that cannot be removed through the API (every
    /// `kind` other than `user`).
    pub protected: bool,
    /// Why this directory is shared — lets the UI label it (Downloads, Pins,
    /// Category, Config) instead of treating every protected dir as the
    /// download dir. Defaults to `user` for backward compatibility.
    #[serde(default)]
    pub kind: SharedDirKind,
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
            ed2k: None, // the ed2k hash lives in a separate table, keyed by path
        }
    }
}
