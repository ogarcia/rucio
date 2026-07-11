use crate::protocol::file::FileDescriptor;

/// How a directory's `extensions` list is applied when deciding which files to
/// share. `All` (the default) ignores the list and shares every file.
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
pub enum ExtFilterMode {
    /// Share every file regardless of extension.
    #[default]
    All,
    /// Share only files whose extension is in the list.
    Only,
    /// Share every file except those whose extension is in the list.
    Except,
}

impl ExtFilterMode {
    /// Stable integer encoding for the DB `ext_mode` column.
    pub fn as_i64(self) -> i64 {
        match self {
            ExtFilterMode::All => 0,
            ExtFilterMode::Only => 1,
            ExtFilterMode::Except => 2,
        }
    }

    /// Decode from the DB integer; unknown values fall back to `All`.
    pub fn from_i64(v: i64) -> Self {
        match v {
            1 => ExtFilterMode::Only,
            2 => ExtFilterMode::Except,
            _ => ExtFilterMode::All,
        }
    }
}

/// Which files under a shared directory to actually index and share. The
/// default (`recursive = true`, `ext_mode = all`) shares the whole tree — the
/// original behaviour.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ShareFilter {
    /// Recurse into subdirectories. When false, only files directly in the
    /// directory are shared (its subdirectories are ignored).
    #[serde(default = "default_recursive")]
    pub recursive: bool,
    /// How `extensions` is applied.
    #[serde(default)]
    pub ext_mode: ExtFilterMode,
    /// `'|'`-separated file extensions to match, case-insensitive, without the
    /// dot — e.g. `"mp3|mkv|avi"` (same style as a category's keywords). Ignored
    /// when `ext_mode` is `all`; empty/null otherwise means "match nothing".
    #[serde(default)]
    pub extensions: Option<String>,
}

fn default_recursive() -> bool {
    true
}

impl Default for ShareFilter {
    fn default() -> Self {
        Self {
            recursive: true,
            ext_mode: ExtFilterMode::All,
            extensions: None,
        }
    }
}

/// POST /api/v1/shares
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct AddShareRequest {
    /// Absolute path to a directory to watch and share.
    /// Individual files are not accepted; wrap them in a directory first.
    pub path: String,
    /// Which files under the directory to share. Omit for the default (share the
    /// whole tree).
    #[serde(default)]
    pub filter: ShareFilter,
}

/// PUT /api/v1/shares — update an existing shared directory's file filter.
/// A reconcile then indexes newly-matching files and de-indexes newly-excluded
/// ones.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct UpdateSharedDirRequest {
    /// Absolute path of the shared directory to update.
    pub path: String,
    /// The new filter to apply.
    pub filter: ShareFilter,
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
    /// Which files under this directory are shared (recursive flag + extension
    /// filter). Defaults to "share the whole tree".
    #[serde(default)]
    pub filter: ShareFilter,
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
