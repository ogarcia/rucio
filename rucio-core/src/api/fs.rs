//! DTOs for the server-side filesystem browser (`GET /api/v1/fs/list`).
//!
//! The panel uses these to offer a folder picker when adding a share, so the
//! user (especially on Windows, where typing an absolute path is awkward) can
//! navigate the **daemon host's** filesystem instead of guessing paths. Only
//! directories are listed — a share is always a folder.

/// One sub-directory in a server-side directory listing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct FsEntry {
    /// Display name (the final path component of `path`).
    pub name: String,
    /// Absolute path to the directory, ready to be listed again or shared.
    pub path: String,
}

/// `GET /api/v1/fs/list` — one level of the daemon host's filesystem.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct FsListResponse {
    /// Absolute path of the directory that was listed. This is also the path the
    /// "select this folder" action returns.
    pub path: String,
    /// Parent directory, or `null` when `path` is a filesystem root (`/` on
    /// Unix, a drive root like `C:\` on Windows). The UI uses `roots` to move
    /// between drives when there is no parent.
    pub parent: Option<String>,
    /// Filesystem roots: the drive letters that exist on Windows (e.g. `C:\`,
    /// `D:\`), or a single `/` on Unix. Lets the UI offer a drive switcher.
    pub roots: Vec<String>,
    /// Immediate sub-directories of `path`, sorted case-insensitively by name.
    pub entries: Vec<FsEntry>,
}
