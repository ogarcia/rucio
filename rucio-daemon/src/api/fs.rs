//! GET /api/v1/fs/list — browse the daemon host's filesystem.
//!
//! Backs the panel's folder picker for adding a share. It lists a single
//! directory level (sub-directories only — a share is a folder, not a file) and
//! reports the filesystem roots so the UI can switch drives on Windows.
//!
//! There is deliberately **no sandbox**: a share can live anywhere the daemon
//! can read, exactly like `POST /api/v1/shares`, which already accepts any path
//! and indexes it. This endpoint therefore grants no capability the API did not
//! already have — it only makes picking a path convenient. Access control is the
//! reverse proxy's job, as documented for the whole API (the daemon has no
//! built-in auth).

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

use crate::api::AppState;
use rucio_core::api::fs::{FsEntry, FsListResponse};

#[derive(Debug, Deserialize)]
pub struct FsListParams {
    /// Absolute directory to list. When omitted or empty, the daemon's home
    /// directory is used as a sensible starting point.
    #[serde(default)]
    pub path: Option<String>,
}

/// The filesystem roots the browser can jump between.
///
/// On Windows there is no single root, so we probe the 26 possible drive
/// letters and keep the ones that resolve to a directory — done with `std` only,
/// no `winapi`/`windows` crate. On Unix there is exactly one root, `/`.
#[cfg(windows)]
fn filesystem_roots() -> Vec<String> {
    ('A'..='Z')
        .map(|c| format!("{c}:\\"))
        .filter(|p| Path::new(p).is_dir())
        .collect()
}

#[cfg(not(windows))]
fn filesystem_roots() -> Vec<String> {
    vec!["/".to_string()]
}

/// Where the browser opens when no path is given: the user's home directory if
/// it resolves to a real directory, otherwise the first filesystem root.
fn default_start_dir() -> PathBuf {
    dirs::home_dir()
        .filter(|p| p.is_absolute() && p.is_dir())
        .or_else(|| filesystem_roots().into_iter().next().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// List the immediate sub-directories of `dir`, sorted case-insensitively.
/// Symlinks are followed so a symlinked media folder is still browsable;
/// unreadable individual entries are skipped rather than failing the whole call.
fn read_subdirs(dir: &Path) -> std::io::Result<Vec<FsEntry>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let Ok(entry) = entry else { continue };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let is_dir = if file_type.is_symlink() {
            // Resolve the link target; a dangling or non-dir link is dropped.
            std::fs::metadata(entry.path())
                .map(|m| m.is_dir())
                .unwrap_or(false)
        } else {
            file_type.is_dir()
        };
        if !is_dir {
            continue;
        }
        entries.push(FsEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            path: entry.path().to_string_lossy().into_owned(),
        });
    }
    entries.sort_by_key(|e| e.name.to_lowercase());
    Ok(entries)
}

/// GET /api/v1/fs/list
///
/// Browse the daemon host's filesystem one directory at a time so the panel can
/// offer a folder picker when adding a share. Returns the listed directory, its
/// parent (for "up" navigation), the filesystem roots (drive switcher on
/// Windows), and the immediate sub-directories.
#[utoipa::path(
    get,
    path = "/api/v1/fs/list",
    params(
        ("path" = Option<String>, Query,
            description = "Absolute directory to list; the home directory when omitted")
    ),
    responses(
        (status = 200, description = "Directory listing", body = FsListResponse),
        (status = 400, description = "Path is missing or not a readable directory")
    )
)]
pub async fn list_dir(
    State(_state): State<AppState>,
    Query(params): Query<FsListParams>,
) -> Result<Json<FsListResponse>, (StatusCode, Json<Value>)> {
    let requested = params.path.unwrap_or_default();
    let dir = if requested.trim().is_empty() {
        default_start_dir()
    } else {
        PathBuf::from(requested)
    };

    let entries = read_subdirs(&dir).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("cannot read directory: {e}") })),
        )
    })?;

    let parent = dir.parent().map(|p| p.to_string_lossy().into_owned());

    Ok(Json(FsListResponse {
        path: dir.to_string_lossy().into_owned(),
        parent,
        roots: filesystem_roots(),
        entries,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn read_subdirs_lists_only_directories_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join("Zebra")).unwrap();
        fs::create_dir(root.join("alpha")).unwrap();
        fs::write(root.join("a_file.txt"), b"x").unwrap();

        let entries = read_subdirs(root).unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        // Files are excluded; directories are sorted case-insensitively.
        assert_eq!(names, vec!["alpha", "Zebra"]);
        // Each entry's path ends with its name.
        assert!(entries.iter().all(|e| e.path.ends_with(&e.name)));
    }

    #[test]
    fn read_subdirs_errors_on_unreadable_path() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_subdirs(&tmp.path().join("does-not-exist")).is_err());
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_root_is_slash() {
        assert_eq!(filesystem_roots(), vec!["/".to_string()]);
    }

    #[test]
    fn default_start_dir_is_an_absolute_directory() {
        let d = default_start_dir();
        assert!(d.is_absolute());
        assert!(d.is_dir());
    }
}
