//! Queries for the `shared_dirs` table.
//!
//! A `shared_dir` represents a directory that the daemon watches and indexes.
//! Entries with `protected = true` (currently only `download_dir`) cannot be
//! removed by the user.

use std::path::Path;

use anyhow::Result;
use rucio_core::api::shares::ExtFilterMode;
use sqlx::Row;

use super::Db;

/// A shared directory record as stored in the database.
#[derive(Debug, Clone)]
pub struct SharedDirRow {
    pub id: i64,
    /// Absolute path, no trailing slash.
    pub path: String,
    /// True if this directory cannot be removed by the user.
    pub protected: bool,
    /// Share subdirectories too (false = only files directly in this dir).
    pub recursive: bool,
    /// Extension filter mode (0 all, 1 only, 2 except). Decode with
    /// `ExtFilterMode::from_i64`.
    pub ext_mode: i64,
    /// `'|'`-separated extensions the filter applies to (lowercase, no dots).
    pub ext_list: Option<String>,
    pub added_at: i64,
}

/// Insert a shared directory.  If the path already exists the row is left
/// unchanged (INSERT OR IGNORE).  New rows get the default file filter (share
/// the whole tree) from the column defaults.  Returns the row id.
pub async fn insert(db: &Db, path: &str, protected: bool, added_at: u64) -> Result<i64> {
    // Normalise: strip trailing slash
    let path = path.trim_end_matches('/');

    sqlx::query(
        "INSERT OR IGNORE INTO shared_dirs (path, protected, added_at) VALUES (?1, ?2, ?3)",
    )
    .bind(path)
    .bind(protected as i64)
    .bind(added_at as i64)
    .execute(db)
    .await?;

    let id: i64 = sqlx::query_scalar("SELECT id FROM shared_dirs WHERE path = ?1")
        .bind(path)
        .fetch_one(db)
        .await?;
    Ok(id)
}

/// Insert a shared directory with an explicit file filter. If the path already
/// exists the filter is updated (re-adding a directory re-applies the given
/// filter), while its `protected` flag and `added_at` are preserved. Returns the
/// row id.
pub async fn insert_with_filter(
    db: &Db,
    path: &str,
    protected: bool,
    recursive: bool,
    ext_mode: i64,
    ext_list: Option<&str>,
    added_at: u64,
) -> Result<i64> {
    let path = path.trim_end_matches('/');

    sqlx::query(
        "INSERT INTO shared_dirs (path, protected, recursive, ext_mode, ext_list, added_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(path) DO UPDATE SET recursive = ?3, ext_mode = ?4, ext_list = ?5",
    )
    .bind(path)
    .bind(protected as i64)
    .bind(recursive as i64)
    .bind(ext_mode)
    .bind(ext_list)
    .bind(added_at as i64)
    .execute(db)
    .await?;

    let id: i64 = sqlx::query_scalar("SELECT id FROM shared_dirs WHERE path = ?1")
        .bind(path)
        .fetch_one(db)
        .await?;
    Ok(id)
}

/// Update just the file filter of an existing shared directory. Returns `true`
/// if a row was updated.
pub async fn update_filter(
    db: &Db,
    path: &str,
    recursive: bool,
    ext_mode: i64,
    ext_list: Option<&str>,
) -> Result<bool> {
    let path = path.trim_end_matches('/');
    let affected = sqlx::query(
        "UPDATE shared_dirs SET recursive = ?1, ext_mode = ?2, ext_list = ?3 WHERE path = ?4",
    )
    .bind(recursive as i64)
    .bind(ext_mode)
    .bind(ext_list)
    .bind(path)
    .execute(db)
    .await?
    .rows_affected();
    Ok(affected > 0)
}

/// Make exactly `paths` the protected shared directories — no more, no less.
///
/// Registers each path (creating it if needed) with `protected = 1`, and clears
/// the protected flag from every directory not in the set. Idempotent and
/// order-independent; duplicate paths are harmless.
///
/// Called with the set of destination directories that must not be removable:
/// today just the configured `download_dir`, but designed for several (the
/// global download dir plus a directory per download category). Passing the
/// current set on every change reconciles the flag in one shot — a directory
/// that stops being a destination (a former `download_dir`, or a category whose
/// directory changed or was deleted) is demoted to an ordinary, removable share
/// instead of a protected orphan the user can no longer delete, while a path
/// that was already shared unprotected is promoted.
pub async fn set_protected_dirs(db: &Db, paths: &[&str], added_at: u64) -> Result<()> {
    // Normalise (strip trailing slash) and de-duplicate, preserving nothing but
    // membership — the set is what matters.
    let normalised: Vec<&str> = {
        let mut v: Vec<&str> = paths.iter().map(|p| p.trim_end_matches('/')).collect();
        v.sort_unstable();
        v.dedup();
        v
    };

    let mut tx = db.begin().await?;

    // Demote everything first, then re-protect exactly the current set. Done in
    // one transaction so no intermediate "nothing protected" state is ever
    // observable. This keeps all SQL static (no dynamic `NOT IN (…)` list) and
    // handles the empty set naturally: demote all, protect none.
    sqlx::query("UPDATE shared_dirs SET protected = 0 WHERE protected = 1")
        .execute(&mut *tx)
        .await?;

    for path in &normalised {
        sqlx::query(
            "INSERT OR IGNORE INTO shared_dirs (path, protected, added_at) VALUES (?1, 1, ?2)",
        )
        .bind(path)
        .bind(added_at as i64)
        .execute(&mut *tx)
        .await?;
        // Force the flag on (the dir may have pre-existed unprotected).
        sqlx::query("UPDATE shared_dirs SET protected = 1 WHERE path = ?1")
            .bind(path)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// List all shared directories ordered by `added_at`.
pub async fn list(db: &Db) -> Result<Vec<SharedDirRow>> {
    let rows = sqlx::query(
        "SELECT id, path, protected, recursive, ext_mode, ext_list, added_at \
         FROM shared_dirs ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .iter()
        .map(|r| SharedDirRow {
            id: r.get("id"),
            path: r.get("path"),
            protected: r.get::<i64, _>("protected") != 0,
            recursive: r.get::<i64, _>("recursive") != 0,
            ext_mode: r.get("ext_mode"),
            ext_list: r.get("ext_list"),
            added_at: r.get("added_at"),
        })
        .collect())
}

/// True if this directory's filter would share `path` (assuming `path` already
/// lives under `dir.path`): applies the recursive flag and the extension filter.
pub fn shares_file(dir: &SharedDirRow, path: &Path) -> bool {
    // Non-recursive: only files sitting directly in the directory, not nested.
    if !dir.recursive && path.parent() != Some(Path::new(&dir.path)) {
        return false;
    }
    match ExtFilterMode::from_i64(dir.ext_mode) {
        ExtFilterMode::All => true,
        ExtFilterMode::Only => ext_in_list(path, dir.ext_list.as_deref()),
        ExtFilterMode::Except => !ext_in_list(path, dir.ext_list.as_deref()),
    }
}

/// True if `path`'s extension (case-insensitive) is in the `'|'`-separated
/// `list`. A file with no extension never matches.
fn ext_in_list(path: &Path, list: Option<&str>) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    let ext = ext.to_ascii_lowercase();
    list.unwrap_or("")
        .split('|')
        .map(|e| e.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|e| !e.is_empty())
        .any(|e| e == ext)
}

/// Whether `path` should be shared, given a snapshot of the shared directories:
/// the most-specific directory that contains it (longest path prefix) decides,
/// applying its filter. `false` when `path` is under no shared directory.
pub fn dirs_share(dirs: &[SharedDirRow], path: &Path) -> bool {
    dirs.iter()
        .filter(|d| path.starts_with(Path::new(&d.path)))
        .max_by_key(|d| d.path.len())
        .is_some_and(|dir| shares_file(dir, path))
}

/// Async convenience over [`dirs_share`]: loads the current shared directories
/// and decides for `path`. Fail-open (`true`) on a DB error, matching the old
/// "still under a shared dir" gate the watcher relied on.
pub async fn should_share(db: &Db, path: &Path) -> bool {
    match list(db).await {
        Ok(dirs) => dirs_share(&dirs, path),
        Err(_) => true,
    }
}

/// Returns `true` if the directory at `path` is marked as protected.
/// Returns `false` if the path is not registered at all.
pub async fn is_protected(db: &Db, path: &str) -> Result<bool> {
    let path = path.trim_end_matches('/');
    let protected: Option<i64> =
        sqlx::query_scalar("SELECT protected FROM shared_dirs WHERE path = ?1")
            .bind(path)
            .fetch_optional(db)
            .await?;
    Ok(protected.unwrap_or(0) != 0)
}

/// Delete a shared directory by path.
/// Returns `Err` if the directory is protected.
/// Returns `Ok(false)` if the path was not registered.
pub async fn delete(db: &Db, path: &str) -> Result<bool> {
    let path = path.trim_end_matches('/');

    if is_protected(db, path).await? {
        anyhow::bail!("Cannot remove protected shared directory: {path}");
    }

    let affected = sqlx::query("DELETE FROM shared_dirs WHERE path = ?1")
        .bind(path)
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> (Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        super::super::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    #[test]
    fn share_filter_predicate() {
        use std::path::Path;
        fn dir(path: &str, recursive: bool, ext_mode: i64, ext: Option<&str>) -> SharedDirRow {
            SharedDirRow {
                id: 0,
                path: path.into(),
                protected: false,
                recursive,
                ext_mode,
                ext_list: ext.map(String::from),
                added_at: 0,
            }
        }

        // Non-recursive: only files directly inside the directory.
        let d = dir("/a", false, 0, None);
        assert!(shares_file(&d, Path::new("/a/x.mp3")));
        assert!(!shares_file(&d, Path::new("/a/sub/y.mp3")));

        // Only these extensions (case-insensitive; no-extension never matches).
        let d = dir("/a", true, 1, Some("mp3|mkv"));
        assert!(shares_file(&d, Path::new("/a/x.MP3")));
        assert!(!shares_file(&d, Path::new("/a/x.txt")));
        assert!(!shares_file(&d, Path::new("/a/noext")));

        // Except these extensions (no-extension is shared).
        let d = dir("/a", true, 2, Some("txt"));
        assert!(!shares_file(&d, Path::new("/a/x.txt")));
        assert!(shares_file(&d, Path::new("/a/x.mp3")));
        assert!(shares_file(&d, Path::new("/a/noext")));

        // Longest-prefix: /a shares all, /a/b only mp3.
        let dirs = vec![dir("/a", true, 0, None), dir("/a/b", true, 1, Some("mp3"))];
        assert!(dirs_share(&dirs, Path::new("/a/file.txt")));
        assert!(dirs_share(&dirs, Path::new("/a/b/song.mp3")));
        assert!(!dirs_share(&dirs, Path::new("/a/b/doc.txt")));
        assert!(!dirs_share(&dirs, Path::new("/other/x.mp3")));
    }

    #[tokio::test]
    async fn insert_and_list() {
        let (db, _dir) = test_db().await;
        insert(&db, "/home/user/Downloads/rucio", true, 1_000_000)
            .await
            .unwrap();
        insert(&db, "/home/user/movies", false, 1_000_001)
            .await
            .unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].protected);
        assert!(!rows[1].protected);
    }

    #[tokio::test]
    async fn insert_idempotent() {
        let (db, _dir) = test_db().await;
        insert(&db, "/home/user/music", false, 1_000_000)
            .await
            .unwrap();
        insert(&db, "/home/user/music", false, 1_000_001)
            .await
            .unwrap(); // should not error
        assert_eq!(list(&db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delete_unprotected() {
        let (db, _dir) = test_db().await;
        insert(&db, "/home/user/music", false, 1_000_000)
            .await
            .unwrap();
        let deleted = delete(&db, "/home/user/music").await.unwrap();
        assert!(deleted);
        assert!(list(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_protected_is_error() {
        let (db, _dir) = test_db().await;
        insert(&db, "/home/user/Downloads/rucio", true, 1_000_000)
            .await
            .unwrap();
        let result = delete(&db, "/home/user/Downloads/rucio").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn set_protected_dirs_moves_protection() {
        let (db, _dir) = test_db().await;
        // Old download_dir (protected) plus a user share that already exists
        // unprotected and happens to be the new download_dir.
        insert(&db, "/old/downloads", true, 1_000_000)
            .await
            .unwrap();
        insert(&db, "/new/downloads", false, 1_000_001)
            .await
            .unwrap();

        set_protected_dirs(&db, &["/new/downloads"], 1_000_002)
            .await
            .unwrap();

        // New dir is now protected; old one is demoted and removable.
        assert!(is_protected(&db, "/new/downloads").await.unwrap());
        assert!(!is_protected(&db, "/old/downloads").await.unwrap());
        assert!(delete(&db, "/old/downloads").await.unwrap());
        assert!(delete(&db, "/new/downloads").await.is_err()); // still protected
    }

    #[tokio::test]
    async fn set_protected_dirs_creates_when_absent() {
        let (db, _dir) = test_db().await;
        set_protected_dirs(&db, &["/fresh/downloads"], 1_000_000)
            .await
            .unwrap();
        assert!(is_protected(&db, "/fresh/downloads").await.unwrap());
        assert_eq!(list(&db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn set_protected_dirs_protects_several_and_demotes_the_rest() {
        let (db, _dir) = test_db().await;
        // A previous global download_dir and a previous category dir, both
        // protected; plus an ordinary user share.
        insert(&db, "/global", true, 1_000_000).await.unwrap();
        insert(&db, "/cat/old", true, 1_000_001).await.unwrap();
        insert(&db, "/user/share", false, 1_000_002).await.unwrap();

        // New set: keep the global, swap the category dir for a fresh one.
        set_protected_dirs(&db, &["/global", "/cat/new"], 1_000_003)
            .await
            .unwrap();

        assert!(is_protected(&db, "/global").await.unwrap());
        assert!(is_protected(&db, "/cat/new").await.unwrap());
        // The old category dir is demoted (removable) and the user share is left
        // alone (still unprotected, still present).
        assert!(!is_protected(&db, "/cat/old").await.unwrap());
        assert!(delete(&db, "/cat/old").await.unwrap());
        assert!(!is_protected(&db, "/user/share").await.unwrap());
    }

    #[tokio::test]
    async fn set_protected_dirs_is_idempotent_with_duplicates() {
        let (db, _dir) = test_db().await;
        set_protected_dirs(&db, &["/d/", "/d", "/d"], 1_000_000)
            .await
            .unwrap();
        // Trailing slash normalised and dedup'd → a single row.
        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].path, "/d");
        assert!(rows[0].protected);
    }

    #[tokio::test]
    async fn set_protected_dirs_empty_demotes_everything() {
        let (db, _dir) = test_db().await;
        insert(&db, "/was/protected", true, 1_000_000)
            .await
            .unwrap();
        set_protected_dirs(&db, &[], 1_000_001).await.unwrap();
        // Empty set means "nothing is protected" — the dir becomes removable.
        assert!(!is_protected(&db, "/was/protected").await.unwrap());
        assert!(delete(&db, "/was/protected").await.unwrap());
    }

    #[tokio::test]
    async fn trailing_slash_normalised() {
        let (db, _dir) = test_db().await;
        insert(&db, "/home/user/music/", false, 1_000_000)
            .await
            .unwrap();
        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].path, "/home/user/music");
    }
}
