//! Queries for the `shared_dirs` table.
//!
//! A `shared_dir` represents a directory that the daemon watches and indexes.
//! Entries with `protected = true` (currently only `download_dir`) cannot be
//! removed by the user.

use anyhow::Result;
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
    pub added_at: i64,
}

/// Insert a shared directory.  If the path already exists the row is left
/// unchanged (INSERT OR IGNORE).  Returns the row id.
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
    let rows =
        sqlx::query("SELECT id, path, protected, added_at FROM shared_dirs ORDER BY added_at ASC")
            .fetch_all(db)
            .await?;

    Ok(rows
        .iter()
        .map(|r| SharedDirRow {
            id: r.get("id"),
            path: r.get("path"),
            protected: r.get::<i64, _>("protected") != 0,
            added_at: r.get("added_at"),
        })
        .collect())
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
