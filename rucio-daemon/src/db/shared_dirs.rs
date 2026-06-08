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

/// Make `path` the one and only protected shared directory.
///
/// Registers `path` (creating it if needed) with `protected = 1`, and clears
/// the protected flag from every other directory. Called at startup with the
/// configured `download_dir`: this way, changing `download_dir` in the config
/// and restarting leaves the *previous* download dir as an ordinary, removable
/// share instead of a protected orphan the user can no longer delete — and the
/// current one is always protected even if it was already shared unprotected.
pub async fn set_sole_protected(db: &Db, path: &str, added_at: u64) -> Result<()> {
    let path = path.trim_end_matches('/');
    let mut tx = db.begin().await?;

    sqlx::query("INSERT OR IGNORE INTO shared_dirs (path, protected, added_at) VALUES (?1, 1, ?2)")
        .bind(path)
        .bind(added_at as i64)
        .execute(&mut *tx)
        .await?;
    // Force the flag on for the current dir (it may have pre-existed unprotected)
    // and off for any other dir that was protected (a previous download_dir).
    sqlx::query("UPDATE shared_dirs SET protected = 1 WHERE path = ?1")
        .bind(path)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE shared_dirs SET protected = 0 WHERE path <> ?1 AND protected = 1")
        .bind(path)
        .execute(&mut *tx)
        .await?;

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
    async fn set_sole_protected_moves_protection() {
        let (db, _dir) = test_db().await;
        // Old download_dir (protected) plus a user share that already exists
        // unprotected and happens to be the new download_dir.
        insert(&db, "/old/downloads", true, 1_000_000)
            .await
            .unwrap();
        insert(&db, "/new/downloads", false, 1_000_001)
            .await
            .unwrap();

        set_sole_protected(&db, "/new/downloads", 1_000_002)
            .await
            .unwrap();

        // New dir is now protected; old one is demoted and removable.
        assert!(is_protected(&db, "/new/downloads").await.unwrap());
        assert!(!is_protected(&db, "/old/downloads").await.unwrap());
        assert!(delete(&db, "/old/downloads").await.unwrap());
        assert!(delete(&db, "/new/downloads").await.is_err()); // still protected
    }

    #[tokio::test]
    async fn set_sole_protected_creates_when_absent() {
        let (db, _dir) = test_db().await;
        set_sole_protected(&db, "/fresh/downloads", 1_000_000)
            .await
            .unwrap();
        assert!(is_protected(&db, "/fresh/downloads").await.unwrap());
        assert_eq!(list(&db).await.unwrap().len(), 1);
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
