//! Queries for the `mirror_owned` table — hashes whose local copy exists only
//! because the reconcile fetched it to mirror a subscription.
//!
//! This is the discriminator eviction relies on: only content we own as a
//! mirror may be deleted, never the user's own downloads or shares. A hash is
//! marked when the reconcile decides to fetch a missing wanted entry, and
//! unmarked when that content is evicted.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// Record that `root_hash` is mirror-owned (we fetched it solely to mirror).
/// Idempotent: re-marking keeps the original `added_at`.
pub async fn mark(db: &Db, root_hash: &[u8; 32], added_at: u64) -> Result<()> {
    sqlx::query(
        "INSERT INTO mirror_owned (root_hash, added_at) VALUES (?1, ?2)
         ON CONFLICT(root_hash) DO NOTHING",
    )
    .bind(root_hash.as_slice())
    .bind(added_at as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Drop the mirror-owned mark. Returns whether a row existed.
pub async fn unmark(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let res = sqlx::query("DELETE FROM mirror_owned WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Whether this hash is mirror-owned.
pub async fn is_owned(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM mirror_owned WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

/// All mirror-owned hashes (the eviction sweep's candidate set).
pub async fn list(db: &Db) -> Result<Vec<[u8; 32]>> {
    let rows = sqlx::query("SELECT root_hash FROM mirror_owned")
        .fetch_all(db)
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let bytes: Vec<u8> = r.get("root_hash");
        if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
            out.push(arr);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> (Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        super::super::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    #[tokio::test]
    async fn mark_is_owned_unmark_list() {
        let (db, _dir) = test_db().await;
        let a = [1u8; 32];
        let b = [2u8; 32];

        assert!(!is_owned(&db, &a).await.unwrap());
        mark(&db, &a, 10).await.unwrap();
        mark(&db, &b, 11).await.unwrap();
        // Re-marking is a no-op, keeps added_at.
        mark(&db, &a, 999).await.unwrap();
        assert!(is_owned(&db, &a).await.unwrap());

        let mut listed = list(&db).await.unwrap();
        listed.sort();
        assert_eq!(listed, vec![a, b]);

        assert!(unmark(&db, &a).await.unwrap());
        assert!(!unmark(&db, &a).await.unwrap()); // already gone
        assert!(!is_owned(&db, &a).await.unwrap());
        assert_eq!(list(&db).await.unwrap(), vec![b]);
    }
}
