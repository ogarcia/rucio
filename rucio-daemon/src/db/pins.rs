//! Queries for the `pins` table — manually pinned content (a root hash the
//! user wants kept available on this node).
//!
//! A pin records intent only; the content itself lives as a normal share once
//! fetched. Pins are the source of truth for "kept on purpose" and are sacred
//! against the future cooperative-mirror eviction.

use anyhow::Result;
use sqlx::Row;

use super::Db;

#[derive(Debug, Clone)]
pub struct PinRow {
    pub root_hash: Vec<u8>,
    pub added_at: i64,
}

/// Record a pin. Idempotent: re-pinning a hash leaves the original `added_at`.
pub async fn add(db: &Db, root_hash: &[u8; 32], added_at: u64) -> Result<()> {
    sqlx::query("INSERT OR IGNORE INTO pins (root_hash, added_at) VALUES (?1, ?2)")
        .bind(root_hash.as_slice())
        .bind(added_at as i64)
        .execute(db)
        .await?;
    Ok(())
}

/// Remove a pin. Returns whether a row was actually deleted (so callers can
/// report 404 for an unknown hash). Does not touch the content on disk.
pub async fn remove(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let res = sqlx::query("DELETE FROM pins WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Whether `root_hash` is pinned.
pub async fn exists(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM pins WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

/// List all pins, newest first.
pub async fn list(db: &Db) -> Result<Vec<PinRow>> {
    let rows = sqlx::query("SELECT root_hash, added_at FROM pins ORDER BY added_at DESC")
        .fetch_all(db)
        .await?;
    Ok(rows
        .iter()
        .map(|r| PinRow {
            root_hash: r.get("root_hash"),
            added_at: r.get("added_at"),
        })
        .collect())
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
    async fn add_exists_list_remove() {
        let (db, _dir) = test_db().await;
        let h = [7u8; 32];

        assert!(!exists(&db, &h).await.unwrap());
        add(&db, &h, 1000).await.unwrap();
        assert!(exists(&db, &h).await.unwrap());

        // Idempotent: re-add keeps the original added_at.
        add(&db, &h, 2000).await.unwrap();
        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].added_at, 1000);
        assert_eq!(rows[0].root_hash, h);

        assert!(remove(&db, &h).await.unwrap());
        assert!(!exists(&db, &h).await.unwrap());
        // Removing a hash that isn't pinned reports false.
        assert!(!remove(&db, &h).await.unwrap());
    }
}
