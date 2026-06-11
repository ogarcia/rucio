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
    /// Publishing collection, NULL = uncollected.
    pub collection: Option<String>,
    pub added_at: i64,
}

/// Record a pin in `collection` (None = uncollected). Re-pinning a hash keeps
/// the original `added_at` but updates the collection, so the user can move a
/// pin between collections by pinning it again.
pub async fn add(
    db: &Db,
    root_hash: &[u8; 32],
    collection: Option<&str>,
    added_at: u64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO pins (root_hash, collection, added_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(root_hash) DO UPDATE SET collection = excluded.collection",
    )
    .bind(root_hash.as_slice())
    .bind(collection)
    .bind(added_at as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Re-assign an existing pin's collection (None = uncollected). Returns whether
/// the pin existed.
pub async fn set_collection(
    db: &Db,
    root_hash: &[u8; 32],
    collection: Option<&str>,
) -> Result<bool> {
    let res = sqlx::query("UPDATE pins SET collection = ?2 WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .bind(collection)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Distinct non-empty collection labels currently in use, alphabetically. Feeds
/// the publisher UI's collection picker.
pub async fn collections(db: &Db) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT DISTINCT collection FROM pins
         WHERE collection IS NOT NULL AND collection <> '' ORDER BY collection ASC",
    )
    .fetch_all(db)
    .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String, _>("collection"))
        .collect())
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
    let rows =
        sqlx::query("SELECT root_hash, collection, added_at FROM pins ORDER BY added_at DESC")
            .fetch_all(db)
            .await?;
    Ok(rows
        .iter()
        .map(|r| PinRow {
            root_hash: r.get("root_hash"),
            collection: r.get("collection"),
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
        add(&db, &h, Some("Manuals"), 1000).await.unwrap();
        assert!(exists(&db, &h).await.unwrap());

        // Re-add keeps the original added_at but updates the collection.
        add(&db, &h, Some("Series"), 2000).await.unwrap();
        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].added_at, 1000);
        assert_eq!(rows[0].root_hash, h);
        assert_eq!(rows[0].collection.as_deref(), Some("Series"));

        // Collections lists distinct non-empty labels.
        assert_eq!(collections(&db).await.unwrap(), vec!["Series".to_string()]);
        // Re-assign to uncollected.
        assert!(set_collection(&db, &h, None).await.unwrap());
        assert!(collections(&db).await.unwrap().is_empty());

        assert!(remove(&db, &h).await.unwrap());
        assert!(!exists(&db, &h).await.unwrap());
        // Removing a hash that isn't pinned reports false.
        assert!(!remove(&db, &h).await.unwrap());
    }
}
