//! Queries for the `pin_subscriptions` table — peers whose published pin-set we
//! mirror, each with a disk quota and the last-synced pin-set version.

use anyhow::Result;
use sqlx::Row;

use super::Db;

#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    pub peer_id: String,
    pub quota_bytes: i64,
    pub last_version: i64,
    pub last_synced_at: i64,
    pub added_at: i64,
}

fn row_to_sub(r: &sqlx::sqlite::SqliteRow) -> SubscriptionRow {
    SubscriptionRow {
        peer_id: r.get("peer_id"),
        quota_bytes: r.get("quota_bytes"),
        last_version: r.get("last_version"),
        last_synced_at: r.get("last_synced_at"),
        added_at: r.get("added_at"),
    }
}

/// Subscribe to (or re-configure) a peer. Re-subscribing updates the quota and
/// keeps the original `added_at`, `last_version` and `last_synced_at`.
pub async fn upsert(db: &Db, peer_id: &str, quota_bytes: i64, added_at: u64) -> Result<()> {
    sqlx::query(
        "INSERT INTO pin_subscriptions (peer_id, quota_bytes, added_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(peer_id) DO UPDATE SET quota_bytes = excluded.quota_bytes",
    )
    .bind(peer_id)
    .bind(quota_bytes)
    .bind(added_at as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Record a completed sync: the pin-set version we applied and when.
pub async fn set_synced(db: &Db, peer_id: &str, version: i64, now: u64) -> Result<()> {
    sqlx::query(
        "UPDATE pin_subscriptions SET last_version = ?2, last_synced_at = ?3 WHERE peer_id = ?1",
    )
    .bind(peer_id)
    .bind(version)
    .bind(now as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Remove a subscription. Returns whether a row existed. The `ON DELETE CASCADE`
/// on `mirror_pins` drops its mirror rows; the caller is responsible for any
/// on-disk cleanup of content no longer wanted.
pub async fn remove(db: &Db, peer_id: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM pin_subscriptions WHERE peer_id = ?1")
        .bind(peer_id)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn get(db: &Db, peer_id: &str) -> Result<Option<SubscriptionRow>> {
    let row = sqlx::query(
        "SELECT peer_id, quota_bytes, last_version, last_synced_at, added_at
         FROM pin_subscriptions WHERE peer_id = ?1",
    )
    .bind(peer_id)
    .fetch_optional(db)
    .await?;
    Ok(row.as_ref().map(row_to_sub))
}

pub async fn list(db: &Db) -> Result<Vec<SubscriptionRow>> {
    let rows = sqlx::query(
        "SELECT peer_id, quota_bytes, last_version, last_synced_at, added_at
         FROM pin_subscriptions ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;
    Ok(rows.iter().map(row_to_sub).collect())
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
    async fn upsert_get_sync_remove() {
        let (db, _dir) = test_db().await;
        upsert(&db, "12D3KooPeer", 500, 100).await.unwrap();
        let s = get(&db, "12D3KooPeer").await.unwrap().unwrap();
        assert_eq!(s.quota_bytes, 500);
        assert_eq!(s.last_version, 0);

        // Re-subscribe updates quota, keeps added_at.
        upsert(&db, "12D3KooPeer", 999, 200).await.unwrap();
        let s = get(&db, "12D3KooPeer").await.unwrap().unwrap();
        assert_eq!(s.quota_bytes, 999);
        assert_eq!(s.added_at, 100);

        set_synced(&db, "12D3KooPeer", 7, 300).await.unwrap();
        let s = get(&db, "12D3KooPeer").await.unwrap().unwrap();
        assert_eq!(s.last_version, 7);
        assert_eq!(s.last_synced_at, 300);

        assert!(remove(&db, "12D3KooPeer").await.unwrap());
        assert!(get(&db, "12D3KooPeer").await.unwrap().is_none());
        assert!(!remove(&db, "12D3KooPeer").await.unwrap());
    }
}
