//! Queries for the `mirror_optouts` table — files the user cancelled from a
//! subscription's mirror.
//!
//! This is the durable "don't mirror this hash from this peer" record. The
//! reconcile consults it to skip fetching (and materialises a `cancelled`
//! mirror_pins row from it each sync, for display); it survives clearing the
//! download history, pin-set version changes, and the publisher re-pinning.
//! Cleared only by a re-request or by `ON DELETE CASCADE` on unsubscribe.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// Record that the user opted out of mirroring `root_hash` from `peer_id`.
/// Idempotent.
pub async fn add(db: &Db, peer_id: &str, root_hash: &[u8; 32], added_at: u64) -> Result<()> {
    sqlx::query(
        "INSERT INTO mirror_optouts (peer_id, root_hash, added_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(peer_id, root_hash) DO NOTHING",
    )
    .bind(peer_id)
    .bind(root_hash.as_slice())
    .bind(added_at as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Drop the opt-out (a re-request). Returns whether a row existed.
pub async fn remove(db: &Db, peer_id: &str, root_hash: &[u8; 32]) -> Result<bool> {
    let res = sqlx::query("DELETE FROM mirror_optouts WHERE peer_id = ?1 AND root_hash = ?2")
        .bind(peer_id)
        .bind(root_hash.as_slice())
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Whether the user opted out of mirroring this hash from this peer.
pub async fn is_optout(db: &Db, peer_id: &str, root_hash: &[u8; 32]) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM mirror_optouts WHERE peer_id = ?1 AND root_hash = ?2")
        .bind(peer_id)
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

/// The set of hashes the user opted out of for a peer — read once per sync so
/// the reconcile can materialise their `cancelled` state without a query per
/// entry.
pub async fn list_for_peer(db: &Db, peer_id: &str) -> Result<Vec<[u8; 32]>> {
    let rows = sqlx::query("SELECT root_hash FROM mirror_optouts WHERE peer_id = ?1")
        .bind(peer_id)
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
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        super::super::apply_schema(&pool).await.unwrap();
        // mirror_optouts has an FK to pin_subscriptions; create the parent first.
        super::super::pin_subscriptions::upsert(&pool, "peerA", 1000, 1)
            .await
            .unwrap();
        (pool, dir)
    }

    #[tokio::test]
    async fn add_is_optout_remove_list() {
        let (db, _dir) = test_db().await;
        let a = [1u8; 32];
        let b = [2u8; 32];

        assert!(!is_optout(&db, "peerA", &a).await.unwrap());
        add(&db, "peerA", &a, 10).await.unwrap();
        add(&db, "peerA", &b, 11).await.unwrap();
        add(&db, "peerA", &a, 99).await.unwrap(); // idempotent
        assert!(is_optout(&db, "peerA", &a).await.unwrap());

        let mut listed = list_for_peer(&db, "peerA").await.unwrap();
        listed.sort();
        assert_eq!(listed, vec![a, b]);

        assert!(remove(&db, "peerA", &a).await.unwrap());
        assert!(!remove(&db, "peerA", &a).await.unwrap()); // already gone
        assert!(!is_optout(&db, "peerA", &a).await.unwrap());

        // Removing the subscription cascades the opt-outs away.
        super::super::pin_subscriptions::remove(&db, "peerA")
            .await
            .unwrap();
        assert!(list_for_peer(&db, "peerA").await.unwrap().is_empty());
    }
}
