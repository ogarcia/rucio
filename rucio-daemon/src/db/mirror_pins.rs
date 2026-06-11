//! Queries for the `mirror_pins` table — content mirrored on behalf of a
//! [`super::pin_subscriptions`] entry.
//!
//! The reconcile loop computes the wanted set for a peer (within its quota) and
//! replaces that peer's rows wholesale via [`set_for_peer`]. Retention asks
//! [`is_wanted`]: a hash stays on disk while it is a manual pin OR wanted by at
//! least one subscription.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// `state` of a mirror entry.
pub const STATE_WANTED: &str = "wanted";
pub const STATE_SKIPPED: &str = "skipped"; // over quota — intentionally not mirrored

#[derive(Debug, Clone)]
pub struct MirrorPinRow {
    pub root_hash: Vec<u8>,
    pub peer_id: String,
    pub name: Option<String>,
    pub size: i64,
    pub state: String,
    /// Publisher's collection for this pin, NULL = uncollected.
    pub collection: Option<String>,
}

/// One entry the reconcile wants to record for a peer.
#[derive(Debug, Clone)]
pub struct MirrorEntry {
    pub root_hash: [u8; 32],
    pub name: Option<String>,
    pub size: i64,
    pub state: String,
    pub collection: Option<String>,
}

fn row_to_mirror(r: &sqlx::sqlite::SqliteRow) -> MirrorPinRow {
    MirrorPinRow {
        root_hash: r.get("root_hash"),
        peer_id: r.get("peer_id"),
        name: r.get("name"),
        size: r.get("size"),
        state: r.get("state"),
        collection: r.get("collection"),
    }
}

/// Replace all of a peer's mirror rows with `entries`, in one transaction. The
/// reconcile loop calls this with the freshly-computed wanted/skipped set.
pub async fn set_for_peer(
    db: &Db,
    peer_id: &str,
    entries: &[MirrorEntry],
    added_at: u64,
) -> Result<()> {
    let mut tx = db.begin().await?;
    sqlx::query("DELETE FROM mirror_pins WHERE peer_id = ?1")
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
    for e in entries {
        sqlx::query(
            "INSERT INTO mirror_pins (root_hash, peer_id, name, size, state, collection, added_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(e.root_hash.as_slice())
        .bind(peer_id)
        .bind(e.name.as_deref())
        .bind(e.size)
        .bind(e.state.as_str())
        .bind(e.collection.as_deref())
        .bind(added_at as i64)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// All mirror rows for a peer.
pub async fn list_for_peer(db: &Db, peer_id: &str) -> Result<Vec<MirrorPinRow>> {
    let rows = sqlx::query(
        "SELECT root_hash, peer_id, name, size, state, collection
         FROM mirror_pins WHERE peer_id = ?1",
    )
    .bind(peer_id)
    .fetch_all(db)
    .await?;
    Ok(rows.iter().map(row_to_mirror).collect())
}

/// Distinct collection labels seen in a peer's synced pin-set, alphabetically.
/// NULL/uncollected pins are reported as the empty string "". Feeds the
/// subscriber UI's "which collections does this peer publish" selector.
pub async fn collections_for_peer(db: &Db, peer_id: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT DISTINCT COALESCE(collection, '') AS c
         FROM mirror_pins WHERE peer_id = ?1 ORDER BY c ASC",
    )
    .bind(peer_id)
    .fetch_all(db)
    .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("c")).collect())
}

/// Whether any subscription wants this hash (state = 'wanted'). Used by the
/// retention check together with the manual `pins` table.
pub async fn is_wanted(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM mirror_pins WHERE root_hash = ?1 AND state = ?2")
        .bind(root_hash.as_slice())
        .bind(STATE_WANTED)
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

/// Whether a subscription *other than* `peer_id` wants this hash. Used when a
/// peer drops a hash (unsubscribe/narrow) to decide if it still has a keeper.
pub async fn wanted_by_other(db: &Db, root_hash: &[u8; 32], peer_id: &str) -> Result<bool> {
    let row = sqlx::query(
        "SELECT 1 FROM mirror_pins WHERE root_hash = ?1 AND state = ?2 AND peer_id <> ?3",
    )
    .bind(root_hash.as_slice())
    .bind(STATE_WANTED)
    .bind(peer_id)
    .fetch_optional(db)
    .await?;
    Ok(row.is_some())
}

/// Total bytes a peer's mirror currently wants (for the storage meter / quota
/// accounting). Skipped (over-quota) entries are not counted.
pub async fn wanted_bytes_for_peer(db: &Db, peer_id: &str) -> Result<i64> {
    let total: Option<i64> =
        sqlx::query_scalar("SELECT SUM(size) FROM mirror_pins WHERE peer_id = ?1 AND state = ?2")
            .bind(peer_id)
            .bind(STATE_WANTED)
            .fetch_one(db)
            .await?;
    Ok(total.unwrap_or(0))
}

/// Count and byte-total of a peer's *wanted* entries we actually hold (present
/// as a share) — i.e. genuinely mirrored, as opposed to still being fetched.
/// Returns `(count, bytes)`.
pub async fn present_for_peer(db: &Db, peer_id: &str) -> Result<(i64, i64)> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n, COALESCE(SUM(m.size), 0) AS b
         FROM mirror_pins m
         WHERE m.peer_id = ?1 AND m.state = ?2
           AND EXISTS (SELECT 1 FROM shared_files s WHERE s.root_hash = m.root_hash)",
    )
    .bind(peer_id)
    .bind(STATE_WANTED)
    .fetch_one(db)
    .await?;
    Ok((row.get("n"), row.get("b")))
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
        // mirror_pins has an FK to pin_subscriptions; create the parent first.
        super::super::pin_subscriptions::upsert(&pool, "peerA", 1000, 1)
            .await
            .unwrap();
        (pool, dir)
    }

    #[tokio::test]
    async fn set_replace_wanted_and_bytes() {
        let (db, _dir) = test_db().await;
        let entries = vec![
            MirrorEntry {
                root_hash: [1u8; 32],
                name: Some("a".into()),
                size: 100,
                state: STATE_WANTED.into(),
                collection: None,
            },
            MirrorEntry {
                root_hash: [2u8; 32],
                name: Some("b".into()),
                size: 5000,
                state: STATE_SKIPPED.into(),
                collection: Some("big".into()),
            },
        ];
        set_for_peer(&db, "peerA", &entries, 10).await.unwrap();

        assert!(is_wanted(&db, &[1u8; 32]).await.unwrap());
        assert!(!is_wanted(&db, &[2u8; 32]).await.unwrap()); // skipped, not wanted
        assert_eq!(wanted_bytes_for_peer(&db, "peerA").await.unwrap(), 100);
        assert_eq!(list_for_peer(&db, "peerA").await.unwrap().len(), 2);

        // Replace wholesale with a different set.
        set_for_peer(&db, "peerA", &[], 11).await.unwrap();
        assert!(!is_wanted(&db, &[1u8; 32]).await.unwrap());
        assert_eq!(list_for_peer(&db, "peerA").await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn removing_subscription_cascades() {
        let (db, _dir) = test_db().await;
        set_for_peer(
            &db,
            "peerA",
            &[MirrorEntry {
                root_hash: [9u8; 32],
                name: None,
                size: 1,
                state: STATE_WANTED.into(),
                collection: None,
            }],
            1,
        )
        .await
        .unwrap();
        super::super::pin_subscriptions::remove(&db, "peerA")
            .await
            .unwrap();
        // CASCADE removed the mirror row.
        assert!(!is_wanted(&db, &[9u8; 32]).await.unwrap());
    }
}
