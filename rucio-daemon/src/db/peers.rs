//! Queries for the `known_peers` table.

use anyhow::Result;
use sqlx::Row;

use super::Db;

#[derive(Debug, Clone)]
pub struct PeerRow {
    pub id: i64,
    pub peer_id: String,
    pub addrs: String, // JSON array
    pub first_seen: i64,
    pub last_seen: i64,
    pub high_id: bool,
}

/// Upsert a peer: insert or update `addrs`, `last_seen`, and `high_id`.
pub async fn upsert(
    db: &Db,
    peer_id: &str,
    addrs_json: &str,
    now: u64,
    high_id: bool,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO known_peers (peer_id, addrs, first_seen, last_seen, high_id)
         VALUES (?1, ?2, ?3, ?3, ?4)
         ON CONFLICT(peer_id) DO UPDATE SET
             addrs     = excluded.addrs,
             last_seen = excluded.last_seen,
             high_id   = excluded.high_id",
    )
    .bind(peer_id)
    .bind(addrs_json)
    .bind(now as i64)
    .bind(high_id as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// Return up to `limit` peers most recently seen.
pub async fn list_recent(db: &Db, limit: u32) -> Result<Vec<PeerRow>> {
    let rows = sqlx::query(
        "SELECT id, peer_id, addrs, first_seen, last_seen, high_id
         FROM known_peers ORDER BY last_seen DESC LIMIT ?1",
    )
    .bind(limit as i64)
    .fetch_all(db)
    .await?;

    Ok(rows
        .iter()
        .map(|r| PeerRow {
            id: r.get("id"),
            peer_id: r.get("peer_id"),
            addrs: r.get("addrs"),
            first_seen: r.get("first_seen"),
            last_seen: r.get("last_seen"),
            high_id: r.get::<i64, _>("high_id") != 0,
        })
        .collect())
}
