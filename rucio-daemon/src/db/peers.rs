//! Queries for the `known_peers` table.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// Drop learned peers not seen for this long. `known_peers` is only a warm-start
/// hint cache — a peer unseen for months has almost certainly changed address,
/// so dialling it wastes time. Kept generous on purpose: while the network is
/// sparse, a short horizon would empty the cache faster than it can refill, and
/// the real bootstrap anchors live in `BUILTIN_BOOTSTRAP_PEERS` (not the DB), so
/// even an empty table still boots.
const MAX_PEER_AGE_SECS: i64 = 60 * 24 * 60 * 60;
/// Hard cap on rows kept, newest-`last_seen` first. Warm-start only reads the
/// top ~50, so this is comfortably above what boot needs while bounding growth.
const MAX_PEERS: i64 = 1000;

#[derive(Debug, Clone)]
pub struct PeerRow {
    pub id: i64,
    pub peer_id: String,
    pub addrs: String, // JSON array
    pub first_seen: i64,
    pub last_seen: i64,
    pub high_id: bool,
    /// Identify agent string; `None` until the peer's Identify exchange has run
    /// (e.g. a peer freshly discovered via mDNS but not yet connected).
    pub agent_version: Option<String>,
}

/// Upsert a peer: insert or update `addrs`, `last_seen`, `high_id`, and
/// `agent_version`. A `None` agent (e.g. an mDNS rediscovery before Identify)
/// must not wipe an agent we already learned, so it is COALESCEd with the
/// stored value.
pub async fn upsert(
    db: &Db,
    peer_id: &str,
    addrs_json: &str,
    now: u64,
    high_id: bool,
    agent_version: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO known_peers (peer_id, addrs, first_seen, last_seen, high_id, agent_version)
         VALUES (?1, ?2, ?3, ?3, ?4, ?5)
         ON CONFLICT(peer_id) DO UPDATE SET
             addrs         = excluded.addrs,
             last_seen     = excluded.last_seen,
             high_id       = excluded.high_id,
             agent_version = COALESCE(excluded.agent_version, known_peers.agent_version)",
    )
    .bind(peer_id)
    .bind(addrs_json)
    .bind(now as i64)
    .bind(high_id as i64)
    .bind(agent_version)
    .execute(db)
    .await?;

    // Bound the hint cache on the write path (no background reaper): first drop
    // anything too stale to be a useful dial target, then hard-cap the rest.
    prune(db, now).await?;
    Ok(())
}

/// Evict stale and excess rows from the hint cache. Runs after every `upsert`,
/// so retention is bounded by the insert path, not by the schema or a poller.
async fn prune(db: &Db, now: u64) -> Result<()> {
    let cutoff = (now as i64).saturating_sub(MAX_PEER_AGE_SECS);
    sqlx::query("DELETE FROM known_peers WHERE last_seen < ?1")
        .bind(cutoff)
        .execute(db)
        .await?;
    sqlx::query(
        "DELETE FROM known_peers
         WHERE id NOT IN (
             SELECT id FROM known_peers ORDER BY last_seen DESC LIMIT ?1
         )",
    )
    .bind(MAX_PEERS)
    .execute(db)
    .await?;
    Ok(())
}

/// Return the first known network address for a peer, if any. Used to label
/// per-peer download detail with a human-meaningful endpoint.
pub async fn first_addr(db: &Db, peer_id: &str) -> Result<Option<String>> {
    let row = sqlx::query("SELECT addrs FROM known_peers WHERE peer_id = ?1")
        .bind(peer_id)
        .fetch_optional(db)
        .await?;
    Ok(row.and_then(|r| {
        let addrs: Vec<String> =
            serde_json::from_str(r.get::<String, _>("addrs").as_str()).unwrap_or_default();
        addrs.into_iter().next()
    }))
}

/// Return up to `limit` peers most recently seen.
pub async fn list_recent(db: &Db, limit: u32) -> Result<Vec<PeerRow>> {
    let rows = sqlx::query(
        "SELECT id, peer_id, addrs, first_seen, last_seen, high_id, agent_version
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
            agent_version: r.get("agent_version"),
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

    async fn agent_of(db: &Db, peer_id: &str) -> Option<String> {
        list_recent(db, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.peer_id == peer_id)
            .and_then(|r| r.agent_version)
    }

    /// An Identify upsert records the agent; a later mDNS rediscovery (None
    /// agent) must not wipe it — the COALESCE keeps the known value.
    #[tokio::test]
    async fn agent_version_is_recorded_and_not_clobbered_by_none() {
        let (db, _dir) = test_db().await;
        let pid = "12D3KooExamplePeer";

        // First sight via mDNS: addresses only, no agent yet.
        upsert(&db, pid, "[]", 100, true, None).await.unwrap();
        assert_eq!(agent_of(&db, pid).await, None);

        // Identify completes: agent learned.
        let agent = "Rucio/0.28.0 (Linux x86_64) libp2p/0.56.0";
        upsert(&db, pid, "[]", 200, true, Some(agent))
            .await
            .unwrap();
        assert_eq!(agent_of(&db, pid).await.as_deref(), Some(agent));

        // mDNS rediscovery (no agent) refreshes last_seen but keeps the agent.
        upsert(&db, pid, "[]", 300, true, None).await.unwrap();
        assert_eq!(agent_of(&db, pid).await.as_deref(), Some(agent));
    }

    /// A peer whose `last_seen` is older than the retention horizon is dropped
    /// on the next upsert; a fresh one survives.
    #[tokio::test]
    async fn stale_peers_are_pruned_by_age() {
        let (db, _dir) = test_db().await;
        let now = 10 * MAX_PEER_AGE_SECS as u64; // far enough that cutoff is positive

        // An old peer, then a fresh peer that triggers the prune.
        upsert(
            &db,
            "old",
            "[]",
            now - MAX_PEER_AGE_SECS as u64 - 1,
            true,
            None,
        )
        .await
        .unwrap();
        upsert(&db, "fresh", "[]", now, true, None).await.unwrap();

        let ids: Vec<String> = list_recent(&db, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.peer_id)
            .collect();
        assert_eq!(ids, vec!["fresh".to_string()]);
    }

    /// Within the age window the table is still hard-capped to `MAX_PEERS`,
    /// keeping the most recently seen rows.
    #[tokio::test]
    async fn peers_are_hard_capped_by_count() {
        let (db, _dir) = test_db().await;
        let now = 10 * MAX_PEER_AGE_SECS as u64;
        // Insert MAX_PEERS + 5 peers, each with a distinct, increasing last_seen.
        let total = MAX_PEERS as u64 + 5;
        for i in 0..total {
            upsert(&db, &format!("peer-{i:04}"), "[]", now + i, true, None)
                .await
                .unwrap();
        }
        let rows = list_recent(&db, MAX_PEERS as u32 + 100).await.unwrap();
        assert_eq!(rows.len(), MAX_PEERS as usize);
        // The oldest 5 must have been evicted; the newest is still present.
        let kept: std::collections::HashSet<String> = rows.into_iter().map(|r| r.peer_id).collect();
        assert!(kept.contains(&format!("peer-{:04}", total - 1)));
        assert!(!kept.contains("peer-0000"));
    }
}
