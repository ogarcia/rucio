//! SQLite store for the passive DHT indexer.
//!
//! Records every `(hash, provider)` announcement seen on the DHT with
//! first/last-seen timestamps. Pre-stable policy mirrors the daemon: the schema
//! is applied with `CREATE TABLE IF NOT EXISTS` on startup, no migrations.

use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{FromRow, SqlitePool};
use utoipa::ToSchema;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS provider_records (
    hash       TEXT NOT NULL,
    provider   TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (hash, provider)
);
CREATE INDEX IF NOT EXISTS idx_pr_last_seen ON provider_records (last_seen);
";

pub type Db = SqlitePool;

/// Open (or create) the index database and apply the schema.
pub async fn open(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating index db directory {}", parent.display()))?;
    }
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .context("parsing sqlite URL")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("opening index db at {}", path.display()))?;
    for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt)
            .execute(&pool)
            .await
            .context("applying index schema")?;
    }
    Ok(pool)
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Record (or refresh the `last_seen` of) a `(hash, provider)` announcement.
pub async fn upsert(db: &Db, hash_hex: &str, provider: &str) -> Result<()> {
    let ts = now();
    sqlx::query(
        "INSERT INTO provider_records (hash, provider, first_seen, last_seen)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(hash, provider) DO UPDATE SET last_seen = ?3",
    )
    .bind(hash_hex)
    .bind(provider)
    .bind(ts)
    .execute(db)
    .await?;
    Ok(())
}

/// One row per distinct hash, aggregating its providers.
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct HashRow {
    /// Content hash, lowercase hex.
    pub hash: String,
    /// Number of distinct providers announcing this hash.
    pub providers: i64,
    /// Unix seconds when this hash was first seen.
    pub first_seen: i64,
    /// Unix seconds when this hash was last announced.
    pub last_seen: i64,
}

/// Hashes whose hex starts with `prefix` (empty matches everything), most
/// recently announced first.
pub async fn search(db: &Db, prefix: &str, limit: i64, offset: i64) -> Result<Vec<HashRow>> {
    let rows = sqlx::query_as::<_, HashRow>(
        "SELECT hash,
                COUNT(*)        AS providers,
                MIN(first_seen) AS first_seen,
                MAX(last_seen)  AS last_seen
         FROM provider_records
         WHERE hash LIKE ?1 || '%'
         GROUP BY hash
         ORDER BY last_seen DESC
         LIMIT ?2 OFFSET ?3",
    )
    .bind(prefix.to_lowercase())
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await?;
    Ok(rows)
}

/// Aggregate counters over the whole index.
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct Stats {
    pub total_records: i64,
    pub distinct_hashes: i64,
    pub distinct_providers: i64,
    /// Oldest `first_seen` (unix seconds), or null when empty.
    pub oldest: Option<i64>,
    /// Newest `last_seen` (unix seconds), or null when empty.
    pub newest: Option<i64>,
}

pub async fn stats(db: &Db) -> Result<Stats> {
    let s = sqlx::query_as::<_, Stats>(
        "SELECT COUNT(*)                 AS total_records,
                COUNT(DISTINCT hash)     AS distinct_hashes,
                COUNT(DISTINCT provider) AS distinct_providers,
                MIN(first_seen)          AS oldest,
                MAX(last_seen)           AS newest
         FROM provider_records",
    )
    .fetch_one(db)
    .await?;
    Ok(s)
}

/// Delete records not refreshed within `retention_days`. Returns rows deleted.
pub async fn prune(db: &Db, retention_days: i64) -> Result<u64> {
    let cutoff = now() - retention_days * 86_400;
    let res = sqlx::query("DELETE FROM provider_records WHERE last_seen < ?1")
        .bind(cutoff)
        .execute(db)
        .await?;
    Ok(res.rows_affected())
}
