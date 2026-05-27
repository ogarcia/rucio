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

CREATE TABLE IF NOT EXISTS files (
    hash       TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    size       INTEGER NOT NULL,
    indexed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_name ON files (name);
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

/// One row per distinct hash, aggregating its providers and (if enriched) its
/// file name and size.
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct HashRow {
    /// Content hash, lowercase hex.
    pub hash: String,
    /// File name, once the hash has been enriched; null otherwise.
    pub name: Option<String>,
    /// File size in bytes, once enriched; null otherwise.
    pub size: Option<i64>,
    /// Number of distinct providers announcing this hash.
    pub providers: i64,
    /// Unix seconds when this hash was first seen.
    pub first_seen: i64,
    /// Unix seconds when this hash was last announced.
    pub last_seen: i64,
}

/// Hashes matching `query` — either by hash hex prefix or, once enriched, by
/// file-name substring. An empty `query` matches everything. Most recently
/// announced first.
pub async fn search(db: &Db, query: &str, limit: i64, offset: i64) -> Result<Vec<HashRow>> {
    let rows = sqlx::query_as::<_, HashRow>(
        "SELECT pr.hash            AS hash,
                f.name             AS name,
                f.size             AS size,
                COUNT(*)           AS providers,
                MIN(pr.first_seen) AS first_seen,
                MAX(pr.last_seen)  AS last_seen
         FROM provider_records pr
         LEFT JOIN files f ON f.hash = pr.hash
         WHERE pr.hash LIKE ?1 || '%'
            OR (f.name IS NOT NULL AND f.name LIKE '%' || ?2 || '%')
         GROUP BY pr.hash
         ORDER BY last_seen DESC
         LIMIT ?3 OFFSET ?4",
    )
    .bind(query.to_lowercase())
    .bind(query)
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await?;
    Ok(rows)
}

/// Whether a file's metadata (name/size) is already recorded for `hash_hex`.
pub async fn has_file(db: &Db, hash_hex: &str) -> Result<bool> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM files WHERE hash = ?1")
        .bind(hash_hex)
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

/// Record (or refresh) the file metadata for a hash.
pub async fn upsert_file(db: &Db, hash_hex: &str, name: &str, size: i64) -> Result<()> {
    let ts = now();
    sqlx::query(
        "INSERT INTO files (hash, name, size, indexed_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(hash) DO UPDATE SET name = ?2, size = ?3, indexed_at = ?4",
    )
    .bind(hash_hex)
    .bind(name)
    .bind(size)
    .bind(ts)
    .execute(db)
    .await?;
    Ok(())
}

/// Aggregate counters over the whole index.
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct Stats {
    pub total_records: i64,
    pub distinct_hashes: i64,
    pub distinct_providers: i64,
    /// Distinct hashes enriched with a file name/size.
    pub enriched_files: i64,
    /// Oldest `first_seen` (unix seconds), or null when empty.
    pub oldest: Option<i64>,
    /// Newest `last_seen` (unix seconds), or null when empty.
    pub newest: Option<i64>,
}

pub async fn stats(db: &Db) -> Result<Stats> {
    let s = sqlx::query_as::<_, Stats>(
        "SELECT (SELECT COUNT(*) FROM provider_records)                 AS total_records,
                (SELECT COUNT(DISTINCT hash) FROM provider_records)     AS distinct_hashes,
                (SELECT COUNT(DISTINCT provider) FROM provider_records) AS distinct_providers,
                (SELECT COUNT(*) FROM files)                            AS enriched_files,
                (SELECT MIN(first_seen) FROM provider_records)          AS oldest,
                (SELECT MAX(last_seen) FROM provider_records)           AS newest",
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
