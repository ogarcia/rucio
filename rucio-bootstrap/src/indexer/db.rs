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

/// Escape the LIKE metacharacters (`%`, `_`, and the escape char itself) in a
/// user term so it is matched literally. Pair with `ESCAPE '\'` in the query.
fn like_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Hashes matching `query`, most recently announced first.
///
/// The query is matched two ways:
///
/// * **Hash prefix** — a single whitespace-free token is also tried as a hex
///   prefix of the content hash.
/// * **File name** — the query is split on whitespace into terms, and a record
///   matches only if *every* term occurs (case-insensitive substring) in the
///   enriched file name. So `ghost in the shell` matches
///   `Ghost.in.the.Shell.ARISE...` even though the words are dot-separated.
///
/// An empty `query` matches everything (used by the list endpoint).
pub async fn search(db: &Db, query: &str, limit: i64, offset: i64) -> Result<Vec<HashRow>> {
    let terms: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();

    let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT pr.hash            AS hash,
                f.name             AS name,
                f.size             AS size,
                COUNT(*)           AS providers,
                MIN(pr.first_seen) AS first_seen,
                MAX(pr.last_seen)  AS last_seen
         FROM provider_records pr
         LEFT JOIN files f ON f.hash = pr.hash",
    );

    if !terms.is_empty() {
        qb.push(" WHERE (");

        // Hash-prefix match only makes sense for a single, whitespace-free term.
        if terms.len() == 1 {
            qb.push("pr.hash LIKE ");
            qb.push_bind(format!("{}%", like_escape(&terms[0])));
            qb.push(" ESCAPE '\\' OR ");
        }

        // File-name match: AND over every term as a case-insensitive substring.
        qb.push("(f.name IS NOT NULL");
        for term in &terms {
            qb.push(" AND LOWER(f.name) LIKE ");
            qb.push_bind(format!("%{}%", like_escape(term)));
            qb.push(" ESCAPE '\\'");
        }
        qb.push("))");
    }

    qb.push(" GROUP BY pr.hash ORDER BY last_seen DESC LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build_query_as::<HashRow>().fetch_all(db).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    /// In-memory DB on a single connection (so the schema persists for the
    /// lifetime of the pool).
    async fn mem_db() -> Db {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        pool
    }

    /// Insert one enriched hash with a provider and a file name.
    async fn insert(db: &Db, hash: &str, name: &str) {
        upsert(db, hash, "12D3KooWprovider").await.unwrap();
        upsert_file(db, hash, name, 1234).await.unwrap();
    }

    fn hashes(rows: &[HashRow]) -> Vec<&str> {
        rows.iter().map(|r| r.hash.as_str()).collect()
    }

    #[tokio::test]
    async fn multi_word_query_matches_separator_delimited_name() {
        let db = mem_db().await;
        insert(
            &db,
            "5040a9f7e363afc4",
            "Ghost.in.the.Shell.ARISE.Border.04.BDRip.1080p.mkv",
        )
        .await;
        insert(&db, "deadbeefdeadbeef", "Some.Other.Movie.1080p.mkv").await;

        // Single word (the case that already worked).
        let r = search(&db, "ghost", 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Multi-word query across dot separators — the reported bug.
        let r = search(&db, "ghost in the shell", 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Case-insensitive and order-independent.
        let r = search(&db, "SHELL ghost", 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // A term that is absent excludes the record (AND semantics).
        let r = search(&db, "ghost batman", 50, 0).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn hash_prefix_and_empty_query() {
        let db = mem_db().await;
        insert(&db, "5040a9f7e363afc4", "Ghost.in.the.Shell.mkv").await;
        insert(&db, "deadbeefdeadbeef", "Other.mkv").await;

        // Single token is also tried as a hash prefix.
        let r = search(&db, "5040a9f7", 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Empty query returns everything.
        let r = search(&db, "", 50, 0).await.unwrap();
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn like_wildcards_are_matched_literally() {
        let db = mem_db().await;
        insert(&db, "aaaa", "50%.discount.flyer.pdf").await;
        insert(&db, "bbbb", "5012.report.pdf").await;

        // `%` must not act as a wildcard: only the literal "50%" name matches.
        let r = search(&db, "50%", 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["aaaa"]);
    }
}
