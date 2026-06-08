//! SQLite store for the passive DHT indexer.
//!
//! Records every `(hash, provider)` announcement seen on the DHT with
//! first/last-seen timestamps. Pre-stable policy mirrors the daemon: the schema
//! is applied with `CREATE TABLE IF NOT EXISTS` on startup, no migrations.

use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rucio_core::protocol::search::normalize_search_term;
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
    name_norm  TEXT,
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

/// Result ordering for [`search`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Sort {
    /// Most recently announced first (freshness). The default.
    #[default]
    Newest,
    /// Oldest announcement first (age).
    Oldest,
    /// Most providers first — availability, used as a relevance proxy.
    Providers,
    /// Largest file first (records without a known size sort last).
    Size,
}

impl Sort {
    /// Map a query-string value to a variant; unknown/empty → [`Sort::Newest`].
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "oldest" | "age" => Sort::Oldest,
            "providers" | "relevance" => Sort::Providers,
            "size" => Sort::Size,
            _ => Sort::Newest,
        }
    }

    /// Canonical query-string value, for round-tripping through links/forms.
    pub fn as_param(self) -> &'static str {
        match self {
            Sort::Newest => "newest",
            Sort::Oldest => "oldest",
            Sort::Providers => "providers",
            Sort::Size => "size",
        }
    }

    /// The `ORDER BY` body. A fixed literal per variant — never user input — so
    /// it is safe to splice into the query. A `last_seen DESC` tie-breaker keeps
    /// ordering stable and useful within equal keys.
    fn order_by(self) -> &'static str {
        match self {
            Sort::Newest => "last_seen DESC",
            Sort::Oldest => "last_seen ASC",
            Sort::Providers => "providers DESC, last_seen DESC",
            Sort::Size => "size DESC, last_seen DESC",
        }
    }
}

/// Hashes matching `query`, ordered per `sort`.
///
/// The query is matched two ways:
///
/// * **Hash prefix** — a single whitespace-free token is also tried as a hex
///   prefix of the content hash.
/// * **File name** — the query is split on whitespace into terms, and a record
///   matches only if *every* term occurs as a substring of the enriched file
///   name. Matching is case- and accent-insensitive: both the stored name and
///   the terms are folded with [`normalize_search_term`], so `ghost in the
///   shell` matches `Ghost.in.the.Shell.ARISE...` and `camion` matches
///   `Camión...`.
///
/// An empty `query` matches everything (used by the list endpoint).
pub async fn search(
    db: &Db,
    query: &str,
    sort: Sort,
    limit: i64,
    offset: i64,
) -> Result<Vec<HashRow>> {
    let terms: Vec<String> = query
        .split_whitespace()
        .map(normalize_search_term)
        .filter(|t| !t.is_empty())
        .collect();

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

        // File-name match: AND over every term as a substring of the folded
        // name. Terms are already normalized to name_norm's character space.
        qb.push("(f.name_norm IS NOT NULL");
        for term in &terms {
            qb.push(" AND f.name_norm LIKE ");
            qb.push_bind(format!("%{}%", like_escape(term)));
            qb.push(" ESCAPE '\\'");
        }
        qb.push("))");
    }

    qb.push(" GROUP BY pr.hash ORDER BY ");
    qb.push(sort.order_by());
    qb.push(" LIMIT ");
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
///
/// Stores both the display `name` and an accent-folded, lowercased `name_norm`
/// (via [`normalize_search_term`]) so searches match the same character space
/// as the rucio network.
pub async fn upsert_file(db: &Db, hash_hex: &str, name: &str, size: i64) -> Result<()> {
    let ts = now();
    let name_norm = normalize_search_term(name);
    sqlx::query(
        "INSERT INTO files (hash, name, name_norm, size, indexed_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(hash) DO UPDATE SET name = ?2, name_norm = ?3, size = ?4, indexed_at = ?5",
    )
    .bind(hash_hex)
    .bind(name)
    .bind(&name_norm)
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

    #[test]
    fn sort_parse_maps_values() {
        assert_eq!(Sort::parse("size"), Sort::Size);
        assert_eq!(Sort::parse("PROVIDERS"), Sort::Providers);
        assert_eq!(Sort::parse("relevance"), Sort::Providers);
        assert_eq!(Sort::parse("oldest"), Sort::Oldest);
        assert_eq!(Sort::parse("newest"), Sort::Newest);
        assert_eq!(Sort::parse(""), Sort::Newest);
        assert_eq!(Sort::parse("nonsense"), Sort::Newest);
    }

    #[tokio::test]
    async fn sort_by_size_and_providers() {
        let db = mem_db().await;
        // aaa: small (100 B), 3 providers.
        upsert_file(&db, "aaa", "small but popular", 100)
            .await
            .unwrap();
        for p in ["p1", "p2", "p3"] {
            upsert(&db, "aaa", p).await.unwrap();
        }
        // bbb: large (9000 B), 1 provider.
        upsert_file(&db, "bbb", "big but lonely", 9000)
            .await
            .unwrap();
        upsert(&db, "bbb", "q1").await.unwrap();
        // ccc: medium (500 B), 2 providers.
        upsert_file(&db, "ccc", "middle", 500).await.unwrap();
        upsert(&db, "ccc", "r1").await.unwrap();
        upsert(&db, "ccc", "r2").await.unwrap();

        // Largest first.
        let r = search(&db, "", Sort::Size, 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["bbb", "ccc", "aaa"]);

        // Most providers first (relevance proxy).
        let r = search(&db, "", Sort::Providers, 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["aaa", "ccc", "bbb"]);
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
        let r = search(&db, "ghost", Sort::default(), 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Multi-word query across dot separators — the reported bug.
        let r = search(&db, "ghost in the shell", Sort::default(), 50, 0)
            .await
            .unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Case-insensitive and order-independent.
        let r = search(&db, "SHELL ghost", Sort::default(), 50, 0)
            .await
            .unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // A term that is absent excludes the record (AND semantics).
        let r = search(&db, "ghost batman", Sort::default(), 50, 0)
            .await
            .unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn hash_prefix_and_empty_query() {
        let db = mem_db().await;
        insert(&db, "5040a9f7e363afc4", "Ghost.in.the.Shell.mkv").await;
        insert(&db, "deadbeefdeadbeef", "Other.mkv").await;

        // Single token is also tried as a hash prefix.
        let r = search(&db, "5040a9f7", Sort::default(), 50, 0)
            .await
            .unwrap();
        assert_eq!(hashes(&r), vec!["5040a9f7e363afc4"]);

        // Empty query returns everything.
        let r = search(&db, "", Sort::default(), 50, 0).await.unwrap();
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn search_is_accent_insensitive() {
        let db = mem_db().await;
        insert(&db, "c0c0", "Camión.de.Bomberos.Documental.mkv").await;
        insert(&db, "d0d0", "Ano.Nuevo.2024.mkv").await;
        insert(&db, "e0e0", "El.Año.del.Dragón.mkv").await;

        // Folded query matches the accented name (the reported gap).
        assert_eq!(
            hashes(&search(&db, "camion", Sort::default(), 50, 0).await.unwrap()),
            vec!["c0c0"]
        );
        // Accented query still matches (symmetric).
        assert_eq!(
            hashes(&search(&db, "camión", Sort::default(), 50, 0).await.unwrap()),
            vec!["c0c0"]
        );
        // ñ is a distinct letter, not folded: "ano" must not match "Año".
        assert_eq!(
            hashes(&search(&db, "ano", Sort::default(), 50, 0).await.unwrap()),
            vec!["d0d0"]
        );
        // "año" matches the ñ name, and "dragon" folds to match "Dragón".
        assert_eq!(
            hashes(
                &search(&db, "año dragon", Sort::default(), 50, 0)
                    .await
                    .unwrap()
            ),
            vec!["e0e0"]
        );
    }

    #[tokio::test]
    async fn like_wildcards_are_matched_literally() {
        let db = mem_db().await;
        insert(&db, "aaaa", "50%.discount.flyer.pdf").await;
        insert(&db, "bbbb", "5012.report.pdf").await;

        // `%` must not act as a wildcard: only the literal "50%" name matches.
        let r = search(&db, "50%", Sort::default(), 50, 0).await.unwrap();
        assert_eq!(hashes(&r), vec!["aaaa"]);
    }
}
