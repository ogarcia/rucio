//! Database layer — SQLite via sqlx.
//!
//! Pre-stable policy: the schema is applied with `CREATE TABLE IF NOT EXISTS`
//! on every startup, which creates everything a fresh database needs. For
//! additive changes to an *existing* database (new columns), lightweight
//! `user_version` migrations run after the schema is applied — see [`migrate`].
//! These stay collapsible: at 1.0 the accumulated migrations fold back into a
//! clean schema. Non-additive changes still mean deleting the DB file.

pub mod categories;
pub mod downloads;
pub mod emule_downloads;
pub mod emule_shared_files;
pub mod metrics;
pub mod mirror_optouts;
pub mod mirror_owned;
pub mod mirror_pins;
pub mod notifications;
pub mod peers;
pub mod pin_subscriptions;
pub mod pins;
pub mod shared_dirs;
pub mod shares;

use anyhow::{Context, Result};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};
use std::path::Path;
use std::str::FromStr;
use tracing::info;

/// Shared handle to the SQLite connection pool.
pub type Db = SqlitePool;

/// Open (or create) the SQLite database at `path` and apply the schema.
pub async fn open(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating db directory {}", parent.display()))?;
    }

    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .context("parsing sqlite URL")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        // NORMAL is the recommended pairing with WAL: it fsyncs only at
        // checkpoint, not on every commit, which is what FULL does. A power loss
        // can lose the last few transactions but never corrupts the database (WAL
        // guarantees that). Without this, the default FULL fsyncs on every commit,
        // and since indexing writes one transaction per file a large share (or a
        // re-scan of a churning tree) turns into tens of thousands of serial
        // fsyncs — minutes of disk-bound work where this is a few seconds.
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .foreign_keys(true);

    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("opening sqlite db at {}", path.display()))?;

    apply_schema(&pool).await?;
    migrate(&pool).await?;
    info!(path = %path.display(), "Database ready");
    Ok(pool)
}

/// Current schema version. Bumped when a new additive migration step is added
/// below; stored in SQLite's `PRAGMA user_version`.
const SCHEMA_VERSION: i64 = 1;

/// Apply additive migrations an existing database is missing, keyed on
/// `user_version`. A fresh database already has the current schema (so each
/// step is a column-existence-guarded no-op), after which its version is
/// stamped. Every step must be idempotent.
pub(crate) async fn migrate(pool: &Db) -> Result<()> {
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await
        .context("reading user_version")?;
    if version >= SCHEMA_VERSION {
        return Ok(());
    }

    // v1: localisable notifications — add `code`/`args` to pre-existing DBs.
    add_column_if_missing(pool, "notifications", "code", "TEXT").await?;
    add_column_if_missing(pool, "notifications", "args", "TEXT").await?;

    // PRAGMA can't be parameterised; the value is a compile-time constant.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "PRAGMA user_version = {SCHEMA_VERSION}"
    )))
    .execute(pool)
    .await
    .context("stamping user_version")?;
    info!(version = SCHEMA_VERSION, "Database migrated");
    Ok(())
}

/// `ALTER TABLE … ADD COLUMN` unless the column already exists. SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so check `table_info` first. Keeps the migration
/// safe to run against both a fresh DB (column already present) and an old one.
async fn add_column_if_missing(pool: &Db, table: &str, column: &str, decl: &str) -> Result<()> {
    let cols = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})")))
        .fetch_all(pool)
        .await
        .with_context(|| format!("reading columns of {table}"))?;
    let exists = cols.iter().any(|r| {
        let name: String = r.get("name");
        name == column
    });
    if !exists {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} ADD COLUMN {column} {decl}"
        )))
        .execute(pool)
        .await
        .with_context(|| format!("adding column {table}.{column}"))?;
    }
    Ok(())
}

/// Execute the embedded schema SQL against `pool`.
pub(crate) async fn apply_schema(pool: &Db) -> Result<()> {
    let schema = include_str!("schema.sql");
    // Strip line comments, then split on ';' and execute each non-empty statement.
    let statements: Vec<String> = schema
        .lines()
        .map(|line| {
            if let Some(pos) = line.find("--") {
                line[..pos].to_owned()
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();

    for stmt in statements {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(pool)
            .await
            .with_context(|| format!("applying schema statement: {stmt}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A clean pool close must drop the last SQLite connection properly, which
    /// in WAL mode lets SQLite remove its `-wal`/`-shm` sidecar files. Guards
    /// the graceful-shutdown path (which calls `db.close()`).
    #[tokio::test]
    async fn close_removes_wal_and_shm() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = open(&path).await.unwrap();
        // Force WAL activity so the sidecar files exist while open.
        sqlx::query("CREATE TABLE probe (x INTEGER)")
            .execute(&db)
            .await
            .unwrap();
        let wal = dir.path().join("t.db-wal");
        let shm = dir.path().join("t.db-shm");
        assert!(wal.exists(), "-wal should exist while the DB is open");

        db.close().await;

        assert!(!wal.exists(), "-wal should be gone after a clean close");
        assert!(!shm.exists(), "-shm should be gone after a clean close");
    }

    async fn has_column(db: &Db, table: &str, column: &str) -> bool {
        sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})")))
            .fetch_all(db)
            .await
            .unwrap()
            .iter()
            .any(|r| r.get::<String, _>("name") == column)
    }

    /// The v1 migration adds `code`/`args` to a database created before they
    /// existed, stamps the version, and is safe to run twice.
    #[tokio::test]
    async fn migrate_adds_notification_columns_to_old_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir.path().join("t.db")).await.unwrap();

        // Simulate an old DB: drop the modern table and recreate the pre-v1
        // shape, then rewind the version so the migration runs.
        sqlx::query("DROP TABLE notifications")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE notifications (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, \
             title TEXT NOT NULL, body TEXT NOT NULL, ref_key TEXT, \
             created_at INTEGER NOT NULL, read INTEGER NOT NULL DEFAULT 0)",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("PRAGMA user_version = 0")
            .execute(&db)
            .await
            .unwrap();
        assert!(!has_column(&db, "notifications", "code").await);

        migrate(&db).await.unwrap();
        assert!(has_column(&db, "notifications", "code").await);
        assert!(has_column(&db, "notifications", "args").await);
        let v: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        // Idempotent: a second run is a no-op and doesn't error.
        migrate(&db).await.unwrap();
    }
}
