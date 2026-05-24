//! Database layer — SQLite via sqlx.
//!
//! Pre-stable policy: schema is applied with `CREATE TABLE IF NOT EXISTS` on
//! every startup.  If the schema changes, delete the DB file and restart.
//! No migrations, no versioning until the project reaches a stable release.

pub mod downloads;
pub mod peers;
pub mod shared_dirs;
pub mod shares;

use anyhow::{Context, Result};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
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
        .foreign_keys(true);

    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("opening sqlite db at {}", path.display()))?;

    apply_schema(&pool).await?;
    info!(path = %path.display(), "Database ready");
    Ok(pool)
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
