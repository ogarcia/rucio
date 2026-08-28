//! The single SQLite database shared by the bootstrap's optional roles.
//!
//! Both the stats recorder and the DHT index persist to **one** file (opened
//! once here, cloned to each role) rather than a database per role: one file to
//! back up, one pool, and — because the roles share the pool — the stats panel
//! can read the index counters straight from its own handle.
//!
//! The schema is the union of whatever roles are compiled in: the stats tables
//! (always, under `stats`) and the index tables (under `web`). Each role owns
//! its own `apply_schema`; this module just opens the pool and calls them. Like
//! the rest of the project pre-1.0, tables are created with
//! `CREATE TABLE IF NOT EXISTS` — no migrations.

use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;

pub type Db = SqlitePool;

/// Open (or create) the shared database and apply every enabled role's schema.
pub async fn open(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating database directory {}", parent.display()))?;
    }
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .context("parsing sqlite URL")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("opening database at {}", path.display()))?;

    // The stats recorder is present on every `stats`/`web` build (this module is
    // only compiled then); the index tables come with `web`.
    crate::stats::apply_schema(&pool).await?;
    #[cfg(feature = "web")]
    crate::indexer::apply_schema(&pool).await?;

    Ok(pool)
}
