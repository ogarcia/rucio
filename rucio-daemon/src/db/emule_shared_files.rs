//! Queries for the `emule_shared_files` table.
//!
//! Files downloaded from eMule that we keep serving to the Kad network after
//! the download finishes (good-citizen seeding). Decoupled from
//! `emule_downloads` on purpose: clearing the completed-downloads list must not
//! stop sharing. A file is shared until it changes or disappears on disk.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// A row from the `emule_shared_files` table.
#[derive(Debug, Clone)]
pub struct EmuleSharedFile {
    /// 16-byte MD4 hash (the canonical eMule file identifier).
    pub ed2k_hash: Vec<u8>,
    pub name: String,
    pub size: i64,
    /// Absolute path of the final file on disk.
    pub path: String,
    /// File mtime in Unix seconds, the change signal for the rescan.
    pub mtime: i64,
    /// ed2k part-hash set (concatenated 16-byte MD4 part hashes); empty for
    /// single-part files (or shares recorded before hashset support).
    pub hashset: Vec<u8>,
}

/// Register (or refresh) a completed eMule download as a shared file.
#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    db: &Db,
    ed2k_hash: &[u8; 16],
    name: &str,
    size: u64,
    path: &str,
    mtime: i64,
    hashset: &[u8],
    now: u64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO emule_shared_files (ed2k_hash, name, size, path, mtime, hashset, added_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(ed2k_hash) DO UPDATE SET \
             name = excluded.name, size = excluded.size, path = excluded.path, \
             mtime = excluded.mtime, hashset = excluded.hashset",
    )
    .bind(ed2k_hash.as_slice())
    .bind(name)
    .bind(size as i64)
    .bind(path)
    .bind(mtime)
    .bind(hashset)
    .bind(now as i64)
    .execute(db)
    .await?;
    Ok(())
}

/// List every shared eMule file.
pub async fn list(db: &Db) -> Result<Vec<EmuleSharedFile>> {
    let rows = sqlx::query(
        "SELECT ed2k_hash, name, size, path, mtime, hashset FROM emule_shared_files ORDER BY added_at",
    )
    .fetch_all(db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| EmuleSharedFile {
            ed2k_hash: r.get("ed2k_hash"),
            name: r.get("name"),
            size: r.get("size"),
            path: r.get("path"),
            mtime: r.get("mtime"),
            hashset: r.get("hashset"),
        })
        .collect())
}

/// Remove a shared file by its ed2k hash. Returns `true` if a row was deleted.
pub async fn delete_by_hash(db: &Db, ed2k_hash: &[u8]) -> Result<bool> {
    let res = sqlx::query("DELETE FROM emule_shared_files WHERE ed2k_hash = ?1")
        .bind(ed2k_hash)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Look up a shared file by its on-disk path, if present.
pub async fn get_by_path(db: &Db, path: &str) -> Result<Option<EmuleSharedFile>> {
    let row = sqlx::query(
        "SELECT ed2k_hash, name, size, path, mtime, hashset FROM emule_shared_files WHERE path = ?1",
    )
    .bind(path)
    .fetch_optional(db)
    .await?;
    Ok(row.map(|r| EmuleSharedFile {
        ed2k_hash: r.get("ed2k_hash"),
        name: r.get("name"),
        size: r.get("size"),
        path: r.get("path"),
        mtime: r.get("mtime"),
        hashset: r.get("hashset"),
    }))
}
