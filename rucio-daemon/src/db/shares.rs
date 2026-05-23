//! Queries for the `shared_files` and `chunks` tables.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// A shared file record as stored in the database.
#[derive(Debug, Clone)]
pub struct SharedFileRow {
    pub id: i64,
    pub root_hash: Vec<u8>,
    pub name: String,
    pub size: i64,
    pub mime_type: Option<String>,
    pub path: String,
    pub chunk_size: i64,
    pub added_at: i64,
}

/// Parameters for inserting a new shared file.
pub struct NewSharedFile<'a> {
    pub root_hash: &'a [u8; 32],
    pub name: &'a str,
    pub size: u64,
    pub mime_type: Option<&'a str>,
    pub path: &'a str,
    pub chunk_size: u32,
    pub added_at: u64,
    /// (idx, hash, size)
    pub chunks: &'a [(u32, [u8; 32], u32)],
}

/// Insert a new shared file and its chunks in a single transaction.
/// Returns the new `shared_files.id`.
pub async fn insert(db: &Db, f: NewSharedFile<'_>) -> Result<i64> {
    let mut tx = db.begin().await?;

    let file_id: i64 = sqlx::query(
        "INSERT INTO shared_files (root_hash, name, size, mime_type, path, chunk_size, added_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(f.root_hash.as_slice())
    .bind(f.name)
    .bind(f.size as i64)
    .bind(f.mime_type)
    .bind(f.path)
    .bind(f.chunk_size as i64)
    .bind(f.added_at as i64)
    .execute(&mut *tx)
    .await?
    .last_insert_rowid();

    for (idx, hash, chunk_sz) in f.chunks {
        sqlx::query("INSERT INTO chunks (shared_file_id, idx, hash, size) VALUES (?1, ?2, ?3, ?4)")
            .bind(file_id)
            .bind(*idx as i64)
            .bind(hash.as_slice())
            .bind(*chunk_sz as i64)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(file_id)
}

/// List all shared files.
pub async fn list(db: &Db) -> Result<Vec<SharedFileRow>> {
    let rows = sqlx::query("SELECT id, root_hash, name, size, mime_type, path, chunk_size, added_at FROM shared_files ORDER BY added_at DESC")
        .fetch_all(db)
        .await?;

    Ok(rows
        .iter()
        .map(|r| SharedFileRow {
            id: r.get("id"),
            root_hash: r.get("root_hash"),
            name: r.get("name"),
            size: r.get("size"),
            mime_type: r.get("mime_type"),
            path: r.get("path"),
            chunk_size: r.get("chunk_size"),
            added_at: r.get("added_at"),
        })
        .collect())
}

/// Delete a shared file (and its chunks via CASCADE) by root hash.
pub async fn delete(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let affected = sqlx::query("DELETE FROM shared_files WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}
