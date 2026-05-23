//! Queries for the `downloads` and `download_chunks` tables.

use anyhow::Result;
use sqlx::Row;

use super::Db;

#[derive(Debug, Clone)]
pub struct DownloadRow {
    pub id: i64,
    pub root_hash: Vec<u8>,
    pub name: String,
    pub total_size: i64,
    pub dest_path: String,
    pub status: String,
    pub bytes_done: i64,
    pub error_msg: Option<String>,
    pub added_at: i64,
    pub updated_at: i64,
}

/// Queue a new download. Returns the new `downloads.id`.
pub async fn enqueue(
    db: &Db,
    root_hash: &[u8; 32],
    name: &str,
    total_size: u64,
    dest_path: &str,
    now: u64,
    chunks: &[(u32, [u8; 32], u32)], // (idx, hash, size)
) -> Result<i64> {
    let mut tx = db.begin().await?;

    let dl_id: i64 = sqlx::query(
        "INSERT INTO downloads (root_hash, name, total_size, dest_path, added_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(root_hash.as_slice())
    .bind(name)
    .bind(total_size as i64)
    .bind(dest_path)
    .bind(now as i64)
    .bind(now as i64)
    .execute(&mut *tx)
    .await?
    .last_insert_rowid();

    for (idx, hash, size) in chunks {
        sqlx::query(
            "INSERT INTO download_chunks (download_id, idx, hash, size) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(dl_id)
        .bind(*idx as i64)
        .bind(hash.as_slice())
        .bind(*size as i64)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(dl_id)
}

/// List all downloads.
pub async fn list(db: &Db) -> Result<Vec<DownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at
         FROM downloads ORDER BY added_at DESC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .iter()
        .map(|r| DownloadRow {
            id: r.get("id"),
            root_hash: r.get("root_hash"),
            name: r.get("name"),
            total_size: r.get("total_size"),
            dest_path: r.get("dest_path"),
            status: r.get("status"),
            bytes_done: r.get("bytes_done"),
            error_msg: r.get("error_msg"),
            added_at: r.get("added_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

/// Mark a chunk as done and update `bytes_done` on the parent download.
pub async fn chunk_done(db: &Db, download_id: i64, chunk_idx: u32, chunk_size: u32) -> Result<()> {
    let mut tx = db.begin().await?;

    sqlx::query(
        "UPDATE download_chunks SET status = 'done'
         WHERE download_id = ?1 AND idx = ?2",
    )
    .bind(download_id)
    .bind(chunk_idx as i64)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE downloads SET bytes_done = bytes_done + ?1, updated_at = ?2
         WHERE id = ?3",
    )
    .bind(chunk_size as i64)
    .bind(now_secs() as i64)
    .bind(download_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Set a download's status (e.g. 'completed', 'error', 'paused').
pub async fn set_status(
    db: &Db,
    download_id: i64,
    status: &str,
    error_msg: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE downloads SET status = ?1, error_msg = ?2, updated_at = ?3 WHERE id = ?4")
        .bind(status)
        .bind(error_msg)
        .bind(now_secs() as i64)
        .bind(download_id)
        .execute(db)
        .await?;
    Ok(())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
