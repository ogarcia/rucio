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
    /// Non-null for eMule (ed2k) downloads; `None` for libp2p downloads.
    pub ed2k_link: Option<String>,
}

/// Insert a download row for an eMule (ed2k) download.
///
/// The `ed2k_hash` is the 16-byte MD4 hash from the ed2k link.  We store it
/// padded to 32 bytes (MD4 bytes + 16 zero bytes) as a provisional identifier;
/// the row is updated to the real BLAKE3 hash once the download completes.
/// The original `ed2k_link` string is persisted so the download can be
/// resumed after a daemon restart.
///
/// Returns the new `downloads.id`.
pub async fn create_emule_pending(
    db: &Db,
    ed2k_hash: &[u8; 16],
    name: &str,
    total_size: u64,
    ed2k_link: &str,
    now: u64,
) -> Result<i64> {
    let mut root_hash = [0u8; 32];
    root_hash[..16].copy_from_slice(ed2k_hash);

    // If a row with this provisional hash already exists (e.g. duplicate
    // submission), return its ID without inserting a duplicate.
    let existing: Option<i64> = sqlx::query_scalar("SELECT id FROM downloads WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await?;

    if let Some(id) = existing {
        return Ok(id);
    }

    let id = sqlx::query(
        "INSERT INTO downloads (root_hash, name, total_size, dest_path, status, ed2k_link, added_at, updated_at)
         VALUES (?1, ?2, ?3, '', 'finding_providers', ?4, ?5, ?5)",
    )
    .bind(root_hash.as_slice())
    .bind(name)
    .bind(total_size as i64)
    .bind(ed2k_link)
    .bind(now as i64)
    .execute(db)
    .await?
    .last_insert_rowid();
    Ok(id)
}

/// Insert a placeholder row for a download that has not yet received its
/// manifest (no chunks known yet).  The initial status reflects whether we
/// already have providers (`"queued"`) or are still searching (`"finding_providers"`).
/// Returns the new `downloads.id`.
pub async fn create_pending(
    db: &Db,
    root_hash: &[u8; 32],
    name: Option<&str>,
    now: u64,
    has_providers: bool,
) -> Result<i64> {
    let status = if has_providers {
        "queued"
    } else {
        "finding_providers"
    };
    let id = sqlx::query(
        "INSERT INTO downloads (root_hash, name, total_size, dest_path, status, added_at, updated_at)
         VALUES (?1, ?2, 0, '', ?3, ?4, ?4)",
    )
    .bind(root_hash.as_slice())
    .bind(name)
    .bind(status)
    .bind(now as i64)
    .execute(db)
    .await?
    .last_insert_rowid();
    Ok(id)
}

/// Update the placeholder row created by `create_pending()` with the real
/// manifest data (name, size, dest_path) and insert the chunk rows.
/// Sets status to 'downloading'.
pub async fn finalize_pending(
    db: &Db,
    id: i64,
    name: &str,
    total_size: u64,
    dest_path: &str,
    now: u64,
    chunks: &[(u32, [u8; 32], u32)], // (idx, hash, size)
) -> Result<()> {
    let mut tx = db.begin().await?;

    sqlx::query(
        "UPDATE downloads SET name = ?1, total_size = ?2, dest_path = ?3,
         status = 'downloading', updated_at = ?4 WHERE id = ?5",
    )
    .bind(name)
    .bind(total_size as i64)
    .bind(dest_path)
    .bind(now as i64)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    for (idx, hash, size) in chunks {
        sqlx::query(
            "INSERT INTO download_chunks (download_id, idx, hash, size) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(id)
        .bind(*idx as i64)
        .bind(hash.as_slice())
        .bind(*size as i64)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// List all downloads.
pub async fn list(db: &Db) -> Result<Vec<DownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at, ed2k_link
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
            ed2k_link: r.get("ed2k_link"),
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

/// Update the final destination path of a completed download.
pub async fn set_dest_path(db: &Db, download_id: i64, dest_path: &str) -> Result<()> {
    sqlx::query("UPDATE downloads SET dest_path = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(dest_path)
        .bind(now_secs() as i64)
        .bind(download_id)
        .execute(db)
        .await?;
    Ok(())
}

/// Return the current status string for a download, or `None` if not found.
pub async fn get_status(db: &Db, download_id: i64) -> Result<Option<String>> {
    let row = sqlx::query("SELECT status FROM downloads WHERE id = ?1")
        .bind(download_id)
        .fetch_optional(db)
        .await?;
    Ok(row.map(|r| r.get("status")))
}

/// Return the root_hash for a single download row, or `None` if not found.
pub async fn get_root_hash(db: &Db, download_id: i64) -> Result<Option<Vec<u8>>> {
    let row = sqlx::query("SELECT root_hash FROM downloads WHERE id = ?1")
        .bind(download_id)
        .fetch_optional(db)
        .await?;
    Ok(row.map(|r| r.get("root_hash")))
}

/// Permanently delete a download record and its chunks from the DB.
///
/// Only intended for finished downloads (completed / cancelled / error).
/// Returns `true` if a row was deleted.
pub async fn delete(db: &Db, download_id: i64) -> Result<bool> {
    let affected = sqlx::query("DELETE FROM downloads WHERE id = ?1")
        .bind(download_id)
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// Mark any pending/active download for `root_hash` as failed.
/// Used when the manifest cannot be retrieved from any provider.
pub async fn fail_by_hash(db: &Db, root_hash: &[u8; 32]) -> Result<()> {
    sqlx::query(
        "UPDATE downloads SET status = 'error', error_msg = 'manifest timeout: all providers exhausted', \
         updated_at = ?1 \
         WHERE root_hash = ?2 AND status IN ('pending', 'active')",
    )
    .bind(now_secs() as i64)
    .bind(root_hash.as_slice())
    .execute(db)
    .await?;
    Ok(())
}

/// A single chunk row returned from the DB.
#[derive(Debug, Clone)]
pub struct ChunkRow {
    pub idx: u32,
    pub hash: Vec<u8>,
    pub size: u32,
    pub status: String, // 'pending' | 'downloading' | 'done'
}

/// Return all downloads that were interrupted and should be resumed on startup.
/// These are rows whose status is 'finding_providers', 'queued' or 'downloading'.
pub async fn list_resumable(db: &Db) -> Result<Vec<DownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at, ed2k_link
         FROM downloads
         WHERE status IN ('finding_providers', 'queued', 'downloading')
         ORDER BY added_at ASC",
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
            ed2k_link: r.get("ed2k_link"),
        })
        .collect())
}

/// Return all chunk rows for the given download, ordered by idx.
pub async fn chunks_for(db: &Db, download_id: i64) -> Result<Vec<ChunkRow>> {
    let rows = sqlx::query(
        "SELECT idx, hash, size, status
         FROM download_chunks
         WHERE download_id = ?1
         ORDER BY idx ASC",
    )
    .bind(download_id)
    .fetch_all(db)
    .await?;

    Ok(rows
        .iter()
        .map(|r| ChunkRow {
            idx: r.get::<i64, _>("idx") as u32,
            hash: r.get("hash"),
            size: r.get::<i64, _>("size") as u32,
            status: r.get("status"),
        })
        .collect())
}

/// Reset any chunks that were left in 'downloading' state back to 'pending'.
/// Called at startup when resuming an interrupted download.
pub async fn reset_in_flight_chunks(db: &Db, download_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE download_chunks SET status = 'pending'
         WHERE download_id = ?1 AND status = 'downloading'",
    )
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> (Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;

        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        super::super::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    fn hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn chunks(n: u32) -> Vec<(u32, [u8; 32], u32)> {
        (0..n).map(|i| (i, hash(i as u8 + 20), 4096)).collect()
    }

    #[tokio::test]
    async fn enqueue_and_list() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(1), Some("movie.mkv"), 1_000, true)
            .await
            .unwrap();
        finalize_pending(
            &db,
            id,
            "movie.mkv",
            8192,
            "/tmp/movie.mkv",
            1_000,
            &chunks(2),
        )
        .await
        .unwrap();
        assert!(id > 0);

        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "movie.mkv");
        assert_eq!(rows[0].total_size, 8192);
        assert_eq!(rows[0].status, "downloading");
        assert_eq!(rows[0].bytes_done, 0);
    }

    #[tokio::test]
    async fn chunk_done_updates_bytes() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(2), Some("file.bin"), 1_000, true)
            .await
            .unwrap();
        finalize_pending(
            &db,
            id,
            "file.bin",
            8192,
            "/tmp/file.bin",
            1_000,
            &chunks(2),
        )
        .await
        .unwrap();

        chunk_done(&db, id, 0, 4096).await.unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].bytes_done, 4096);
    }

    #[tokio::test]
    async fn chunk_done_twice_accumulates() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(3), Some("file.bin"), 1_000, true)
            .await
            .unwrap();
        finalize_pending(
            &db,
            id,
            "file.bin",
            8192,
            "/tmp/file.bin",
            1_000,
            &chunks(2),
        )
        .await
        .unwrap();

        chunk_done(&db, id, 0, 4096).await.unwrap();
        chunk_done(&db, id, 1, 4096).await.unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].bytes_done, 8192);
    }

    #[tokio::test]
    async fn set_status_completed() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(4), Some("track.flac"), 1_000, true)
            .await
            .unwrap();

        set_status(&db, id, "completed", None).await.unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "completed");
        assert!(rows[0].error_msg.is_none());
    }

    #[tokio::test]
    async fn set_status_error_stores_message() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(5), Some("doc.pdf"), 1_000, true)
            .await
            .unwrap();

        set_status(&db, id, "error", Some("peer disconnected"))
            .await
            .unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "error");
        assert_eq!(rows[0].error_msg.as_deref(), Some("peer disconnected"));
    }

    #[tokio::test]
    async fn download_chunks_cascade_on_delete() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(6), Some("big.iso"), 1_000, true)
            .await
            .unwrap();
        finalize_pending(&db, id, "big.iso", 16384, "/tmp/big.iso", 1_000, &chunks(4))
            .await
            .unwrap();

        sqlx::query("DELETE FROM downloads WHERE id = ?1")
            .bind(id)
            .execute(&db)
            .await
            .unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM download_chunks")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
