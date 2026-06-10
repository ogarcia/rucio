//! Queries for the `downloads` and `download_chunks` tables (libp2p network).

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
    /// Category this download is filed under (NULL = global download dir).
    pub category_id: Option<i64>,
}

/// Build a [`DownloadRow`] from a query row. The SELECT must list the columns in
/// the struct (see [`list`]).
fn row_to_download(r: &sqlx::sqlite::SqliteRow) -> DownloadRow {
    DownloadRow {
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
        category_id: r.get("category_id"),
    }
}

/// Outcome of [`create_pending`] when a row for the same hash already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePendingResult {
    /// A fresh row was inserted.  Contains the new ID.
    Inserted(i64),
    /// A row already exists and is active — do not start a duplicate task.
    AlreadyActive(i64),
    /// A cancelled or failed row was reset to the given status and is ready
    /// to be retried.  Contains the existing ID.
    Reactivated(i64),
    /// A completed row exists — the file was already downloaded successfully.
    AlreadyCompleted(i64),
}

impl CreatePendingResult {
    pub fn id(&self) -> i64 {
        match *self {
            CreatePendingResult::Inserted(id)
            | CreatePendingResult::AlreadyActive(id)
            | CreatePendingResult::Reactivated(id)
            | CreatePendingResult::AlreadyCompleted(id) => id,
        }
    }
}

/// Insert a placeholder row for a download that has not yet received its
/// manifest (no chunks known yet), handling duplicates gracefully.
///
/// - No existing row → insert and return [`CreatePendingResult::Inserted`].
/// - Existing row is active (`finding_providers`/`queued`/`downloading`) →
///   [`CreatePendingResult::AlreadyActive`].
/// - Existing row is terminal (`cancelled`/`error`) → reset to the appropriate
///   initial status and return [`CreatePendingResult::Reactivated`].
/// - Existing row is `completed` → [`CreatePendingResult::AlreadyCompleted`].
pub async fn create_pending(
    db: &Db,
    root_hash: &[u8; 32],
    name: Option<&str>,
    now: u64,
    has_providers: bool,
    category_id: Option<i64>,
) -> Result<CreatePendingResult> {
    let status = if has_providers {
        "queued"
    } else {
        "finding_providers"
    };

    let existing = sqlx::query("SELECT id, status FROM downloads WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await?;

    if let Some(row) = existing {
        let id: i64 = row.get("id");
        let existing_status: String = row.get("status");
        return match existing_status.as_str() {
            "completed" => Ok(CreatePendingResult::AlreadyCompleted(id)),
            "finding_providers" | "queued" | "downloading" => {
                Ok(CreatePendingResult::AlreadyActive(id))
            }
            // "cancelled" | "error" | anything else → reactivate
            _ => {
                sqlx::query(
                    "UPDATE downloads \
                     SET status = ?1, bytes_done = 0, dest_path = '', \
                         error_msg = NULL, category_id = ?2, updated_at = ?3 \
                     WHERE id = ?4",
                )
                .bind(status)
                .bind(category_id)
                .bind(now as i64)
                .bind(id)
                .execute(db)
                .await?;
                // Also clear any stale chunk rows from the previous attempt.
                sqlx::query("DELETE FROM download_chunks WHERE download_id = ?1")
                    .bind(id)
                    .execute(db)
                    .await?;
                Ok(CreatePendingResult::Reactivated(id))
            }
        };
    }

    let id = sqlx::query(
        "INSERT INTO downloads (root_hash, name, total_size, dest_path, status, category_id, added_at, updated_at)
         VALUES (?1, ?2, 0, '', ?3, ?4, ?5, ?5)",
    )
    .bind(root_hash.as_slice())
    .bind(name)
    .bind(status)
    .bind(category_id)
    .bind(now as i64)
    .execute(db)
    .await?
    .last_insert_rowid();
    Ok(CreatePendingResult::Inserted(id))
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
                bytes_done, error_msg, added_at, updated_at, category_id
         FROM downloads ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows.iter().map(row_to_download).collect())
}

/// Fetch a single download by ID, or `None` if it does not exist.
pub async fn get(db: &Db, id: i64) -> Result<Option<DownloadRow>> {
    let row = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at, category_id
         FROM downloads WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(db)
    .await?;

    Ok(row.as_ref().map(row_to_download))
}

/// Fetch a single download by its root hash, or `None` if there is no row.
pub async fn get_by_root_hash(db: &Db, root_hash: &[u8; 32]) -> Result<Option<DownloadRow>> {
    let row = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at, category_id
         FROM downloads WHERE root_hash = ?1",
    )
    .bind(root_hash.as_slice())
    .fetch_optional(db)
    .await?;

    Ok(row.as_ref().map(row_to_download))
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

/// Rename the file: change the `name` the download will be saved as. The
/// `.part` file and the final destination are updated by the transfer engine
/// (it owns the in-memory path); this only updates the `name` column so the
/// API/list reflect it immediately.
pub async fn set_name(db: &Db, download_id: i64, name: &str) -> Result<()> {
    sqlx::query("UPDATE downloads SET name = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(name)
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

/// The download's category id, or `None` if it's unassigned (or there is no
/// such row). Used to resolve its destination directory on completion.
pub async fn get_category_id(db: &Db, download_id: i64) -> Result<Option<i64>> {
    let v: Option<Option<i64>> =
        sqlx::query_scalar("SELECT category_id FROM downloads WHERE id = ?1")
            .bind(download_id)
            .fetch_optional(db)
            .await?;
    Ok(v.flatten())
}

/// Assign (or clear, with `None`) the download's category. Returns `true` if a
/// row was updated.
pub async fn set_category(db: &Db, download_id: i64, category_id: Option<i64>) -> Result<bool> {
    let affected = sqlx::query("UPDATE downloads SET category_id = ?1 WHERE id = ?2")
        .bind(category_id)
        .bind(download_id)
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// Status of the download with this root hash, if any. Lets the HTTP layer
/// answer synchronously (the engine's authoritative `create_pending` runs
/// asynchronously and its "already completed/active" result is otherwise lost).
pub async fn status_by_root_hash(db: &Db, root_hash: &[u8; 32]) -> Result<Option<String>> {
    let row = sqlx::query("SELECT status FROM downloads WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
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

/// Delete every finished download (completed/error/cancelled) in one statement.
/// Active and paused downloads are left untouched. Returns the number removed.
pub async fn delete_terminal(db: &Db) -> Result<u64> {
    let affected =
        sqlx::query("DELETE FROM downloads WHERE status IN ('completed', 'error', 'cancelled')")
            .execute(db)
            .await?
            .rows_affected();
    Ok(affected)
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

/// Return libp2p downloads that were interrupted and should be resumed on startup.
/// These are rows whose status is 'finding_providers', 'queued' or 'downloading'.
pub async fn list_resumable(db: &Db) -> Result<Vec<DownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, root_hash, name, total_size, dest_path, status,
                bytes_done, error_msg, added_at, updated_at, category_id
         FROM downloads
         WHERE status IN ('finding_providers', 'queued', 'downloading')
         ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows.iter().map(row_to_download).collect())
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
        let id = create_pending(&db, &hash(1), Some("movie.mkv"), 1_000, true, None)
            .await
            .unwrap()
            .id();
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
        let id = create_pending(&db, &hash(2), Some("file.bin"), 1_000, true, None)
            .await
            .unwrap()
            .id();
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
        let id = create_pending(&db, &hash(3), Some("file.bin"), 1_000, true, None)
            .await
            .unwrap()
            .id();
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
        let id = create_pending(&db, &hash(4), Some("track.flac"), 1_000, true, None)
            .await
            .unwrap()
            .id();

        set_status(&db, id, "completed", None).await.unwrap();

        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "completed");
        assert!(rows[0].error_msg.is_none());
    }

    #[tokio::test]
    async fn set_status_error_stores_message() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(5), Some("doc.pdf"), 1_000, true, None)
            .await
            .unwrap()
            .id();

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
        let id = create_pending(&db, &hash(6), Some("big.iso"), 1_000, true, None)
            .await
            .unwrap()
            .id();
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

    #[tokio::test]
    async fn reactivate_cancelled_download() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(7), Some("test.bin"), 1_000, false, None)
            .await
            .unwrap()
            .id();
        set_status(&db, id, "cancelled", None).await.unwrap();

        let result = create_pending(&db, &hash(7), Some("test.bin"), 1_000, false, None)
            .await
            .unwrap();
        assert_eq!(result, CreatePendingResult::Reactivated(id));
        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "finding_providers");
    }

    #[tokio::test]
    async fn already_completed_returns_variant() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(8), Some("done.bin"), 1_000, false, None)
            .await
            .unwrap()
            .id();
        set_status(&db, id, "completed", None).await.unwrap();

        let result = create_pending(&db, &hash(8), Some("done.bin"), 1_000, false, None)
            .await
            .unwrap();
        assert_eq!(result, CreatePendingResult::AlreadyCompleted(id));
    }

    #[tokio::test]
    async fn already_active_returns_variant() {
        let (db, _dir) = test_db().await;
        let id = create_pending(&db, &hash(9), Some("active.bin"), 1_000, true, None)
            .await
            .unwrap()
            .id();

        let result = create_pending(&db, &hash(9), Some("active.bin"), 1_000, true, None)
            .await
            .unwrap();
        assert_eq!(result, CreatePendingResult::AlreadyActive(id));
    }
}
