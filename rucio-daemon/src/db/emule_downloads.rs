//! Queries for the `emule_downloads` table.
//!
//! eMule (ed2k) downloads are stored completely separately from libp2p
//! downloads so that the eMule subsystem can be removed without touching the
//! core `downloads` table.

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// Outcome of [`create`] when a row for the same hash already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateResult {
    /// A fresh row was inserted.  Contains the new ID.
    Inserted(i64),
    /// A row already existed and was active — the caller should not start a
    /// second download task.  Contains the existing ID.
    AlreadyActive(i64),
    /// A cancelled or failed row was reset to `finding_providers` and is ready
    /// to be retried.  Contains the existing ID.
    Reactivated(i64),
    /// A completed row exists — the file was already downloaded successfully.
    /// The caller should return a conflict error to the user.
    AlreadyCompleted(i64),
}

impl CreateResult {
    /// Returns the row ID regardless of the variant.
    pub fn id(&self) -> i64 {
        match *self {
            CreateResult::Inserted(id)
            | CreateResult::AlreadyActive(id)
            | CreateResult::Reactivated(id)
            | CreateResult::AlreadyCompleted(id) => id,
        }
    }
}

/// A row from the `emule_downloads` table.
#[derive(Debug, Clone)]
pub struct EmuleDownloadRow {
    pub id: i64,
    /// 16-byte MD4 hash (the canonical eMule file identifier).
    pub ed2k_hash: Vec<u8>,
    pub name: String,
    pub total_size: i64,
    /// Original `ed2k://` link stored for resume across restarts.
    pub ed2k_link: String,
    pub status: String,
    pub bytes_done: i64,
    pub dest_path: String,
    pub error_msg: Option<String>,
    pub added_at: i64,
    pub updated_at: i64,
}

/// Insert a new eMule download row, handling duplicates gracefully.
///
/// - If no row exists for `ed2k_hash`: inserts and returns [`CreateResult::Inserted`].
/// - If a row exists and is active (`finding_providers` / `downloading`): returns
///   [`CreateResult::AlreadyActive`] — the caller should not start a duplicate task.
/// - If a row exists and is terminal (`cancelled` / `error`): resets it to
///   `finding_providers` and returns [`CreateResult::Reactivated`] — the caller
///   should start a new download task against the existing row ID.
/// - If a row exists and is `completed`: returns [`CreateResult::AlreadyCompleted`]
///   — the caller should surface a conflict error to the user.
pub async fn create(
    db: &Db,
    ed2k_hash: &[u8; 16],
    name: &str,
    total_size: u64,
    ed2k_link: &str,
    now: u64,
) -> Result<CreateResult> {
    // Check for an existing row with the same hash.
    let existing = sqlx::query("SELECT id, status FROM emule_downloads WHERE ed2k_hash = ?1")
        .bind(ed2k_hash.as_slice())
        .fetch_optional(db)
        .await?;

    if let Some(row) = existing {
        let id: i64 = row.get("id");
        let status: String = row.get("status");
        return match status.as_str() {
            "completed" => Ok(CreateResult::AlreadyCompleted(id)),
            "finding_providers" | "downloading" => Ok(CreateResult::AlreadyActive(id)),
            // "cancelled" | "error" | anything else → reactivate
            _ => {
                sqlx::query(
                    "UPDATE emule_downloads \
                     SET status = 'finding_providers', bytes_done = 0, dest_path = '', \
                         error_msg = NULL, updated_at = ?1 \
                     WHERE id = ?2",
                )
                .bind(now as i64)
                .bind(id)
                .execute(db)
                .await?;
                Ok(CreateResult::Reactivated(id))
            }
        };
    }

    let id = sqlx::query(
        "INSERT INTO emule_downloads \
         (ed2k_hash, name, total_size, ed2k_link, status, bytes_done, dest_path, added_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 'finding_providers', 0, '', ?5, ?5)",
    )
    .bind(ed2k_hash.as_slice())
    .bind(name)
    .bind(total_size as i64)
    .bind(ed2k_link)
    .bind(now as i64)
    .execute(db)
    .await?
    .last_insert_rowid();

    Ok(CreateResult::Inserted(id))
}

/// List all eMule downloads ordered newest-first.
pub async fn list(db: &Db) -> Result<Vec<EmuleDownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, ed2k_hash, name, total_size, ed2k_link, status,
                bytes_done, dest_path, error_msg, added_at, updated_at
         FROM emule_downloads ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows.iter().map(row_from_sqlx).collect())
}

/// Return downloads that should be resumed on daemon startup.
///
/// Includes all non-terminal states: `finding_providers`, `downloading`,
/// `queued` (was waiting for a concurrency slot), and `stalled` (no sources
/// found, will retry).  Terminal states (`completed`, `cancelled`, `error`)
/// are intentionally excluded.
pub async fn list_resumable(db: &Db) -> Result<Vec<EmuleDownloadRow>> {
    let rows = sqlx::query(
        "SELECT id, ed2k_hash, name, total_size, ed2k_link, status,
                bytes_done, dest_path, error_msg, added_at, updated_at
         FROM emule_downloads
         WHERE status IN ('finding_providers', 'downloading', 'queued', 'stalled')
         ORDER BY added_at ASC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows.iter().map(row_from_sqlx).collect())
}

/// Return the current status string for a download, or `None` if not found.
pub async fn get_status(db: &Db, id: i64) -> Result<Option<String>> {
    let row = sqlx::query("SELECT status FROM emule_downloads WHERE id = ?1")
        .bind(id)
        .fetch_optional(db)
        .await?;
    Ok(row.map(|r| r.get("status")))
}

/// Fetch a single eMule download by ID, or `None` if it does not exist.
pub async fn get(db: &Db, id: i64) -> Result<Option<EmuleDownloadRow>> {
    let row = sqlx::query(
        "SELECT id, ed2k_hash, name, total_size, ed2k_link, status,
                bytes_done, dest_path, error_msg, added_at, updated_at
         FROM emule_downloads WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(db)
    .await?;
    Ok(row.as_ref().map(row_from_sqlx))
}

/// Update the status (and optional error message) of an eMule download.
pub async fn set_status(db: &Db, id: i64, status: &str, error_msg: Option<&str>) -> Result<()> {
    sqlx::query(
        "UPDATE emule_downloads SET status = ?1, error_msg = ?2, updated_at = ?3 WHERE id = ?4",
    )
    .bind(status)
    .bind(error_msg)
    .bind(now_secs() as i64)
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}

/// Update the status, but only while the download is still running — i.e. not
/// already in a user-controlled stop state (`paused` / `cancelled`).
///
/// The download loop writes its progress status (`finding_providers`,
/// `downloading`, `stalled`, …) once per round.  Without this guard a `pause`
/// or `cancel` issued mid-round would be silently overwritten by the next
/// progress update, so the loop's own stop check would never fire.  Making the
/// write conditional in a single atomic statement closes that race.
pub async fn set_status_if_running(db: &Db, id: i64, status: &str) -> Result<()> {
    sqlx::query(
        "UPDATE emule_downloads SET status = ?1, error_msg = NULL, updated_at = ?2 \
         WHERE id = ?3 AND status NOT IN ('paused', 'cancelled')",
    )
    .bind(status)
    .bind(now_secs() as i64)
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}

/// Update `bytes_done` and `dest_path` on completion.
pub async fn set_completed(db: &Db, id: i64, dest_path: &str) -> Result<()> {
    sqlx::query(
        "UPDATE emule_downloads \
         SET status = 'completed', dest_path = ?1, updated_at = ?2 WHERE id = ?3",
    )
    .bind(dest_path)
    .bind(now_secs() as i64)
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}

/// Accumulate downloaded bytes into `bytes_done`.
pub async fn add_bytes(db: &Db, id: i64, bytes: u64) -> Result<()> {
    sqlx::query(
        "UPDATE emule_downloads SET bytes_done = bytes_done + ?1, updated_at = ?2 WHERE id = ?3",
    )
    .bind(bytes as i64)
    .bind(now_secs() as i64)
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}

/// Set `bytes_done` to an absolute value (used for resume progress tracking).
///
/// Uses MAX so that concurrent workers with stale cumulative offsets cannot
/// overwrite a higher value already written by a worker that finished later.
pub async fn set_bytes_done(db: &Db, id: i64, bytes: u64) -> Result<()> {
    sqlx::query(
        "UPDATE emule_downloads SET bytes_done = MAX(bytes_done, ?1), updated_at = ?2 WHERE id = ?3",
    )
    .bind(bytes as i64)
    .bind(now_secs() as i64)
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}

/// Permanently delete an eMule download record.
///
/// Returns `true` if a row was deleted.
pub async fn delete(db: &Db, id: i64) -> Result<bool> {
    let affected = sqlx::query("DELETE FROM emule_downloads WHERE id = ?1")
        .bind(id)
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// Delete every finished eMule download (completed/error/cancelled) in one
/// statement. Active and paused downloads are left untouched. Returns the
/// number removed.
pub async fn delete_terminal(db: &Db) -> Result<u64> {
    let affected = sqlx::query(
        "DELETE FROM emule_downloads WHERE status IN ('completed', 'error', 'cancelled')",
    )
    .execute(db)
    .await?
    .rows_affected();
    Ok(affected)
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn row_from_sqlx(r: &sqlx::sqlite::SqliteRow) -> EmuleDownloadRow {
    EmuleDownloadRow {
        id: r.get("id"),
        ed2k_hash: r.get("ed2k_hash"),
        name: r.get("name"),
        total_size: r.get("total_size"),
        ed2k_link: r.get("ed2k_link"),
        status: r.get("status"),
        bytes_done: r.get("bytes_done"),
        dest_path: r.get("dest_path"),
        error_msg: r.get("error_msg"),
        added_at: r.get("added_at"),
        updated_at: r.get("updated_at"),
    }
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

    fn hash() -> [u8; 16] {
        [0xab; 16]
    }

    #[tokio::test]
    async fn create_and_list() {
        let (db, _dir) = test_db().await;
        let result = create(
            &db,
            &hash(),
            "movie.mkv",
            1_000_000,
            "ed2k://|file|movie.mkv|1000000|abababababababababababababababababab|/",
            1_000,
        )
        .await
        .unwrap();
        assert!(matches!(result, CreateResult::Inserted(_)));
        assert!(result.id() > 0);

        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "movie.mkv");
        assert_eq!(rows[0].status, "finding_providers");
        assert_eq!(rows[0].bytes_done, 0);
    }

    #[tokio::test]
    async fn active_duplicate_returns_already_active() {
        let (db, _dir) = test_db().await;
        let link = "ed2k://|file|movie.mkv|1000000|abababababababababababababababababab|/";
        let id1 = create(&db, &hash(), "movie.mkv", 1_000_000, link, 1_000)
            .await
            .unwrap()
            .id();
        let result2 = create(&db, &hash(), "movie.mkv", 1_000_000, link, 2_000)
            .await
            .unwrap();
        assert_eq!(result2, CreateResult::AlreadyActive(id1));
        assert_eq!(list(&db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancelled_download_is_reactivated() {
        let (db, _dir) = test_db().await;
        let link = "ed2k://|file|movie.mkv|1000000|abababababababababababababababababab|/";
        let id = create(&db, &hash(), "movie.mkv", 1_000_000, link, 1_000)
            .await
            .unwrap()
            .id();
        set_status(&db, id, "cancelled", None).await.unwrap();

        let result = create(&db, &hash(), "movie.mkv", 1_000_000, link, 2_000)
            .await
            .unwrap();
        assert_eq!(result, CreateResult::Reactivated(id));
        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "finding_providers");
        assert_eq!(rows[0].bytes_done, 0);
    }

    #[tokio::test]
    async fn error_download_is_reactivated() {
        let (db, _dir) = test_db().await;
        let link = "ed2k://|file|movie.mkv|1000000|abababababababababababababababababab|/";
        let id = create(&db, &hash(), "movie.mkv", 1_000_000, link, 1_000)
            .await
            .unwrap()
            .id();
        set_status(&db, id, "error", Some("no sources"))
            .await
            .unwrap();

        let result = create(&db, &hash(), "movie.mkv", 1_000_000, link, 2_000)
            .await
            .unwrap();
        assert_eq!(result, CreateResult::Reactivated(id));
        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "finding_providers");
        assert!(rows[0].error_msg.is_none());
    }

    #[tokio::test]
    async fn completed_download_returns_already_completed() {
        let (db, _dir) = test_db().await;
        let link = "ed2k://|file|movie.mkv|1000000|abababababababababababababababababab|/";
        let id = create(&db, &hash(), "movie.mkv", 1_000_000, link, 1_000)
            .await
            .unwrap()
            .id();
        set_completed(&db, id, "/downloads/movie.mkv")
            .await
            .unwrap();

        let result = create(&db, &hash(), "movie.mkv", 1_000_000, link, 2_000)
            .await
            .unwrap();
        assert_eq!(result, CreateResult::AlreadyCompleted(id));
    }

    #[tokio::test]
    async fn set_status_and_get() {
        let (db, _dir) = test_db().await;
        let id = create(
            &db,
            &hash(),
            "file.bin",
            512,
            "ed2k://|file|file.bin|512|abababababababababababababababababab|/",
            1_000,
        )
        .await
        .unwrap()
        .id();
        set_status(&db, id, "downloading", None).await.unwrap();
        assert_eq!(
            get_status(&db, id).await.unwrap().as_deref(),
            Some("downloading")
        );
    }

    #[tokio::test]
    async fn set_completed_stores_path() {
        let (db, _dir) = test_db().await;
        let id = create(
            &db,
            &hash(),
            "track.flac",
            512,
            "ed2k://|file|track.flac|512|abababababababababababababababababab|/",
            1_000,
        )
        .await
        .unwrap()
        .id();
        set_completed(&db, id, "/downloads/track.flac")
            .await
            .unwrap();
        let rows = list(&db).await.unwrap();
        assert_eq!(rows[0].status, "completed");
        assert_eq!(rows[0].dest_path, "/downloads/track.flac");
    }

    #[tokio::test]
    async fn list_resumable_filters_correctly() {
        let (db, _dir) = test_db().await;
        let h1 = [0x01u8; 16];
        let h2 = [0x02u8; 16];
        let h3 = [0x03u8; 16];
        create(
            &db,
            &h1,
            "a.bin",
            1,
            "ed2k://|file|a.bin|1|01010101010101010101010101010101|/",
            1,
        )
        .await
        .unwrap();
        let id2 = create(
            &db,
            &h2,
            "b.bin",
            2,
            "ed2k://|file|b.bin|2|02020202020202020202020202020202|/",
            2,
        )
        .await
        .unwrap()
        .id();
        let id3 = create(
            &db,
            &h3,
            "c.bin",
            3,
            "ed2k://|file|c.bin|3|03030303030303030303030303030303|/",
            3,
        )
        .await
        .unwrap()
        .id();
        set_status(&db, id2, "downloading", None).await.unwrap();
        set_status(&db, id3, "completed", None).await.unwrap();

        let resumable = list_resumable(&db).await.unwrap();
        // id1 (finding_providers) and id2 (downloading) should appear; id3 (completed) should not
        assert_eq!(resumable.len(), 2);
        assert!(resumable.iter().any(|r| r.id == id2));
    }

    #[tokio::test]
    async fn set_status_if_running_respects_stop_states() {
        let (db, _dir) = test_db().await;
        let id = create(
            &db,
            &hash(),
            "f.bin",
            512,
            "ed2k://|file|f.bin|512|abababababababababababababababababab|/",
            1_000,
        )
        .await
        .unwrap()
        .id();

        // While running, a progress update goes through.
        set_status_if_running(&db, id, "downloading").await.unwrap();
        assert_eq!(
            get_status(&db, id).await.unwrap().as_deref(),
            Some("downloading")
        );

        // Once paused, a progress update from the download loop is ignored.
        set_status(&db, id, "paused", None).await.unwrap();
        set_status_if_running(&db, id, "finding_providers")
            .await
            .unwrap();
        assert_eq!(
            get_status(&db, id).await.unwrap().as_deref(),
            Some("paused")
        );

        // Same guard protects the cancelled state.
        set_status(&db, id, "cancelled", None).await.unwrap();
        set_status_if_running(&db, id, "downloading").await.unwrap();
        assert_eq!(
            get_status(&db, id).await.unwrap().as_deref(),
            Some("cancelled")
        );
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let (db, _dir) = test_db().await;
        let id = create(
            &db,
            &hash(),
            "del.bin",
            1,
            "ed2k://|file|del.bin|1|abababababababababababababababababab|/",
            1,
        )
        .await
        .unwrap()
        .id();
        assert!(delete(&db, id).await.unwrap());
        assert!(list(&db).await.unwrap().is_empty());
    }
}
