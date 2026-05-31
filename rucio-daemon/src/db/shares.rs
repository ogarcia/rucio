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
    /// File modification time (Unix seconds) at index time. The rescan compares
    /// it (with `size`) against disk to detect files changed while offline.
    pub mtime: i64,
    pub chunk_count: i64,
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
    /// File modification time (Unix seconds); change signal for the rescan.
    pub mtime: i64,
    /// (idx, hash, size)
    pub chunks: &'a [(u32, [u8; 32], u32)],
}

/// Insert a new shared file and its chunks in a single transaction.
/// Returns the new `shared_files.id`.
pub async fn insert(db: &Db, f: NewSharedFile<'_>) -> Result<i64> {
    let mut tx = db.begin().await?;

    let file_id: i64 = sqlx::query(
        "INSERT INTO shared_files (root_hash, name, size, mime_type, path, chunk_size, added_at, mtime)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind(f.root_hash.as_slice())
    .bind(f.name)
    .bind(f.size as i64)
    .bind(f.mime_type)
    .bind(f.path)
    .bind(f.chunk_size as i64)
    .bind(f.added_at as i64)
    .bind(f.mtime)
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

/// List all shared files, including their chunk count.
pub async fn list(db: &Db) -> Result<Vec<SharedFileRow>> {
    let rows = sqlx::query(
        "SELECT sf.id, sf.root_hash, sf.name, sf.size, sf.mime_type, sf.path,
                sf.chunk_size, sf.added_at, sf.mtime,
                COUNT(c.id) AS chunk_count
         FROM shared_files sf
         LEFT JOIN chunks c ON c.shared_file_id = sf.id
         GROUP BY sf.id
         ORDER BY sf.added_at DESC",
    )
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
            mtime: r.get("mtime"),
            chunk_count: r.get("chunk_count"),
        })
        .collect())
}

/// Fetch a single shared file by its root hash. Returns `None` if not found.
pub async fn get_by_hash(db: &Db, root_hash: &[u8; 32]) -> Result<Option<SharedFileRow>> {
    let row = sqlx::query(
        "SELECT sf.id, sf.root_hash, sf.name, sf.size, sf.mime_type, sf.path,
                sf.chunk_size, sf.added_at, sf.mtime,
                COUNT(c.id) AS chunk_count
         FROM shared_files sf
         LEFT JOIN chunks c ON c.shared_file_id = sf.id
         WHERE sf.root_hash = ?1
         GROUP BY sf.id",
    )
    .bind(root_hash.as_slice())
    .fetch_optional(db)
    .await?;

    Ok(row.map(|r| SharedFileRow {
        id: r.get("id"),
        root_hash: r.get("root_hash"),
        name: r.get("name"),
        size: r.get("size"),
        mime_type: r.get("mime_type"),
        path: r.get("path"),
        chunk_size: r.get("chunk_size"),
        added_at: r.get("added_at"),
        mtime: r.get("mtime"),
        chunk_count: r.get("chunk_count"),
    }))
}

/// Delete a shared file (and its chunks via CASCADE) by root hash.
/// Returns `true` if a row was deleted.
pub async fn delete_by_hash(db: &Db, root_hash: &[u8; 32]) -> Result<bool> {
    let affected = sqlx::query("DELETE FROM shared_files WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// Delete all shared files whose `path` starts with `prefix`.
///
/// Use this to remove a whole directory tree: pass the directory path.
/// Returns the root hashes of the deleted files.
pub async fn delete_by_path_prefix(db: &Db, prefix: &str) -> Result<Vec<Vec<u8>>> {
    // Ensure the prefix ends with the path separator so we don't accidentally
    // match "/home/user/movies-extra" when asked to remove "/home/user/movies".
    let pattern = if prefix.ends_with(std::path::MAIN_SEPARATOR) {
        format!("{prefix}%")
    } else {
        format!("{prefix}{}%", std::path::MAIN_SEPARATOR)
    };

    // Fetch hashes first, then delete.
    let rows = sqlx::query("SELECT root_hash FROM shared_files WHERE path = ?1 OR path LIKE ?2")
        .bind(prefix)
        .bind(&pattern)
        .fetch_all(db)
        .await?;

    let hashes: Vec<Vec<u8>> = rows.iter().map(|r| r.get("root_hash")).collect();

    sqlx::query("DELETE FROM shared_files WHERE path = ?1 OR path LIKE ?2")
        .bind(prefix)
        .bind(&pattern)
        .execute(db)
        .await?;

    Ok(hashes)
}

/// Count the shared files under `prefix` (a directory) and sum their sizes.
/// Uses the same prefix semantics as [`delete_by_path_prefix`], so the count
/// matches exactly what removing that directory would unshare.
pub async fn count_and_size_by_prefix(db: &Db, prefix: &str) -> Result<(i64, i64)> {
    let pattern = if prefix.ends_with(std::path::MAIN_SEPARATOR) {
        format!("{prefix}%")
    } else {
        format!("{prefix}{}%", std::path::MAIN_SEPARATOR)
    };

    let row = sqlx::query(
        "SELECT COUNT(*) AS cnt, COALESCE(SUM(size), 0) AS total \
         FROM shared_files WHERE path = ?1 OR path LIKE ?2",
    )
    .bind(prefix)
    .bind(&pattern)
    .fetch_one(db)
    .await?;

    Ok((row.get::<i64, _>("cnt"), row.get::<i64, _>("total")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Open a temporary-file SQLite DB with the full schema applied.
    ///
    /// Returns `(pool, TempDir)` — the caller must keep `TempDir` alive for
    /// the duration of the test or the underlying file will be deleted.
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

    fn dummy_hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn dummy_chunks(n: u32) -> Vec<(u32, [u8; 32], u32)> {
        (0..n)
            .map(|i| (i, dummy_hash(i as u8 + 10), 4096))
            .collect()
    }

    #[tokio::test]
    async fn insert_and_list() {
        let (db, _dir) = test_db().await;
        let hash = dummy_hash(1);
        let chunks = dummy_chunks(3);

        let id = insert(
            &db,
            NewSharedFile {
                root_hash: &hash,
                name: "hello.txt",
                size: 12288,
                mime_type: Some("text/plain"),
                path: "/tmp/hello.txt",
                chunk_size: 4096,
                added_at: 1_000_000,
                mtime: 0,
                chunks: &chunks,
            },
        )
        .await
        .unwrap();

        assert!(id > 0);

        let rows = list(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "hello.txt");
        assert_eq!(rows[0].size, 12288);
        assert_eq!(rows[0].root_hash, hash);
    }

    #[tokio::test]
    async fn delete_by_hash_existing() {
        let (db, _dir) = test_db().await;
        let hash = dummy_hash(2);
        insert(
            &db,
            NewSharedFile {
                root_hash: &hash,
                name: "file.bin",
                size: 100,
                mime_type: None,
                path: "/tmp/file.bin",
                chunk_size: 4096,
                added_at: 1_000_000,
                mtime: 0,
                chunks: &[],
            },
        )
        .await
        .unwrap();

        assert_eq!(list(&db).await.unwrap().len(), 1);
        let deleted = delete_by_hash(&db, &hash).await.unwrap();
        assert!(deleted);
        assert_eq!(list(&db).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn delete_by_hash_missing() {
        let (db, _dir) = test_db().await;
        let deleted = delete_by_hash(&db, &dummy_hash(99)).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn delete_by_path_prefix_directory() {
        let (db, _dir) = test_db().await;

        // Insert three files: two inside /music, one outside.
        for (seed, path) in [
            (1u8, "/music/a.mp3"),
            (2u8, "/music/sub/b.mp3"),
            (3u8, "/videos/c.mp4"),
        ] {
            insert(
                &db,
                NewSharedFile {
                    root_hash: &dummy_hash(seed),
                    name: "f",
                    size: 10,
                    mime_type: None,
                    path,
                    chunk_size: 4096,
                    added_at: 1_000_000,
                    mtime: 0,
                    chunks: &[],
                },
            )
            .await
            .unwrap();
        }

        let removed = delete_by_path_prefix(&db, "/music").await.unwrap();
        assert_eq!(removed.len(), 2);

        let remaining = list(&db).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].path, "/videos/c.mp4");
    }

    #[tokio::test]
    async fn delete_by_path_prefix_no_partial_match() {
        let (db, _dir) = test_db().await;

        // "/music-extra" must NOT be removed when prefix is "/music".
        for (seed, path) in [(1u8, "/music/a.mp3"), (2u8, "/music-extra/b.mp3")] {
            insert(
                &db,
                NewSharedFile {
                    root_hash: &dummy_hash(seed),
                    name: "f",
                    size: 10,
                    mime_type: None,
                    path,
                    chunk_size: 4096,
                    added_at: 1_000_000,
                    mtime: 0,
                    chunks: &[],
                },
            )
            .await
            .unwrap();
        }

        let removed = delete_by_path_prefix(&db, "/music").await.unwrap();
        assert_eq!(removed.len(), 1);

        let remaining = list(&db).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].path, "/music-extra/b.mp3");
    }

    #[tokio::test]
    async fn chunks_cascade_on_file_delete() {
        let (db, _dir) = test_db().await;
        let hash = dummy_hash(5);
        let chunks = dummy_chunks(4);

        insert(
            &db,
            NewSharedFile {
                root_hash: &hash,
                name: "big.bin",
                size: 16384,
                mime_type: None,
                path: "/tmp/big.bin",
                chunk_size: 4096,
                added_at: 1_000_000,
                mtime: 0,
                chunks: &chunks,
            },
        )
        .await
        .unwrap();

        delete_by_hash(&db, &hash).await.unwrap();

        // Chunks should have been deleted by CASCADE.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
