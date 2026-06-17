//! Queries for the `shared_files` table.

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
}

/// Insert a new shared file. Returns the new `shared_files.id`.
///
/// No per-chunk rows are stored: with bao verified streaming a chunk is served
/// as a slice verified against `root_hash`, and `chunk_count` is derived from
/// `size` / `chunk_size`. The Merkle tree lives in a regenerable outboard sidecar.
pub async fn insert(db: &Db, f: NewSharedFile<'_>) -> Result<i64> {
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
    .execute(db)
    .await?
    .last_insert_rowid();

    Ok(file_id)
}

/// Number of chunks a fully-indexed file has: `ceil(size / chunk_size)`.
fn derive_chunk_count(size: i64, chunk_size: i64) -> i64 {
    if chunk_size > 0 {
        (size as u64).div_ceil(chunk_size as u64) as i64
    } else {
        0
    }
}

/// List all shared files (no chunk join).
///
/// `chunk_count` is derived as `ceil(size / chunk_size)` — the number of chunks
/// a fully-indexed file has — rather than counted from the `chunks` table. Every
/// caller of this full-table listing (watcher reconcile, magnet-by-prefix
/// lookup, search responses) needs only the file metadata, and the
/// `COUNT(c.id) … GROUP BY` over the whole `chunks` table was the dominant cost
/// once a library reaches a few thousand files (seconds per rescan). The
/// paginated [`list_page`] still returns the exact stored count for the UI.
pub async fn list(db: &Db) -> Result<Vec<SharedFileRow>> {
    let rows = sqlx::query(
        "SELECT id, root_hash, name, size, mime_type, path, chunk_size, added_at, mtime
         FROM shared_files
         ORDER BY added_at DESC",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let size: i64 = r.get("size");
            let chunk_size: i64 = r.get("chunk_size");
            let chunk_count = if chunk_size > 0 {
                (size as u64).div_ceil(chunk_size as u64) as i64
            } else {
                0
            };
            SharedFileRow {
                id: r.get("id"),
                root_hash: r.get("root_hash"),
                name: r.get("name"),
                size,
                mime_type: r.get("mime_type"),
                path: r.get("path"),
                chunk_size,
                added_at: r.get("added_at"),
                mtime: r.get("mtime"),
                chunk_count,
            }
        })
        .collect())
}

/// One page of shared files matching an optional name substring (`q`) and/or
/// directory (`dir`, exact dir or any file under it), newest first, plus the
/// total number of matches. The filtering, ordering and slicing all happen in
/// SQL so the daemon never materialises the whole share list — this is what
/// lets the Shares UI scale to hundreds of thousands of files. `chunk_count` is
/// computed only for the rows on the page, so the join stays cheap.
pub async fn list_page(
    db: &Db,
    q: Option<&str>,
    dir: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<(Vec<SharedFileRow>, i64)> {
    // `q` is a literal substring: escape LIKE's wildcards so a stray % or _ in
    // the query doesn't match unexpectedly.
    let name_pat = q.map(|s| {
        let e = s
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        format!("%{e}%")
    });
    let (dir_eq, dir_pat) = match dir {
        Some(d) => {
            let d = d.trim_end_matches('/');
            (Some(d.to_string()), Some(format!("{d}/%")))
        }
        None => (None, None),
    };

    // Same WHERE in both queries (filter is `(?1 IS NULL OR name LIKE ?1) AND
    // (?2 IS NULL OR path = ?2 OR path LIKE ?3)`). Kept as full literals because
    // sqlx only accepts statically-safe query strings.
    const COUNT_SQL: &str = "SELECT COUNT(*) FROM shared_files \
         WHERE (?1 IS NULL OR name LIKE ?1 ESCAPE '\\') \
           AND (?2 IS NULL OR path = ?2 OR path LIKE ?3 ESCAPE '\\')";
    const PAGE_SQL: &str = "SELECT id, root_hash, name, size, mime_type, path, \
                chunk_size, added_at, mtime \
         FROM shared_files \
         WHERE (?1 IS NULL OR name LIKE ?1 ESCAPE '\\') \
           AND (?2 IS NULL OR path = ?2 OR path LIKE ?3 ESCAPE '\\') \
         ORDER BY added_at DESC \
         LIMIT ?4 OFFSET ?5";

    let total: i64 = sqlx::query_scalar(COUNT_SQL)
        .bind(name_pat.as_deref())
        .bind(dir_eq.as_deref())
        .bind(dir_pat.as_deref())
        .fetch_one(db)
        .await?;

    // Slice the page directly; chunk_count is derived from size/chunk_size.
    let rows = sqlx::query(PAGE_SQL)
        .bind(name_pat.as_deref())
        .bind(dir_eq.as_deref())
        .bind(dir_pat.as_deref())
        .bind(limit)
        .bind(offset)
        .fetch_all(db)
        .await?;

    let files = rows
        .iter()
        .map(|r| {
            let size: i64 = r.get("size");
            let chunk_size: i64 = r.get("chunk_size");
            SharedFileRow {
                id: r.get("id"),
                root_hash: r.get("root_hash"),
                name: r.get("name"),
                size,
                mime_type: r.get("mime_type"),
                path: r.get("path"),
                chunk_size,
                added_at: r.get("added_at"),
                mtime: r.get("mtime"),
                chunk_count: derive_chunk_count(size, chunk_size),
            }
        })
        .collect();

    Ok((files, total))
}

/// List just the root hashes of every shared file.
///
/// Lightweight counterpart to [`list`] for the periodic Kademlia re-announce,
/// which only needs the hashes: no `chunks` join, no metadata columns. Matters
/// on very large libraries where the re-provide runs over every file.
pub async fn list_root_hashes(db: &Db) -> Result<Vec<Vec<u8>>> {
    let rows = sqlx::query("SELECT root_hash FROM shared_files")
        .fetch_all(db)
        .await?;
    Ok(rows.iter().map(|r| r.get("root_hash")).collect())
}

/// Fetch a single shared file by its root hash. Returns `None` if not found.
pub async fn get_by_hash(db: &Db, root_hash: &[u8; 32]) -> Result<Option<SharedFileRow>> {
    let row = sqlx::query(
        "SELECT id, root_hash, name, size, mime_type, path, chunk_size, added_at, mtime
         FROM shared_files
         WHERE root_hash = ?1",
    )
    .bind(root_hash.as_slice())
    .fetch_optional(db)
    .await?;

    Ok(row.map(|r| {
        let size: i64 = r.get("size");
        let chunk_size: i64 = r.get("chunk_size");
        SharedFileRow {
            id: r.get("id"),
            root_hash: r.get("root_hash"),
            name: r.get("name"),
            size,
            mime_type: r.get("mime_type"),
            path: r.get("path"),
            chunk_size,
            added_at: r.get("added_at"),
            mtime: r.get("mtime"),
            chunk_count: derive_chunk_count(size, chunk_size),
        }
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

/// Delete the single share at exactly `path` (no prefix/`LIKE`), returning its
/// root hash if a row was removed. Used when re-indexing one file: it matches by
/// equality, so it uses the `path` index (O(log n)) instead of the table scan
/// `delete_by_path_prefix` incurs — the difference between O(n) and O(n²) when
/// the watcher rescans a large share file by file.
pub async fn delete_by_path(db: &Db, path: &str) -> Result<Option<Vec<u8>>> {
    let row = sqlx::query("SELECT root_hash FROM shared_files WHERE path = ?1")
        .bind(path)
        .fetch_optional(db)
        .await?;

    let hash: Option<Vec<u8>> = row.map(|r| r.get("root_hash"));

    if hash.is_some() {
        sqlx::query("DELETE FROM shared_files WHERE path = ?1")
            .bind(path)
            .execute(db)
            .await?;
    }

    Ok(hash)
}

/// Look up a shared file by its exact on-disk path (uses the `path` index).
pub async fn get_by_path(db: &Db, path: &str) -> Result<Option<SharedFileRow>> {
    let row = sqlx::query(
        "SELECT id, root_hash, name, size, mime_type, path, chunk_size, added_at, mtime
         FROM shared_files
         WHERE path = ?1",
    )
    .bind(path)
    .fetch_optional(db)
    .await?;

    Ok(row.map(|r| {
        let size: i64 = r.get("size");
        let chunk_size: i64 = r.get("chunk_size");
        SharedFileRow {
            id: r.get("id"),
            root_hash: r.get("root_hash"),
            name: r.get("name"),
            size,
            mime_type: r.get("mime_type"),
            path: r.get("path"),
            chunk_size,
            added_at: r.get("added_at"),
            mtime: r.get("mtime"),
            chunk_count: derive_chunk_count(size, chunk_size),
        }
    }))
}

/// Repoint a share to a new path/name without re-hashing — used for a pure
/// rename, where the content (and therefore the root hash and chunks) is
/// unchanged. Returns `true` if a row was updated.
pub async fn rename_path(db: &Db, old_path: &str, new_path: &str, new_name: &str) -> Result<bool> {
    let affected = sqlx::query("UPDATE shared_files SET path = ?2, name = ?3 WHERE path = ?1")
        .bind(old_path)
        .bind(new_path)
        .bind(new_name)
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

    #[tokio::test]
    async fn insert_and_list() {
        let (db, _dir) = test_db().await;
        let hash = dummy_hash(1);

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
        // chunk_count is derived as ceil(size / chunk_size), not counted from
        // the chunks table — 12288 / 4096 = 3.
        assert_eq!(rows[0].chunk_count, 3);
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
    async fn rename_path_repoints_keeping_hash() {
        let (db, _dir) = test_db().await;
        let hash = dummy_hash(7);
        insert(
            &db,
            NewSharedFile {
                root_hash: &hash,
                name: "old.bin",
                size: 8192,
                mime_type: None,
                path: "/dl/old.bin",
                chunk_size: 4096,
                added_at: 1,
                mtime: 55,
            },
        )
        .await
        .unwrap();

        // Repoint to the new path/name; the hash, size, mtime and chunks stay.
        assert!(
            rename_path(&db, "/dl/old.bin", "/dl/new.bin", "new.bin")
                .await
                .unwrap()
        );
        assert!(get_by_path(&db, "/dl/old.bin").await.unwrap().is_none());
        let moved = get_by_path(&db, "/dl/new.bin").await.unwrap().unwrap();
        assert_eq!(moved.name, "new.bin");
        assert_eq!(moved.root_hash, hash);
        assert_eq!(moved.size, 8192);
        assert_eq!(moved.mtime, 55);
        assert_eq!(moved.chunk_count, 2);

        // Renaming an unknown path updates nothing.
        assert!(
            !rename_path(&db, "/dl/nope.bin", "/dl/x.bin", "x.bin")
                .await
                .unwrap()
        );
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
    async fn list_page_filters_and_paginates() {
        let (db, _dir) = test_db().await;
        for (i, (seed, name, path)) in [
            (1u8, "alpha.mkv", "/movies/alpha.mkv"),
            (2u8, "beta.mkv", "/movies/beta.mkv"),
            (3u8, "gamma.txt", "/docs/gamma.txt"),
        ]
        .into_iter()
        .enumerate()
        {
            insert(
                &db,
                NewSharedFile {
                    root_hash: &dummy_hash(seed),
                    name,
                    size: 10,
                    mime_type: None,
                    path,
                    chunk_size: 4096,
                    added_at: 1_000_000 + i as u64,
                    mtime: 0,
                },
            )
            .await
            .unwrap();
        }

        // No filter: 3 total, first page of 2 newest-first.
        let (page, total) = list_page(&db, None, None, 2, 0).await.unwrap();
        assert_eq!(total, 3);
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].name, "gamma.txt"); // most recent added_at

        // Offset reaches the last row.
        let (page2, _) = list_page(&db, None, None, 2, 2).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].name, "alpha.mkv");

        // Name substring filter (case-insensitive).
        let (m, total_m) = list_page(&db, Some("MKV"), None, 50, 0).await.unwrap();
        assert_eq!(total_m, 2);
        assert_eq!(m.len(), 2);

        // Directory filter.
        let (d, total_d) = list_page(&db, None, Some("/docs"), 50, 0).await.unwrap();
        assert_eq!(total_d, 1);
        assert_eq!(d[0].name, "gamma.txt");
    }

    #[tokio::test]
    async fn delete_by_path_exact_only() {
        let (db, _dir) = test_db().await;

        // Exact-match delete must remove only the named file and return its
        // hash; a sibling under the same directory stays put (no prefix match).
        for (seed, path) in [(1u8, "/music/a.mp3"), (2u8, "/music/b.mp3")] {
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
                },
            )
            .await
            .unwrap();
        }

        let removed = delete_by_path(&db, "/music/a.mp3").await.unwrap();
        assert_eq!(removed.as_deref(), Some(dummy_hash(1).as_slice()));

        let remaining = list(&db).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].path, "/music/b.mp3");

        // Deleting a path that isn't shared is a no-op returning None.
        assert!(
            delete_by_path(&db, "/music/missing.mp3")
                .await
                .unwrap()
                .is_none()
        );
    }
}
