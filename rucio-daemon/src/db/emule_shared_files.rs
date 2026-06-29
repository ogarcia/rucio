//! Queries for the `emule_shared_files` table.
//!
//! Every file we offer to the eMule Kad network as a source, keyed by its ed2k
//! (MD4) hash. Two populations live here: completed eMule downloads we keep
//! seeding (good-citizen policy), and Rucio-network shares the backfill task has
//! hashed so we seed them on eMule too. Decoupled from `emule_downloads` on
//! purpose: clearing the completed-downloads list must not stop sharing. A file
//! is shared until it changes or disappears on disk.

use std::collections::HashMap;

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

/// A Rucio share (from `shared_files`) that has no ed2k hash yet, i.e. a
/// candidate for the eMule backfill so we can seed it to Kad as a source too.
#[derive(Debug, Clone)]
pub struct BackfillCandidate {
    pub path: String,
    pub name: String,
    pub size: i64,
    pub mtime: i64,
}

/// List Rucio shared files that are not yet registered for eMule seeding,
/// oldest-indexed first. Cross-referenced by on-disk path: a share is a
/// candidate when no `emule_shared_files` row points at the same path.
pub async fn list_backfill_candidates(db: &Db, limit: i64) -> Result<Vec<BackfillCandidate>> {
    let rows = sqlx::query(
        "SELECT s.path, s.name, s.size, s.mtime \
         FROM shared_files s \
         LEFT JOIN emule_shared_files e ON e.path = s.path \
         WHERE e.path IS NULL \
         ORDER BY s.added_at \
         LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| BackfillCandidate {
            path: r.get("path"),
            name: r.get("name"),
            size: r.get("size"),
            mtime: r.get("mtime"),
        })
        .collect())
}

/// Repoint a seeded file to a new path/name on a pure rename, keeping its ed2k
/// hash and hashset (the content is unchanged, so no MD4 recompute is needed).
/// Returns `true` if a row was updated.
pub async fn rename_path(db: &Db, old_path: &str, new_path: &str, new_name: &str) -> Result<bool> {
    let affected =
        sqlx::query("UPDATE emule_shared_files SET path = ?2, name = ?3 WHERE path = ?1")
            .bind(old_path)
            .bind(new_path)
            .bind(new_name)
            .execute(db)
            .await?
            .rows_affected();
    Ok(affected > 0)
}

/// Remove a shared file by its ed2k hash. Returns `true` if a row was deleted.
pub async fn delete_by_hash(db: &Db, ed2k_hash: &[u8]) -> Result<bool> {
    let res = sqlx::query("DELETE FROM emule_shared_files WHERE ed2k_hash = ?1")
        .bind(ed2k_hash)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Look up a shared file by its ed2k hash — used to warn the user that content
/// they're about to download is already present (and where).
pub async fn get_by_hash(db: &Db, ed2k_hash: &[u8]) -> Result<Option<EmuleSharedFile>> {
    let row = sqlx::query(
        "SELECT ed2k_hash, name, size, path, mtime, hashset FROM emule_shared_files WHERE ed2k_hash = ?1",
    )
    .bind(ed2k_hash)
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

/// Fetch the ed2k (MD4) hash for each of `paths` that has one, as a
/// `path -> hash` map. Used to decorate a page of Rucio shares with their eMule
/// link in one query instead of one lookup per row. Paths without a seeded
/// ed2k hash are simply absent from the map.
pub async fn ed2k_hashes_for_paths(db: &Db, paths: &[String]) -> Result<HashMap<String, Vec<u8>>> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }
    // SQLite caps a statement at 999 bound parameters; a shares page is far
    // below that (a couple hundred), so a single IN-list is safe here.
    // Only `?` placeholders are interpolated (one per path); the paths
    // themselves are bound, so the assembled string carries no user data.
    let placeholders = vec!["?"; paths.len()].join(",");
    let sql =
        format!("SELECT path, ed2k_hash FROM emule_shared_files WHERE path IN ({placeholders})");
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for p in paths {
        query = query.bind(p);
    }
    let rows = query.fetch_all(db).await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<String, _>("path"), r.get::<Vec<u8>, _>("ed2k_hash")))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        crate::db::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    async fn insert_share(db: &Db, path: &str, seed: u8) {
        let hash = [seed; 32];
        crate::db::shares::insert(
            db,
            crate::db::shares::NewSharedFile {
                root_hash: &hash,
                name: "file.bin",
                size: 4096,
                mime_type: None,
                path,
                chunk_size: 4096,
                added_at: 1_000_000,
                mtime: 42,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ed2k_hashes_for_paths_maps_only_seeded() {
        let (db, _dir) = test_db().await;
        upsert(&db, &[7u8; 16], "a.bin", 10, "/dl/a.bin", 1, b"", 1)
            .await
            .unwrap();
        upsert(&db, &[8u8; 16], "b.bin", 20, "/dl/b.bin", 1, b"", 2)
            .await
            .unwrap();

        // Empty input short-circuits to an empty map.
        assert!(ed2k_hashes_for_paths(&db, &[]).await.unwrap().is_empty());

        let map = ed2k_hashes_for_paths(
            &db,
            &[
                "/dl/a.bin".to_string(),
                "/dl/missing.bin".to_string(),
                "/dl/b.bin".to_string(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("/dl/a.bin").unwrap(), &vec![7u8; 16]);
        assert_eq!(map.get("/dl/b.bin").unwrap(), &vec![8u8; 16]);
        assert!(!map.contains_key("/dl/missing.bin"));
    }

    #[tokio::test]
    async fn rename_path_repoints_keeping_ed2k_hash() {
        let (db, _dir) = test_db().await;
        upsert(
            &db,
            &[3u8; 16],
            "old.bin",
            4096,
            "/dl/old.bin",
            9,
            b"\x01\x02",
            1,
        )
        .await
        .unwrap();

        assert!(
            rename_path(&db, "/dl/old.bin", "/dl/new.bin", "new.bin")
                .await
                .unwrap()
        );
        assert!(get_by_path(&db, "/dl/old.bin").await.unwrap().is_none());
        let moved = get_by_path(&db, "/dl/new.bin").await.unwrap().unwrap();
        assert_eq!(moved.name, "new.bin");
        assert_eq!(moved.ed2k_hash, vec![3u8; 16]);
        assert_eq!(moved.hashset, b"\x01\x02");
        assert_eq!(moved.mtime, 9);

        assert!(!rename_path(&db, "/dl/nope", "/dl/x", "x").await.unwrap());
    }

    #[tokio::test]
    async fn backfill_candidates_excludes_already_seeded() {
        let (db, _dir) = test_db().await;
        insert_share(&db, "/tmp/a.bin", 1).await;
        insert_share(&db, "/tmp/b.bin", 2).await;

        // Both Rucio shares lack an ed2k hash → both are candidates.
        let cands = list_backfill_candidates(&db, 10).await.unwrap();
        assert_eq!(cands.len(), 2);
        assert_eq!(cands[0].path, "/tmp/a.bin");
        assert_eq!(cands[0].mtime, 42);

        // Once one is registered for eMule seeding, it drops out by path.
        upsert(&db, &[9u8; 16], "file.bin", 4096, "/tmp/a.bin", 42, &[], 1)
            .await
            .unwrap();
        let cands = list_backfill_candidates(&db, 10).await.unwrap();
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].path, "/tmp/b.bin");
    }
}
