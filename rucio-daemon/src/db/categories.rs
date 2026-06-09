//! Queries for the `categories` table.
//!
//! A category groups downloads and may pin its own `download_dir`. A download
//! with `category_id = NULL` (or a category whose `download_dir` is NULL) uses
//! the global `storage.download_dir`. A category directory is shared and
//! protected just like the global one — see
//! [`shared_dirs::set_protected_dirs`](super::shared_dirs::set_protected_dirs).

use anyhow::Result;
use sqlx::Row;

use super::Db;

/// A download category as stored in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Category {
    pub id: i64,
    pub name: String,
    /// Absolute path, no trailing slash. `None` → use the global download_dir.
    pub download_dir: Option<String>,
    pub added_at: i64,
}

/// Normalise an optional directory: trim a trailing slash and treat blank as
/// `None` so an empty string from the API never becomes a literal path.
fn norm_dir(dir: Option<&str>) -> Option<String> {
    dir.map(|d| d.trim().trim_end_matches('/'))
        .filter(|d| !d.is_empty())
        .map(str::to_string)
}

fn row_to_category(r: &sqlx::sqlite::SqliteRow) -> Category {
    Category {
        id: r.get("id"),
        name: r.get("name"),
        download_dir: r.get::<Option<String>, _>("download_dir"),
        added_at: r.get("added_at"),
    }
}

/// Create a category. `name` must be unique. Returns the new row id.
pub async fn create(db: &Db, name: &str, download_dir: Option<&str>, added_at: u64) -> Result<i64> {
    let name = name.trim();
    let dir = norm_dir(download_dir);
    let id = sqlx::query(
        "INSERT INTO categories (name, download_dir, added_at) VALUES (?1, ?2, ?3) RETURNING id",
    )
    .bind(name)
    .bind(dir.as_deref())
    .bind(added_at as i64)
    .fetch_one(db)
    .await?
    .get::<i64, _>("id");
    Ok(id)
}

/// Update a category's name and/or directory in place.
pub async fn update(db: &Db, id: i64, name: &str, download_dir: Option<&str>) -> Result<()> {
    let name = name.trim();
    let dir = norm_dir(download_dir);
    sqlx::query("UPDATE categories SET name = ?1, download_dir = ?2 WHERE id = ?3")
        .bind(name)
        .bind(dir.as_deref())
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

/// Delete a category. Downloads assigned to it have `category_id` set back to
/// NULL by the foreign key (`ON DELETE SET NULL`), so they fall back to the
/// global download_dir. Returns `true` if a row was removed.
pub async fn delete(db: &Db, id: i64) -> Result<bool> {
    let affected = sqlx::query("DELETE FROM categories WHERE id = ?1")
        .bind(id)
        .execute(db)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// List all categories ordered by name.
pub async fn list(db: &Db) -> Result<Vec<Category>> {
    let rows =
        sqlx::query("SELECT id, name, download_dir, added_at FROM categories ORDER BY name ASC")
            .fetch_all(db)
            .await?;
    Ok(rows.iter().map(row_to_category).collect())
}

/// Fetch a single category by id.
pub async fn get(db: &Db, id: i64) -> Result<Option<Category>> {
    let row = sqlx::query("SELECT id, name, download_dir, added_at FROM categories WHERE id = ?1")
        .bind(id)
        .fetch_optional(db)
        .await?;
    Ok(row.as_ref().map(row_to_category))
}

/// Every category-pinned directory (the non-NULL `download_dir` values). These
/// join the global download_dir to form the protected/shared set.
pub async fn pinned_dirs(db: &Db) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT download_dir FROM categories WHERE download_dir IS NOT NULL AND download_dir <> ''",
    )
    .fetch_all(db)
    .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String, _>("download_dir"))
        .collect())
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

    #[tokio::test]
    async fn create_list_get() {
        let (db, _d) = test_db().await;
        let id = create(&db, "Movies", Some("/data/movies"), 1_000)
            .await
            .unwrap();
        let none_id = create(&db, "Misc", None, 1_001).await.unwrap();

        let all = list(&db).await.unwrap();
        assert_eq!(all.len(), 2);
        // Ordered by name: Misc, Movies.
        assert_eq!(all[0].name, "Misc");
        assert_eq!(all[0].download_dir, None);
        assert_eq!(all[1].name, "Movies");
        assert_eq!(all[1].download_dir.as_deref(), Some("/data/movies"));

        let got = get(&db, id).await.unwrap().unwrap();
        assert_eq!(got.name, "Movies");
        assert!(get(&db, 9999).await.unwrap().is_none());
        let _ = none_id;
    }

    #[tokio::test]
    async fn dir_is_normalised_and_blank_is_null() {
        let (db, _d) = test_db().await;
        let trail = create(&db, "Trail", Some("/data/x/"), 1_000).await.unwrap();
        let blank = create(&db, "Blank", Some("   "), 1_001).await.unwrap();
        assert_eq!(
            get(&db, trail)
                .await
                .unwrap()
                .unwrap()
                .download_dir
                .as_deref(),
            Some("/data/x")
        );
        assert_eq!(get(&db, blank).await.unwrap().unwrap().download_dir, None);
    }

    #[tokio::test]
    async fn update_changes_name_and_dir() {
        let (db, _d) = test_db().await;
        let id = create(&db, "Old", None, 1_000).await.unwrap();
        update(&db, id, "New", Some("/data/new")).await.unwrap();
        let c = get(&db, id).await.unwrap().unwrap();
        assert_eq!(c.name, "New");
        assert_eq!(c.download_dir.as_deref(), Some("/data/new"));
    }

    #[tokio::test]
    async fn delete_removes_and_returns_flag() {
        let (db, _d) = test_db().await;
        let id = create(&db, "Tmp", None, 1_000).await.unwrap();
        assert!(delete(&db, id).await.unwrap());
        assert!(!delete(&db, id).await.unwrap()); // already gone
        assert!(list(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_name_is_rejected() {
        let (db, _d) = test_db().await;
        create(&db, "Dup", None, 1_000).await.unwrap();
        assert!(create(&db, "Dup", None, 1_001).await.is_err());
    }

    #[tokio::test]
    async fn pinned_dirs_lists_only_non_null() {
        let (db, _d) = test_db().await;
        create(&db, "A", Some("/data/a"), 1_000).await.unwrap();
        create(&db, "B", None, 1_001).await.unwrap();
        create(&db, "C", Some("/data/c"), 1_002).await.unwrap();
        let mut dirs = pinned_dirs(&db).await.unwrap();
        dirs.sort();
        assert_eq!(dirs, vec!["/data/a".to_string(), "/data/c".to_string()]);
    }

    #[tokio::test]
    async fn deleting_a_category_nulls_assigned_downloads() {
        let (db, _d) = test_db().await;
        let cat = create(&db, "Movies", Some("/data/movies"), 1_000)
            .await
            .unwrap();
        // A download assigned to the category.
        sqlx::query(
            "INSERT INTO downloads (root_hash, name, total_size, dest_path, category_id, added_at, updated_at)
             VALUES (X'00', 'f', 1, '/tmp/f', ?1, 1, 1)",
        )
        .bind(cat)
        .execute(&db)
        .await
        .unwrap();

        delete(&db, cat).await.unwrap();

        // ON DELETE SET NULL must have cleared the assignment, not deleted the row.
        let cid: Option<i64> =
            sqlx::query_scalar("SELECT category_id FROM downloads WHERE name = 'f'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(cid, None);
    }
}
