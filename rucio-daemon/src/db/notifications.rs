//! Queries for the `notifications` table — the in-app notification centre.
//!
//! Records are generic (kind + title + body + optional resource reference) and
//! bounded: each insert trims the table back to [`MAX_NOTIFICATIONS`] so a
//! long-running daemon never grows it without limit. The same shape is intended
//! to later feed outbound webhooks.

use anyhow::Result;
use rucio_core::api::notifications::{NotificationDto, NotificationKind};
use sqlx::Row;

use super::Db;

/// How many notifications to keep. The newest survive; older rows are pruned on
/// every insert. Generous enough to be a useful history, small enough to never
/// matter for storage.
const MAX_NOTIFICATIONS: i64 = 200;

/// Insert a notification and prune the table back to [`MAX_NOTIFICATIONS`].
/// Returns the new row id.
pub async fn insert(
    db: &Db,
    kind: NotificationKind,
    title: &str,
    body: &str,
    ref_key: Option<&str>,
    now: i64,
) -> Result<i64> {
    let id = sqlx::query(
        "INSERT INTO notifications (kind, title, body, ref_key, created_at, read) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0)",
    )
    .bind(kind.as_str())
    .bind(title)
    .bind(body)
    .bind(ref_key)
    .bind(now)
    .execute(db)
    .await?
    .last_insert_rowid();

    // Prune everything older than the newest MAX_NOTIFICATIONS rows.
    sqlx::query(
        "DELETE FROM notifications WHERE id NOT IN \
         (SELECT id FROM notifications ORDER BY id DESC LIMIT ?1)",
    )
    .bind(MAX_NOTIFICATIONS)
    .execute(db)
    .await?;

    Ok(id)
}

/// Build a `NotificationDto` from a query row selecting all columns.
fn row_to_dto(r: &sqlx::sqlite::SqliteRow) -> NotificationDto {
    let kind: String = r.get("kind");
    let ref_key: Option<String> = r.get("ref_key");
    let read: i64 = r.get("read");
    NotificationDto {
        id: r.get("id"),
        kind: NotificationKind::from_db(&kind),
        title: r.get("title"),
        body: r.get("body"),
        ref_key,
        created_at: r.get("created_at"),
        read: read != 0,
    }
}

/// List the most recent notifications, newest first, capped at `limit`.
pub async fn list(db: &Db, limit: i64) -> Result<Vec<NotificationDto>> {
    let rows = sqlx::query(
        "SELECT id, kind, title, body, ref_key, created_at, read \
         FROM notifications ORDER BY id DESC LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(db)
    .await?;
    Ok(rows.iter().map(row_to_dto).collect())
}

/// Number of unread notifications (for the bell badge).
pub async fn unread_count(db: &Db) -> Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) AS n FROM notifications WHERE read = 0")
        .fetch_one(db)
        .await?;
    Ok(row.get("n"))
}

/// Mark every notification as read. Returns the number of rows changed.
pub async fn mark_all_read(db: &Db) -> Result<u64> {
    let res = sqlx::query("UPDATE notifications SET read = 1 WHERE read = 0")
        .execute(db)
        .await?;
    Ok(res.rows_affected())
}

/// Delete a single notification by id. Returns `true` if a row was removed.
pub async fn delete(db: &Db, id: i64) -> Result<bool> {
    let res = sqlx::query("DELETE FROM notifications WHERE id = ?1")
        .bind(id)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Delete every notification.
pub async fn clear(db: &Db) -> Result<()> {
    sqlx::query("DELETE FROM notifications").execute(db).await?;
    Ok(())
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

    #[tokio::test]
    async fn insert_list_unread_and_read() {
        let (db, _dir) = test_db().await;
        insert(
            &db,
            NotificationKind::Download,
            "Done",
            "movie.mkv",
            Some("abcd"),
            100,
        )
        .await
        .unwrap();
        insert(
            &db,
            NotificationKind::System,
            "Indexed",
            "12 files",
            None,
            200,
        )
        .await
        .unwrap();

        let items = list(&db, 50).await.unwrap();
        assert_eq!(items.len(), 2);
        // Newest first.
        assert_eq!(items[0].title, "Indexed");
        assert_eq!(items[0].kind, NotificationKind::System);
        assert_eq!(items[1].ref_key.as_deref(), Some("abcd"));
        assert_eq!(unread_count(&db).await.unwrap(), 2);

        mark_all_read(&db).await.unwrap();
        assert_eq!(unread_count(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn delete_and_clear() {
        let (db, _dir) = test_db().await;
        let id = insert(&db, NotificationKind::System, "x", "y", None, 1)
            .await
            .unwrap();
        assert!(delete(&db, id).await.unwrap());
        assert!(!delete(&db, id).await.unwrap());

        insert(&db, NotificationKind::System, "a", "b", None, 2)
            .await
            .unwrap();
        clear(&db).await.unwrap();
        assert!(list(&db, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn insert_prunes_to_cap() {
        let (db, _dir) = test_db().await;
        for i in 0..(MAX_NOTIFICATIONS + 25) {
            insert(&db, NotificationKind::System, "n", "b", None, i)
                .await
                .unwrap();
        }
        let items = list(&db, MAX_NOTIFICATIONS + 100).await.unwrap();
        assert_eq!(items.len() as i64, MAX_NOTIFICATIONS);
        // The newest (highest created_at) survived, the oldest were pruned.
        assert_eq!(items[0].created_at, MAX_NOTIFICATIONS + 24);
    }
}
