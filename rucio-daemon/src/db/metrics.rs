//! Database helpers for the `metrics` singleton row.

use anyhow::Result;
use sqlx::Row;

use crate::db::Db;
use rucio_core::api::metrics::TotalMetrics;

/// Load the cumulative totals from the DB singleton row.
pub async fn load(db: &Db) -> Result<TotalMetrics> {
    let row = sqlx::query(
        "SELECT uploaded_bytes, downloaded_bytes, chunks_served, chunks_received, chunks_rejected, uptime_seconds
         FROM metrics WHERE id = 1",
    )
    .fetch_one(db)
    .await?;

    Ok(TotalMetrics {
        uploaded_bytes: row.get::<i64, _>("uploaded_bytes") as u64,
        downloaded_bytes: row.get::<i64, _>("downloaded_bytes") as u64,
        chunks_served: row.get::<i64, _>("chunks_served") as u64,
        chunks_received: row.get::<i64, _>("chunks_received") as u64,
        chunks_rejected: row.get::<i64, _>("chunks_rejected") as u64,
        uptime_seconds: row.get::<i64, _>("uptime_seconds") as u64,
    })
}

/// Add `delta` to the stored totals.
///
/// Uses a single atomic SQL statement so concurrent calls are safe.
pub async fn add(db: &Db, delta: &TotalMetrics) -> Result<()> {
    sqlx::query(
        "UPDATE metrics SET
            uploaded_bytes   = uploaded_bytes   + ?1,
            downloaded_bytes = downloaded_bytes + ?2,
            chunks_served    = chunks_served    + ?3,
            chunks_received  = chunks_received  + ?4,
            chunks_rejected  = chunks_rejected  + ?5,
            uptime_seconds   = uptime_seconds   + ?6
         WHERE id = 1",
    )
    .bind(delta.uploaded_bytes as i64)
    .bind(delta.downloaded_bytes as i64)
    .bind(delta.chunks_served as i64)
    .bind(delta.chunks_received as i64)
    .bind(delta.chunks_rejected as i64)
    .bind(delta.uptime_seconds as i64)
    .execute(db)
    .await?;
    Ok(())
}
