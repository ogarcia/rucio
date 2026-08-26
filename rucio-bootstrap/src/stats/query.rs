//! Read queries backing the stats panel and its JSON API.
//!
//! Everything here is a pure aggregate over the `samples`/`host_info` tables the
//! recorder writes ([`super`]); the panel and the `/api/v1/stats/*` endpoints
//! render the same structs, so UI and API never drift.

use anyhow::Result;
use serde::Serialize;
use sqlx::FromRow;
use utoipa::ToSchema;

use super::{Db, now_unix};

/// The box this node ran on, framing the samples (a single row).
#[derive(Debug, Default, Serialize, ToSchema, FromRow)]
pub struct HostInfo {
    /// Unix seconds when these facts were last captured (node start).
    pub captured_at: i64,
    /// System hostname, if readable.
    pub hostname: Option<String>,
    /// Kernel release string, if readable.
    pub kernel: Option<String>,
    /// Logical CPU count.
    pub num_cpus: Option<i64>,
    /// Total system RAM in kB.
    pub mem_total_kb: Option<i64>,
}

/// Aggregate resource usage over a time window — the numbers you size hardware
/// from. Peaks drive the ceiling (RAM, cores, FDs); traffic is a running total.
#[derive(Debug, Default, Serialize, ToSchema, FromRow)]
pub struct Summary {
    /// The requested window in seconds (`0` = all recorded history).
    pub window_secs: i64,
    /// Samples in the window.
    pub samples: i64,
    /// Wall-clock span actually covered (newest − oldest sample), seconds.
    pub span_secs: Option<i64>,
    /// Highest concurrent connected peers seen.
    pub peak_peers: Option<i64>,
    /// Highest concurrent connections seen.
    pub peak_connections: Option<i64>,
    /// Total connections accepted over the window.
    pub conns_opened: Option<i64>,
    /// Total connections dropped over the window.
    pub conns_closed: Option<i64>,
    /// Peak process resident memory, kB.
    pub peak_rss_kb: Option<i64>,
    /// Mean process CPU use, as a percentage of one core.
    pub avg_cpu_pct: Option<f64>,
    /// Peak process CPU use, as a percentage of one core.
    pub peak_cpu_pct: Option<f64>,
    /// Total bytes received by the machine over the window.
    pub net_rx_bytes: Option<i64>,
    /// Total bytes transmitted by the machine over the window.
    pub net_tx_bytes: Option<i64>,
    /// Peak 1-minute system load average.
    pub peak_load1: Option<f64>,
    /// Peak open file-descriptor count.
    pub peak_open_fds: Option<i64>,
    /// Peak thread count.
    pub peak_threads: Option<i64>,
}

/// The single host-facts row, if recorded yet.
pub async fn host_info(db: &Db) -> Result<Option<HostInfo>> {
    let row = sqlx::query_as::<_, HostInfo>(
        "SELECT captured_at, hostname, kernel, num_cpus, mem_total_kb
         FROM host_info WHERE id = 1",
    )
    .fetch_optional(db)
    .await?;
    Ok(row)
}

/// Aggregate the samples over the last `window_secs` (`<= 0` = all history).
///
/// CPU percent per sample is `cpu_ms / (interval_secs * 10)` — i.e.
/// `cpu_ms/1000 / interval_secs * 100`, the fraction of one core expressed as a
/// percentage; SQLite's `AVG`/`MAX` skip the `NULL`s from non-Linux hosts.
pub async fn summary(db: &Db, window_secs: i64) -> Result<Summary> {
    let cutoff = if window_secs > 0 {
        now_unix() - window_secs
    } else {
        0
    };
    let s = sqlx::query_as::<_, Summary>(
        "SELECT
            ?1 AS window_secs,
            COUNT(*)                 AS samples,
            MAX(ts) - MIN(ts)        AS span_secs,
            MAX(connected_peers)     AS peak_peers,
            MAX(connections)         AS peak_connections,
            SUM(conns_opened)        AS conns_opened,
            SUM(conns_closed)        AS conns_closed,
            MAX(peak_rss_kb)         AS peak_rss_kb,
            AVG(CASE WHEN interval_secs > 0 AND cpu_ms IS NOT NULL
                     THEN cpu_ms / (interval_secs * 10.0) END) AS avg_cpu_pct,
            MAX(CASE WHEN interval_secs > 0 AND cpu_ms IS NOT NULL
                     THEN cpu_ms / (interval_secs * 10.0) END) AS peak_cpu_pct,
            SUM(net_rx_bytes)        AS net_rx_bytes,
            SUM(net_tx_bytes)        AS net_tx_bytes,
            MAX(load1)               AS peak_load1,
            MAX(open_fds)            AS peak_open_fds,
            MAX(threads)             AS peak_threads
         FROM samples
         WHERE ts >= ?2",
    )
    .bind(window_secs)
    .bind(cutoff)
    .fetch_one(db)
    .await?;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn seeded_db() -> Db {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for stmt in super::super::SCHEMA
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        // Two samples: peaks and sums are what we assert on.
        let now = now_unix();
        for (ts, peers, conns, opened, cpu_ms, interval, rss, rx, tx) in [
            (
                now - 120,
                4i64,
                6i64,
                4i64,
                3_000i64,
                60.0f64,
                90_000i64,
                1_000i64,
                2_000i64,
            ),
            (now - 60, 9, 15, 5, 6_000, 60.0, 120_000, 5_000, 8_000),
        ] {
            sqlx::query(
                "INSERT INTO samples (ts, interval_secs, connected_peers, connections,
                    conns_opened, conns_closed, cpu_ms, rss_kb, peak_rss_kb,
                    net_rx_bytes, net_tx_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?7, ?8, ?9)",
            )
            .bind(ts)
            .bind(interval)
            .bind(peers)
            .bind(conns)
            .bind(opened)
            .bind(cpu_ms)
            .bind(rss)
            .bind(rx)
            .bind(tx)
            .execute(&pool)
            .await
            .unwrap();
        }
        pool
    }

    #[tokio::test]
    async fn summary_reports_peaks_sums_and_cpu_percent() {
        let db = seeded_db().await;
        let s = summary(&db, 3600).await.unwrap();
        assert_eq!(s.samples, 2);
        assert_eq!(s.peak_peers, Some(9));
        assert_eq!(s.peak_connections, Some(15));
        assert_eq!(s.conns_opened, Some(9)); // 4 + 5
        assert_eq!(s.peak_rss_kb, Some(120_000));
        assert_eq!(s.net_rx_bytes, Some(6_000)); // 1000 + 5000
        assert_eq!(s.net_tx_bytes, Some(10_000)); // 2000 + 8000
        // 6000 ms over a 60 s interval = 10% of one core.
        assert_eq!(s.peak_cpu_pct, Some(10.0));
        assert_eq!(s.avg_cpu_pct, Some(7.5)); // (5% + 10%) / 2
    }

    #[tokio::test]
    async fn empty_window_is_all_nulls_not_an_error() {
        let db = seeded_db().await;
        // A 1-second window excludes both samples.
        let s = summary(&db, 1).await.unwrap();
        assert_eq!(s.samples, 0);
        assert_eq!(s.peak_peers, None);
        assert_eq!(s.net_rx_bytes, None);
    }
}
