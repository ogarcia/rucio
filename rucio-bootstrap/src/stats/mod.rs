//! Usage statistics store for `rucio-bootstrap`.
//!
//! Records a periodic snapshot of the node's resource usage into SQLite so an
//! operator (or a would-be infrastructure sponsor) can see exactly what a
//! bootstrap node consumes and size the hardware accordingly: peak concurrent
//! peers/connections (RAM), CPU time, process memory, open file descriptors,
//! machine traffic (the metric a VPS bills on) and system load.
//!
//! Compiled in only with the `stats` feature (it pulls `sqlx`); when built that
//! way it runs by default and is turned off with `--no-stats`. A plain bootstrap
//! binary built without the feature carries none of this.
//!
//! All the resource figures are read from Linux `/proc`. On a non-Linux host
//! those reads simply return `None` and the corresponding columns are stored as
//! `NULL` — the node still runs and still records peer/connection counts. This
//! is deliberate: a bootstrap node realistically runs on Linux, and pulling in a
//! cross-platform system-metrics crate (and its dependency tree) to cover hosts
//! that will almost never run one is not worth it. Pre-stable policy mirrors the
//! rest of the project: the schema is applied with `CREATE TABLE IF NOT EXISTS`
//! on startup, no migrations.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use tracing::{info, warn};

// Web role: the panel + JSON API, mounted onto the shared HTTP server. Pulled in
// only with the `stats-web` feature (the extra `axum`/`utoipa` deps).
#[cfg(feature = "stats-web")]
mod api;
#[cfg(feature = "stats-web")]
mod query;
#[cfg(feature = "stats-web")]
mod web;

/// Kernel clock ticks per second (`sysconf(_SC_CLK_TCK)`). This is 100 on every
/// mainstream Linux target; `/proc` CPU times are expressed in these ticks.
const USER_HZ: u64 = 100;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS host_info (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    captured_at  INTEGER NOT NULL,
    hostname     TEXT,
    kernel       TEXT,
    num_cpus     INTEGER,
    mem_total_kb INTEGER
);

CREATE TABLE IF NOT EXISTS samples (
    ts               INTEGER NOT NULL,
    interval_secs    REAL    NOT NULL,
    connected_peers  INTEGER NOT NULL,
    connections      INTEGER NOT NULL,
    conns_opened     INTEGER NOT NULL,
    conns_closed     INTEGER NOT NULL,
    cpu_ms           INTEGER,
    rss_kb           INTEGER,
    peak_rss_kb      INTEGER,
    threads          INTEGER,
    open_fds         INTEGER,
    net_rx_bytes     INTEGER,
    net_tx_bytes     INTEGER,
    load1            REAL,
    load5            REAL,
    load15           REAL,
    mem_available_kb INTEGER
);
CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples (ts);
";

pub type Db = SqlitePool;

/// Runtime options for the stats role.
pub struct StatsOpts {
    pub db_path: PathBuf,
    /// Drop samples older than this many days.
    pub retention_days: i64,
}

/// A running stats recorder: owns the DB pool and the previous cumulative
/// readings needed to turn `/proc` counters into per-interval deltas.
pub struct Stats {
    db: Db,
    /// Wall-clock instant of the previous sample (baseline captured at startup).
    last_instant: Instant,
    /// Previous cumulative process CPU time, in kernel ticks.
    prev_cpu_ticks: Option<u64>,
    /// Previous cumulative machine (rx, tx) byte counters.
    prev_net: Option<(u64, u64)>,
}

impl Stats {
    /// Open the DB, record host facts, start the retention sweep and capture the
    /// baseline counters so the first sample already yields valid deltas.
    pub async fn start(opts: StatsOpts) -> Result<Self> {
        let db = open(&opts.db_path).await.context("opening stats db")?;
        info!(
            db = %opts.db_path.display(),
            retention_days = opts.retention_days,
            "Stats enabled"
        );

        record_host_info(&db).await;

        // Retention sweep: prune once at startup (the interval's first tick is
        // immediate) then once a day.
        let rdb = db.clone();
        let days = opts.retention_days;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(24 * 3600));
            loop {
                tick.tick().await;
                match prune(&rdb, days).await {
                    Ok(n) if n > 0 => info!(deleted = n, "Stats retention sweep"),
                    Ok(_) => {}
                    Err(e) => warn!("Stats retention sweep failed: {e}"),
                }
            }
        });

        Ok(Self {
            db,
            last_instant: Instant::now(),
            prev_cpu_ticks: proc::cpu_ticks(),
            prev_net: proc::net_bytes(),
        })
    }

    /// Close the SQLite pool cleanly on shutdown so SQLite checkpoints and
    /// removes its `-wal`/`-shm` sidecar files.
    pub async fn close(&self) {
        self.db.close().await;
    }

    /// The stats panel + JSON API routes, ready to merge onto the shared server.
    /// The data is public (resource usage, not sensitive), so there is no token.
    #[cfg(feature = "stats-web")]
    pub fn api_router(&self) -> axum::Router {
        api::router(api::AppState {
            db: self.db.clone(),
        })
    }

    /// The stats role's OpenAPI spec, for the shared server's merged docs.
    #[cfg(feature = "stats-web")]
    pub fn api_doc() -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    /// Take one snapshot: the caller supplies the live peer/connection counts and
    /// the churn accumulated since the previous call; everything else is read
    /// from `/proc`. Counters (CPU, traffic) are stored as the delta over the
    /// elapsed interval; if a counter went backwards (a reboot reset the machine
    /// traffic counters) the delta is stored as `NULL` for that one sample.
    pub async fn sample(
        &mut self,
        connected_peers: i64,
        connections: i64,
        conns_opened: u64,
        conns_closed: u64,
    ) -> Result<()> {
        let now = Instant::now();
        let interval_secs = now.duration_since(self.last_instant).as_secs_f64();
        self.last_instant = now;

        // Counters → per-interval deltas (guard against wrap / counter reset).
        let cur_cpu = proc::cpu_ticks();
        let cpu_ms = match (self.prev_cpu_ticks, cur_cpu) {
            (Some(p), Some(c)) if c >= p => Some(((c - p) * 1000 / USER_HZ) as i64),
            _ => None,
        };
        self.prev_cpu_ticks = cur_cpu;

        let cur_net = proc::net_bytes();
        let (net_rx, net_tx) = match (self.prev_net, cur_net) {
            (Some((pr, pt)), Some((cr, ct))) if cr >= pr && ct >= pt => {
                (Some((cr - pr) as i64), Some((ct - pt) as i64))
            }
            _ => (None, None),
        };
        self.prev_net = cur_net;

        let g = proc::gauges();

        sqlx::query(
            "INSERT INTO samples (
                ts, interval_secs, connected_peers, connections,
                conns_opened, conns_closed, cpu_ms, rss_kb, peak_rss_kb,
                threads, open_fds, net_rx_bytes, net_tx_bytes,
                load1, load5, load15, mem_available_kb
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17
            )",
        )
        .bind(now_unix())
        .bind(interval_secs)
        .bind(connected_peers)
        .bind(connections)
        .bind(conns_opened as i64)
        .bind(conns_closed as i64)
        .bind(cpu_ms)
        .bind(g.rss_kb)
        .bind(g.peak_rss_kb)
        .bind(g.threads)
        .bind(g.open_fds)
        .bind(net_rx)
        .bind(net_tx)
        .bind(g.load1)
        .bind(g.load5)
        .bind(g.load15)
        .bind(g.mem_available_kb)
        .execute(&self.db)
        .await?;
        Ok(())
    }
}

/// Open (or create) the stats database and apply the schema.
async fn open(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating stats db directory {}", parent.display()))?;
    }
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .context("parsing sqlite URL")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePool::connect_with(opts)
        .await
        .with_context(|| format!("opening stats db at {}", path.display()))?;
    for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt)
            .execute(&pool)
            .await
            .context("applying stats schema")?;
    }
    Ok(pool)
}

pub(crate) fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Record (or refresh) the single-row host facts that frame the samples: on what
/// box this node ran (CPU count, total RAM, kernel). Best-effort — a failure
/// here must not stop the node.
async fn record_host_info(db: &Db) {
    let num_cpus = std::thread::available_parallelism()
        .ok()
        .map(|n| n.get() as i64);
    let res = sqlx::query(
        "INSERT INTO host_info (id, captured_at, hostname, kernel, num_cpus, mem_total_kb)
         VALUES (1, ?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(id) DO UPDATE SET
            captured_at = ?1, hostname = ?2, kernel = ?3, num_cpus = ?4, mem_total_kb = ?5",
    )
    .bind(now_unix())
    .bind(proc::hostname())
    .bind(proc::kernel())
    .bind(num_cpus)
    .bind(proc::mem_total_kb())
    .execute(db)
    .await;
    if let Err(e) = res {
        warn!("recording host info failed: {e}");
    }
}

/// Delete samples older than `retention_days`. Returns rows deleted.
async fn prune(db: &Db, retention_days: i64) -> Result<u64> {
    let cutoff = now_unix() - retention_days * 86_400;
    let res = sqlx::query("DELETE FROM samples WHERE ts < ?1")
        .bind(cutoff)
        .execute(db)
        .await?;
    Ok(res.rows_affected())
}

/// Linux `/proc` readers. Every function is best-effort and returns `None` when
/// the file is absent (non-Linux) or cannot be parsed, so a snapshot degrades to
/// storing `NULL` for that field rather than failing.
mod proc {
    use std::fs;

    /// Instantaneous gauges read from `/proc` in one snapshot.
    #[derive(Default)]
    pub struct Gauges {
        pub rss_kb: Option<i64>,
        pub peak_rss_kb: Option<i64>,
        pub threads: Option<i64>,
        pub open_fds: Option<i64>,
        pub load1: Option<f64>,
        pub load5: Option<f64>,
        pub load15: Option<f64>,
        pub mem_available_kb: Option<i64>,
    }

    pub fn gauges() -> Gauges {
        let (rss_kb, peak_rss_kb, threads) = status();
        let (load1, load5, load15) = loadavg();
        Gauges {
            rss_kb,
            peak_rss_kb,
            threads,
            open_fds: open_fds(),
            load1,
            load5,
            load15,
            mem_available_kb: meminfo_kb("MemAvailable"),
        }
    }

    /// Cumulative process CPU time (utime + stime), in kernel ticks, from
    /// `/proc/self/stat`.
    pub fn cpu_ticks() -> Option<u64> {
        let raw = fs::read_to_string("/proc/self/stat").ok()?;
        // The `comm` field (2nd) is wrapped in parentheses and may itself contain
        // spaces or ')'; skip past the last ')' so field positions are stable.
        let rest = &raw[raw.rfind(')')? + 1..];
        let f: Vec<&str> = rest.split_whitespace().collect();
        // After the ')', tokens start at field 3 (state); utime is field 14 →
        // index 11, stime is field 15 → index 12.
        let utime: u64 = f.get(11)?.parse().ok()?;
        let stime: u64 = f.get(12)?.parse().ok()?;
        Some(utime + stime)
    }

    /// Cumulative machine (rx, tx) bytes summed over every non-loopback
    /// interface, from `/proc/net/dev`.
    pub fn net_bytes() -> Option<(u64, u64)> {
        let raw = fs::read_to_string("/proc/net/dev").ok()?;
        let mut rx = 0u64;
        let mut tx = 0u64;
        // First two lines are headers.
        for line in raw.lines().skip(2) {
            let Some((iface, rest)) = line.split_once(':') else {
                continue;
            };
            if iface.trim() == "lo" {
                continue;
            }
            let cols: Vec<&str> = rest.split_whitespace().collect();
            // Receive bytes = col 0, Transmit bytes = col 8.
            if let (Some(r), Some(t)) = (cols.first(), cols.get(8))
                && let (Ok(r), Ok(t)) = (r.parse::<u64>(), t.parse::<u64>())
            {
                rx += r;
                tx += t;
            }
        }
        Some((rx, tx))
    }

    /// `(VmRSS, VmHWM, Threads)` from `/proc/self/status`.
    fn status() -> (Option<i64>, Option<i64>, Option<i64>) {
        let Ok(raw) = fs::read_to_string("/proc/self/status") else {
            return (None, None, None);
        };
        let mut rss = None;
        let mut hwm = None;
        let mut threads = None;
        for line in raw.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                rss = v.split_whitespace().next().and_then(|n| n.parse().ok());
            } else if let Some(v) = line.strip_prefix("VmHWM:") {
                hwm = v.split_whitespace().next().and_then(|n| n.parse().ok());
            } else if let Some(v) = line.strip_prefix("Threads:") {
                threads = v.split_whitespace().next().and_then(|n| n.parse().ok());
            }
        }
        (rss, hwm, threads)
    }

    /// Number of open file descriptors = entries in `/proc/self/fd`.
    fn open_fds() -> Option<i64> {
        Some(fs::read_dir("/proc/self/fd").ok()?.count() as i64)
    }

    /// The 1/5/15-minute load averages from `/proc/loadavg`.
    fn loadavg() -> (Option<f64>, Option<f64>, Option<f64>) {
        let Ok(raw) = fs::read_to_string("/proc/loadavg") else {
            return (None, None, None);
        };
        let mut it = raw.split_whitespace();
        let a = it.next().and_then(|s| s.parse().ok());
        let b = it.next().and_then(|s| s.parse().ok());
        let c = it.next().and_then(|s| s.parse().ok());
        (a, b, c)
    }

    /// Total system RAM in kB from `/proc/meminfo`.
    pub fn mem_total_kb() -> Option<i64> {
        meminfo_kb("MemTotal")
    }

    /// A `kB` value from `/proc/meminfo` by key (e.g. `MemTotal`, `MemAvailable`).
    fn meminfo_kb(key: &str) -> Option<i64> {
        let raw = fs::read_to_string("/proc/meminfo").ok()?;
        for line in raw.lines() {
            if let Some(v) = line.strip_prefix(key)
                && v.starts_with(':')
            {
                return v[1..]
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok());
            }
        }
        None
    }

    pub fn hostname() -> Option<String> {
        fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .map(|s| s.trim().to_string())
    }

    pub fn kernel() -> Option<String> {
        fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .map(|s| s.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn mem_db() -> Db {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        pool
    }

    fn recorder(db: Db) -> Stats {
        Stats {
            db,
            last_instant: Instant::now(),
            prev_cpu_ticks: proc::cpu_ticks(),
            prev_net: proc::net_bytes(),
        }
    }

    #[tokio::test]
    async fn sample_inserts_a_row_with_the_supplied_counts() {
        let db = mem_db().await;
        let mut st = recorder(db.clone());
        st.sample(7, 12, 3, 1).await.unwrap();

        let (peers, conns, opened, closed): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT connected_peers, connections, conns_opened, conns_closed FROM samples",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!((peers, conns, opened, closed), (7, 12, 3, 1));
    }

    #[tokio::test]
    async fn retention_drops_old_samples_only() {
        let db = mem_db().await;
        let old = now_unix() - 40 * 86_400;
        let fresh = now_unix() - 86_400;
        for ts in [old, fresh] {
            sqlx::query(
                "INSERT INTO samples (ts, interval_secs, connected_peers, connections, conns_opened, conns_closed)
                 VALUES (?1, 60.0, 0, 0, 0, 0)",
            )
            .bind(ts)
            .execute(&db)
            .await
            .unwrap();
        }
        let deleted = prune(&db, 30).await.unwrap();
        assert_eq!(deleted, 1);
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM samples")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test]
    async fn host_info_is_a_single_upserted_row() {
        let db = mem_db().await;
        record_host_info(&db).await;
        record_host_info(&db).await;
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM host_info")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(rows, 1);
    }

    /// On Linux the process gauges must resolve; this pins the `/proc` parsing.
    #[cfg(target_os = "linux")]
    #[test]
    fn proc_reads_resolve_on_linux() {
        let g = proc::gauges();
        assert!(g.rss_kb.unwrap_or(0) > 0, "RSS should be readable");
        assert!(g.open_fds.unwrap_or(0) > 0, "open fds should be readable");
        assert!(proc::cpu_ticks().is_some(), "cpu ticks should be readable");
        assert!(proc::net_bytes().is_some(), "net bytes should be readable");
        assert!(proc::mem_total_kb().unwrap_or(0) > 0, "MemTotal readable");
    }
}
