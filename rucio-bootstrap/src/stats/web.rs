//! The node dashboard: a server-rendered, no-JS panel at `GET /stats`.
//!
//! It renders the same aggregates the JSON API serves ([`super::query`]) as the
//! search-index counts (when the indexer runs), three metric cards with
//! sparklines (from [`super::query::series`]), a compact facts strip and a rough
//! suggested-instance line for a sponsor pitch. The page shell, palette, theme
//! switch and helpers are shared with the other roles via [`crate::http`].

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::Html,
};
use serde::Deserialize;

use crate::http;

use super::api::AppState;
use super::query::{self, HostInfo, SeriesPoint, Summary};

/// Selectable aggregation windows: (label, seconds; `0` = all history). "All"
/// is hidden on mobile (see the `w-all` class), leaving the four fixed windows.
const WINDOWS: [(&str, i64); 5] = [
    ("1h", 3_600),
    ("24h", 86_400),
    ("7d", 604_800),
    ("30d", 2_592_000),
    ("All", 0),
];

/// Buckets requested for the sparklines.
const SPARK_POINTS: i64 = 48;

#[derive(Deserialize)]
pub struct PanelQuery {
    #[serde(default)]
    w: Option<i64>,
}

/// `GET /stats` — the node dashboard for the selected window.
pub async fn panel(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<PanelQuery>,
) -> Html<String> {
    let theme = http::theme_from_cookies(&headers);
    let window = p.w.unwrap_or(86_400).max(0);
    let host = query::host_info(&s.db).await.ok().flatten();

    // Search-index counts, read from the shared pool when the indexer runs.
    let index = if s.index_enabled {
        crate::indexer::index_stats(&s.db).await.ok()
    } else {
        None
    };

    let header = header_bar(theme, window, host.as_ref(), s.index_enabled);

    let body = match query::summary(&s.db, window).await {
        Ok(sum) => {
            let series = query::series(&s.db, window, SPARK_POINTS)
                .await
                .map(|s| s.points)
                .unwrap_or_default();
            format!(
                "{dhead}{metrics}{strip}{suggest}",
                dhead = dhead(index.as_ref(), window),
                metrics = metrics(&sum, host.as_ref(), &series),
                strip = strip(&sum),
                suggest = suggest(&sum, host.as_ref()),
            )
        }
        Err(_) => format!(
            r#"{dhead}<div class="card"><p class="empty">Resource statistics are unavailable.</p></div>"#,
            dhead = dhead(index.as_ref(), window),
        ),
    };
    http::html_page(
        "Rucio bootstrap — status",
        &format!("<div class=\"stick\">{header}</div><div class=\"wrap\">{body}</div>"),
        theme,
    )
}

// ── Header ───────────────────────────────────────────────────────────────────

fn header_bar(theme: http::Theme, window: i64, host: Option<&HostInfo>, index_on: bool) -> String {
    let search = if index_on {
        r#"<a class="navlink" href="/">Search the index →</a>"#
    } else {
        ""
    };
    // The host summary sits inline right after the brand and wraps to the next
    // line only when it does not fit.
    let hs = host_summary(host);
    let hostsum = if hs.is_empty() {
        String::new()
    } else {
        format!(r#"<span class="hostsum">{hs}</span>"#)
    };
    format!(
        r#"<header class="bar dash"><div class="bar-in">{brand}{hostsum}<span class="spacer"></span>{search}<span class="tsw-wrap">{tsw}</span></div></header>"#,
        brand = http::brand("Node status"),
        tsw = http::theme_switch(theme, &format!("/stats?w={window}")),
    )
}

/// One-line host summary for the header (`host · N vCPU · RAM · kernel`).
fn host_summary(host: Option<&HostInfo>) -> String {
    let Some(h) = host else {
        return String::new();
    };
    let mut parts: Vec<String> = Vec::new();
    if let Some(n) = h.hostname.as_deref().filter(|s| !s.is_empty()) {
        parts.push(http::esc(n));
    }
    if let Some(c) = h.num_cpus {
        parts.push(format!("{c} vCPU"));
    }
    if let Some(kb) = h.mem_total_kb {
        parts.push(http::human_size(kb as u64 * 1024));
    }
    if let Some(k) = h.kernel.as_deref().filter(|s| !s.is_empty()) {
        parts.push(http::esc(k));
    }
    parts.join(" · ")
}

// ── Dashboard head: index counts + window tabs ───────────────────────────────

fn dhead(index: Option<&crate::indexer::Stats>, window: i64) -> String {
    format!(
        r#"<div class="dhead">{idx}{tabs}</div>"#,
        idx = idxrow(index),
        tabs = tabs(window),
    )
}

/// Search-index counters, shown when the indexer runs and has records.
fn idxrow(index: Option<&crate::indexer::Stats>) -> String {
    let Some(st) = index.filter(|s| s.distinct_hashes > 0) else {
        return r#"<div></div>"#.to_string();
    };
    let named = format!(
        "{:.0}%",
        st.enriched_files as f64 / st.distinct_hashes as f64 * 100.0
    );
    format!(
        r#"<div class="idxrow">
  <div><div class="k">Files indexed</div><div class="v">{files}</div></div>
  <div><div class="k">Providers</div><div class="v">{providers}</div></div>
  <div><div class="k">Named</div><div class="v">{named}</div></div>
</div>"#,
        files = http::group(st.distinct_hashes),
        providers = http::group(st.distinct_providers),
    )
}

fn tabs(active: i64) -> String {
    let mut out = String::from(r#"<div class="tabs">"#);
    for (label, secs) in WINDOWS {
        let mut cls = if secs == active { "pill on" } else { "pill" }.to_string();
        // "All" (secs == 0) is dropped on mobile, leaving the four fixed windows.
        if secs == 0 {
            cls.push_str(" w-all");
        }
        out.push_str(&format!(
            r#"<a class="{cls}" href="/stats?w={secs}">{label}</a>"#
        ));
    }
    out.push_str("</div>");
    out
}

// ── Metric cards with sparklines ─────────────────────────────────────────────

fn metrics(s: &Summary, host: Option<&HostInfo>, series: &[SeriesPoint]) -> String {
    if s.samples == 0 {
        return r#"<div class="card"><p class="empty">No samples in this window yet — the node records one per minute.</p></div>"#.to_string();
    }
    let cores = host.and_then(|h| h.num_cpus);

    // Peers.
    let peers_spark = sparkline(&series.iter().filter_map(|p| p.peers).collect::<Vec<_>>());
    let peers = metric(
        "Peers",
        &format!(
            r#"{}<small> peak</small>"#,
            s.peak_peers.map(|n| n.to_string()).unwrap_or_else(dash)
        ),
        &format!("{} connections peak", opt_i(s.peak_connections)),
        &peers_spark,
    );

    // CPU (percent of one core → core-equivalent).
    let cpu_spark = sparkline(&series.iter().filter_map(|p| p.cpu_pct).collect::<Vec<_>>());
    let cpu_v = match s.peak_cpu_pct {
        Some(p) => {
            let of = cores.map(|c| format!(" of {c}")).unwrap_or_default();
            format!(r#"{:.2}<small> cores{of}</small>"#, p / 100.0)
        }
        None => dash(),
    };
    let cpu_sub = s
        .avg_cpu_pct
        .map(|a| format!("avg {a:.0} %/core"))
        .unwrap_or_else(|| "Linux only".to_string());
    let cpu = metric("CPU", &cpu_v, &cpu_sub, &cpu_spark);

    // Traffic.
    let traffic_spark = sparkline(
        &series
            .iter()
            .filter_map(|p| p.traffic_bytes.map(|b| b as f64))
            .collect::<Vec<_>>(),
    );
    let (rx, tx) = (s.net_rx_bytes, s.net_tx_bytes);
    let total = match (rx, tx) {
        (Some(r), Some(t)) => Some(r + t),
        _ => None,
    };
    let per_day = match (total, s.span_secs) {
        (Some(tot), Some(span)) if span > 0 => {
            Some(tot as f64 / (span as f64 / 86_400.0).max(1.0 / 24.0))
        }
        _ => None,
    };
    let traffic_sub = match (rx, tx, per_day) {
        (Some(r), Some(t), Some(pd)) => format!(
            "↓ {} · ↑ {} · {}/day",
            http::human_size(r as u64),
            http::human_size(t as u64),
            http::human_size(pd as u64)
        ),
        _ => "Linux only".to_string(),
    };
    let traffic = metric(
        "Traffic",
        &total
            .map(|b| http::human_size(b as u64))
            .unwrap_or_else(dash),
        &traffic_sub,
        &traffic_spark,
    );

    format!(r#"<div class="metrics">{peers}{cpu}{traffic}</div>"#)
}

fn metric(k: &str, v: &str, sub: &str, spark: &str) -> String {
    format!(
        r#"<div class="metric"><div class="k">{k}</div><div class="v">{v}</div>{spark}<div class="sub">{sub}</div></div>"#
    )
}

/// A polyline sparkline over the values, normalised to its own min/max. Fewer
/// than two points renders nothing (no shape to draw).
fn sparkline(vals: &[f64]) -> String {
    if vals.len() < 2 {
        return String::new();
    }
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for &v in vals {
        lo = lo.min(v);
        hi = hi.max(v);
    }
    let span = (hi - lo).max(f64::MIN_POSITIVE);
    let n = vals.len() as f64;
    let pts: Vec<String> = vals
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let x = i as f64 / (n - 1.0) * 200.0;
            let y = 36.0 - (v - lo) / span * 32.0; // 4..36, inverted (SVG y-down)
            format!("{x:.1},{y:.1}")
        })
        .collect();
    format!(
        r#"<svg class="spark" viewBox="0 0 200 40" preserveAspectRatio="none" aria-hidden="true"><polyline points="{}"/></svg>"#,
        pts.join(" ")
    )
}

// ── Facts strip + suggested instance ─────────────────────────────────────────

fn strip(s: &Summary) -> String {
    if s.samples == 0 {
        return String::new();
    }
    let span = s.span_secs.map(fmt_duration).unwrap_or_else(|| "—".into());
    let row = |k: &str, v: &str| {
        format!(r#"<div class="srow"><span class="k">{k}</span><span class="v">{v}</span></div>"#)
    };
    format!(
        r#"<div class="strip">{mem}{load}{fds}{threads}{conns}{samples}</div>"#,
        mem = row("Memory", &opt_kb(s.peak_rss_kb)),
        load = row(
            "Load",
            &s.peak_load1.map(|l| format!("{l:.2}")).unwrap_or_else(dash)
        ),
        fds = row("Open files", &opt_i(s.peak_open_fds)),
        threads = row("Threads", &opt_i(s.peak_threads)),
        conns = row(
            "Connections",
            &format!("{} / {}", opt_i(s.conns_opened), opt_i(s.conns_closed))
        ),
        samples = row("Samples", &format!("{} over {span}", s.samples)),
    )
}

/// A rough "what instance should I ask for" banner — clearly heuristic, a
/// starting point for a sponsor conversation, not a guarantee.
fn suggest(s: &Summary, host: Option<&HostInfo>) -> String {
    if s.samples == 0 {
        return String::new();
    }
    let ram = s.peak_rss_kb.map(|kb| {
        let gib = kb as f64 / 1024.0 / 1024.0;
        (gib * 2.0).ceil().max(1.0) as u64
    });
    let cores = s.peak_cpu_pct.map(|p| (p / 100.0).ceil().max(1.0) as u64);
    let per_month = match (s.net_rx_bytes, s.net_tx_bytes, s.span_secs) {
        (Some(r), Some(t), Some(span)) if span > 0 => {
            let per_day = (r + t) as f64 / (span as f64 / 86_400.0).max(1.0 / 24.0);
            Some((per_day * 30.0) as u64)
        }
        _ => None,
    };
    let parts: Vec<String> = [
        cores.map(|c| format!("{c} vCPU")),
        ram.map(|g| format!("{g} GB RAM")),
        per_month.map(|b| format!("~{}/month", http::human_size(b))),
    ]
    .into_iter()
    .flatten()
    .collect();
    if parts.is_empty() {
        return String::new();
    }
    let measured = host
        .and_then(|h| match (h.num_cpus, h.mem_total_kb) {
            (Some(c), Some(m)) => Some(format!(
                " Measured on {c} vCPU / {}.",
                http::human_size(m as u64 * 1024)
            )),
            _ => None,
        })
        .unwrap_or_default();
    format!(
        r#"<div class="suggest"><div class="suggest-head"><span class="k">Suggested instance</span><span class="v">{joined}</span><span class="sub">to sustain these peaks</span></div><p class="note">Rough heuristic from the peaks in this window (RAM = peak memory ×2, vCPU = peak core-equivalent, traffic projected to 30 days).{measured}</p></div>"#,
        joined = parts.join(" · "),
    )
}

// ── Small formatting helpers ────────────────────────────────────────────────

fn dash() -> String {
    "—".to_string()
}

fn opt_i(v: Option<i64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(dash)
}

fn opt_kb(kb: Option<i64>) -> String {
    kb.map(|k| http::human_size(k as u64 * 1024))
        .unwrap_or_else(dash)
}

/// Coarse human duration for the sample span.
fn fmt_duration(secs: i64) -> String {
    let secs = secs.max(0);
    let (d, h, m) = (secs / 86_400, secs / 3_600, secs / 60);
    if d >= 1 {
        format!("{d}d")
    } else if h >= 1 {
        format!("{h}h")
    } else if m >= 1 {
        format!("{m}m")
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(samples: i64) -> Summary {
        Summary {
            window_secs: 86_400,
            samples,
            span_secs: Some(3_600),
            peak_peers: Some(12),
            peak_connections: Some(20),
            conns_opened: Some(30),
            conns_closed: Some(25),
            peak_rss_kb: Some(120_000),
            avg_cpu_pct: Some(5.0),
            peak_cpu_pct: Some(140.0),
            net_rx_bytes: Some(1_000_000),
            net_tx_bytes: Some(2_000_000),
            peak_load1: Some(0.42),
            peak_open_fds: Some(64),
            peak_threads: Some(8),
        }
    }

    #[test]
    fn metrics_render_the_three_cards() {
        let html = metrics(&summary(10), None, &[]);
        assert!(html.contains("Peers"));
        assert!(html.contains(">12<")); // peak peers
        assert!(html.contains("CPU"));
        assert!(html.contains("1.40")); // 140% → 1.40 cores
        assert!(html.contains("Traffic"));
    }

    #[test]
    fn empty_window_shows_a_placeholder_not_cards() {
        let html = metrics(&summary(0), None, &[]);
        assert!(html.contains("No samples"));
        assert!(!html.contains(r#"class="metrics""#));
    }

    #[test]
    fn sparkline_draws_a_polyline_for_two_or_more_points() {
        assert!(sparkline(&[]).is_empty());
        assert!(sparkline(&[5.0]).is_empty());
        let svg = sparkline(&[1.0, 5.0, 3.0]);
        assert!(svg.contains("<polyline"));
        assert!(svg.contains("0.0,")); // first x at 0
        assert!(svg.contains("200.0,")); // last x at 200
    }

    #[test]
    fn suggest_names_cores_ram_and_traffic() {
        let html = suggest(&summary(10), None);
        assert!(html.contains("2 vCPU")); // ceil(140% → 1.4) = 2
        assert!(html.contains("GB RAM"));
        assert!(html.contains("/month"));
    }

    #[test]
    fn suggest_is_empty_without_samples() {
        assert!(suggest(&summary(0), None).is_empty());
    }

    #[test]
    fn tabs_mark_the_active_window() {
        let html = tabs(86_400);
        assert!(html.contains(r#"class="pill on" href="/stats?w=86400""#));
        assert!(html.contains(r#"class="pill" href="/stats?w=3600""#));
    }

    #[test]
    fn idxrow_shows_counts_and_named_share() {
        let st = crate::indexer::Stats {
            total_records: 100,
            distinct_hashes: 40,
            distinct_providers: 12,
            enriched_files: 20,
            oldest: Some(1_000),
            newest: Some(2_000),
        };
        let html = idxrow(Some(&st));
        assert!(html.contains("Files indexed"));
        assert!(html.contains(">40<")); // distinct hashes
        assert!(html.contains(">50%<")); // 20 of 40 named
    }

    #[test]
    fn idxrow_empty_without_index() {
        assert!(!idxrow(None).contains("Files indexed"));
    }
}
