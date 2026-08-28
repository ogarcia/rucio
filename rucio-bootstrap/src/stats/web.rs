//! The stats dashboard: a server-rendered, no-JS panel at `GET /stats`.
//!
//! It renders the same aggregates the JSON API serves ([`super::query`]) as a
//! host card, a window selector and a grid of the numbers you size a bootstrap
//! server from — plus a rough suggested-instance line for a sponsor pitch. The
//! page shell, palette and helpers are shared with the other roles via
//! [`crate::http`].

use axum::{
    extract::{Query, State},
    response::Html,
};
use serde::Deserialize;

use crate::http;

use super::api::AppState;
use super::query::{self, HostInfo, Summary};

/// Selectable aggregation windows: (label, seconds; `0` = all history).
const WINDOWS: [(&str, i64); 5] = [
    ("1h", 3_600),
    ("24h", 86_400),
    ("7d", 604_800),
    ("30d", 2_592_000),
    ("All", 0),
];

#[derive(Deserialize)]
pub struct PanelQuery {
    #[serde(default)]
    w: Option<i64>,
}

/// `GET /stats` — the node dashboard for the selected window: the search index
/// it serves (when the indexer role is on) and the resources it consumes.
pub async fn panel(State(s): State<AppState>, Query(p): Query<PanelQuery>) -> Html<String> {
    let window = p.w.unwrap_or(86_400).max(0);
    let host = query::host_info(&s.db).await.ok().flatten();

    // The search-index card, rendered only when the indexer is enabled — read
    // from the shared pool, which carries the index tables on a `web` build.
    let index = if s.index_enabled {
        index_card(crate::indexer::index_stats(&s.db).await.ok().as_ref())
    } else {
        String::new()
    };
    let has_index = !index.is_empty();

    let body = match query::summary(&s.db, window).await {
        Ok(sum) => format!(
            "{head}{host}{index}{tabs}{grid}{sizing}{footer}",
            head = head(has_index),
            host = host_card(host.as_ref()),
            tabs = tabs(window),
            grid = stat_grid(&sum, host.as_ref()),
            sizing = sizing_card(&sum, host.as_ref()),
            footer = http::footer(),
        ),
        Err(_) => format!(
            r#"{head}{index}<div class="card"><p class="empty">Resource statistics are unavailable.</p></div>{footer}"#,
            head = head(has_index),
            footer = http::footer(),
        ),
    };
    http::html_page(
        "Rucio bootstrap — status",
        &format!(r#"<div class="wrap">{body}</div>"#),
    )
}

// ── Sections ────────────────────────────────────────────────────────────────

fn head(has_index: bool) -> String {
    let (sub, search_link) = if has_index {
        (
            "The search index this node serves, and what it consumes to run",
            r#"<p class="note"><a href="/">← Search the index</a></p>"#,
        )
    } else {
        (
            "What this node consumes — to size the hardware it needs",
            "",
        )
    };
    format!(
        r#"<div class="head">
  <span class="logo">{logo}</span>
  <div>
    <h1>Bootstrap node</h1>
    <p class="sub">{sub}</p>
    {search_link}
  </div>
</div>"#,
        logo = http::LOGO_SVG,
    )
}

/// The search-index card: aggregate counters from the indexer, shown when this
/// node also indexes the DHT. Frames the resource numbers below with what the
/// node is actually storing and serving.
fn index_card(stats: Option<&crate::indexer::Stats>) -> String {
    let Some(st) = stats else {
        return String::new();
    };
    let pct = if st.distinct_hashes > 0 {
        format!(
            "{:.0}%",
            st.enriched_files as f64 / st.distinct_hashes as f64 * 100.0
        )
    } else {
        dash()
    };
    let span = match (st.oldest, st.newest) {
        (Some(o), Some(n)) if n >= o => fmt_duration(n - o),
        _ => dash(),
    };
    format!(
        r#"<div class="card">
  <h2>Search index</h2>
  <div class="facts">
    <div><div class="k">Files indexed</div>{hashes}</div>
    <div><div class="k">Enriched</div>{enriched} ({pct})</div>
    <div><div class="k">Providers</div>{providers}</div>
    <div><div class="k">Provider records</div>{records}</div>
    <div><div class="k">History</div>{span}</div>
  </div>
</div>"#,
        hashes = st.distinct_hashes,
        enriched = st.enriched_files,
        providers = st.distinct_providers,
        records = st.total_records,
    )
}

fn host_card(host: Option<&HostInfo>) -> String {
    let Some(h) = host else {
        return String::new();
    };
    let cpus = h.num_cpus.map(|n| n.to_string()).unwrap_or_else(dash);
    let ram = h
        .mem_total_kb
        .map(|kb| http::human_size(kb as u64 * 1024))
        .unwrap_or_else(dash);
    let hostname = h.hostname.as_deref().map(http::esc).unwrap_or_else(dash);
    let kernel = h.kernel.as_deref().map(http::esc).unwrap_or_else(dash);
    format!(
        r#"<div class="card">
  <h2>Host</h2>
  <div class="facts">
    <div><div class="k">Hostname</div>{hostname}</div>
    <div><div class="k">CPUs</div>{cpus}</div>
    <div><div class="k">RAM</div>{ram}</div>
    <div><div class="k">Kernel</div>{kernel}</div>
  </div>
</div>"#,
    )
}

fn tabs(active: i64) -> String {
    let mut out = String::from(r#"<div class="tabs">"#);
    for (label, secs) in WINDOWS {
        let cls = if secs == active { "tab active" } else { "tab" };
        out.push_str(&format!(
            r#"<a class="{cls}" href="/stats?w={secs}">{label}</a>"#
        ));
    }
    out.push_str("</div>");
    out
}

fn stat_grid(s: &Summary, host: Option<&HostInfo>) -> String {
    if s.samples == 0 {
        return r#"<div class="card"><p class="empty">No samples in this window yet — the node records one per minute.</p></div>"#.to_string();
    }

    let cores = host.and_then(|h| h.num_cpus);
    let total_ram = host.and_then(|h| h.mem_total_kb);
    let (rx, tx) = (s.net_rx_bytes, s.net_tx_bytes);
    let total_traffic = match (rx, tx) {
        (Some(r), Some(t)) => Some(r + t),
        _ => None,
    };
    let span_days = s.span_secs.map(|d| (d as f64 / 86_400.0).max(1.0 / 24.0));
    let per_day = match (total_traffic, span_days) {
        (Some(tot), Some(days)) if days > 0.0 => Some(tot as f64 / days),
        _ => None,
    };

    let mut tiles = String::new();

    tiles.push_str(&tile(
        "Peak peers",
        &opt_i(s.peak_peers),
        &format!("{} connections peak", opt_i(s.peak_connections)),
    ));

    // RSS peak, with what fraction of the box's RAM that is.
    let rss_sub = match (s.peak_rss_kb, total_ram) {
        (Some(rss), Some(tot)) if tot > 0 => {
            format!(
                "{:.0}% of {}",
                rss as f64 / tot as f64 * 100.0,
                http::human_size(tot as u64 * 1024)
            )
        }
        _ => "process resident".to_string(),
    };
    tiles.push_str(&tile("Peak memory", &opt_kb(s.peak_rss_kb), &rss_sub));

    // CPU peak as % of one core, plus the core-equivalent and the average.
    let cpu_v = s
        .peak_cpu_pct
        .map(|p| format!(r#"{p:.0}<small> %/core</small>"#))
        .unwrap_or_else(dash);
    let cpu_sub = match (s.peak_cpu_pct, s.avg_cpu_pct) {
        (Some(peak), Some(avg)) => {
            let cores_txt = cores.map(|c| format!(" of {c}")).unwrap_or_default();
            format!("{:.2} cores{} · avg {avg:.0}%", peak / 100.0, cores_txt)
        }
        _ => "Linux only".to_string(),
    };
    tiles.push_str(&tile("Peak CPU", &cpu_v, &cpu_sub));

    // Traffic total + direction split.
    let traffic_sub = match (rx, tx) {
        (Some(r), Some(t)) => format!(
            "↓ {} · ↑ {}",
            http::human_size(r as u64),
            http::human_size(t as u64)
        ),
        _ => "Linux only".to_string(),
    };
    tiles.push_str(&tile("Traffic", &opt_bytes(total_traffic), &traffic_sub));

    // Traffic per day + a rough monthly projection (what a VPS bills on).
    let (perday_v, perday_sub) = match per_day {
        Some(pd) => (
            http::human_size(pd as u64),
            format!("≈ {}/month", http::human_size((pd * 30.0) as u64)),
        ),
        None => (dash(), "Linux only".to_string()),
    };
    tiles.push_str(&tile("Traffic / day", &perday_v, &perday_sub));

    tiles.push_str(&tile(
        "Peak load",
        &s.peak_load1.map(|l| format!("{l:.2}")).unwrap_or_else(dash),
        "1-minute average",
    ));
    tiles.push_str(&tile(
        "Peak open files",
        &opt_i(s.peak_open_fds),
        &format!("{} threads peak", opt_i(s.peak_threads)),
    ));
    tiles.push_str(&tile(
        "Connections",
        &opt_i(s.conns_opened),
        &format!(
            "{} opened · {} closed",
            opt_i(s.conns_opened),
            opt_i(s.conns_closed)
        ),
    ));

    let span = s
        .span_secs
        .map(fmt_duration)
        .unwrap_or_else(|| "—".to_string());
    tiles.push_str(&tile(
        "Samples",
        &s.samples.to_string(),
        &format!("over {span}"),
    ));

    format!(r#"<div class="grid">{tiles}</div>"#)
}

/// A rough "what instance should I ask for" line — clearly heuristic, meant as
/// a starting point for a sponsor conversation, not a guarantee.
fn sizing_card(s: &Summary, host: Option<&HostInfo>) -> String {
    if s.samples == 0 {
        return String::new();
    }
    // RAM: peak RSS with generous headroom (×2), rounded up to a whole GB.
    let ram = s.peak_rss_kb.map(|kb| {
        let gib = kb as f64 / 1024.0 / 1024.0;
        (gib * 2.0).ceil().max(1.0) as u64
    });
    // Cores: ceil of the peak core-equivalent, at least 1.
    let cores = s.peak_cpu_pct.map(|p| (p / 100.0).ceil().max(1.0) as u64);
    // Monthly traffic projection.
    let per_month = match (s.net_rx_bytes, s.net_tx_bytes, s.span_secs) {
        (Some(r), Some(t), Some(span)) if span > 0 => {
            let per_day = (r + t) as f64 / (span as f64 / 86_400.0).max(1.0 / 24.0);
            Some((per_day * 30.0) as u64)
        }
        _ => None,
    };

    let ram_txt = ram.map(|g| format!("{g} GB RAM")).unwrap_or_default();
    let cores_txt = cores.map(|c| format!("{c} vCPU")).unwrap_or_default();
    let traffic_txt = per_month
        .map(|b| format!("~{}/month traffic", http::human_size(b)))
        .unwrap_or_default();
    let parts: Vec<String> = [cores_txt, ram_txt, traffic_txt]
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect();
    if parts.is_empty() {
        return String::new();
    }
    let host_note = host
        .and_then(|h| match (h.num_cpus, h.mem_total_kb) {
            (Some(c), Some(m)) => Some(format!(
                " Measured on {c} vCPU / {}.",
                http::human_size(m as u64 * 1024)
            )),
            _ => None,
        })
        .unwrap_or_default();

    format!(
        r#"<div class="card">
  <h2>Suggested instance</h2>
  <div class="tile"><div class="v">{joined}</div></div>
  <p class="note">Rough heuristic from the peaks in this window (RAM = peak memory ×2, vCPU = peak core-equivalent, traffic projected to 30 days).{host_note}</p>
</div>"#,
        joined = parts.join(" · "),
    )
}

// ── Small formatting helpers ────────────────────────────────────────────────

fn tile(k: &str, v: &str, sub: &str) -> String {
    format!(
        r#"<div class="tile"><div class="k">{k}</div><div class="v">{v}</div><div class="sub">{sub}</div></div>"#
    )
}

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

fn opt_bytes(b: Option<i64>) -> String {
    b.map(|n| http::human_size(n as u64)).unwrap_or_else(dash)
}

/// Coarse human duration for the sample span.
fn fmt_duration(secs: i64) -> String {
    let secs = secs.max(0);
    let days = secs / 86_400;
    let hours = secs / 3_600;
    let mins = secs / 60;
    if days >= 1 {
        format!("{days}d")
    } else if hours >= 1 {
        format!("{hours}h")
    } else if mins >= 1 {
        format!("{mins}m")
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
    fn grid_renders_key_numbers() {
        let html = stat_grid(&summary(10), None);
        assert!(html.contains("Peak peers"));
        assert!(html.contains(">12<")); // peak peers value
        assert!(html.contains("Peak CPU"));
        assert!(html.contains("1.40 cores")); // 140% → 1.4 cores
    }

    #[test]
    fn empty_window_shows_a_placeholder_not_tiles() {
        let html = stat_grid(&summary(0), None);
        assert!(html.contains("No samples"));
        assert!(!html.contains(r#"class="grid""#));
    }

    #[test]
    fn sizing_suggests_cores_ram_and_traffic() {
        let html = sizing_card(&summary(10), None);
        assert!(html.contains("2 vCPU")); // ceil(140% → 1.4) = 2
        assert!(html.contains("GB RAM"));
        assert!(html.contains("month traffic"));
    }

    #[test]
    fn sizing_is_empty_without_samples() {
        assert!(sizing_card(&summary(0), None).is_empty());
    }

    #[test]
    fn tabs_mark_the_active_window() {
        let html = tabs(86_400);
        assert!(html.contains(r#"class="tab active" href="/stats?w=86400""#));
        assert!(html.contains(r#"class="tab" href="/stats?w=3600""#));
    }

    #[test]
    fn index_card_shows_counters_and_enriched_share() {
        let st = crate::indexer::Stats {
            total_records: 100,
            distinct_hashes: 40,
            distinct_providers: 12,
            enriched_files: 20,
            oldest: Some(1_000),
            newest: Some(1_000 + 3 * 86_400),
        };
        let html = index_card(Some(&st));
        assert!(html.contains("Search index"));
        assert!(html.contains(">40<")); // files indexed (distinct hashes)
        assert!(html.contains("20 (50%)")); // 20 of 40 hashes enriched
        assert!(html.contains("3d")); // oldest→newest span
    }

    #[test]
    fn index_card_is_empty_without_stats() {
        assert!(index_card(None).is_empty());
    }

    #[test]
    fn head_links_to_search_only_with_an_index() {
        assert!(head(true).contains(r#"href="/""#));
        assert!(!head(false).contains(r#"href="/""#));
    }
}
