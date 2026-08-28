//! A minimal search front-end for the DHT indexer.
//!
//! Server-rendered, no JavaScript: `GET /` is a search box (Google/DuckDuckGo
//! style) and `GET /search?q=…` renders results. It reuses the same
//! [`super::db::search`] the JSON API uses, so the web UI and the API never
//! drift apart. File names come from the untrusted network, so everything
//! interpolated into HTML is escaped (via [`crate::http::esc`]). The page shell,
//! palette and generic helpers are shared with the other web roles in
//! [`crate::http`].

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    response::Html,
};
use serde::Deserialize;

use crate::http;

use super::api::AppState;
use super::db::{self, HashRow};

/// Results per results page (matches the JSON API's default page size).
const PAGE: i64 = 50;

/// Query parameters for the web search page.
#[derive(Deserialize)]
pub struct WebQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    offset: Option<i64>,
}

/// `GET /` — the landing page: logo + search box.
pub async fn landing(State(s): State<AppState>) -> Html<String> {
    let status_link = if s.stats_panel {
        r#"<p class="note"><a href="/stats">Node status →</a></p>"#
    } else {
        ""
    };
    let body = format!(
        r#"<div class="home">
  <span class="logo">{logo}</span>
  <h1>Rucio</h1>
  <p class="tag">Search the decentralized network</p>
  <form class="search" action="/search" method="get" role="search">
    <input type="text" name="q" placeholder="Search files by name or hash…" autofocus aria-label="Search" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
    <select name="sort" aria-label="Sort order">{sort_opts}</select>
    <button type="submit">Search</button>
  </form>
  {status_link}
</div>
{footer}"#,
        logo = http::LOGO_SVG,
        sort_opts = sort_options(db::Sort::default()),
        footer = http::footer(),
    );
    http::html_page("Rucio — search", &body)
}

/// `GET /search?q=…` — results page with a compact header search box.
pub async fn search_page(State(s): State<AppState>, Query(p): Query<WebQuery>) -> Html<String> {
    let q = p.q.unwrap_or_default();
    let q_trim = q.trim();
    let offset = p.offset.unwrap_or(0).max(0);
    let sort = db::Sort::parse(p.sort.as_deref().unwrap_or(""));

    let records = db::search(&s.db, q_trim, sort, PAGE, offset)
        .await
        .unwrap_or_default();

    let header = format!(
        r#"<header class="bar"><div class="inner">
  <a class="logo" href="/" title="Home">{logo}</a>
  <form class="search" action="/search" method="get" role="search">
    <input type="text" name="q" value="{q}" placeholder="Search files by name or hash…" aria-label="Search" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
    <select name="sort" aria-label="Sort order">{sort_opts}</select>
    <button type="submit">Search</button>
  </form>
</div></header>"#,
        logo = http::LOGO_SVG,
        q = http::esc(&q),
        sort_opts = sort_options(sort),
    );

    let mut main = String::new();
    if records.is_empty() {
        main.push_str(if q_trim.is_empty() {
            r#"<p class="empty">The index is empty — no records announced yet.</p>"#
        } else {
            r#"<p class="empty">No results.</p>"#
        });
    } else {
        let first = offset + 1;
        let last = offset + records.len() as i64;
        main.push_str(&format!(
            r#"<p class="count">Results {first}–{last}{more}</p>"#,
            more = if records.len() as i64 == PAGE {
                ""
            } else {
                " (end)"
            },
        ));
        for r in &records {
            main.push_str(&result_row(r));
        }
        main.push_str(&pager(q_trim, sort, offset, records.len() as i64));
    }

    let body = format!(
        "{header}<main>{main}</main>{footer}",
        footer = http::footer()
    );
    let title = if q_trim.is_empty() {
        "Rucio — search".to_string()
    } else {
        format!("{} — Rucio search", http::esc(q_trim))
    };
    http::html_page(&title, &body)
}

// ── Rendering helpers ────────────────────────────────────────────────────────

/// Render one search result. The title is the file name (or the hash when the
/// record isn't enriched yet). The magnet is the canonical `rucio:` link.
fn result_row(r: &HashRow) -> String {
    let title = match r.name.as_deref() {
        Some(n) if !n.is_empty() => http::esc(n),
        _ => http::esc(&r.hash),
    };

    // Canonical magnet: enriched records carry name + size, bare ones are just
    // the hash. magnet_from_parts URL-encodes the name, so the magnet string is
    // already safe inside an href; it's HTML-escaped for the visible text too.
    let magnet = match (r.name.as_deref(), r.size) {
        (Some(n), Some(sz)) if !n.is_empty() && sz >= 0 => {
            rucio_core::protocol::search::SearchResult::magnet_from_parts(
                &r.hash, n, sz as u64, None,
            )
        }
        _ => format!("rucio:{}", r.hash),
    };
    let magnet_e = http::esc(&magnet);

    // Meta as chips. The provider chip is coloured by availability: a single
    // source is poor (red), a handful is fair (amber), many is good (green).
    let mut chips = String::new();
    if let Some(sz) = r.size.filter(|&s| s > 0) {
        chips.push_str(&format!(
            r#"<span class="chip">{}</span>"#,
            http::human_size(sz as u64)
        ));
    }
    let plabel = if r.providers == 1 {
        "1 provider".to_string()
    } else {
        format!("{} providers", r.providers)
    };
    chips.push_str(&format!(
        r#"<span class="chip {}">{plabel}</span>"#,
        provider_chip_class(r.providers)
    ));
    chips.push_str(&format!(
        r#"<span class="chip">seen {}</span>"#,
        seen_ago(r.last_seen)
    ));

    format!(
        r#"<div class="hit">
  <h2 class="hit-title"><a href="{magnet_e}">{title}</a></h2>
  <div class="hit-meta">{chips}</div>
  <code class="magnet">{magnet_e}</code>
</div>"#,
    )
}

/// Colour band for the provider-count chip: availability at a glance.
fn provider_chip_class(providers: i64) -> &'static str {
    if providers >= 5 {
        "chip-high"
    } else if providers >= 2 {
        "chip-mid"
    } else {
        "chip-low"
    }
}

/// The sort `<option>`s, with the active one marked `selected`.
fn sort_options(current: db::Sort) -> String {
    // (value, label) — value must match db::Sort::parse / as_param. `oldest`
    // exists in the API but isn't offered in the web UI (rarely what a human
    // browsing for files wants).
    const OPTS: [(&str, &str); 3] = [
        ("newest", "Newest"),
        ("providers", "Most sources"),
        ("size", "Largest"),
    ];
    let cur = current.as_param();
    OPTS.iter()
        .map(|(val, label)| {
            let sel = if *val == cur { " selected" } else { "" };
            format!(r#"<option value="{val}"{sel}>{label}</option>"#)
        })
        .collect()
}

/// Previous/next links, preserving the query and sort order.
fn pager(q: &str, sort: db::Sort, offset: i64, got: i64) -> String {
    let qe = urlencoding::encode(q);
    let sort = sort.as_param();
    let prev = if offset > 0 {
        let o = (offset - PAGE).max(0);
        format!(r#"<a href="/search?q={qe}&sort={sort}&offset={o}">← Previous</a>"#)
    } else {
        "<span></span>".to_string()
    };
    let next = if got == PAGE {
        let o = offset + PAGE;
        format!(r#"<a href="/search?q={qe}&sort={sort}&offset={o}">Next →</a>"#)
    } else {
        "<span></span>".to_string()
    };
    format!(r#"<div class="pager">{prev}{next}</div>"#)
}

/// Coarse "time since last announced", without pulling in a date library.
fn seen_ago(unix_secs: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let secs = (now - unix_secs).max(0);
    let days = secs / 86_400;
    let hours = secs / 3_600;
    let mins = secs / 60;
    if days >= 1 {
        format!("{days}d ago")
    } else if hours >= 1 {
        format!("{hours}h ago")
    } else if mins >= 1 {
        format!("{mins}m ago")
    } else {
        "just now".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: Option<&str>, size: Option<i64>) -> HashRow {
        HashRow {
            hash: "abc123".to_string(),
            name: name.map(String::from),
            size,
            providers: 3,
            first_seen: 0,
            last_seen: 0,
        }
    }

    #[test]
    fn result_row_neutralizes_a_malicious_name() {
        // File names come from the untrusted network — must never reach the
        // browser as live markup.
        let html = result_row(&row(Some("<script>alert(1)</script>"), Some(1024)));
        assert!(!html.contains("<script>"), "raw script tag leaked: {html}");
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("rucio:abc123"));
    }

    #[test]
    fn result_row_falls_back_to_hash_and_bare_magnet_when_unnamed() {
        let html = result_row(&row(None, None));
        assert!(html.contains("abc123"));
        assert!(html.contains("rucio:abc123"));
    }

    #[test]
    fn provider_chip_class_bands() {
        assert_eq!(provider_chip_class(1), "chip-low");
        assert_eq!(provider_chip_class(2), "chip-mid");
        assert_eq!(provider_chip_class(4), "chip-mid");
        assert_eq!(provider_chip_class(5), "chip-high");
        assert_eq!(provider_chip_class(50), "chip-high");
    }

    #[test]
    fn result_row_emits_colored_provider_chip() {
        let html = result_row(&row(Some("x.mkv"), Some(1024)));
        assert!(html.contains(r#"class="chip chip-mid""#)); // 3 providers → mid
        assert!(html.contains("3 providers"));
    }
}
