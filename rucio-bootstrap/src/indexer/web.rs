//! A minimal search front-end for the DHT indexer.
//!
//! Server-rendered, no JavaScript: `GET /` is a landing page with live index
//! counts and a search box, `GET /search?q=…` renders results as cards. It
//! reuses the same [`super::db::search`] the JSON API uses, so the web UI and
//! the API never drift apart. File names come from the untrusted network, so
//! everything interpolated into HTML is escaped (via [`crate::http::esc`]). The
//! page shell, palette, theme switch and helpers are shared with the other web
//! roles in [`crate::http`].

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    http::HeaderMap,
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

/// `GET /` — the landing page: header, a headline with live index counts and a
/// search box.
pub async fn landing(State(s): State<AppState>, headers: HeaderMap) -> Html<String> {
    let theme = http::theme_from_cookies(&headers);
    let st = db::stats(&s.db).await.unwrap_or_default();

    // The landing header carries only the brand and nav — the search box lives
    // in the hero below. The "Node status" link appears only here, on the home
    // page (the results and dashboard pages do not repeat it).
    let nav = if s.stats_panel {
        r#"<a class="navlink" href="/stats">Node status</a><a class="navlink" href="/api/docs">API</a>"#
    } else {
        r#"<a class="navlink" href="/api/docs">API</a>"#
    };
    let header = format!(
        r#"<header class="bar landing"><div class="bar-in">{brand}<span class="spacer"></span><span class="hdr-nav">{nav}</span><span class="tsw-wrap">{tsw}</span></div></header>"#,
        brand = http::brand("Rucio"),
        tsw = http::theme_switch(theme, "/"),
    );

    let (h1, lead) = if st.distinct_hashes > 0 {
        (
            format!(
                "{} files announced by {} nodes",
                http::group(st.distinct_hashes),
                http::group(st.distinct_providers)
            ),
            "This node watches the DHT and remembers what flows through it — search by file name or content hash.",
        )
    } else {
        (
            "Search the Rucio network".to_string(),
            "This node watches the DHT and remembers what flows through it — search by file name or content hash.",
        )
    };

    let facts = if st.distinct_hashes > 0 {
        format!(
            r#"<div class="facts3">
  <div><div class="n">{named}</div><div class="k">files named so far</div></div>
  <div><div class="n">{last}</div><div class="k">since the last announcement</div></div>
  <div><div class="n">{hist}</div><div class="k">of history in the index</div></div>
</div>"#,
            named = http::group(st.enriched_files),
            last = st.newest.map(seen_ago).unwrap_or_else(|| "—".into()),
            hist = match (st.oldest, st.newest) {
                (Some(o), Some(n)) if n >= o => fmt_dur(n - o),
                _ => "—".into(),
            },
        )
    } else {
        String::new()
    };

    // On mobile the header nav is hidden; the node-status link appears at the
    // foot of the hero instead.
    let hero_status = if s.stats_panel {
        r#"<a class="navlink hero-status" href="/stats">Node status →</a>"#
    } else {
        ""
    };
    let body = format!(
        r#"<div class="stick">{header}</div><div class="hero">
  <h1>{h1}</h1>
  <p class="lead">{lead}</p>
  {search}
  {facts}
  {hero_status}
</div>"#,
        search = search_form_big(),
    );
    http::html_page("Rucio — search", &body, theme)
}

/// `GET /search?q=…` — results page with a compact header search box.
pub async fn search_page(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<WebQuery>,
) -> Html<String> {
    let theme = http::theme_from_cookies(&headers);
    let q = p.q.unwrap_or_default();
    let q_trim = q.trim();
    let offset = p.offset.unwrap_or(0).max(0);
    let sort = db::Sort::parse(p.sort.as_deref().unwrap_or(""));

    let records = db::search(&s.db, q_trim, sort, PAGE, offset)
        .await
        .unwrap_or_default();

    let next = format!(
        "/search?q={}&sort={}&offset={offset}",
        urlencoding::encode(q_trim),
        sort.as_param()
    );
    // Results header: brand + search + the sort pills (in-header on mobile, in
    // the sub-bar on desktop) + theme switch (desktop only).
    let header = format!(
        r#"<header class="bar results"><div class="bar-in">{brand}{search}<div class="sorts hdr-sorts">{pills}</div><span class="spacer"></span><span class="tsw-wrap">{tsw}</span></div></header>"#,
        brand = http::brand("Rucio"),
        search = search_box(q_trim),
        pills = sort_pills(q_trim, sort),
        tsw = http::theme_switch(theme, &next),
    );

    // The header and sub-bar stay pinned at the top (`.stick`); only the results
    // scroll under them.
    let (subbar, results) = if records.is_empty() {
        let msg = if q_trim.is_empty() {
            "The index is empty — no records announced yet."
        } else {
            "No results."
        };
        (String::new(), format!(r#"<p class="empty">{msg}</p>"#))
    } else {
        let first = offset + 1;
        let last = offset + records.len() as i64;
        let more = if records.len() as i64 == PAGE {
            ""
        } else {
            " (end)"
        };
        let subbar = format!(
            r#"<div class="subbar"><span class="count">Results {first}–{last}{more}</span><div class="sorts subbar-sorts"><span class="lbl">· sort by</span>{pills}</div></div>"#,
            pills = sort_pills(q_trim, sort),
        );
        let mut r = String::new();
        for rec in &records {
            r.push_str(&result_row(rec));
        }
        r.push_str(&pager(q_trim, sort, offset, records.len() as i64));
        (subbar, r)
    };

    let body =
        format!(r#"<div class="stick">{header}{subbar}</div><main class="main">{results}</main>"#);
    let title = if q_trim.is_empty() {
        "Rucio — search".to_string()
    } else {
        format!("{} — Rucio search", http::esc(q_trim))
    };
    http::html_page(&title, &body, theme)
}

// ── Header / search box ──────────────────────────────────────────────────────

/// The compact header search form (prefilled with the current query).
fn search_box(q: &str) -> String {
    format!(
        r#"<form class="search sm" action="/search" method="get" role="search">
  <input type="text" name="q" value="{q}" placeholder="File name or hash…" aria-label="Search" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
  <button type="submit">Search</button>
</form>"#,
        q = http::esc(q),
    )
}

/// The big landing search form: search row + no-JS sort pills (radio buttons
/// styled as pills, so the chosen order submits with the GET form).
fn search_form_big() -> String {
    const OPTS: [(&str, &str, &str); 3] = [
        ("newest", "so-new", "newest"),
        ("providers", "so-prov", "most sources"),
        ("size", "so-size", "largest"),
    ];
    let default = db::Sort::default().as_param();
    let pills: String = OPTS
        .iter()
        .map(|(val, id, label)| {
            let checked = if *val == default { " checked" } else { "" };
            format!(
                r#"<input type="radio" name="sort" id="{id}" value="{val}"{checked}><label class="pill" for="{id}">{label}</label>"#
            )
        })
        .collect();
    format!(
        r#"<form class="search-hero" action="/search" method="get" role="search">
  <div class="search">
    <input type="text" name="q" placeholder="Search files by name or hash…" autofocus aria-label="Search" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
    <button type="submit">Search</button>
  </div>
  <div class="sorts"><span class="lbl">sort by</span>{pills}</div>
</form>"#,
    )
}

// ── Rendering helpers ────────────────────────────────────────────────────────

/// Render one search result as a card: an availability column (source count +
/// coloured bar) beside the file name, meta and canonical `rucio:` magnet.
fn result_row(r: &HashRow) -> String {
    let (band, pct) = avail_band(r.providers);
    let count_label = if r.providers == 1 {
        "source"
    } else {
        "sources"
    };
    let named = r.name.as_deref().filter(|n| !n.is_empty());

    // Canonical magnet: enriched records carry name + size, bare ones are just
    // the hash. magnet_from_parts URL-encodes the name, so the string is already
    // safe inside an href; it is HTML-escaped for the visible text too.
    let magnet = match (named, r.size) {
        (Some(n), Some(sz)) if sz >= 0 => {
            rucio_core::protocol::search::SearchResult::magnet_from_parts(
                &r.hash, n, sz as u64, None,
            )
        }
        _ => format!("rucio:{}", r.hash),
    };
    let magnet_e = http::esc(&magnet);

    let (title, title_cls) = match named {
        Some(n) => (http::esc(n), "t"),
        None => (http::esc(&r.hash), "t hash"),
    };

    let mut meta = String::new();
    match r.size.filter(|&s| s > 0) {
        Some(sz) => meta.push_str(&format!("<span>{}</span>", http::human_size(sz as u64))),
        None if named.is_none() => meta.push_str("<span>unknown size</span>"),
        None => {}
    }
    meta.push_str(&format!("<span>seen {}</span>", seen_ago(r.last_seen)));
    if named.is_none() {
        meta.push_str("<span>not named yet</span>");
    } else {
        meta.push_str(&format!(
            "<span>first seen {}</span>",
            seen_ago(r.first_seen)
        ));
    }

    // Only enriched records show the magnet line (a bare hash link is enough).
    let magnet_html = if named.is_some() {
        format!(r#"<code class="magnet">{magnet_e}</code>"#)
    } else {
        String::new()
    };
    let bare = if named.is_some() { "" } else { " bare" };

    format!(
        r#"<div class="hit{bare}">
  <div class="avail {band}"><span class="c">{providers}</span><span class="k">{count_label}</span><span class="track"><i style="width:{pct}%"></i></span></div>
  <div class="hit-main"><a class="{title_cls}" href="{magnet_e}">{title}</a><div class="hit-meta">{meta}</div>{magnet_html}</div>
</div>"#,
        providers = r.providers,
    )
}

/// Availability band + bar width (%) for a provider count: a single source is
/// poor (red), a handful fair (amber), many good (green).
fn avail_band(providers: i64) -> (&'static str, u32) {
    let pct = ((providers as f64 / 14.0) * 100.0).clamp(4.0, 100.0) as u32;
    let band = if providers >= 5 {
        "high"
    } else if providers >= 2 {
        "mid"
    } else {
        "low"
    };
    (band, pct)
}

/// The sort pills for the results sub-bar: links that re-run the query in the
/// chosen order.
fn sort_pills(q: &str, current: db::Sort) -> String {
    const OPTS: [(&str, &str); 3] = [
        ("newest", "newest"),
        ("providers", "most sources"),
        ("size", "largest"),
    ];
    let cur = current.as_param();
    let qe = urlencoding::encode(q);
    OPTS.iter()
        .map(|(val, label)| {
            let cls = if *val == cur { "pill on" } else { "pill" };
            format!(r#"<a class="{cls}" href="/search?q={qe}&sort={val}">{label}</a>"#)
        })
        .collect()
}

/// Previous/next links (centred), preserving the query and sort order. Only the
/// links that apply are rendered.
fn pager(q: &str, sort: db::Sort, offset: i64, got: i64) -> String {
    let qe = urlencoding::encode(q);
    let sort = sort.as_param();
    let mut out = String::new();
    if offset > 0 {
        let o = (offset - PAGE).max(0);
        out.push_str(&format!(
            r#"<a href="/search?q={qe}&sort={sort}&offset={o}">← Previous</a>"#
        ));
    }
    if got == PAGE {
        let o = offset + PAGE;
        out.push_str(&format!(
            r#"<a href="/search?q={qe}&sort={sort}&offset={o}">Next 50 →</a>"#
        ));
    }
    if out.is_empty() {
        return String::new();
    }
    format!(r#"<div class="pager">{out}</div>"#)
}

/// Coarse "time since", without pulling in a date library.
fn seen_ago(unix_secs: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let secs = (now - unix_secs).max(0);
    let (d, h, m) = (secs / 86_400, secs / 3_600, secs / 60);
    if d >= 1 {
        format!("{d}d ago")
    } else if h >= 1 {
        format!("{h}h ago")
    } else if m >= 1 {
        format!("{m}m ago")
    } else {
        "just now".to_string()
    }
}

/// Coarse duration for the index history span.
fn fmt_dur(secs: i64) -> String {
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

    fn row(name: Option<&str>, size: Option<i64>, providers: i64) -> HashRow {
        HashRow {
            hash: "abc123".to_string(),
            name: name.map(String::from),
            size,
            providers,
            first_seen: 0,
            last_seen: 0,
        }
    }

    #[test]
    fn result_row_neutralizes_a_malicious_name() {
        // File names come from the untrusted network — must never reach the
        // browser as live markup.
        let html = result_row(&row(Some("<script>alert(1)</script>"), Some(1024), 3));
        assert!(!html.contains("<script>"), "raw script tag leaked: {html}");
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("rucio:abc123"));
    }

    #[test]
    fn result_row_falls_back_to_hash_when_unnamed() {
        let html = result_row(&row(None, None, 1));
        assert!(html.contains("abc123"));
        assert!(html.contains("rucio:abc123"));
        assert!(html.contains("hit bare")); // dashed card for a bare hash
        assert!(html.contains("not named yet"));
    }

    #[test]
    fn avail_band_bands() {
        assert_eq!(avail_band(1).0, "low");
        assert_eq!(avail_band(2).0, "mid");
        assert_eq!(avail_band(4).0, "mid");
        assert_eq!(avail_band(5).0, "high");
        assert_eq!(avail_band(50).0, "high");
    }

    #[test]
    fn result_row_shows_source_count_and_band() {
        let html = result_row(&row(Some("x.mkv"), Some(1024), 3));
        assert!(html.contains(r#"class="avail mid""#)); // 3 sources → mid
        assert!(html.contains(">3</span>"));
        assert!(html.contains("sources"));
    }
}
