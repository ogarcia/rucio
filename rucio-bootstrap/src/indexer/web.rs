//! A minimal search front-end for the DHT indexer.
//!
//! Server-rendered, no JavaScript: `GET /` is a search box (Google/DuckDuckGo
//! style) and `GET /search?q=…` renders results. It reuses the same
//! [`super::db::search`] the JSON API uses, so the web UI and the API never
//! drift apart. File names come from the untrusted network, so everything
//! interpolated into HTML is escaped (see [`esc`]).

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    response::Html,
};
use serde::Deserialize;

use super::api::AppState;
use super::db::{self, HashRow};

/// Results per results page.
const PAGE: i64 = 30;

/// Accent colour, kept in sync with the favicon and the project landing page.
const LOGO_SVG: &str = r##"<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M19.3 12.4 C16.7 9.2 15.9 8.2 16.0 7.9 A16.3 16.3 0 0 0 17.1 6.3 C17.3 6.0 17.7 5.0 17.0 4.3 C16.4 3.7 15.7 3.7 15.1 4.3 S14.3 5.0 13.8 5.4 L13.2 4.3 C13.0 3.8 12.5 3.0 11.9 2.7 S10.5 3.0 10.5 4.3 A10.0 10.0 0 0 1 10.2 6.8 C10.1 7.1 9.9 7.6 5.3 17.1 L4.0 19.7 L9.9 19.7 C10.5 18.7 10.3 18.7 11.2 16.9 L11.8 15.4 L13.0 15.9 C14.4 16.5 15.7 17.0 17.1 17.6 A2.1 2.1 0 0 0 19.4 17.0 A3.5 3.5 0 0 0 19.3 12.4 Z"/></svg>"##;

/// Shared `<style>`, mirroring the project landing page palette (light/dark).
const CSS: &str = r#"
*{box-sizing:border-box}
:root{color-scheme:light;--bg:#f8fafc;--surface:#fff;--surface-2:#f1f5f9;--border:#e2e8f0;--text:#0f172a;--text-2:#475569;--text-3:#64748b;--accent:#4f6ef7;--accent-2:#3b5bdb;--accent-fg:#fff;--shadow:0 12px 36px rgba(15,23,42,.12)}
@media(prefers-color-scheme:dark){:root{color-scheme:dark;--bg:#0f0f1a;--surface:#1a1a2e;--surface-2:#16162a;--border:#2d2d4e;--text:#e2e8f0;--text-2:#94a3b8;--text-3:#64748b;--accent:#7c93f0;--accent-2:#a5b4fc;--accent-fg:#0f0f1a;--shadow:0 12px 36px rgba(0,0,0,.4)}}
body{margin:0;font-family:system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.search{display:flex;gap:.5rem;width:100%}
.search input{flex:1;min-width:0;padding:.7rem 1rem;font-size:1rem;font-family:inherit;color:var(--text);background:var(--surface);border:1px solid var(--border);border-radius:.6rem;outline:none;transition:border-color .15s}
.search input:focus{border-color:var(--accent)}
.search button{padding:.7rem 1.25rem;font-size:.95rem;font-weight:600;font-family:inherit;color:var(--accent-fg);background:var(--accent);border:1px solid var(--accent);border-radius:.6rem;cursor:pointer;white-space:nowrap}
.search button:hover{background:var(--accent-2);border-color:var(--accent-2)}
/* Landing */
.home{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:1.5rem;text-align:center}
.home .logo{width:72px;height:72px;color:var(--accent)}
.home h1{font-size:2.4rem;letter-spacing:-.02em;margin:.5rem 0 .25rem}
.home p.tag{color:var(--text-2);margin:0 0 1.75rem}
.home .search{max-width:34rem}
/* Results */
header.bar{position:sticky;top:0;z-index:5;background:color-mix(in srgb,var(--bg) 90%,transparent);backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
header.bar .inner{max-width:48rem;margin:0 auto;display:flex;align-items:center;gap:.9rem;padding:.7rem 1.25rem}
header.bar .logo{width:30px;height:30px;color:var(--accent);flex-shrink:0}
main{max-width:48rem;margin:0 auto;padding:1.25rem}
.count{color:var(--text-3);font-size:.85rem;margin:.25rem 0 1rem}
.result{padding:.9rem 0;border-bottom:1px solid var(--border)}
.result h2{font-size:1.05rem;margin:0 0 .2rem;font-weight:600;overflow-wrap:break-word}
.result .meta{color:var(--text-3);font-size:.82rem;margin:0 0 .4rem}
.result .meta b{color:var(--text-2);font-weight:600}
.magnet{display:block;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.78rem;color:var(--text-2);background:var(--surface-2);border:1px solid var(--border);border-radius:.4rem;padding:.4rem .6rem;overflow-x:auto;white-space:nowrap}
.empty{color:var(--text-2);padding:2rem 0;text-align:center}
.pager{display:flex;justify-content:space-between;margin:1.5rem 0;gap:1rem}
.pager span{color:var(--text-3)}
footer{max-width:48rem;margin:0 auto;padding:1.5rem 1.25rem;color:var(--text-3);font-size:.8rem;text-align:center}
"#;

/// Query parameters for the web search page.
#[derive(Deserialize)]
pub struct WebQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    offset: Option<i64>,
}

/// `GET /` — the landing page: logo + search box.
pub async fn landing() -> Html<String> {
    let body = format!(
        r#"<div class="home">
  <span class="logo">{logo}</span>
  <h1>Rucio</h1>
  <p class="tag">Search the decentralized network</p>
  <form class="search" action="/search" method="get" role="search">
    <input type="text" name="q" placeholder="Search files by name or hash…" autofocus aria-label="Search">
    <button type="submit">Search</button>
  </form>
</div>
{footer}"#,
        logo = LOGO_SVG,
        footer = footer(),
    );
    Html(page("Rucio — search", &body))
}

/// `GET /search?q=…` — results page with a compact header search box.
pub async fn search_page(State(s): State<AppState>, Query(p): Query<WebQuery>) -> Html<String> {
    let q = p.q.unwrap_or_default();
    let q_trim = q.trim();
    let offset = p.offset.unwrap_or(0).max(0);

    let records = db::search(&s.db, q_trim, PAGE, offset)
        .await
        .unwrap_or_default();

    let header = format!(
        r#"<header class="bar"><div class="inner">
  <a class="logo" href="/" title="Home">{logo}</a>
  <form class="search" action="/search" method="get" role="search">
    <input type="text" name="q" value="{q}" placeholder="Search files by name or hash…" aria-label="Search">
    <button type="submit">Search</button>
  </form>
</div></header>"#,
        logo = LOGO_SVG,
        q = esc(&q),
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
        main.push_str(&pager(q_trim, offset, records.len() as i64));
    }

    let body = format!("{header}<main>{main}</main>{footer}", footer = footer());
    let title = if q_trim.is_empty() {
        "Rucio — search".to_string()
    } else {
        format!("{} — Rucio search", esc(q_trim))
    };
    Html(page(&title, &body))
}

// ── Rendering helpers ────────────────────────────────────────────────────────

/// Wrap a `<body>` fragment in the full HTML document with the shared style.
fn page(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title><meta name="robots" content="noindex"><style>{css}</style>
</head><body>{body}</body></html>"#,
        css = CSS,
    )
}

fn footer() -> String {
    r#"<footer>Rucio — decentralized P2P file sharing · <a href="https://github.com/ogarcia/rucio">github.com/ogarcia/rucio</a></footer>"#.to_string()
}

/// Render one search result. The title is the file name (or the hash when the
/// record isn't enriched yet). The magnet is the canonical `rucio:` link.
fn result_row(r: &HashRow) -> String {
    let title = match r.name.as_deref() {
        Some(n) if !n.is_empty() => esc(n),
        _ => esc(&r.hash),
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
    let magnet_e = esc(&magnet);

    let mut meta = format!("<b>{}</b> provider(s)", r.providers);
    if let Some(sz) = r.size.filter(|&s| s > 0) {
        meta = format!("{} · {}", human_size(sz as u64), meta);
    }
    meta.push_str(&format!(" · seen {}", seen_ago(r.last_seen)));

    format!(
        r#"<div class="result">
  <h2><a href="{magnet_e}">{title}</a></h2>
  <p class="meta">{meta}</p>
  <code class="magnet">{magnet_e}</code>
</div>"#,
    )
}

/// Previous/next links, preserving the query.
fn pager(q: &str, offset: i64, got: i64) -> String {
    let qe = urlencoding::encode(q);
    let prev = if offset > 0 {
        let o = (offset - PAGE).max(0);
        format!(r#"<a href="/search?q={qe}&offset={o}">← Previous</a>"#)
    } else {
        "<span></span>".to_string()
    };
    let next = if got == PAGE {
        let o = offset + PAGE;
        format!(r#"<a href="/search?q={qe}&offset={o}">Next →</a>"#)
    } else {
        "<span></span>".to_string()
    };
    format!(r#"<div class="pager">{prev}{next}</div>"#)
}

/// Escape the five HTML-significant characters. Applied to every value that
/// originates from the network (file names) or the user (the query).
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Human-readable byte size (binary units).
fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
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
    fn esc_escapes_all_html_metacharacters() {
        assert_eq!(esc("a<b>&\"'"), "a&lt;b&gt;&amp;&quot;&#39;");
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
    fn human_size_picks_a_unit() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1536), "1.5 KB");
    }
}
