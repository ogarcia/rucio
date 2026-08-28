//! Shared HTTP server for the bootstrap node's optional web roles.
//!
//! Both the DHT indexer and the stats panel expose a web UI + JSON API. Rather
//! than each binding its own port (which complicates deployment — firewall
//! rules, reverse-proxy config), this module owns a **single** listener and
//! mounts whichever roles are enabled onto it: the indexer serves `/` and
//! `/search`, the stats panel serves `/stats`, and their `/api/v1/*` routes and
//! OpenAPI specs are merged into one `/api/docs`.
//!
//! A role contributes an [`axum::Router`] (state already applied) and an
//! [`utoipa::openapi::OpenApi`]; [`Api::merge_role`] folds both in. This module
//! adds the generic `/health` probe and the Scalar docs, then serves everything.
//!
//! It also holds the presentation helpers ([`page`], [`CSS`], [`esc`],
//! [`human_size`], [`LOGO_SVG`]) shared by every role's server-rendered pages so
//! they look like one site.

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, response::Html, routing::get};
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};
use utoipa_scalar::{Scalar, Servable as _};

/// A composed HTTP API: the merged router of every enabled role plus their
/// merged OpenAPI spec. Build with [`Api::new`], add roles with
/// [`Api::merge_role`], then [`Api::serve`].
pub struct Api {
    router: Router,
    doc: utoipa::openapi::OpenApi,
    roles: usize,
}

impl Default for Api {
    fn default() -> Self {
        Self::new()
    }
}

impl Api {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            doc: BaseDoc::openapi(),
            roles: 0,
        }
    }

    /// Fold one role's routes and API spec into the composition. Paths must not
    /// collide across roles: each owns a distinct URL space, and where they
    /// share the `/api/v1/stats/` prefix they take distinct leaves (the indexer
    /// serves `/api/v1/stats/index`; the stats role `/resources` and `/host`).
    pub fn merge_role(&mut self, router: Router, doc: utoipa::openapi::OpenApi) {
        let base = std::mem::take(&mut self.router);
        self.router = base.merge(router);
        self.doc.merge(doc);
        self.roles += 1;
    }

    /// Whether any role was merged in (nothing to serve otherwise).
    pub fn has_roles(&self) -> bool {
        self.roles > 0
    }

    /// Bind `listen` and start serving in a background task. `started_at` backs
    /// the `/health` uptime. Returns once the socket is bound (or on bind error).
    pub async fn serve(self, listen: SocketAddr, started_at: Instant) -> Result<()> {
        let app = Router::new()
            .route("/health", get(get_health))
            .with_state(started_at)
            .merge(self.router)
            .merge(Scalar::with_url("/api/docs", self.doc).custom_html(SCALAR_HTML));
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding API on {listen}"))?;
        tracing::info!(%listen, "API listening (docs at /api/docs)");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!("API server stopped: {e}");
            }
        });
        Ok(())
    }
}

/// Base OpenAPI document: the unified title/description and the generic health
/// probe. Role specs are merged in (their paths, schemas and tags are kept; a
/// merge does not touch this document's `info`).
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rucio Bootstrap API",
        version = "1",
        description = "\
HTTP API of a `rucio-bootstrap` node. The available endpoints depend on which \
optional roles the node was built and started with:

- **Indexer** (`indexer` feature) — a passive DHT search index: `/` and \
  `/search` web UI, `/api/v1/search` and `/api/v1/records`, plus the index \
  counters at `/api/v1/stats/index`. Only `/api/v1/admin/prune` needs a token.
- **Stats** (`stats-web` feature) — the node dashboard at `/stats` (resource \
  usage, and the index counters when the indexer is on) plus \
  `/api/v1/stats/resources` and `/api/v1/stats/host`.

All read endpoints live under `/api/v1/stats/*` and need no auth; the only \
token-guarded endpoint is the `/api/v1/admin/prune` mutation. Timestamps are \
Unix seconds."
    ),
    paths(get_health),
    components(schemas(HealthResponse)),
    tags((name = "Status", description = "Liveness and health checks"))
)]
struct BaseDoc;

/// Liveness probe payload.
#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    /// Always `"ok"` while the node is serving.
    pub status: String,
    /// Seconds since the API server started.
    pub uptime_secs: u64,
}

/// Liveness check.
///
/// Returns `200` with the node status and uptime as long as the API is serving.
/// Unauthenticated; outside any `/api/v1` prefix so it can double as a
/// container/load-balancer health probe.
#[utoipa::path(
    get, path = "/health",
    tag = "Status",
    responses((status = 200, description = "Node is alive", body = HealthResponse))
)]
async fn get_health(State(started_at): State<Instant>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        uptime_secs: started_at.elapsed().as_secs(),
    })
}

const SCALAR_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>Rucio Bootstrap API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      type="application/json"
      data-configuration='{"operationTitleSource":"path"}'
    >$spec</script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>
"#;

// ── Shared presentation helpers ─────────────────────────────────────────────

/// Accent-coloured logo mark, shared across the web pages.
pub const LOGO_SVG: &str = r##"<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M19.3 12.4 C16.7 9.2 15.9 8.2 16.0 7.9 A16.3 16.3 0 0 0 17.1 6.3 C17.3 6.0 17.7 5.0 17.0 4.3 C16.4 3.7 15.7 3.7 15.1 4.3 S14.3 5.0 13.8 5.4 L13.2 4.3 C13.0 3.8 12.5 3.0 11.9 2.7 S10.5 3.0 10.5 4.3 A10.0 10.0 0 0 1 10.2 6.8 C10.1 7.1 9.9 7.6 5.3 17.1 L4.0 19.7 L9.9 19.7 C10.5 18.7 10.3 18.7 11.2 16.9 L11.8 15.4 L13.0 15.9 C14.4 16.5 15.7 17.0 17.1 17.6 A2.1 2.1 0 0 0 19.4 17.0 A3.5 3.5 0 0 0 19.3 12.4 Z"/></svg>"##;

/// Shared `<style>`, mirroring the project landing page palette (light/dark).
pub const CSS: &str = r#"
*{box-sizing:border-box}
:root{color-scheme:light;--bg:#f8fafc;--surface:#fff;--surface-2:#f1f5f9;--border:#e2e8f0;--text:#0f172a;--text-2:#475569;--text-3:#64748b;--accent:#4f6ef7;--accent-2:#3b5bdb;--accent-fg:#fff;--shadow:0 12px 36px rgba(15,23,42,.12);--indent:4.25rem;--low-bg:#fef2f2;--low-fg:#b91c1c;--low-bd:#fecaca;--warn-bg:#fffbeb;--warn-fg:#b45309;--warn-bd:#fde68a;--ok-bg:#f0fdf4;--ok-fg:#15803d;--ok-bd:#bbf7d0}
@media(prefers-color-scheme:dark){:root{color-scheme:dark;--bg:#0f0f1a;--surface:#1a1a2e;--surface-2:#16162a;--border:#2d2d4e;--text:#e2e8f0;--text-2:#94a3b8;--text-3:#64748b;--accent:#7c93f0;--accent-2:#a5b4fc;--accent-fg:#0f0f1a;--shadow:0 12px 36px rgba(0,0,0,.4);--low-bg:#3a1f24;--low-fg:#fca5a5;--low-bd:#7f2a35;--warn-bg:#3a311c;--warn-fg:#fcd34d;--warn-bd:#7a5e22;--ok-bg:#15301f;--ok-fg:#86efac;--ok-bd:#225c38}}
body{margin:0;font-family:system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.search{display:flex;gap:.5rem;width:100%}
.search input{flex:1;min-width:0;padding:.7rem 1rem;font-size:1rem;font-family:inherit;color:var(--text);background:var(--surface);border:1px solid var(--border);border-radius:.6rem;outline:none;transition:border-color .15s}
.search input:focus{border-color:var(--accent)}
.search button{padding:.7rem 1.25rem;font-size:.95rem;font-weight:600;font-family:inherit;color:var(--accent-fg);background:var(--accent);border:1px solid var(--accent);border-radius:.6rem;cursor:pointer;white-space:nowrap}
.search button:hover{background:var(--accent-2);border-color:var(--accent-2)}
.search select{padding:.7rem .55rem;font-size:.9rem;font-family:inherit;color:var(--text);background:var(--surface);border:1px solid var(--border);border-radius:.6rem;cursor:pointer;outline:none}
.search select:focus{border-color:var(--accent)}
/* Landing */
.home{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:1.5rem 1.5rem 16vh;text-align:center}
.home .logo{width:72px;height:72px;color:var(--accent)}
.home h1{font-size:2.4rem;letter-spacing:-.02em;margin:.5rem 0 .25rem}
.home p.tag{color:var(--text-2);margin:0 0 1.75rem}
.home .search{max-width:40rem}
/* Results — left-aligned, Google/mnemo style. */
header.bar{position:sticky;top:0;z-index:5;background:color-mix(in srgb,var(--bg) 90%,transparent);backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
header.bar .inner{display:flex;align-items:center;gap:.9rem;padding:.75rem 1.5rem}
header.bar .logo{width:30px;height:30px;color:var(--accent);flex-shrink:0}
header.bar .search{max-width:760px}
main{padding:1.5rem;padding-left:var(--indent);max-width:calc(var(--indent) + 760px)}
.count{color:var(--text-3);font-size:.85rem;margin:0 0 1.25rem}
.hit{margin-bottom:1.5rem;padding-bottom:1.5rem;border-bottom:1px solid var(--border)}
.hit:last-of-type{border-bottom:none;margin-bottom:0}
.hit-title{font-size:1.05rem;font-weight:600;line-height:1.35;margin:0 0 .4rem;overflow-wrap:anywhere}
.hit-title a{color:var(--accent)}
.hit-meta{display:flex;flex-wrap:wrap;gap:.4rem;margin:0 0 .5rem}
.chip{display:inline-flex;align-items:center;font-size:.72rem;font-weight:600;padding:.12rem .55rem;border:1px solid var(--border);border-radius:999px;background:var(--surface-2);color:var(--text-2);white-space:nowrap}
.chip-low{background:var(--low-bg);color:var(--low-fg);border-color:var(--low-bd)}
.chip-mid{background:var(--warn-bg);color:var(--warn-fg);border-color:var(--warn-bd)}
.chip-high{background:var(--ok-bg);color:var(--ok-fg);border-color:var(--ok-bd)}
.magnet{display:block;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.76rem;color:var(--text-2);background:var(--surface-2);border:1px solid var(--border);border-radius:.4rem;padding:.45rem .6rem;line-height:1.5;overflow-wrap:anywhere;word-break:break-all}
.empty{color:var(--text-2);padding:2rem 0}
.pager{display:flex;justify-content:space-between;gap:1rem;margin:1.75rem 0 .5rem}
.pager span{color:var(--text-3)}
.pager a{padding:.45rem 1rem;border:1px solid var(--border);border-radius:.5rem;background:var(--surface);font-size:.85rem;font-weight:600}
.pager a:hover{border-color:var(--accent);text-decoration:none}
footer{padding:1.25rem 1.5rem;padding-left:var(--indent);color:var(--text-3);font-size:.8rem}
/* Dashboard (stats panel): centred column of cards, tabs and stat tiles. */
.wrap{max-width:960px;margin:0 auto;padding:1.5rem}
.head{display:flex;align-items:center;gap:.9rem;margin-bottom:1.5rem}
.head .logo{width:34px;height:34px;color:var(--accent);flex-shrink:0}
.head h1{font-size:1.4rem;margin:0;letter-spacing:-.01em}
.head .sub{color:var(--text-3);font-size:.82rem;margin:.15rem 0 0}
.card{background:var(--surface);border:1px solid var(--border);border-radius:.8rem;padding:1.1rem 1.25rem;margin-bottom:1.25rem;box-shadow:var(--shadow)}
.card h2{font-size:.78rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-3);margin:0 0 .9rem;font-weight:700}
.facts{display:flex;flex-wrap:wrap;gap:.4rem 1.6rem}
.facts div{font-size:.9rem}
.facts .k{color:var(--text-3);font-size:.72rem;text-transform:uppercase;letter-spacing:.04em}
.tabs{display:flex;flex-wrap:wrap;gap:.4rem;margin-bottom:1.25rem}
.tab{padding:.4rem .9rem;border:1px solid var(--border);border-radius:999px;background:var(--surface);color:var(--text-2);font-size:.85rem;font-weight:600}
.tab:hover{border-color:var(--accent);text-decoration:none}
.tab.active{background:var(--accent);border-color:var(--accent);color:var(--accent-fg)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:.9rem;margin-bottom:1.25rem}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:.7rem;padding:.85rem .95rem}
.tile .k{color:var(--text-3);font-size:.72rem;text-transform:uppercase;letter-spacing:.04em;margin-bottom:.3rem}
.tile .v{font-size:1.35rem;font-weight:700;line-height:1.15;letter-spacing:-.01em}
.tile .v small{font-size:.8rem;font-weight:600;color:var(--text-3)}
.tile .sub{color:var(--text-3);font-size:.74rem;margin-top:.15rem}
.note{color:var(--text-3);font-size:.78rem;margin:.25rem 0 0}
@media(min-width:1280px){main{max-width:calc(var(--indent) + 1040px)}}
@media(max-width:640px){
.search{flex-wrap:wrap}
.search input{flex:1 1 100%}
.search select{flex:1}
header.bar .logo{display:none}
header.bar .search{max-width:none}
}
"#;

/// Wrap a `<body>` fragment in the full HTML document with the shared style.
pub fn page(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title><meta name="robots" content="noindex"><style>{CSS}</style>
</head><body>{body}</body></html>"#,
    )
}

/// Convenience: a full HTML page as an axum [`Html`] response.
pub fn html_page(title: &str, body: &str) -> Html<String> {
    Html(page(title, body))
}

pub fn footer() -> String {
    r#"<footer>Rucio — decentralized P2P file sharing · <a href="https://github.com/ogarcia/rucio">github.com/ogarcia/rucio</a></footer>"#.to_string()
}

/// Escape the five HTML-significant characters. Applied to every value that
/// originates from the network or the user.
pub fn esc(s: &str) -> String {
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
pub fn human_size(bytes: u64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn esc_escapes_all_html_metacharacters() {
        assert_eq!(esc("a<b>&\"'"), "a&lt;b&gt;&amp;&quot;&#39;");
    }

    #[test]
    fn human_size_picks_a_unit() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1536), "1.5 KB");
    }

    #[test]
    fn page_embeds_title_and_body() {
        let out = page("T", "<p>B</p>");
        assert!(out.contains("<title>T</title>"));
        assert!(out.contains("<p>B</p>"));
        assert!(out.contains("noindex"));
    }
}
