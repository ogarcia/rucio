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
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
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
            .route("/theme", get(set_theme))
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
  `/api/v1/stats/resources`, `/api/v1/stats/series` (sparkline data) and \
  `/api/v1/stats/host`.

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

// ── Theme (cookie-based, no JavaScript) ─────────────────────────────────────

/// The viewer's theme choice, read from the `theme` cookie. `Auto` follows the
/// operating system via `prefers-color-scheme`.
#[derive(Clone, Copy, PartialEq)]
pub enum Theme {
    Auto,
    Light,
    Dark,
}

impl Theme {
    /// The `<html>` attribute that pins the theme. Empty for `Auto`, so the CSS
    /// falls back to `prefers-color-scheme`.
    fn html_attr(self) -> &'static str {
        match self {
            Theme::Auto => "",
            Theme::Light => r#" data-theme="light""#,
            Theme::Dark => r#" data-theme="dark""#,
        }
    }
}

/// Read the theme choice from the request's `Cookie` header (default `Auto`).
pub fn theme_from_cookies(headers: &HeaderMap) -> Theme {
    let raw = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    for kv in raw.split(';') {
        if let Some(v) = kv.trim().strip_prefix("theme=") {
            return match v {
                "light" => Theme::Light,
                "dark" => Theme::Dark,
                _ => Theme::Auto,
            };
        }
    }
    Theme::Auto
}

#[derive(Deserialize)]
struct ThemeQuery {
    set: Option<String>,
    next: Option<String>,
}

/// `GET /theme?set=light&next=/…` — set the `theme` cookie and redirect back.
///
/// No JavaScript: the header's Auto/Light/Dark links point here, and the page
/// re-renders server-side with the new theme. `next` is restricted to a local
/// path so it cannot be used as an open redirect.
async fn set_theme(Query(p): Query<ThemeQuery>) -> Response {
    let val = match p.set.as_deref() {
        Some("light") => "light",
        Some("dark") => "dark",
        _ => "auto",
    };
    let next = p
        .next
        .filter(|n| n.starts_with('/') && !n.starts_with("//"))
        .unwrap_or_else(|| "/".to_string());
    // "auto" clears the cookie (Max-Age=0); an explicit choice pins it a year.
    let cookie = if val == "auto" {
        "theme=; Path=/; Max-Age=0; SameSite=Lax".to_string()
    } else {
        format!("theme={val}; Path=/; Max-Age=31536000; SameSite=Lax")
    };
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::SET_COOKIE, cookie)
        .header(header::LOCATION, next)
        .body(axum::body::Body::empty())
        .expect("valid redirect response")
}

/// The Auto/Light/Dark switch for a page header. `next` is the current
/// path+query to return to after the cookie is set.
pub fn theme_switch(theme: Theme, next: &str) -> String {
    let enc = urlencoding::encode(next);
    let pill = |label: &str, val: &str, active: bool| {
        let cls = if active { "tsw on" } else { "tsw" };
        format!(r#"<a class="{cls}" href="/theme?set={val}&amp;next={enc}">{label}</a>"#)
    };
    format!(
        r#"<div class="tswitch">{}{}{}</div>"#,
        pill("Auto", "auto", theme == Theme::Auto),
        pill("Light", "light", theme == Theme::Light),
        pill("Dark", "dark", theme == Theme::Dark),
    )
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

/// Shared `<style>` for every server-rendered page. Palette-driven and
/// theme-aware in three states: bare `:root` is light; the dark palette applies
/// under an explicit `[data-theme="dark"]` and, for `Auto`, under
/// `prefers-color-scheme: dark` unless the viewer forced light.
pub const CSS: &str = r#"
*{box-sizing:border-box}
:root{color-scheme:light;
--bg:#f8fafc;--surface:#fff;--card:#fff;--surface-2:#f1f5f9;
--border:#e2e8f0;--border-2:#cbd5e1;
--text:#0f172a;--text-2:#475569;--text-3:#64748b;--text-4:#94a3b8;
--accent:#4f6ef7;--accent-2:#3b5bdb;--accent-fg:#fff;
--shadow:0 12px 36px rgba(15,23,42,.10);--shadow-sm:0 1px 2px rgba(15,23,42,.04);
--low-bg:#fef2f2;--low-fg:#b91c1c;--low-bd:#fecaca;
--warn-bg:#fffbeb;--warn-fg:#b45309;--warn-bd:#fde68a;
--ok-bg:#f0fdf4;--ok-fg:#15803d;--ok-bd:#bbf7d0;
--soft-bg:#eef1fe;--soft-bd:#c7d0fb;--soft-fg:#3b5bdb;--track:#f1f5f9}
:root[data-theme="dark"]{color-scheme:dark;
--bg:#0f0f1a;--surface:#16162a;--card:#1a1a2e;--surface-2:#16162a;
--border:#2d2d4e;--border-2:#2d2d4e;
--text:#e2e8f0;--text-2:#94a3b8;--text-3:#64748b;--text-4:#64748b;
--accent:#7c93f0;--accent-2:#a5b4fc;--accent-fg:#0f0f1a;
--shadow:0 12px 36px rgba(0,0,0,.24);--shadow-sm:0 1px 2px rgba(0,0,0,.2);
--low-bg:#3a1f24;--low-fg:#fca5a5;--low-bd:#7f2a35;
--warn-bg:#3a311c;--warn-fg:#fcd34d;--warn-bd:#7a5e22;
--ok-bg:#15301f;--ok-fg:#86efac;--ok-bd:#225c38;
--soft-bg:#1c2040;--soft-bd:#3d4b8a;--soft-fg:#a5b4fc;--track:#2d2d4e}
@media(prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;
--bg:#0f0f1a;--surface:#16162a;--card:#1a1a2e;--surface-2:#16162a;
--border:#2d2d4e;--border-2:#2d2d4e;
--text:#e2e8f0;--text-2:#94a3b8;--text-3:#64748b;--text-4:#64748b;
--accent:#7c93f0;--accent-2:#a5b4fc;--accent-fg:#0f0f1a;
--shadow:0 12px 36px rgba(0,0,0,.24);--shadow-sm:0 1px 2px rgba(0,0,0,.2);
--low-bg:#3a1f24;--low-fg:#fca5a5;--low-bd:#7f2a35;
--warn-bg:#3a311c;--warn-fg:#fcd34d;--warn-bd:#7a5e22;
--ok-bg:#15301f;--ok-fg:#86efac;--ok-bd:#225c38;
--soft-bg:#1c2040;--soft-bd:#3d4b8a;--soft-fg:#a5b4fc;--track:#2d2d4e}}
html,body{background:var(--bg)}
body{margin:0;font-family:system-ui,-apple-system,"Helvetica Neue",sans-serif;color:var(--text);line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
code,.mono{font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace}

/* ── Top bar (shared header) ────────────────────────────────────────────── */
.bar{background:var(--surface);border-bottom:1px solid var(--border)}
.bar.results{border-bottom:none}
.bar-in{display:flex;align-items:center;flex-wrap:wrap;gap:.5rem .85rem;padding:.8rem 1.5rem;max-width:1100px;margin:0 auto}
.brand{display:flex;align-items:center;gap:.55rem;flex-shrink:0;color:var(--text)}
.brand:hover{text-decoration:none}
.brand .logo{width:24px;height:24px;color:var(--accent);flex-shrink:0}
.brand .name{font-size:.95rem;font-weight:600;letter-spacing:-.01em;color:var(--text)}
.hostsum{color:var(--text-3);font-size:.8rem;white-space:nowrap}
.spacer{flex:1}
.hdr-nav{display:inline-flex;align-items:center;gap:.9rem}
.navlink{font-size:.82rem;color:var(--text-2);white-space:nowrap}
.tswitch{display:flex;gap:2px;padding:2px;background:var(--surface-2);border:1px solid var(--border);border-radius:999px}
.tsw{padding:3px 10px;border-radius:999px;font-size:.72rem;font-weight:600;color:var(--text-2)}
.tsw:hover{text-decoration:none;color:var(--text)}
.tsw.on{color:var(--accent-fg);background:var(--accent)}

/* ── Search form + sort pills ──────────────────────────────────────────── */
.search{display:flex;gap:.5rem;width:100%}
.search input{flex:1;min-width:0;padding:.85rem 1rem;font-size:1rem;font-family:inherit;color:var(--text);background:var(--card);border:1px solid var(--border-2);border-radius:.6rem;outline:none;box-shadow:var(--shadow-sm);transition:border-color .15s}
.search.sm input{padding:.55rem .8rem;font-size:.95rem}
.search input:focus{border-color:var(--accent)}
.search button{padding:.85rem 1.5rem;font-size:.95rem;font-weight:600;font-family:inherit;color:var(--accent-fg);background:var(--accent);border:1px solid var(--accent);border-radius:.6rem;cursor:pointer;white-space:nowrap}
.search.sm button{padding:.55rem 1.1rem;font-size:.9rem}
.search button:hover{background:var(--accent-2);border-color:var(--accent-2)}
/* In a header the search is flexible but capped, so brand + search + switch
   stay on one line on desktop. */
.bar .search{flex:1 1 auto;width:auto;min-width:160px;max-width:560px}
.sorts{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}
.sorts .lbl{font-size:.82rem;color:var(--text-3)}
.hdr-sorts{display:none}
.pill{padding:.32rem .8rem;border-radius:999px;background:var(--card);border:1px solid var(--border);color:var(--text-2);font-size:.8rem;font-weight:600}
.pill:hover{text-decoration:none;border-color:var(--accent)}
.pill.on{background:var(--accent);border-color:var(--accent);color:var(--accent-fg)}
label.pill{cursor:pointer}
/* No-JS radio pills (landing sort): the hidden radio drives its label's look. */
.sorts input[type=radio]{position:absolute;width:1px;height:1px;opacity:0;pointer-events:none}
.sorts input[type=radio]:checked + .pill{background:var(--accent);border-color:var(--accent);color:var(--accent-fg)}
.sorts input[type=radio]:focus-visible + .pill{outline:2px solid var(--accent);outline-offset:2px}

/* ── Landing hero ──────────────────────────────────────────────────────── */
.hero{max-width:820px;margin:0 auto;padding:8vh 1.5rem 3rem;display:flex;flex-direction:column}
.hero h1{font-size:2.4rem;line-height:1.12;letter-spacing:-.025em;margin:0 0 .6rem;text-wrap:pretty}
.hero .lead{color:var(--text-2);font-size:1rem;margin:0 0 1.75rem;max-width:34rem}
.search-hero{display:flex;flex-direction:column;gap:.9rem;max-width:44rem}
.facts3{display:flex;flex-wrap:wrap;gap:1rem 2.25rem;margin-top:2.25rem;padding-top:1.4rem;border-top:1px solid var(--border)}
.facts3 .n{font-size:1.2rem;font-weight:700;letter-spacing:-.01em}
.facts3 .k{font-size:.78rem;color:var(--text-3)}
.hero-status{display:none}

/* ── Search results ────────────────────────────────────────────────────── */
/* Sticky top: the header (and, on results, the sub-bar) stay pinned while the
   results below scroll under them. */
.stick{position:sticky;top:0;z-index:10;background:var(--bg)}
.main{max-width:1100px;margin:0 auto;padding:1.1rem 1.5rem 2.5rem}
.subbar{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap;max-width:1100px;margin:0 auto;padding:.6rem 1.5rem .7rem;border-bottom:1px solid var(--border)}
.subbar .count{color:var(--text-3);font-size:.85rem}
.hit{display:flex;gap:1rem;align-items:flex-start;background:var(--card);border:1px solid var(--border);border-radius:.7rem;padding:1rem 1.1rem;margin-bottom:.75rem;box-shadow:var(--shadow-sm)}
.hit.bare{background:var(--surface-2);border-style:dashed}
.avail{width:64px;flex-shrink:0;display:flex;flex-direction:column;align-items:center;gap:.3rem}
.avail .c{font-size:1.35rem;font-weight:700;line-height:1}
.avail .k{font-size:.62rem;text-transform:uppercase;letter-spacing:.05em;color:var(--text-3)}
.avail .track{width:100%;height:4px;background:var(--track);border-radius:2px;overflow:hidden}
.avail .track i{display:block;height:100%}
.avail.high .c{color:var(--ok-fg)} .avail.high .track i{background:var(--ok-fg)}
.avail.mid .c{color:var(--warn-fg)} .avail.mid .track i{background:var(--warn-fg)}
.avail.low .c{color:var(--low-fg)} .avail.low .track i{background:var(--low-fg)}
.hit-main{flex:1;min-width:0;display:flex;flex-direction:column;gap:.4rem}
.hit-main .t{font-size:1.02rem;font-weight:600;line-height:1.35;overflow-wrap:anywhere}
.hit-main .t.hash{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.9rem;color:var(--text-2)}
.hit-meta{display:flex;flex-wrap:wrap;gap:.9rem;font-size:.8rem;color:var(--text-3)}
.magnet{display:block;min-width:0;max-width:100%;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.74rem;line-height:1.5;color:var(--text-4);overflow-wrap:anywhere}
.empty{color:var(--text-2);padding:2rem 0}
.pager{display:flex;justify-content:center;gap:.75rem;margin:1.75rem 0 .5rem}
.pager span{color:var(--text-3)}
.pager a{padding:.55rem 1.25rem;border:1px solid var(--border);border-radius:.5rem;background:var(--card);font-size:.85rem;font-weight:600}
.pager a:hover{border-color:var(--accent);text-decoration:none}

/* ── Dashboard (stats panel) ───────────────────────────────────────────── */
.wrap{max-width:1000px;margin:0 auto;padding:1.5rem}
.dhead{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem;margin-bottom:1.4rem}
.idxrow{display:flex;flex-wrap:wrap;gap:.4rem 2rem}
.idxrow .k{font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--text-3)}
.idxrow .v{font-size:1.6rem;font-weight:700;letter-spacing:-.02em;line-height:1.1}
.card{background:var(--card);border:1px solid var(--border);border-radius:.8rem;padding:1.1rem 1.25rem;margin-bottom:1.25rem;box-shadow:var(--shadow)}
.card h2{font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-3);margin:0 0 .9rem;font-weight:700}
.facts{display:flex;flex-wrap:wrap;gap:.5rem 1.6rem}
.facts div{font-size:.9rem}
.facts .k{color:var(--text-3);font-size:.7rem;text-transform:uppercase;letter-spacing:.04em}
.tabs{display:flex;flex-wrap:wrap;gap:.4rem}
.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:.9rem;margin-bottom:1.25rem}
.metric{background:var(--card);border:1px solid var(--border);border-radius:.7rem;padding:.95rem 1.05rem;display:flex;flex-direction:column;gap:.5rem}
.metric .k{font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--text-3)}
.metric .v{font-size:1.6rem;font-weight:700;line-height:1;letter-spacing:-.02em}
.metric .v small{font-size:.75rem;font-weight:600;color:var(--text-3)}
.metric .sub{font-size:.76rem;color:var(--text-3)}
.metric .spark{width:100%;height:38px;display:block}
.metric .spark polyline{fill:none;stroke:var(--accent);stroke-width:2;vector-effect:non-scaling-stroke}
.strip{display:flex;flex-wrap:wrap;gap:.4rem 1.75rem;padding:.9rem 1.1rem;background:var(--card);border:1px solid var(--border);border-radius:.7rem;font-size:.85rem;margin-bottom:1.25rem}
.srow{display:flex;gap:.4rem}
.strip .k{color:var(--text-3)}
.strip .v{color:var(--text);font-weight:600}
.suggest{display:flex;flex-direction:column;gap:.45rem;padding:1rem 1.15rem;background:var(--soft-bg);border:1px solid var(--soft-bd);border-radius:.7rem;margin-bottom:1.25rem}
.suggest-head{display:flex;align-items:center;flex-wrap:wrap;gap:.4rem .9rem}
.suggest .k{font-size:.68rem;text-transform:uppercase;letter-spacing:.05em;color:var(--soft-fg);font-weight:700}
.suggest .v{font-size:1.05rem;font-weight:700;letter-spacing:-.01em}
.suggest .note{margin:0}
.note{color:var(--text-3);font-size:.78rem;margin:.25rem 0 0}
footer{max-width:1100px;margin:0 auto;padding:1.5rem;color:var(--text-3);font-size:.8rem;text-align:center}

@media(max-width:720px){
.bar-in{gap:.5rem .7rem;padding:.7rem 1rem}
.tsw{padding:.55rem .85rem}
.pill{padding:.55rem .85rem}

/* Landing hero: search input + button stack full-width; sort pills fill the row. */
.hero{padding:6vh 1.25rem 2.5rem}
.hero h1{font-size:1.9rem}
.search-hero .search{flex-wrap:wrap}
.search-hero .search input{flex:1 1 100%;padding:.95rem 1rem}
.search-hero .search button{flex:1 1 100%;padding:.95rem 1rem}
.sorts{gap:.45rem}
.sorts .lbl{flex:1 1 100%}
.sorts .pill{flex:1;text-align:center;padding:.7rem .4rem}
/* Landing: nav leaves the header (logo + brand + theme only); the node-status
   link moves to the bottom of the hero. */
.landing .hdr-nav{display:none}
.hero-status{display:inline-block;margin-top:1.5rem;font-size:.95rem;font-weight:600}
/* Landing facts → key/value table (label left, value right). */
.facts3{flex-direction:column;gap:0;margin-top:1.75rem;padding-top:0;border-top:none}
.facts3>div{display:flex;flex-direction:row-reverse;justify-content:space-between;align-items:baseline;padding:.75rem 0;border-bottom:1px solid var(--border)}
.facts3 .n{font-size:1rem}

/* Results header: logo + inline search on row 1, sort pills on row 2, no theme. */
.results .brand .name{display:none}
.results .search{flex:1 1 0;min-width:0;max-width:none}
.results .search input{min-width:0}
.results .hdr-sorts{display:flex;order:5;flex:1 1 100%}
.results .hdr-sorts .pill{flex:1;text-align:center;padding:.6rem .3rem}
.results .tsw-wrap{display:none}
.results .spacer{display:none}
.subbar-sorts{display:none}
/* Result cards: availability becomes a compact row above the file name. The
   column layout must stretch children to the card width (not shrink to the
   magnet's content) or the magnet escapes to the right. */
.hit{flex-direction:column;align-items:stretch;gap:.55rem}
.avail{flex-direction:row;width:auto;align-items:center;gap:.5rem}
.avail .c{font-size:1.05rem}
.avail .track{width:72px;order:3}
/* Mobile keeps the magnet on one line (ellipsis) to stay compact; desktop wraps. */
.magnet{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;overflow-wrap:normal}

/* Dashboard header: logo + Node status (+ back link), host on row 2, no theme. */
.dash .hostsum{order:5;flex:1 1 100%;white-space:normal}
.dash .tsw-wrap{display:none}
/* Dashboard: tabs first (big), then index as two boxes (Named omitted). */
.dhead{flex-direction:column;align-items:stretch;gap:.9rem}
.dhead .tabs{order:1}
.dhead .tabs .pill{flex:1;text-align:center;padding:.7rem .3rem}
.dhead .idxrow{order:2;display:grid;grid-template-columns:1fr 1fr;gap:.7rem}
.dhead .idxrow>div{background:var(--card);border:1px solid var(--border);border-radius:.6rem;padding:.75rem .9rem}
.dhead .idxrow>div:nth-child(3){display:none}
.dhead .tabs .w-all{display:none}
.metrics{grid-template-columns:1fr}
/* Facts strip → key/value table. */
.strip{flex-direction:column;gap:0;padding:.2rem 1.1rem}
.srow{justify-content:space-between;padding:.7rem 0;border-bottom:1px solid var(--border)}
.srow:last-child{border-bottom:none}

.pager a{padding:.75rem 1.4rem}
}
"#;

/// Wrap a `<body>` fragment in the full HTML document with the shared style.
/// `theme` pins `<html data-theme>` (empty for `Auto`, so the OS decides).
pub fn page(title: &str, body: &str, theme: Theme) -> String {
    format!(
        r#"<!doctype html><html lang="en"{attr}><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title><meta name="robots" content="noindex"><style>{CSS}</style>
</head><body>{body}</body></html>"#,
        attr = theme.html_attr(),
    )
}

/// Convenience: a full HTML page as an axum [`Html`] response.
pub fn html_page(title: &str, body: &str, theme: Theme) -> Html<String> {
    Html(page(title, body, theme))
}

/// The logo + wordmark for a page header (accent logo, plain-text name).
pub fn brand(name: &str) -> String {
    format!(
        r#"<a class="brand" href="/"><span class="logo">{logo}</span><span class="name">{name}</span></a>"#,
        logo = LOGO_SVG,
    )
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

/// Thousands-separated integer (e.g. `128942` → `128,942`).
// `% 3 == 0` is clearer here than `.is_multiple_of(3)`, which is also newer than
// the project's MSRV (1.85).
#[allow(clippy::manual_is_multiple_of)]
pub fn group(n: i64) -> String {
    let s = n.abs().to_string();
    let len = s.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
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
        let out = page("T", "<p>B</p>", Theme::Auto);
        assert!(out.contains("<title>T</title>"));
        assert!(out.contains("<p>B</p>"));
        assert!(out.contains("noindex"));
    }

    #[test]
    fn theme_pins_the_html_attribute() {
        assert!(page("T", "", Theme::Dark).contains(r#"<html lang="en" data-theme="dark">"#));
        assert!(page("T", "", Theme::Auto).contains(r#"<html lang="en">"#));
    }

    #[test]
    fn group_inserts_thousands_separators() {
        assert_eq!(group(0), "0");
        assert_eq!(group(128_942), "128,942");
        assert_eq!(group(-3_118), "-3,118");
    }
}
