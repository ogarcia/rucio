//! Mount-prefix–aware URL helpers.
//!
//! The panel can be served at the origin root (`/`) or under a subpath
//! (`/rucio/`) behind a reverse proxy. All absolute URLs the app builds — REST
//! calls and the WebSocket — must honour that prefix, otherwise the browser
//! resolves them against the origin root and they bypass the proxy location.
//!
//! The prefix is discovered once at runtime from the document's base URI, which
//! reflects the `<base href>` the daemon injects when `RUCIOD_BASE_PATH` is set.
//! A single WASM binary therefore works at any mount point with no rebuild — the
//! deployment is configured entirely in the daemon/proxy, never baked in here.

use std::sync::OnceLock;

static BASE: OnceLock<String> = OnceLock::new();

/// The path prefix the app is served under, e.g. `/` or `/rucio/`.
///
/// Always normalised to exactly one trailing slash and never empty, so callers
/// can join it without worrying about missing or doubled slashes.
pub fn base_path() -> &'static str {
    BASE.get_or_init(|| {
        let path = web_sys::window()
            .and_then(|w| w.document())
            .and_then(|d| d.base_uri().ok().flatten())
            .as_deref()
            .and_then(|uri| web_sys::Url::new(uri).ok())
            .map(|u| u.pathname())
            .unwrap_or_else(|| "/".to_string());
        format!("{}/", path.trim_end_matches('/'))
    })
}

/// Build an absolute URL for an API path (which must start with `/`, e.g.
/// `"/api/v1/status"`), prefixed with the mount path. With base `/` the path is
/// returned unchanged; with base `/rucio/` it becomes `/rucio/api/v1/status`.
pub fn api(path: &str) -> String {
    debug_assert!(path.starts_with('/'), "api() path must start with '/'");
    format!("{}{}", base_path().trim_end_matches('/'), path)
}
