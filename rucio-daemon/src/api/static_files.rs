use axum::extract::State;
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

use super::AppState;

#[derive(Embed)]
#[folder = "../rucio-web/dist/"]
struct WebAssets;

fn mime_for_path(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".js") || path.ends_with(".mjs") {
        "application/javascript"
    } else if path.ends_with(".wasm") {
        "application/wasm"
    } else if path.ends_with(".css") {
        "text/css"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".webmanifest") {
        "application/manifest+json"
    } else if path.ends_with(".json") {
        "application/json"
    } else {
        "application/octet-stream"
    }
}

pub async fn serve(State(state): State<AppState>, uri: Uri) -> Response {
    let base = state.config.api.base_path.as_str();
    let path = uri.path().trim_start_matches('/');

    if path.is_empty() {
        return serve_index(base);
    }

    match WebAssets::get(path) {
        Some(content) => (
            [(axum::http::header::CONTENT_TYPE, mime_for_path(path))],
            content.data.into_owned(),
        )
            .into_response(),
        // SPA fallback: unknown paths serve index.html so client-side routing works.
        None => serve_index(base),
    }
}

/// Serve `index.html`, injecting a `<base href>` matching the mount prefix.
///
/// The shell is built by trunk with a relative `--public-url`, so its asset
/// references (wasm/JS), the manifest, the service worker and — via
/// `document.baseURI` — the WASM app's own API/WebSocket calls all resolve
/// against this `<base>`. With `base_path = "/"` it's an explicit no-op (`/`),
/// which keeps relative resolution deterministic regardless of the request path;
/// with `/rucio/` it relocates the whole app under the subpath.
fn serve_index(base: &str) -> Response {
    let Some(content) = WebAssets::get("index.html") else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let html_header = [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")];

    // Insert <base> as the first child of <head> so it precedes every relative
    // reference. base is already normalised to a single trailing slash.
    let html = String::from_utf8_lossy(&content.data);
    let injected = html.replacen("<head>", &format!("<head><base href=\"{base}\">"), 1);
    (html_header, injected.into_bytes()).into_response()
}
