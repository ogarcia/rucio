use axum::http::Uri;
use axum::response::IntoResponse;
use rust_embed::Embed;

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
    } else {
        "application/octet-stream"
    }
}

pub async fn serve(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match WebAssets::get(path) {
        Some(content) => (
            [(axum::http::header::CONTENT_TYPE, mime_for_path(path))],
            content.data.into_owned(),
        )
            .into_response(),
        None => {
            // SPA fallback: unknown paths serve index.html so client-side routing works
            match WebAssets::get("index.html") {
                Some(content) => (
                    [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    content.data.into_owned(),
                )
                    .into_response(),
                None => axum::http::StatusCode::NOT_FOUND.into_response(),
            }
        }
    }
}
