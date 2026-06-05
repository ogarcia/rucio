//! Portable Rucio desktop shell.
//!
//! A thin Tauri app that embeds the whole product: it spawns
//! [`rucio_daemon::run`] (which serves the Leptos UI + REST + WebSocket on
//! loopback) and points a single webview window at it. The frontend is *not*
//! ported to Tauri IPC — the page talks to the daemon over HTTP/WS exactly as
//! in a browser, so the web UI works unchanged.
//!
//! Storage is portable: all state lives next to the executable (see
//! [`rucio_daemon::apply_base_dir_env`]).

// Hide the console window on Windows release builds — this audience must never
// see a terminal. No effect on other platforms or debug builds.
#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::time::Duration;

use tauri::{WebviewUrl, WebviewWindowBuilder};

/// Loopback host the embedded daemon serves the web UI + REST + WS on. The
/// shell never exposes it off-loopback — there is no authentication. The port
/// is chosen at startup (see [`pick_free_port`]) so the app never collides with
/// a standalone `ruciod` or anything else already using a fixed port.
const API_HOST: &str = "127.0.0.1";

/// Grab a free loopback TCP port by binding to port 0 and releasing it. There
/// is a tiny window before the daemon rebinds it, negligible on loopback.
fn pick_free_port() -> u16 {
    std::net::TcpListener::bind((API_HOST, 0))
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|addr| addr.port())
        .unwrap_or(3003)
}

fn main() {
    // WebKitGTK's DMABUF renderer crashes on some Wayland compositors / GPUs
    // ("Gdk-Message: Error 71 dispatching to Wayland display"); disabling it
    // falls back to a stable rendering path. Linux-only — Windows uses WebView2
    // and macOS uses WKWebView, where this variable is meaningless. Respect an
    // explicit override, and set it before GTK initialises.
    #[cfg(target_os = "linux")]
    if std::env::var_os("WEBKIT_DISABLE_DMABUF_RENDERER").is_none() {
        // SAFETY: first thing in main, before any thread or GTK init runs.
        unsafe { std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1") };
    }

    // Portable mode: config, identity, database, downloads, temp and the eMule
    // caches all live next to the .exe. Must run before the daemon reads its
    // config and before any Tokio worker is live (see the function's contract).
    rucio_daemon::apply_base_dir_env(true, None);

    // Pick a free loopback port for the embedded daemon's API/UI and hand it to
    // the daemon via RUCIOD_API_LISTEN, so it never clashes with a standalone
    // ruciod or anything else on a fixed port. Set before the runtime starts.
    let port = pick_free_port();
    // SAFETY: set in main before the Tauri runtime / any worker thread exists.
    unsafe { std::env::set_var("RUCIOD_API_LISTEN", format!("{API_HOST}:{port}")) };

    tauri::Builder::default()
        .setup(move |app| {
            let handle = app.handle().clone();

            // Spawn the embedded daemon. When the app exits, the process ends
            // and takes the daemon down with it.
            tauri::async_runtime::spawn(async {
                if let Err(e) = rucio_daemon::run(None).await {
                    eprintln!("rucio daemon exited with error: {e}");
                }
            });

            // Open the window once the daemon's API accepts connections, so the
            // first paint isn't a connection error.
            tauri::async_runtime::spawn(async move {
                wait_for_api(port).await;
                let url = format!("http://{API_HOST}:{port}");
                let built = WebviewWindowBuilder::new(
                    &handle,
                    "main",
                    WebviewUrl::External(url.parse().expect("valid loopback URL")),
                )
                .title("Rucio")
                .inner_size(1100.0, 760.0)
                .min_inner_size(640.0, 480.0)
                .build();
                if let Err(e) = built {
                    eprintln!("failed to open the main window: {e}");
                }
            });

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running the Rucio desktop shell");
}

/// Poll the loopback API port until it accepts a TCP connection — the daemon
/// binds it only after its startup work completes. Bounded (~60 s) so a wedged
/// daemon still opens the window, which then shows its own retry/connection UI.
async fn wait_for_api(port: u16) {
    for _ in 0..600 {
        if tokio::net::TcpStream::connect((API_HOST, port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
