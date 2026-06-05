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

/// Loopback endpoint the embedded daemon serves on (its default). The shell
/// never exposes it off-loopback — there is no authentication.
const API_HOST: &str = "127.0.0.1";
const API_PORT: u16 = 3003;

fn main() {
    // Portable mode: config, identity, database, downloads, temp and the eMule
    // caches all live next to the .exe. Must run before the daemon reads its
    // config and before any Tokio worker is live (see the function's contract).
    rucio_daemon::apply_base_dir_env(true, None);

    tauri::Builder::default()
        .setup(|app| {
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
                wait_for_api().await;
                let url = format!("http://{API_HOST}:{API_PORT}");
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
async fn wait_for_api() {
    for _ in 0..600 {
        if tokio::net::TcpStream::connect((API_HOST, API_PORT))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
