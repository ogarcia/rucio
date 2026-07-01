//! Portable Rucio desktop shell.
//!
//! A thin Tauri app that embeds the whole product: it spawns the daemon (which
//! serves the Leptos UI + REST + WebSocket on loopback) and points a single
//! webview window at it. The frontend is *not* ported to Tauri IPC — the page
//! talks to the daemon over HTTP/WS exactly as in a browser, so the web UI
//! works unchanged.
//!
//! Storage: on Windows and macOS the app is portable — all state lives next to
//! the executable (see [`rucio_daemon::apply_base_dir_env`]). On Linux it ships
//! as a Flatpak (read-only mount), so it uses the XDG base directories instead;
//! inside the sandbox those resolve under `~/.var/app/me.ogarcia.rucio/`,
//! keeping the whole configuration and database within the sandbox.

// Hide the console window on Windows release builds — this audience must never
// see a terminal. No effect on other platforms or debug builds.
#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::sync::Mutex;
use std::time::Duration;

// The system tray is Windows/macOS-only: GNOME ships no tray and the tray-icon
// backend needs libappindicator (absent in the GNOME runtime), so on Linux the
// app runs without one (it quits from GNOME's Background Apps menu instead).
#[cfg(not(target_os = "linux"))]
use tauri::menu::{CheckMenuItem, Menu, MenuItem};
#[cfg(not(target_os = "linux"))]
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Manager, RunEvent, Runtime, WebviewUrl, WebviewWindowBuilder, WindowEvent};
use tauri_plugin_autostart::MacosLauncher;
#[cfg(not(target_os = "linux"))]
use tauri_plugin_autostart::ManagerExt;
use tokio::sync::oneshot;

/// Which window properties to persist across runs: size and maximized state.
/// Position is excluded on purpose — on Wayland the compositor owns window
/// placement (clients cannot set it), and having the plugin save/restore it
/// there misbehaves. Visibility is excluded too: the app manages hiding itself
/// (Linux background mode, Windows/macOS tray), so the plugin must not force the
/// window shown or hidden. The plugin auto-restores on window creation (its
/// `on_window_ready` hook) using these flags.
fn window_state_flags() -> tauri_plugin_window_state::StateFlags {
    use tauri_plugin_window_state::StateFlags;
    StateFlags::SIZE | StateFlags::MAXIMIZED
}

/// Show, un-minimise and focus the main window (from the tray or a second
/// launch). No-op until the window exists.
fn show_main<R: Runtime>(app: &AppHandle<R>) {
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.show();
        let _ = w.unminimize();
        let _ = w.set_focus();
    }
}

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

    // Storage location. On Windows/macOS the app is portable: config, identity,
    // database, downloads, temp and the eMule caches all live next to the .exe.
    // On Linux it runs from a read-only Flatpak mount where that is impossible,
    // so fall back to the XDG base directories — inside the sandbox those live
    // under ~/.var/app/me.ogarcia.rucio/, keeping all state within the sandbox.
    // Must run before the daemon reads its config and before any Tokio worker is
    // live (see the function's contract).
    #[cfg(target_os = "linux")]
    let portable = false;
    #[cfg(not(target_os = "linux"))]
    let portable = true;
    rucio_daemon::apply_base_dir_env(portable, None);

    // Pick a free loopback port for the embedded daemon's API/UI and hand it to
    // the daemon via RUCIOD_API_LISTEN, so it never clashes with a standalone
    // ruciod or anything else on a fixed port. Set before the runtime starts.
    let port = pick_free_port();
    // SAFETY: set in main before the Tauri runtime / any worker thread exists.
    unsafe { std::env::set_var("RUCIOD_API_LISTEN", format!("{API_HOST}:{port}")) };

    // Spawn the embedded daemon with a graceful-shutdown trigger we fire when
    // the window closes, so it flushes metrics, saves the Kad cache and closes
    // SQLite cleanly instead of being killed with the process.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let daemon = tauri::async_runtime::spawn(async move {
        if let Err(e) = rucio_daemon::run_until(None, async move {
            let _ = shutdown_rx.await;
        })
        .await
        {
            eprintln!("rucio daemon exited with error: {e}");
        }
    });
    // Wrapped so the (FnMut) exit handler can take them exactly once.
    let shutdown_tx = Mutex::new(Some(shutdown_tx));
    let daemon = Mutex::new(Some(daemon));

    let app = tauri::Builder::default()
        // Must be registered first. A second launch focuses the running window
        // instead of starting a rival daemon over the same portable data dir.
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            show_main(app);
        }))
        // Launch-at-login support, toggled from the tray on Windows/macOS (an
        // HKCU Run-key entry on Windows, no admin needed). Off by default. On
        // Linux this plugin is unused: autostart goes through the Background
        // portal instead (see `autostart_enabled` / `request_background`), the
        // sandbox-correct path.
        .plugin(tauri_plugin_autostart::init(
            MacosLauncher::LaunchAgent,
            None,
        ))
        // Remember the window size and maximized state across runs (see
        // `window_state_flags` for why position and visibility are excluded).
        .plugin(
            tauri_plugin_window_state::Builder::default()
                .with_state_flags(window_state_flags())
                .build(),
        )
        // Closing the window hides it and keeps the app (and the embedded
        // daemon) running: on Windows/macOS in the system tray, on Linux as a
        // background app (see the Background portal request in `setup`). It is
        // quit from the tray on Windows/macOS, or on Linux from GNOME's
        // Background Apps menu (a SIGKILL) or a terminal Ctrl+C (see `setup`).
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                // Persist size/maximized now, while the window is still alive and
                // visible. Closing only hides it (background app / tray), so the
                // window-state plugin's save-on-exit may never run if the OS
                // reaps the hidden process later.
                use tauri_plugin_window_state::AppHandleExt;
                let _ = window.app_handle().save_window_state(window_state_flags());
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .setup(move |app| {
            // System tray: left-click restores the window, right-click opens a
            // menu (Show / Autostart / Quit). Built up front; its handlers look
            // the window up lazily, so it's fine that the window doesn't exist
            // yet. Skipped on Linux, which has no tray (see the imports).
            #[cfg(not(target_os = "linux"))]
            {
                let show_i = MenuItem::with_id(app, "show", "Show Rucio", true, None::<&str>)?;
                let autostart_i = CheckMenuItem::with_id(
                    app,
                    "autostart",
                    "Start on login",
                    true,
                    app.autolaunch().is_enabled().unwrap_or(false),
                    None::<&str>,
                )?;
                let quit_i = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
                let menu = Menu::with_items(app, &[&show_i, &autostart_i, &quit_i])?;
                let autostart_item = autostart_i.clone();
                let _tray = TrayIconBuilder::with_id("main")
                    .tooltip("Rucio")
                    .icon(app.default_window_icon().expect("app icon").clone())
                    .menu(&menu)
                    .show_menu_on_left_click(false)
                    .on_menu_event(move |app, event| match event.id.as_ref() {
                        "show" => show_main(app),
                        "autostart" => {
                            let mgr = app.autolaunch();
                            let _ = if mgr.is_enabled().unwrap_or(false) {
                                mgr.disable()
                            } else {
                                mgr.enable()
                            };
                            // Reflect the resulting state on the check mark.
                            let _ = autostart_item.set_checked(mgr.is_enabled().unwrap_or(false));
                        }
                        "quit" => app.exit(0),
                        _ => {}
                    })
                    .on_tray_icon_event(|tray, event| {
                        if let TrayIconEvent::Click {
                            button: MouseButton::Left,
                            button_state: MouseButtonState::Up,
                            ..
                        } = event
                        {
                            show_main(tray.app_handle());
                        }
                    })
                    .build(app)?;
            }

            // Linux has no tray, so the close-to-hide behaviour relies on the
            // Background portal to keep the app running (and let the desktop
            // list/manage it), plus SIGTERM/SIGINT handlers so a terminal Ctrl+C
            // (or a plain SIGTERM) stops the embedded daemon gracefully —
            // unmapping UPnP ports and saving caches — instead of killing it.
            // (GNOME's background-apps "Quit" sends SIGKILL, which can't be
            // caught; the UPnP mappings then expire on their own via their lease.)
            #[cfg(target_os = "linux")]
            {
                tauri::async_runtime::spawn(request_background(autostart_enabled()));

                let sig_handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    use tokio::signal::unix::{SignalKind, signal};
                    // Note: GNOME's "Quit" in the background-apps menu sends
                    // SIGKILL, which cannot be caught — the UPnP mappings then
                    // rely on their (finite) lease to expire on their own.
                    let (mut term, mut int) = match (
                        signal(SignalKind::terminate()),
                        signal(SignalKind::interrupt()),
                    ) {
                        (Ok(term), Ok(int)) => (term, int),
                        _ => return,
                    };
                    // Whichever arrives first. Triggers RunEvent::ExitRequested,
                    // which flushes and shuts the daemon down (see `app.run`).
                    tokio::select! {
                        _ = term.recv() => {}
                        _ = int.recv() => {}
                    }
                    sig_handle.exit(0);
                });
            }

            let handle = app.handle().clone();
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
                // Defaults for the very first run; overridden below by any
                // previously saved size/position/maximized state.
                .inner_size(1100.0, 760.0)
                .min_inner_size(640.0, 480.0)
                .build();
                // The window-state plugin restores size/maximized automatically
                // on creation (its on_window_ready hook), so nothing to do here.
                if let Err(e) = built {
                    eprintln!("failed to open the main window: {e}");
                }
            });
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building the Rucio desktop shell");

    app.run(move |_app, event| {
        if let RunEvent::ExitRequested { .. } = event {
            // Trigger the daemon's graceful shutdown and wait for it, bounded so
            // a wedged daemon can't hang the close (then the process exits).
            if let Some(tx) = shutdown_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            if let Some(task) = daemon.lock().unwrap().take() {
                let _ = tauri::async_runtime::block_on(async {
                    tokio::time::timeout(Duration::from_secs(8), task).await
                });
            }
        }
    });
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

/// Ask the XDG Background portal to let the app keep running when its window is
/// closed. On success the desktop treats it as a proper background app (GNOME
/// lists it in its "Background Apps" menu). When `auto_start` is set, the portal
/// also creates the host-side autostart entry so Rucio launches at login (the
/// sandbox-correct equivalent of dropping a .desktop in ~/.config/autostart,
/// which the app itself cannot write to). Best-effort: outside a portal
/// environment this fails and the app simply runs as an ordinary process — the
/// window still hides and the daemon keeps going either way.
#[cfg(target_os = "linux")]
async fn request_background(auto_start: bool) {
    use ashpd::desktop::background::Background;
    if let Err(e) = Background::request()
        .reason("Rucio keeps sharing and downloading files while its window is closed")
        .auto_start(auto_start)
        .dbus_activatable(false)
        .send()
        .await
        .and_then(|request| request.response())
    {
        eprintln!("rucio: Background portal request failed (running without it): {e}");
    }
}

/// Whether to ask the portal to launch Rucio at login. This is a desktop-shell
/// setting only (it never touches the daemon, CLI or web UI) and is edited by
/// hand — there is no in-app toggle, since the web UI talks to the daemon over
/// HTTP, not Tauri IPC. It lives in `desktop.toml` next to the daemon's config
/// (inside the Flatpak: ~/.var/app/me.ogarcia.rucio/config/rucio/desktop.toml):
///
/// ```toml
/// autostart = true
/// ```
///
/// Missing, unreadable or malformed ⇒ `false`.
#[cfg(target_os = "linux")]
fn autostart_enabled() -> bool {
    let Some(config_dir) = dirs::config_dir() else {
        return false;
    };
    let dir = config_dir.join("rucio");
    let path = dir.join("desktop.toml");

    // Seed a commented template on first run so the setting is easy (and safe)
    // to flip by hand. Never overwrite an existing file — that is the user's.
    if !path.exists() {
        let template = "\
# Rucio desktop shell settings (Linux only) — edit by hand.
# Launch Rucio at login, via the XDG Background portal. Set to true to enable.
autostart = false
";
        if let Err(e) = std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, template))
        {
            eprintln!("rucio: could not seed {}: {e}", path.display());
        }
    }

    std::fs::read_to_string(&path)
        .ok()
        .and_then(|contents| toml::from_str::<toml::Table>(&contents).ok())
        .and_then(|table| table.get("autostart").and_then(toml::Value::as_bool))
        .unwrap_or(false)
}
