mod downloads;
mod icons;
mod overlays;
mod searches;
mod types;

// ── Theme ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Theme {
    Auto,
    Light,
    Dark,
}

fn load_theme() -> Theme {
    let stored = web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|ls| ls.get_item("rucio-theme").ok().flatten());
    match stored.as_deref() {
        Some("light") => Theme::Light,
        Some("dark") => Theme::Dark,
        _ => Theme::Auto,
    }
}

/// Apply a theme to the <html> element and persist it to localStorage.
/// Auto = remove the data-theme attribute so the CSS media query takes over.
fn apply_theme(t: Theme) {
    if let Some(el) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.document_element())
    {
        match t {
            Theme::Auto => {
                let _ = el.remove_attribute("data-theme");
            }
            Theme::Light => {
                let _ = el.set_attribute("data-theme", "light");
            }
            Theme::Dark => {
                let _ = el.set_attribute("data-theme", "dark");
            }
        }
    }
    if let Some(ls) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = ls.set_item(
            "rucio-theme",
            match t {
                Theme::Auto => "auto",
                Theme::Light => "light",
                Theme::Dark => "dark",
            },
        );
    }
}

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::time::Duration;

use futures_util::StreamExt;
use gloo_net::websocket::{Message, futures::WebSocket};
use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use downloads::{DownloadsTab, refresh_downloads};
use overlays::{AddressesPanel, NodeStatusPanel, StatsPanel};
use searches::SearchesTab;
use types::{
    DownloadResponse, DownloadState, ResultSource, SearchResult, SearchState, SpeedLimits,
    StatusResponse, TempLimitRequest, TempLimitStatus, WsEvent, WsSearchResult, format_rate_kbps,
    is_streamed_state,
};

/// Preset bandwidth caps offered in the menu dropdowns (KB/s; 0 = unlimited).
const LIMIT_PRESETS: [u64; 9] = [0, 50, 100, 256, 512, 1024, 2048, 5120, 10240];

/// PUT the base speed limits to the daemon (fire-and-forget).
fn put_limits(upload_kbps: u64, download_kbps: u64) {
    spawn_local(async move {
        if let Ok(req) = gloo_net::http::Request::put("/api/v1/config/limits").json(&SpeedLimits {
            upload_kbps,
            download_kbps,
        }) {
            let _ = req.send().await;
        }
    });
}

// Throttling for the post-WS refresh that catches terminal transitions the
// stream doesn't carry. WASM is single-threaded so a plain Cell/RefCell is fine.
//
// REFRESH_IN_FLIGHT prevents stacking refreshes when the GET round-trip outlasts
// the 1 s WS tick.  REFRESHED_IDS keeps a per-id "already refreshed" mark so a
// single download cannot trigger the refresh more than once: by the time a
// refreshed id matters again it would already be in a non-streamed state.
thread_local! {
    static REFRESH_IN_FLIGHT: Cell<bool> = const { Cell::new(false) };
    static REFRESHED_IDS: RefCell<HashSet<i64>> = RefCell::new(HashSet::new());
}

// ── Tab / Panel enums ─────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Downloads,
    Searches,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Panel {
    NodeStatus,
    Addresses,
    Stats,
}

// ── WebSocket ────────────────────────────────────────────────────────────────

fn ws_url() -> String {
    let window = web_sys::window().expect("no window");
    let location = window.location();
    let proto = location.protocol().unwrap_or_default();
    let host = location.host().unwrap_or_default();
    let ws_proto = if proto.starts_with("https") {
        "wss"
    } else {
        "ws"
    };
    format!("{ws_proto}://{host}/api/ws")
}

fn start_ws_loop(
    ws_connected: RwSignal<bool>,
    downloads: RwSignal<Vec<DownloadResponse>>,
    status: RwSignal<Option<StatusResponse>>,
    search_results: RwSignal<Vec<SearchResult>>,
    search_id: RwSignal<Option<u64>>,
    searching: RwSignal<bool>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
) {
    spawn_local(async move {
        let mut backoff_ms = 1_000u64;

        loop {
            // Guard: only notify subscribers when the value actually changes.
            // Leptos always marks the signal dirty on set(), even with the
            // same value, so every iteration would re-render the icon otherwise.
            if ws_connected.get_untracked() {
                ws_connected.set(false);
            }

            match WebSocket::open(&ws_url()) {
                Err(_) => {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(30_000);
                    continue;
                }
                Ok(ws) => {
                    // Set connected on the first message, not on socket creation.
                    // WebSocket::open() only creates the JS object; the TCP
                    // handshake hasn't completed yet, so setting green here makes
                    // failed reconnection attempts appear connected.
                    backoff_ms = 1_000;

                    let mut stream = ws;
                    loop {
                        // Bound the wait on the next frame. The daemon greets
                        // with Ping the instant the socket upgrades and then
                        // emits an event every second, so silence past the
                        // deadline means the socket is dead — typically opened
                        // while the daemon was down and never handshaken (open()
                        // returns Ok before the TCP connect resolves), or the
                        // daemon vanished without a clean close. Either way we
                        // abandon it and reconnect instead of blocking forever
                        // on a socket that will never deliver — which is what
                        // made the indicator slow to turn green on startup.
                        //
                        // Use a short deadline until connected (so a stale
                        // socket is dropped quickly and a freshly started daemon
                        // is picked up within ~2 s) and a looser heartbeat once
                        // connected.
                        let connected = ws_connected.get_untracked();
                        let deadline = if connected { 5_000 } else { 2_000 };
                        let next = std::pin::pin!(stream.next());
                        let timeout = std::pin::pin!(sleep(Duration::from_millis(deadline)));
                        match futures_util::future::select(next, timeout).await {
                            futures_util::future::Either::Left((Some(msg), _)) => {
                                if !connected {
                                    ws_connected.set(true);
                                }
                                if let Ok(Message::Text(text)) = msg {
                                    if let Ok(event) = serde_json::from_str::<WsEvent>(&text) {
                                        handle_event(
                                            event,
                                            downloads,
                                            status,
                                            search_results,
                                            search_id,
                                            searching,
                                            dl_speed,
                                            ul_speed,
                                        );
                                    }
                                }
                            }
                            // Stream closed (Left(None)) or the deadline elapsed
                            // with no frame (Right): give up and reconnect.
                            _ => break,
                        }
                    }

                    // Stream ended: server closed the connection or stopped.
                    if ws_connected.get_untracked() {
                        ws_connected.set(false);
                    }
                }
            }

            sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(30_000);
        }
    });
}

fn handle_event(
    event: WsEvent,
    downloads: RwSignal<Vec<DownloadResponse>>,
    status: RwSignal<Option<StatusResponse>>,
    search_results: RwSignal<Vec<SearchResult>>,
    search_id: RwSignal<Option<u64>>,
    searching: RwSignal<bool>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
) {
    match event {
        WsEvent::DownloadProgress(list) => {
            // The daemon only streams *active* downloads. Merge into the existing
            // list so completed/paused/cancelled rows don't disappear.
            let incoming: HashSet<i64> = list.iter().map(|d| d.id).collect();

            // Find downloads we still track as active but the stream omitted,
            // skipping any id we've already refreshed once.
            let missing: Vec<i64> = downloads.with_untracked(|cur| {
                cur.iter()
                    .filter(|d| is_streamed_state(&d.state) && !incoming.contains(&d.id))
                    .map(|d| d.id)
                    .filter(|id| REFRESHED_IDS.with(|s| !s.borrow().contains(id)))
                    .collect()
            });

            // Compute the merged list without mutating the signal yet.
            // The merge dedupes both cur and incoming by id so <For> never sees
            // duplicate keys — keys repeated across re-renders make Leptos mount
            // two rows with the same id, which appeared as a row "ghosting" at
            // the bottom of the list on every progress tick.
            let new_list = downloads.with_untracked(|cur| {
                let mut merged: Vec<DownloadResponse> = Vec::with_capacity(cur.len() + list.len());
                let mut seen: HashSet<i64> = HashSet::new();
                for d in cur {
                    if seen.insert(d.id) {
                        merged.push(d.clone());
                    }
                }
                for item in list {
                    if let Some(slot) = merged.iter_mut().find(|d| d.id == item.id) {
                        *slot = item;
                    } else if seen.insert(item.id) {
                        merged.push(item);
                    }
                }
                merged
            });

            // Only notify the reactive graph when something actually changed.
            // downloads.update() always marks the signal dirty even with identical
            // data, causing every Memo in <For> to re-evaluate every WS tick.
            if downloads.with_untracked(|cur| cur != &new_list) {
                downloads.set(new_list);
            }

            // Refresh once per "lost" download, never more than one GET in flight.
            // Without these guards, a slow REST round-trip causes the next WS
            // tick to spawn another refresh while the previous is still pending,
            // and refreshes pile up indefinitely.
            if !missing.is_empty() && !REFRESH_IN_FLIGHT.with(|f| f.get()) {
                REFRESH_IN_FLIGHT.with(|f| f.set(true));
                REFRESHED_IDS.with(|s| s.borrow_mut().extend(missing));
                spawn_local(async move {
                    refresh_downloads(downloads).await;
                    REFRESH_IN_FLIGHT.with(|f| f.set(false));
                });
            }
        }

        WsEvent::SearchResult(r) => {
            // Only accumulate if a search is active.
            if searching.get() {
                let result = ws_result_to_search_result(r);
                search_results.update(|v| {
                    // Deduplicate by root_hash.
                    let hash = result.download_link.clone().unwrap_or_default();
                    if !v.iter().any(|x| x.download_link.as_deref() == Some(&hash)) {
                        v.push(result);
                    }
                });
            }
        }

        WsEvent::NodeClassChanged { class } => {
            status.update(|s| {
                if let Some(s) = s {
                    s.class = class;
                }
            });
        }

        WsEvent::PeerConnected { .. } => {
            status.update(|s| {
                if let Some(s) = s {
                    s.connected_peers += 1;
                }
            });
        }

        WsEvent::PeerDisconnected { .. } => {
            status.update(|s| {
                if let Some(s) = s {
                    s.connected_peers = s.connected_peers.saturating_sub(1);
                }
            });
        }

        WsEvent::IndexingCount { .. } => {}

        WsEvent::SessionStats {
            download_speed,
            upload_speed,
        } => {
            if dl_speed.get_untracked() != download_speed {
                dl_speed.set(download_speed);
            }
            if ul_speed.get_untracked() != upload_speed {
                ul_speed.set(upload_speed);
            }
        }

        // Liveness keepalive — receiving it already flipped the connection
        // indicator to connected in the WS loop; nothing else to do.
        WsEvent::Ping => {}
    }
}

fn ws_result_to_search_result(r: WsSearchResult) -> SearchResult {
    SearchResult {
        result_id: 0,
        name: r.name,
        size: r.size,
        source: ResultSource::Rucio,
        download_link: Some(r.magnet),
        provider: Some(r.provider),
    }
}

// ── App ───────────────────────────────────────────────────────────────────────

#[component]
fn App() -> impl IntoView {
    let active_tab: RwSignal<Tab> = RwSignal::new(Tab::Downloads);
    let menu_open: RwSignal<bool> = RwSignal::new(false);
    let active_panel: RwSignal<Option<Panel>> = RwSignal::new(None);

    // Theme — apply immediately so the DOM reflects any stored preference.
    let initial_theme = load_theme();
    apply_theme(initial_theme);
    let theme: RwSignal<Theme> = RwSignal::new(initial_theme);

    let ws_connected: RwSignal<bool> = RwSignal::new(false);
    let status: RwSignal<Option<StatusResponse>> = RwSignal::new(None);
    let downloads: RwSignal<Vec<DownloadResponse>> = RwSignal::new(vec![]);
    let search_results: RwSignal<Vec<SearchResult>> = RwSignal::new(vec![]);
    let searching: RwSignal<bool> = RwSignal::new(false);
    let search_id: RwSignal<Option<u64>> = RwSignal::new(None);
    let dl_speed: RwSignal<u64> = RwSignal::new(0);
    let ul_speed: RwSignal<u64> = RwSignal::new(0);
    // Whether the temporary speed limit is engaged (runtime-only on the daemon).
    let temp_limit: RwSignal<bool> = RwSignal::new(false);
    // Base (normal) caps shown in the menu dropdowns, KB/s (0 = unlimited).
    let base_up: RwSignal<u64> = RwSignal::new(0);
    let base_down: RwSignal<u64> = RwSignal::new(0);
    // Preset temporary caps, for the read-only line under the toggle.
    let temp_up: RwSignal<u64> = RwSignal::new(0);
    let temp_down: RwSignal<u64> = RwSignal::new(0);

    // Initial data fetch.
    spawn_local(async move {
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/status").send().await {
            if let Ok(s) = r.json::<StatusResponse>().await {
                status.set(Some(s));
            }
        }
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/config/temp-limit")
            .send()
            .await
            && let Ok(s) = r.json::<TempLimitStatus>().await
        {
            temp_limit.set(s.active);
            temp_up.set(s.upload_kbps);
            temp_down.set(s.download_kbps);
        }
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/config/limits")
            .send()
            .await
            && let Ok(s) = r.json::<SpeedLimits>().await
        {
            base_up.set(s.upload_kbps);
            base_down.set(s.download_kbps);
        }
        refresh_downloads(downloads).await;
    });

    // Start the persistent WebSocket loop.
    start_ws_loop(
        ws_connected,
        downloads,
        status,
        search_results,
        search_id,
        searching,
        dl_speed,
        ul_speed,
    );

    view! {
        <div class="layout">
            <header class="topbar">
                <span class="brand">"Rucio"</span>

                <nav class="tabs">
                    <button
                        class=move || if active_tab.get() == Tab::Downloads { "tab active" } else { "tab" }
                        on:click=move |_| active_tab.set(Tab::Downloads)
                    >"Downloads"</button>
                    <button
                        class=move || if active_tab.get() == Tab::Searches { "tab active" } else { "tab" }
                        on:click=move |_| active_tab.set(Tab::Searches)
                    >"Searches"</button>
                </nav>

                <div class="menu-wrap">
                    // WS connection icon
                    {move || {
                        let connected = ws_connected.get();
                        view! {
                            <svg
                                class=if connected { "icon ws-icon ws-icon-on" } else { "icon ws-icon ws-icon-off" }
                                viewBox="0 0 24 24" stroke="currentColor" fill="none"
                                stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
                                title=if connected { "Connected" } else { "Disconnected" }
                                inner_html=if connected { icons::NETWORK } else { icons::NETWORK_OFF }
                            ></svg>
                        }
                    }}

                    <button
                        class="menu-btn"
                        on:click=move |_| menu_open.update(|o| *o = !*o)
                    >
                        <icons::Icon paths=icons::MENU/>
                    </button>

                    <Show when=move || menu_open.get()>
                        <div class="dropdown">
                            // ── Theme picker ──────────────────────────────
                            <div class="theme-picker">
                                <button
                                    class=move || if theme.get() == Theme::Auto {
                                        "theme-btn theme-active"
                                    } else {
                                        "theme-btn"
                                    }
                                    title="Auto (follow system)"
                                    on:click=move |_| {
                                        apply_theme(Theme::Auto);
                                        theme.set(Theme::Auto);
                                    }
                                >
                                    <icons::Icon paths=icons::DEVICE_DESKTOP/>
                                </button>
                                <button
                                    class=move || if theme.get() == Theme::Light {
                                        "theme-btn theme-active"
                                    } else {
                                        "theme-btn"
                                    }
                                    title="Light"
                                    on:click=move |_| {
                                        apply_theme(Theme::Light);
                                        theme.set(Theme::Light);
                                    }
                                >
                                    <icons::Icon paths=icons::SUN/>
                                </button>
                                <button
                                    class=move || if theme.get() == Theme::Dark {
                                        "theme-btn theme-active"
                                    } else {
                                        "theme-btn"
                                    }
                                    title="Dark"
                                    on:click=move |_| {
                                        apply_theme(Theme::Dark);
                                        theme.set(Theme::Dark);
                                    }
                                >
                                    <icons::Icon paths=icons::MOON/>
                                </button>
                            </div>
                            <div class="dropdown-sep"/>
                            // ── Speed limits ──────────────────────────────
                            <div class="menu-section">
                                <div class="menu-section-title">"Speed limits"</div>
                                <div class="menu-limit-row">
                                    <span class="menu-limit-label">"Download"</span>
                                    <select
                                        class="menu-select"
                                        prop:value=move || base_down.get().to_string()
                                        on:change=move |e| {
                                            let kbps = event_target_value(&e).parse().unwrap_or(0);
                                            base_down.set(kbps);
                                            put_limits(base_up.get_untracked(), kbps);
                                        }
                                    >
                                        {move || {
                                            let cur = base_down.get();
                                            let mut vals = LIMIT_PRESETS.to_vec();
                                            if !vals.contains(&cur) { vals.push(cur); vals.sort_unstable(); }
                                            vals.into_iter().map(|v| view! {
                                                <option value=v.to_string()>{format_rate_kbps(v)}</option>
                                            }).collect_view()
                                        }}
                                    </select>
                                </div>
                                <div class="menu-limit-row">
                                    <span class="menu-limit-label">"Upload"</span>
                                    <select
                                        class="menu-select"
                                        prop:value=move || base_up.get().to_string()
                                        on:change=move |e| {
                                            let kbps = event_target_value(&e).parse().unwrap_or(0);
                                            base_up.set(kbps);
                                            put_limits(kbps, base_down.get_untracked());
                                        }
                                    >
                                        {move || {
                                            let cur = base_up.get();
                                            let mut vals = LIMIT_PRESETS.to_vec();
                                            if !vals.contains(&cur) { vals.push(cur); vals.sort_unstable(); }
                                            vals.into_iter().map(|v| view! {
                                                <option value=v.to_string()>{format_rate_kbps(v)}</option>
                                            }).collect_view()
                                        }}
                                    </select>
                                </div>
                                <button class="dropdown-item dropdown-toggle" on:click=move |_| {
                                    let next = !temp_limit.get_untracked();
                                    spawn_local(async move {
                                        if let Ok(req) = gloo_net::http::Request::put(
                                            "/api/v1/config/temp-limit",
                                        )
                                        .json(&TempLimitRequest { active: next })
                                            && let Ok(resp) = req.send().await
                                            && let Ok(s) = resp.json::<TempLimitStatus>().await
                                        {
                                            temp_limit.set(s.active);
                                        }
                                    });
                                }>
                                    <span>"Use temp limits"</span>
                                    <span class=move || if temp_limit.get() {
                                        "toggle-pill toggle-on"
                                    } else {
                                        "toggle-pill"
                                    }>
                                        {move || if temp_limit.get() { "On" } else { "Off" }}
                                    </span>
                                </button>
                                <div class="menu-temp-info">
                                    <icons::Icon paths=icons::HOURGLASS/>
                                    <span>
                                        {move || format!(
                                            "{} down, {} up",
                                            format_rate_kbps(temp_down.get()),
                                            format_rate_kbps(temp_up.get()),
                                        )}
                                    </span>
                                </div>
                            </div>
                            <div class="dropdown-sep"/>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::NodeStatus));
                                menu_open.set(false);
                            }>"Node status"</button>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::Addresses));
                                menu_open.set(false);
                            }>"Addresses"</button>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::Stats));
                                menu_open.set(false);
                            }>"Statistics"</button>
                        </div>
                    </Show>
                </div>
            </header>

            <main class="content">
                {move || match active_tab.get() {
                    Tab::Downloads => view! {
                        <DownloadsTab
                            downloads=downloads
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Searches => view! {
                        <SearchesTab
                            results=search_results
                            searching=searching
                            search_id=search_id
                        />
                    }.into_any(),
                }}
            </main>
        </div>

        {move || active_panel.get().map(|panel| match panel {
            Panel::NodeStatus => view! {
                <NodeStatusPanel status=status active_panel=active_panel/>
            }.into_any(),
            Panel::Addresses => view! {
                <AddressesPanel status=status active_panel=active_panel/>
            }.into_any(),
            Panel::Stats => view! {
                <StatsPanel active_panel=active_panel/>
            }.into_any(),
        })}
    }
}

fn main() {
    mount_to_body(App);
}
