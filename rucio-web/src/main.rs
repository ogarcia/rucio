mod downloads;
mod icons;
mod overlays;
mod searches;
mod types;

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::time::Duration;

use futures_util::StreamExt;
use gloo_net::websocket::{Message, futures::WebSocket};
use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use downloads::{DownloadsTab, refresh_downloads};
use overlays::{AddressesPanel, NodeStatusPanel};
use searches::SearchesTab;
use types::{
    DownloadResponse, DownloadState, ResultSource, SearchResult, SearchState, StatusResponse,
    WsEvent, WsSearchResult,
};

/// States the daemon streams over `DownloadProgress`. A download that leaves
/// this set has reached a terminal/paused state the WS does not report, so the
/// list must be refreshed from REST to pick up its final state.
fn is_streamed_state(s: &DownloadState) -> bool {
    matches!(
        s,
        DownloadState::FindingProviders
            | DownloadState::Queued
            | DownloadState::Downloading
            | DownloadState::Stalled
    )
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
                    // Do NOT set connected here. WebSocket::open() only creates
                    // the JS object — the TCP handshake hasn't happened yet.
                    // We flip the icon green only on the first actual message,
                    // so it never shows green when the server is unreachable.
                    backoff_ms = 1_000;

                    let mut stream = ws;
                    while let Some(msg) = stream.next().await {
                        if !ws_connected.get_untracked() {
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
                                );
                            }
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

    let ws_connected: RwSignal<bool> = RwSignal::new(false);
    let status: RwSignal<Option<StatusResponse>> = RwSignal::new(None);
    let downloads: RwSignal<Vec<DownloadResponse>> = RwSignal::new(vec![]);
    let search_results: RwSignal<Vec<SearchResult>> = RwSignal::new(vec![]);
    let searching: RwSignal<bool> = RwSignal::new(false);
    let search_id: RwSignal<Option<u64>> = RwSignal::new(None);

    // Initial data fetch.
    spawn_local(async move {
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/status").send().await {
            if let Ok(s) = r.json::<StatusResponse>().await {
                status.set(Some(s));
            }
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
                                inner_html=if connected { icons::WIFI } else { icons::WIFI_OFF }
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
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::NodeStatus));
                                menu_open.set(false);
                            }>"Node status"</button>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::Addresses));
                                menu_open.set(false);
                            }>"Addresses"</button>
                            <div class="dropdown-sep"/>
                            <button class="dropdown-item" on:click=move |_| {
                                menu_open.set(false);
                                spawn_local(async move {
                                    if let Ok(r) = gloo_net::http::Request::get("/api/v1/status")
                                        .send().await
                                    {
                                        if let Ok(s) = r.json::<StatusResponse>().await {
                                            status.set(Some(s));
                                        }
                                    }
                                    refresh_downloads(downloads).await;
                                });
                            }>"Refresh"</button>
                        </div>
                    </Show>
                </div>
            </header>

            <main class="content">
                {move || match active_tab.get() {
                    Tab::Downloads => view! {
                        <DownloadsTab downloads=downloads/>
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
        })}
    }
}

fn main() {
    mount_to_body(App);
}
