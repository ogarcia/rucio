mod downloads;
mod overlays;
mod searches;
mod types;

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
    DownloadResponse, ResultSource, SearchResult, SearchState, StatusResponse, WsEvent,
    WsSearchResult,
};

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
            ws_connected.set(false);

            match WebSocket::open(&ws_url()) {
                Err(_) => {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(30_000);
                    continue;
                }
                Ok(ws) => {
                    ws_connected.set(true);
                    backoff_ms = 1_000;

                    let mut stream = ws;
                    while let Some(msg) = stream.next().await {
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

                    ws_connected.set(false);
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
            downloads.set(list);
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
                    // WS connection dot
                    <span
                        class=move || if ws_connected.get() { "ws-dot ws-dot-on" } else { "ws-dot ws-dot-off" }
                        title=move || if ws_connected.get() { "Connected" } else { "Disconnected" }
                    />

                    <button
                        class="menu-btn"
                        on:click=move |_| menu_open.update(|o| *o = !*o)
                    >"≡"</button>

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
