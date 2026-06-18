mod categories;
mod config;
mod downloads;
mod icons;
mod notifications;
mod overlays;
mod pins;
mod searches;
mod shares;
mod statusbar;
mod subscriptions;
mod types;
mod uploads;
mod webhooks;

// Load the translation catalogues under `locales/`. English is the source
// locale and the fallback when a key is missing in the active language.
rust_i18n::i18n!("locales", fallback = "en");

use rust_i18n::t;

// ── Language ─────────────────────────────────────────────────────────────────

/// Resolve the UI language once at startup: a stored `rucio-lang` preference if
/// present, otherwise the browser's `navigator.language`, falling back to
/// English. A tag such as `es-ES` is reduced to its base language (`es`).
fn resolve_locale() -> String {
    let win = web_sys::window();
    let stored = win
        .as_ref()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|ls| ls.get_item("rucio-lang").ok().flatten());
    let raw = stored
        .or_else(|| win.as_ref().and_then(|w| w.navigator().language()))
        .unwrap_or_else(|| "en".to_string());
    raw.split(['-', '_']).next().unwrap_or("en").to_lowercase()
}

/// The user's explicit language choice, mirroring the theme picker: `Auto`
/// follows the browser, the rest force a language.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Language {
    Auto,
    En,
    Es,
}

fn load_language() -> Language {
    let stored = web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|ls| ls.get_item("rucio-lang").ok().flatten());
    match stored.as_deref() {
        Some("en") => Language::En,
        Some("es") => Language::Es,
        _ => Language::Auto,
    }
}

/// Persist the language choice and reload, so the whole UI re-renders in the
/// new language. `Auto` clears the override and falls back to the browser.
pub(crate) fn set_language(l: Language) {
    if let Some(ls) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        match l {
            Language::Auto => {
                let _ = ls.remove_item("rucio-lang");
            }
            Language::En => {
                let _ = ls.set_item("rucio-lang", "en");
            }
            Language::Es => {
                let _ = ls.set_item("rucio-lang", "es");
            }
        }
    }
    if let Some(w) = web_sys::window() {
        let _ = w.location().reload();
    }
}

/// Display label for a navigation tab, localized.
fn tab_label(tab: Tab) -> std::borrow::Cow<'static, str> {
    match tab {
        Tab::Downloads => t!("tab.downloads"),
        Tab::Uploads => t!("tab.uploads"),
        Tab::Searches => t!("tab.searches"),
        Tab::Shares => t!("tab.shares"),
        Tab::Pins => t!("tab.pins"),
        Tab::Subscriptions => t!("tab.subscriptions"),
    }
}

// ── Theme ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Theme {
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
pub(crate) fn apply_theme(t: Theme) {
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
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures_util::StreamExt;
use gloo_net::websocket::{Message, futures::WebSocket};
use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use config::ConfigModal;
use downloads::{
    DownloadsTab, any_pausable, any_paused, any_terminal, api_add_links, clear_history, pause_all,
    refresh_downloads, resume_all,
};
use notifications::NotificationsPanel;
use overlays::{AboutPanel, AddressesPanel, NodeStatusPanel, PeersPanel, StatsPanel};
use pins::PinsTab;
use searches::SearchesTab;
use shares::SharesTab;
use subscriptions::SubscriptionsTab;
use types::{
    ActiveUpload, CategoriesResponse, Category, DownloadResponse, Notification, NotificationList,
    NotificationSettings, SearchResult, SearchState, SearchSummary, SpeedLimits, StatusResponse,
    TempLimitRequest, TempLimitStatus, UploadsResponse, WsEvent, format_rate_kbps,
    is_streamed_state,
};
use uploads::UploadsTab;

/// Search state shared across the app: the recent-search list, results keyed by
/// search id, and the currently-selected search. Lives in `App` so the WS keeps
/// it live even while another tab is open, and survives switching tabs.
#[derive(Clone, Copy)]
pub struct SearchStore {
    pub list: RwSignal<Vec<SearchSummary>>,
    pub results: RwSignal<HashMap<u64, Vec<SearchResult>>>,
    pub selected: RwSignal<Option<u64>>,
}

/// Preset bandwidth caps offered in the menu dropdowns (KB/s; 0 = unlimited).
/// Scaled for modern links (a ~500 Mbps line is ~62 MB/s), so the ceiling
/// reaches 100 MB/s rather than the old 10 MB/s.
const LIMIT_PRESETS: [u64; 9] = [0, 512, 1024, 2048, 5120, 10240, 25600, 51200, 102400];

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
// the 1 s WS tick.  REFRESHED_IDS keeps a per-id "already refreshed" mark so one
// stream-exit triggers at most one refresh; the mark is cleared when the id
// reappears in the stream (e.g. a resumed download), so a later exit refreshes
// again.
thread_local! {
    static REFRESH_IN_FLIGHT: Cell<bool> = const { Cell::new(false) };
    static REFRESHED_IDS: RefCell<HashSet<i64>> = RefCell::new(HashSet::new());
}

// ── Tab / Panel enums ─────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Downloads,
    Uploads,
    Searches,
    Shares,
    Pins,
    Subscriptions,
}

impl Tab {
    fn as_str(self) -> &'static str {
        match self {
            Tab::Downloads => "downloads",
            Tab::Uploads => "uploads",
            Tab::Searches => "searches",
            Tab::Shares => "shares",
            Tab::Pins => "pins",
            Tab::Subscriptions => "subscriptions",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "downloads" => Some(Tab::Downloads),
            "uploads" => Some(Tab::Uploads),
            "searches" => Some(Tab::Searches),
            "shares" => Some(Tab::Shares),
            "pins" => Some(Tab::Pins),
            "subscriptions" => Some(Tab::Subscriptions),
            _ => None,
        }
    }
}

/// Load the last-active tab from localStorage, defaulting to Downloads.
fn load_tab() -> Tab {
    web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|ls| ls.get_item("rucio-tab").ok().flatten())
        .and_then(|s| Tab::from_str(&s))
        .unwrap_or(Tab::Downloads)
}

/// Persist the active tab so a reload returns to it.
fn save_tab(t: Tab) {
    if let Some(ls) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = ls.set_item("rucio-tab", t.as_str());
    }
}

/// The navigation sections, shown as top-bar tabs (wide) or sidebar items
/// (narrow). One source so both stay in sync.
const TABS: [Tab; 6] = [
    Tab::Downloads,
    Tab::Uploads,
    Tab::Searches,
    Tab::Shares,
    Tab::Pins,
    Tab::Subscriptions,
];

#[derive(Clone, Copy, PartialEq)]
pub enum Panel {
    NodeStatus,
    Addresses,
    Peers,
    Stats,
    About,
    Notifications,
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

#[allow(clippy::too_many_arguments)]
fn start_ws_loop(
    ws_connected: RwSignal<bool>,
    downloads: RwSignal<Vec<DownloadResponse>>,
    uploads: RwSignal<Vec<ActiveUpload>>,
    status: RwSignal<Option<StatusResponse>>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    search: SearchStore,
    indexing: RwSignal<usize>,
    notifs: RwSignal<Vec<Notification>>,
    unread: RwSignal<i64>,
) {
    spawn_local(async move {
        // Reconnect backoff. The daemon binds to loopback, so retries are cheap;
        // grow gently from 500 ms to a 5 s ceiling. This is reset to the floor
        // only on a *genuine* connection (first frame received), not on
        // WebSocket::open() — which almost always returns Ok before the TCP
        // connect resolves, so resetting there would defeat the backoff and
        // hammer a starting/stopped daemon (and its console) once per second.
        const BACKOFF_MIN_MS: u64 = 500;
        const BACKOFF_MAX_MS: u64 = 5_000;
        let mut backoff_ms = BACKOFF_MIN_MS;

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
                    backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                    continue;
                }
                Ok(ws) => {
                    // Connected/green is set on the first message, not here —
                    // WebSocket::open() only creates the JS object; the TCP
                    // handshake hasn't completed yet, so treating Ok as connected
                    // would make failed reconnection attempts appear connected.
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
                                    // Genuine connection: reset the backoff floor.
                                    backoff_ms = BACKOFF_MIN_MS;
                                }
                                if let Ok(Message::Text(text)) = msg
                                    && let Ok(event) = serde_json::from_str::<WsEvent>(&text)
                                {
                                    handle_event(
                                        event, downloads, uploads, status, dl_speed, ul_speed,
                                        search, indexing, notifs, unread,
                                    );
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
            backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn handle_event(
    event: WsEvent,
    downloads: RwSignal<Vec<DownloadResponse>>,
    uploads: RwSignal<Vec<ActiveUpload>>,
    status: RwSignal<Option<StatusResponse>>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    search: SearchStore,
    indexing: RwSignal<usize>,
    notifs: RwSignal<Vec<Notification>>,
    unread: RwSignal<i64>,
) {
    match event {
        WsEvent::UploadProgress(list) => {
            // Volatile, full-snapshot stream: replace wholesale (the daemon
            // sends one empty list when the last upload ends, clearing the tab).
            uploads.set(list);
        }
        WsEvent::DownloadProgress(list) => {
            // The daemon only streams *active* downloads. Merge into the existing
            // list so completed/paused/cancelled rows don't disappear.
            let incoming: HashSet<i64> = list.iter().map(|d| d.id).collect();

            // A download present in the stream is live again, so drop its
            // "already refreshed" mark. Without this, a download that left the
            // stream once (e.g. paused → refreshed) and then came back (resumed)
            // would keep its mark forever, so its eventual completion would be
            // skipped by the guard below and the row would stay stuck at its last
            // streamed state (e.g. Downloading 100%) instead of flipping to
            // Completed.
            if !incoming.is_empty() {
                REFRESHED_IDS.with(|s| s.borrow_mut().retain(|id| !incoming.contains(id)));
            }

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

        // Live search results (Rucio + eMule) carry their owning search_id.
        WsEvent::SearchResult { search_id, result } => {
            // Drop results for a search that's been deleted (gone from the list)
            // or cancelled. A fast search leaves a backlog of already-broadcast
            // results draining over the WebSocket for a few seconds after the
            // user cancels; without this guard `or_default()` would re-create the
            // entry and they'd reappear. A just-started search is already in the
            // list (Running) before results stream in, and load_detail() backfills
            // any raced early ones, so this never drops legitimate results.
            let accept = search.list.with_untracked(|list| {
                list.iter()
                    .any(|s| s.id == search_id && s.state != SearchState::Cancelled)
            });
            if accept {
                let mut added = false;
                search.results.update(|m| {
                    let v = m.entry(search_id).or_default();
                    // Merged results re-arrive with the same result_id and an
                    // updated provider/peer count: replace in place so the
                    // source count updates live. A genuinely new id is appended.
                    if let Some(existing) = v.iter_mut().find(|r| r.result_id == result.result_id) {
                        *existing = result;
                    } else {
                        v.push(result);
                        added = true;
                    }
                });
                if added {
                    search.list.update(|list| {
                        if let Some(s) = list.iter_mut().find(|s| s.id == search_id) {
                            s.result_count += 1;
                        }
                    });
                }
            }
        }
        // Lifecycle transition (e.g. window closed → done) with authoritative count.
        WsEvent::SearchStateChanged {
            id,
            state,
            result_count,
            emule_queued,
        } => {
            search.list.update(|list| {
                if let Some(s) = list.iter_mut().find(|s| s.id == id) {
                    s.state = state;
                    s.result_count = result_count;
                    s.emule_queued = emule_queued;
                }
            });
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

        WsEvent::IndexingCount { pending } => {
            indexing.set(pending);
        }

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

        WsEvent::Notification(n) => {
            if !n.read {
                unread.update(|u| *u += 1);
            }
            notifs.update(|list| {
                list.insert(0, n);
                list.truncate(200);
            });
        }

        // Liveness keepalive — receiving it already flipped the connection
        // indicator to connected in the WS loop; nothing else to do.
        WsEvent::Ping => {}
    }
}

// ── App ───────────────────────────────────────────────────────────────────────

#[component]
fn App() -> impl IntoView {
    let active_tab: RwSignal<Tab> = RwSignal::new(load_tab());
    // Persist the active tab so a page reload returns to it.
    Effect::new(move |_| save_tab(active_tab.get()));
    let menu_open: RwSignal<bool> = RwSignal::new(false);
    // Navigation drawer (shown via the hamburger on narrow screens).
    let nav_open: RwSignal<bool> = RwSignal::new(false);
    let active_panel: RwSignal<Option<Panel>> = RwSignal::new(None);

    // Theme — apply immediately so the DOM reflects any stored preference.
    let initial_theme = load_theme();
    apply_theme(initial_theme);
    let theme: RwSignal<Theme> = RwSignal::new(initial_theme);
    // Reflects the stored language choice; changing it persists and reloads.
    let lang: RwSignal<Language> = RwSignal::new(load_language());

    let ws_connected: RwSignal<bool> = RwSignal::new(false);
    let status: RwSignal<Option<StatusResponse>> = RwSignal::new(None);
    let downloads: RwSignal<Vec<DownloadResponse>> = RwSignal::new(vec![]);
    // Download categories, shared so the listing can badge each download and the
    // add/detail dialogs can offer them. Reloaded after edits in Settings.
    let categories: RwSignal<Vec<Category>> = RwSignal::new(vec![]);
    // Peers currently downloading from us (driven by the WS UploadProgress).
    let uploads: RwSignal<Vec<ActiveUpload>> = RwSignal::new(vec![]);
    // Number of files currently being indexed (driven by the WS IndexingCount).
    let indexing: RwSignal<usize> = RwSignal::new(0);
    let dl_speed: RwSignal<u64> = RwSignal::new(0);
    let ul_speed: RwSignal<u64> = RwSignal::new(0);
    let search = SearchStore {
        list: RwSignal::new(vec![]),
        results: RwSignal::new(HashMap::new()),
        selected: RwSignal::new(None),
    };
    // Whether the temporary speed limit is engaged (runtime-only on the daemon).
    let temp_limit: RwSignal<bool> = RwSignal::new(false);
    // Base (normal) caps shown in the menu dropdowns, KB/s (0 = unlimited).
    let base_up: RwSignal<u64> = RwSignal::new(0);
    let base_down: RwSignal<u64> = RwSignal::new(0);
    // Preset temporary caps, for the read-only line under the toggle.
    let temp_up: RwSignal<u64> = RwSignal::new(0);
    // Full configuration modal open/closed.
    let config_open: RwSignal<bool> = RwSignal::new(false);
    let temp_down: RwSignal<u64> = RwSignal::new(0);
    // Notification centre: the list (newest first) and the unread badge count.
    let notifications: RwSignal<Vec<Notification>> = RwSignal::new(vec![]);
    let unread: RwSignal<i64> = RwSignal::new(0);
    // Master notification switch, shared with the config modal: when off, the
    // user wants nothing to do with notifications, so the bell is hidden.
    let notif_enabled: RwSignal<bool> = RwSignal::new(true);

    // PWA protocol handler: when launched via an `ed2k:` link (manifest
    // `protocol_handlers`), the app opens at `/?handle=<link>`. Add it as a
    // download, jump to the Downloads tab, and scrub the query so a refresh
    // doesn't re-add it.
    if let Some(link) = web_sys::window()
        .and_then(|w| w.location().search().ok())
        .and_then(|s| web_sys::UrlSearchParams::new_with_str(&s).ok())
        .and_then(|p| p.get("handle"))
        .filter(|l| !l.trim().is_empty())
    {
        active_tab.set(Tab::Downloads);
        if let Some(hist) = web_sys::window().and_then(|w| w.history().ok()) {
            let _ = hist.replace_state_with_url(&wasm_bindgen::JsValue::NULL, "", Some("/"));
        }
        spawn_local(async move {
            // Launched via a protocol handler — no category chosen; let the
            // daemon's keyword auto-match decide.
            api_add_links(link, downloads, None).await;
        });
    }

    // Initial data fetch.
    spawn_local(async move {
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/status").send().await
            && let Ok(s) = r.json::<StatusResponse>().await
        {
            status.set(Some(s));
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
        // Seed the Uploads tab so it shows current activity immediately; the WS
        // UploadProgress stream keeps it live thereafter.
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/uploads").send().await
            && let Ok(s) = r.json::<UploadsResponse>().await
        {
            uploads.set(s.uploads);
        }
        // Seed the notification centre and bell badge from persisted history.
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/notifications")
            .send()
            .await
            && let Ok(s) = r.json::<NotificationList>().await
        {
            notifications.set(s.items);
            unread.set(s.unread);
        }
        // Master switch, so the bell can hide when notifications are off.
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/config/notifications")
            .send()
            .await
            && let Ok(s) = r.json::<NotificationSettings>().await
        {
            notif_enabled.set(s.enabled);
        }
        // Categories, for badges in the download list and the add/detail dialogs.
        if let Ok(r) = gloo_net::http::Request::get("/api/v1/categories")
            .send()
            .await
            && let Ok(s) = r.json::<CategoriesResponse>().await
        {
            categories.set(s.categories);
        }
    });

    // Start the persistent WebSocket loop.
    start_ws_loop(
        ws_connected,
        downloads,
        uploads,
        status,
        dl_speed,
        ul_speed,
        search,
        indexing,
        notifications,
        unread,
    );

    view! {
        <div class="layout">
            <header class="topbar">
                // Navigation hamburger — only shown on narrow screens (CSS).
                <button
                    class="nav-toggle"
                    title=t!("nav.sections")
                    on:click=move |_| nav_open.set(true)
                >
                    <icons::Icon paths=icons::MENU/>
                </button>
                <span class="brand">"Rucio"</span>

                <nav class="tabs">
                    {TABS.iter().map(|&tab| view! {
                        <button
                            class=move || if active_tab.get() == tab { "tab active" } else { "tab" }
                            on:click=move |_| active_tab.set(tab)
                        >{tab_label(tab)}</button>
                    }).collect_view()}
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
                                title=if connected { t!("nav.connected").to_string() } else { t!("nav.disconnected").to_string() }
                                inner_html=if connected { icons::NETWORK } else { icons::NETWORK_OFF }
                            ></svg>
                        }
                    }}

                    // Notification bell with unread badge. Hidden entirely when
                    // the user has switched notifications off — they want none.
                    <Show when=move || notif_enabled.get()>
                        <button
                            class="bell-btn"
                            title=t!("nav.notifications")
                            on:click=move |_| {
                                active_panel.set(Some(Panel::Notifications));
                                // Opening the centre marks everything read.
                                if unread.get_untracked() > 0 {
                                    unread.set(0);
                                    notifications.update(|list| {
                                        for n in list.iter_mut() {
                                            n.read = true;
                                        }
                                    });
                                    spawn_local(async move {
                                        let _ = gloo_net::http::Request::post(
                                            "/api/v1/notifications/read",
                                        )
                                        .send()
                                        .await;
                                    });
                                }
                            }
                        >
                            <icons::Icon paths=icons::BELL/>
                            <Show when=move || { unread.get() > 0 }>
                                <span class="bell-badge">
                                    {move || {
                                        let u = unread.get();
                                        if u > 99 { "99+".to_string() } else { u.to_string() }
                                    }}
                                </span>
                            </Show>
                        </button>
                    </Show>

                    <button
                        class="menu-btn"
                        on:click=move |_| menu_open.update(|o| *o = !*o)
                    >
                        <icons::Icon paths=icons::MENU/>
                    </button>

                    <Show when=move || menu_open.get()>
                        <div class="dropdown">
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
                            // ── Actions (bulk operations on all downloads) ─
                            <div class="menu-section">
                                <div class="menu-section-title">"Actions"</div>
                                <button
                                    class="dropdown-item"
                                    disabled=move || !downloads.with(|v| any_pausable(v))
                                    on:click=move |_| {
                                        menu_open.set(false);
                                        spawn_local(pause_all(downloads));
                                    }
                                >"Pause all"</button>
                                <button
                                    class="dropdown-item"
                                    disabled=move || !downloads.with(|v| any_paused(v))
                                    on:click=move |_| {
                                        menu_open.set(false);
                                        spawn_local(resume_all(downloads));
                                    }
                                >"Resume all"</button>
                                <button
                                    class="dropdown-item"
                                    disabled=move || !downloads.with(|v| any_terminal(v))
                                    on:click=move |_| {
                                    // Destructive (removes finished rows): confirm first.
                                    let ok = web_sys::window()
                                        .and_then(|w| w.confirm_with_message(
                                            "Clear all finished downloads from the history? Files on disk are kept.",
                                        ).ok())
                                        .unwrap_or(false);
                                    if ok {
                                        menu_open.set(false);
                                        spawn_local(clear_history(downloads));
                                    }
                                }>"Clear history"</button>
                            </div>
                            <div class="dropdown-sep"/>
                            // ── Node (read-only info panels) ──────────────
                            <div class="menu-section">
                                <div class="menu-section-title">"Node"</div>
                                <button class="dropdown-item" on:click=move |_| {
                                    active_panel.set(Some(Panel::NodeStatus));
                                    menu_open.set(false);
                                }>"Node status"</button>
                                <button class="dropdown-item" on:click=move |_| {
                                    active_panel.set(Some(Panel::Addresses));
                                    menu_open.set(false);
                                }>"Addresses"</button>
                                <button class="dropdown-item" on:click=move |_| {
                                    active_panel.set(Some(Panel::Peers));
                                    menu_open.set(false);
                                }>"Peers"</button>
                                <button class="dropdown-item" on:click=move |_| {
                                    active_panel.set(Some(Panel::Stats));
                                    menu_open.set(false);
                                }>"Statistics"</button>
                            </div>
                            <div class="dropdown-sep"/>
                            <button class="dropdown-item" on:click=move |_| {
                                config_open.set(true);
                                menu_open.set(false);
                            }>"Settings"</button>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::About));
                                menu_open.set(false);
                            }>"About"</button>
                        </div>
                    </Show>
                </div>
            </header>

            <main class="content">
                {move || match active_tab.get() {
                    Tab::Downloads => view! {
                        <DownloadsTab
                            downloads=downloads
                            categories=categories
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Uploads => view! {
                        <UploadsTab
                            uploads=uploads
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Searches => view! {
                        <SearchesTab
                            search=search
                            downloads=downloads
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Shares => view! {
                        <SharesTab
                            indexing=indexing
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Pins => view! {
                        <PinsTab
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                    Tab::Subscriptions => view! {
                        <SubscriptionsTab
                            dl_speed=dl_speed
                            ul_speed=ul_speed
                            temp_limit=temp_limit
                        />
                    }.into_any(),
                }}
            </main>
        </div>

        // ── Navigation drawer (narrow screens) ────────────────────────────
        <Show when=move || nav_open.get()>
            <div class="sidebar-backdrop" on:click=move |_| nav_open.set(false)>
                <nav class="sidebar" on:click=move |e| e.stop_propagation()>
                    <div class="sidebar-logo">"Rucio"</div>
                    <div class="sidebar-sep"/>
                    {TABS.iter().map(|&tab| view! {
                        <button
                            class=move || if active_tab.get() == tab {
                                "sidebar-item active"
                            } else {
                                "sidebar-item"
                            }
                            on:click=move |_| {
                                active_tab.set(tab);
                                nav_open.set(false);
                            }
                        >{tab_label(tab)}</button>
                    }).collect_view()}
                </nav>
            </div>
        </Show>

        {move || active_panel.get().map(|panel| match panel {
            Panel::NodeStatus => view! {
                <NodeStatusPanel status=status active_panel=active_panel/>
            }.into_any(),
            Panel::Addresses => view! {
                <AddressesPanel status=status active_panel=active_panel/>
            }
            .into_any(),
            Panel::Peers => view! {
                <PeersPanel active_panel=active_panel/>
            }.into_any(),
            Panel::Stats => view! {
                <StatsPanel active_panel=active_panel/>
            }.into_any(),
            Panel::About => view! {
                <AboutPanel active_panel=active_panel/>
            }.into_any(),
            Panel::Notifications => view! {
                <NotificationsPanel
                    notifications=notifications
                    active_panel=active_panel
                />
            }.into_any(),
        })}

        <Show when=move || config_open.get()>
            <ConfigModal
                base_up=base_up
                base_down=base_down
                temp_up=temp_up
                temp_down=temp_down
                notif_enabled=notif_enabled
                categories=categories
                theme=theme
                lang=lang
                on_close=move || config_open.set(false)
            />
        </Show>
    }
}

fn main() {
    rust_i18n::set_locale(&resolve_locale());
    mount_to_body(App);
}
