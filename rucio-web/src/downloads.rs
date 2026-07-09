use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use leptos::prelude::*;
use leptos::task::spawn_local;
use rust_i18n::t;

use gloo_timers::future::sleep;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{
    Category, DownloadDetailResponse, DownloadPiecesResponse, DownloadPriority, DownloadResponse,
    DownloadState, NEUTRAL_CATEGORY_COLOR, PieceState, RenameDownloadRequest, contrast_text,
    format_eta, format_size, format_speed, is_streamed_state,
};

// ── Filter ────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum FilterState {
    All,
    Active, // FindingProviders | Queued | Downloading | Stalled
    Downloading,
    Paused,
    Completed,
    History, // Completed | Failed | Cancelled
}

impl FilterState {
    fn matches(self, s: &DownloadState) -> bool {
        match self {
            FilterState::All => true,
            FilterState::Active => is_streamed_state(s),
            FilterState::Downloading => *s == DownloadState::Downloading,
            FilterState::Paused => *s == DownloadState::Paused,
            FilterState::Completed => *s == DownloadState::Completed,
            FilterState::History => matches!(
                s,
                DownloadState::Completed | DownloadState::Failed | DownloadState::Cancelled
            ),
        }
    }

    /// Stable key for the `<select>` option value and localStorage.
    fn as_key(self) -> &'static str {
        match self {
            FilterState::All => "all",
            FilterState::Active => "active",
            FilterState::Downloading => "downloading",
            FilterState::Paused => "paused",
            FilterState::Completed => "completed",
            FilterState::History => "history",
        }
    }

    /// Parse a key back; unknown values fall back to `All`.
    fn from_key(v: &str) -> Self {
        match v {
            "active" => FilterState::Active,
            "downloading" => FilterState::Downloading,
            "paused" => FilterState::Paused,
            "completed" => FilterState::Completed,
            "history" => FilterState::History,
            _ => FilterState::All,
        }
    }
}

/// Category filter for the download list.
#[derive(Clone, Copy, PartialEq)]
enum CatFilter {
    All,
    Uncategorized,
    Id(i64),
}

impl CatFilter {
    fn matches(self, category_id: Option<i64>) -> bool {
        match self {
            CatFilter::All => true,
            CatFilter::Uncategorized => category_id.is_none(),
            CatFilter::Id(id) => category_id == Some(id),
        }
    }

    /// Parse the `<select>` value: "" = all, "none" = uncategorized, else an id.
    fn from_value(v: &str) -> Self {
        match v {
            "none" => CatFilter::Uncategorized,
            _ => match v.parse::<i64>() {
                Ok(id) => CatFilter::Id(id),
                Err(_) => CatFilter::All,
            },
        }
    }

    /// Inverse of [`CatFilter::from_value`], for the `<select>` value and
    /// localStorage: "" = all, "none" = uncategorized, else the id.
    fn to_value(self) -> String {
        match self {
            CatFilter::All => String::new(),
            CatFilter::Uncategorized => "none".to_string(),
            CatFilter::Id(id) => id.to_string(),
        }
    }
}

// ── Filter persistence ──────────────────────────────────────────────────────

/// localStorage keys for the persisted download filters (state + category),
/// kept across reloads like the active tab.
const FILTER_STATE_KEY: &str = "rucio-dl-filter";
const FILTER_CAT_KEY: &str = "rucio-dl-cat";

fn ls() -> Option<web_sys::Storage> {
    web_sys::window().and_then(|w| w.local_storage().ok().flatten())
}

fn load_filter(key: &str) -> Option<String> {
    ls().and_then(|s| s.get_item(key).ok().flatten())
}

fn save_filter(key: &str, val: &str) {
    if let Some(s) = ls() {
        let _ = s.set_item(key, val);
    }
}

// ── State helpers ─────────────────────────────────────────────────────────────

fn state_label(s: &DownloadState) -> std::borrow::Cow<'static, str> {
    match s {
        DownloadState::FindingProviders => t!("download.state.finding_providers"),
        DownloadState::Queued => t!("download.state.queued"),
        DownloadState::Downloading => t!("download.state.downloading"),
        DownloadState::Stalled => t!("download.state.stalled"),
        DownloadState::Paused => t!("download.state.paused"),
        DownloadState::Completed => t!("download.state.completed"),
        DownloadState::Failed => t!("download.state.failed"),
        DownloadState::Cancelled => t!("download.state.cancelled"),
    }
}

fn state_css(s: &DownloadState) -> &'static str {
    match s {
        DownloadState::Downloading => "dl-state dl-state-active",
        DownloadState::Completed => "dl-state dl-state-done",
        DownloadState::Failed => "dl-state dl-state-failed",
        DownloadState::Stalled => "dl-state dl-state-stalled",
        DownloadState::Paused => "dl-state dl-state-paused",
        _ => "dl-state dl-state-neutral",
    }
}

fn is_terminal(s: &DownloadState) -> bool {
    matches!(
        s,
        DownloadState::Completed | DownloadState::Failed | DownloadState::Cancelled
    )
}

/// States from which a download can be paused (active, non-terminal).
fn is_pausable(s: &DownloadState) -> bool {
    matches!(
        s,
        DownloadState::FindingProviders
            | DownloadState::Queued
            | DownloadState::Downloading
            | DownloadState::Stalled
    )
}

// ── API calls ─────────────────────────────────────────────────────────────────

pub async fn refresh_downloads(downloads: RwSignal<Vec<DownloadResponse>>) {
    if let Ok(resp) = gloo_net::http::Request::get(&crate::api::api("/api/v1/downloads"))
        .send()
        .await
        && let Ok(body) = resp.json::<crate::types::DownloadsResponse>().await
        && downloads.with_untracked(|cur| cur != &body.downloads)
    {
        downloads.set(body.downloads);
    }
}

async fn api_cancel(id: i64) {
    let _ =
        gloo_net::http::Request::post(&crate::api::api(&format!("/api/v1/downloads/{id}/cancel")))
            .send()
            .await;
}

async fn api_remove(id: i64) {
    let _ = gloo_net::http::Request::delete(&crate::api::api(&format!("/api/v1/downloads/{id}")))
        .send()
        .await;
}

async fn api_pause(id: i64) {
    let _ =
        gloo_net::http::Request::post(&crate::api::api(&format!("/api/v1/downloads/{id}/pause")))
            .send()
            .await;
}

/// Rename an in-progress download. Returns `true` on success (HTTP 2xx).
async fn api_rename(id: i64, name: String) -> bool {
    let body = RenameDownloadRequest { name };
    if let Ok(req) =
        gloo_net::http::Request::post(&crate::api::api(&format!("/api/v1/downloads/{id}/rename")))
            .json(&body)
        && let Ok(resp) = req.send().await
    {
        return resp.ok();
    }
    false
}

async fn api_resume(id: i64) {
    let _ =
        gloo_net::http::Request::post(&crate::api::api(&format!("/api/v1/downloads/{id}/resume")))
            .send()
            .await;
}

// ── Bulk actions (used by the menu) ─────────────────────────────────────────
//
// These iterate the current download list client-side, reusing the per-id
// endpoints (same approach as the multi-selection toolbar). Fine for the
// occasional manual action; if histories ever get huge, a bulk endpoint would
// be more efficient.

/// Pause every active (pausable) download.
pub async fn pause_all(downloads: RwSignal<Vec<DownloadResponse>>) {
    let ids: Vec<i64> = downloads
        .get_untracked()
        .iter()
        .filter(|d| is_pausable(&d.state))
        .map(|d| d.id)
        .collect();
    for id in ids {
        api_pause(id).await;
    }
    refresh_downloads(downloads).await;
}

/// Resume every paused download.
pub async fn resume_all(downloads: RwSignal<Vec<DownloadResponse>>) {
    let ids: Vec<i64> = downloads
        .get_untracked()
        .iter()
        .filter(|d| d.state == DownloadState::Paused)
        .map(|d| d.id)
        .collect();
    for id in ids {
        api_resume(id).await;
    }
    refresh_downloads(downloads).await;
}

/// Remove every finished (completed/failed/cancelled) download from the history
/// in a single request. Files already on disk are kept.
pub async fn clear_history(downloads: RwSignal<Vec<DownloadResponse>>) {
    let _ = gloo_net::http::Request::delete(&crate::api::api("/api/v1/downloads/history"))
        .send()
        .await;
    refresh_downloads(downloads).await;
}

// Predicates for enabling/disabling the bulk menu actions.

/// True if any download can be paused (active/non-terminal).
pub fn any_pausable(list: &[DownloadResponse]) -> bool {
    list.iter().any(|d| is_pausable(&d.state))
}

/// True if any download is currently paused (and could be resumed).
pub fn any_paused(list: &[DownloadResponse]) -> bool {
    list.iter().any(|d| d.state == DownloadState::Paused)
}

/// True if any finished download exists (and could be cleared from history).
pub fn any_terminal(list: &[DownloadResponse]) -> bool {
    list.iter().any(|d| is_terminal(&d.state))
}

async fn api_fetch_detail(id: i64) -> Option<DownloadDetailResponse> {
    gloo_net::http::Request::get(&crate::api::api(&format!("/api/v1/downloads/{id}")))
        .send()
        .await
        .ok()?
        .json::<DownloadDetailResponse>()
        .await
        .ok()
}

async fn api_fetch_pieces(id: i64) -> Option<DownloadPiecesResponse> {
    gloo_net::http::Request::get(&crate::api::api(&format!("/api/v1/downloads/{id}/pieces")))
        .send()
        .await
        .ok()?
        .json::<DownloadPiecesResponse>()
        .await
        .ok()
}

/// Add each non-empty line as a download. The valid links go through; the
/// returned vec holds the lines we could *not* accept — an unrecognised scheme,
/// or a link the daemon rejected (e.g. a malformed magnet) — so the caller can
/// report exactly which ones failed without blocking the good ones.
/// Outcome of adding one link.
enum LinkOutcome {
    /// Queued (202).
    Accepted,
    /// Already present (409) — the message says where (shares / downloading / …).
    Duplicate(String),
    /// Not a valid link, or the daemon refused it.
    Rejected,
}

/// Result of adding a batch: lines we couldn't accept (to keep in the box and
/// fix) and human messages for links already present (informational).
pub struct AddOutcome {
    pub rejected: Vec<String>,
    pub duplicates: Vec<String>,
}

pub async fn api_add_links(
    text: String,
    downloads: RwSignal<Vec<DownloadResponse>>,
    category_id: Option<i64>,
) -> AddOutcome {
    let mut rejected = Vec::new();
    let mut duplicates = Vec::new();
    for line in text.lines() {
        let link = line.trim();
        if link.is_empty() {
            continue;
        }
        let outcome = if link.starts_with("ed2k://") {
            let mut body = serde_json::json!({ "link": link });
            if let Some(c) = category_id {
                body["category_id"] = c.into();
            }
            post_link(&crate::api::api("/api/v1/downloads/ed2k"), &body).await
        } else if link.starts_with("rucio:") {
            let mut body = serde_json::json!({ "magnet": link, "providers": [] });
            if let Some(c) = category_id {
                body["category_id"] = c.into();
            }
            post_link(&crate::api::api("/api/v1/downloads"), &body).await
        } else {
            // Not a rucio: or ed2k:// link — don't even send it.
            LinkOutcome::Rejected
        };
        match outcome {
            LinkOutcome::Accepted => {}
            LinkOutcome::Duplicate(msg) => duplicates.push(msg),
            LinkOutcome::Rejected => rejected.push(link.to_string()),
        }
    }
    refresh_downloads(downloads).await;
    AddOutcome {
        rejected,
        duplicates,
    }
}

/// POST `body` and classify the daemon's answer: `202` queued, `409` already
/// present (with the daemon's "you already have it in X" message), anything
/// else a rejection.
async fn post_link(url: &str, body: &serde_json::Value) -> LinkOutcome {
    let Ok(req) = gloo_net::http::Request::post(url).json(body) else {
        return LinkOutcome::Rejected;
    };
    match req.send().await {
        Ok(resp) if resp.status() == 202 => LinkOutcome::Accepted,
        Ok(resp) if resp.status() == 409 => {
            let msg = resp
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or_else(|| t!("download.already_have").to_string());
            LinkOutcome::Duplicate(msg)
        }
        _ => LinkOutcome::Rejected,
    }
}

// ── Tab ───────────────────────────────────────────────────────────────────────

#[component]
pub fn DownloadsTab(
    downloads: RwSignal<Vec<DownloadResponse>>,
    categories: RwSignal<Vec<Category>>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    // Re-sync the full list when the tab is opened. The WS only streams *active*
    // downloads, so terminal rows deleted out-of-band — e.g. a completed mirror
    // evicted when its subscription was removed — would otherwise linger in the
    // cached list until a manual refresh.
    spawn_local(async move { refresh_downloads(downloads).await });

    // Multi-selection: the set of selected ids, plus the anchor row used as the
    // pivot for shift+click range selection.
    let selected_ids: RwSignal<HashSet<i64>> = RwSignal::new(HashSet::new());
    let anchor: RwSignal<Option<i64>> = RwSignal::new(None);
    let add_open: RwSignal<bool> = RwSignal::new(false);
    let detail: RwSignal<Option<DownloadDetailResponse>> = RwSignal::new(None);
    // Open flag for the multi-selection bulk-edit modal (category + priority).
    let bulk_open: RwSignal<bool> = RwSignal::new(false);

    // State and category filters persist across reloads (like the active tab);
    // the name search stays transient. Restore them from localStorage.
    let filter_state: RwSignal<FilterState> = RwSignal::new(
        load_filter(FILTER_STATE_KEY)
            .map(|s| FilterState::from_key(&s))
            .unwrap_or(FilterState::All),
    );
    let filter_name: RwSignal<String> = RwSignal::new(String::new());
    let filter_cat: RwSignal<CatFilter> = RwSignal::new(
        load_filter(FILTER_CAT_KEY)
            .map(|s| CatFilter::from_value(&s))
            .unwrap_or(CatFilter::All),
    );

    // If the selected category is later deleted, fall back to "all" so the user
    // isn't stranded on an empty view they can't change (the category picker
    // hides when there are none). The `loaded` latch — carried as the effect's
    // previous return value — avoids resetting during the initial fetch, when
    // `categories` is briefly empty before it arrives.
    Effect::new(move |loaded: Option<bool>| {
        let loaded = loaded.unwrap_or(false) || categories.with(|c| !c.is_empty());
        if loaded
            && let CatFilter::Id(id) = filter_cat.get_untracked()
            && categories.with(|c| !c.iter().any(|x| x.id == id))
        {
            filter_cat.set(CatFilter::All);
            save_filter(FILTER_CAT_KEY, &CatFilter::All.to_value());
        }
        loaded
    });

    // The DownloadResponses currently selected.
    let selected_dls = move || {
        downloads.with(|v| {
            v.iter()
                .filter(|d| selected_ids.with(|s| s.contains(&d.id)))
                .cloned()
                .collect::<Vec<_>>()
        })
    };

    // Enabled with any selection: one row opens the detail panel, several open
    // the bulk-edit modal (category + priority).
    let can_info = move || !selected_ids.with(|s| s.is_empty());
    // Cancel/remove act on whichever selected rows qualify.
    let can_cancel = move || selected_dls().iter().any(|d| !is_terminal(&d.state));
    let can_remove = move || selected_dls().iter().any(|d| is_terminal(&d.state));
    let any_active = move || selected_dls().iter().any(|d| is_pausable(&d.state));
    let any_paused = move || {
        selected_dls()
            .iter()
            .any(|d| d.state == DownloadState::Paused)
    };
    // The toggle resumes only when nothing is active and something is paused;
    // otherwise it pauses (the common case for a mixed selection).
    let show_resume = move || !any_active() && any_paused();
    let can_pause_toggle = move || any_active() || any_paused();

    // Visible (filtered) ids in display order — used by the list and by
    // shift+click to resolve the range between the anchor and the clicked row.
    let visible_ids = move || {
        let q = filter_name.get().to_lowercase();
        let fs = filter_state.get();
        downloads.with(|v| {
            v.iter()
                .filter(|d| fs.matches(&d.state))
                .filter(|d| {
                    q.is_empty() || d.name.as_deref().unwrap_or("").to_lowercase().contains(&q)
                })
                .map(|d| d.id)
                .collect::<Vec<i64>>()
        })
    };

    // Row click with modifier keys: plain = select only this row; ctrl/⌘ =
    // toggle this row; shift = select the range from the anchor to this row.
    let on_row_click = Callback::new(move |(id, additive, range): (i64, bool, bool)| {
        if range && let Some(a) = anchor.get_untracked() {
            let vis = visible_ids();
            if let (Some(i1), Some(i2)) = (
                vis.iter().position(|&x| x == a),
                vis.iter().position(|&x| x == id),
            ) {
                let (lo, hi) = if i1 <= i2 { (i1, i2) } else { (i2, i1) };
                selected_ids.set(vis[lo..=hi].iter().copied().collect());
                return;
            }
        }
        if additive {
            selected_ids.update(|s| {
                if !s.insert(id) {
                    s.remove(&id);
                }
            });
        } else {
            selected_ids.set(HashSet::from([id]));
        }
        anchor.set(Some(id));
    });

    view! {
        <div class="tab-content">
            // ── Toolbar ───────────────────────────────────────────────────
            <div class="tab-toolbar">
            <div class="dl-toolbar">
                <button
                    class="toolbar-btn"
                    title=t!("download.toolbar.add_title")
                    on:click=move |_| add_open.set(true)
                >
                    <Icon paths=icons::PLUS/>
                    <span class="btn-label">{t!("download.toolbar.add")}</span>
                </button>
                <button
                    class="toolbar-btn"
                    title=t!("download.toolbar.info_title")
                    disabled=move || !can_info()
                    on:click=move |_| {
                        let ids: Vec<i64> = selected_ids.with(|s| s.iter().copied().collect());
                        match ids[..] {
                            [id] => {
                                // Single selection: full detail panel.
                                spawn_local(async move {
                                    if let Some(d) = api_fetch_detail(id).await {
                                        detail.set(Some(d));
                                    }
                                });
                            }
                            [_, _, ..] => bulk_open.set(true), // several: bulk edit
                            [] => {}
                        }
                    }
                >
                    <Icon paths=icons::INFO_CIRCLE/>
                    <span class="btn-label">{t!("download.toolbar.info")}</span>
                </button>
                <button
                    class="toolbar-btn"
                    title=move || if show_resume() { t!("download.toolbar.resume_title").to_string() } else { t!("download.toolbar.pause_title").to_string() }
                    disabled=move || !can_pause_toggle()
                    on:click=move |_| {
                        let resume = show_resume();
                        let targets: Vec<i64> = selected_dls()
                            .into_iter()
                            .filter(|d| {
                                if resume {
                                    d.state == DownloadState::Paused
                                } else {
                                    is_pausable(&d.state)
                                }
                            })
                            .map(|d| d.id)
                            .collect();
                        spawn_local(async move {
                            for id in targets {
                                if resume {
                                    api_resume(id).await;
                                } else {
                                    api_pause(id).await;
                                }
                            }
                            refresh_downloads(downloads).await;
                        });
                    }
                >
                    <Show when=show_resume fallback=|| view! { <Icon paths=icons::PLAYER_PAUSE/> }>
                        <Icon paths=icons::PLAYER_PLAY/>
                    </Show>
                    <span class="btn-label">
                        {move || if show_resume() { t!("download.toolbar.resume").to_string() } else { t!("download.toolbar.pause").to_string() }}
                    </span>
                </button>
                <button
                    class="toolbar-btn toolbar-btn-danger"
                    title=t!("download.toolbar.cancel_title")
                    disabled=move || !can_cancel()
                    on:click=move |_| {
                        let targets: Vec<i64> = selected_dls()
                            .into_iter()
                            .filter(|d| !is_terminal(&d.state))
                            .map(|d| d.id)
                            .collect();
                        spawn_local(async move {
                            for id in targets {
                                api_cancel(id).await;
                            }
                            selected_ids.set(HashSet::new());
                            refresh_downloads(downloads).await;
                        });
                    }
                >
                    <Icon paths=icons::CIRCLE_X/>
                    <span class="btn-label">{t!("download.toolbar.cancel")}</span>
                </button>
                <button
                    class="toolbar-btn"
                    title=t!("download.toolbar.clear_title")
                    disabled=move || !can_remove()
                    on:click=move |_| {
                        let targets: Vec<i64> = selected_dls()
                            .into_iter()
                            .filter(|d| is_terminal(&d.state))
                            .map(|d| d.id)
                            .collect();
                        spawn_local(async move {
                            for id in targets {
                                api_remove(id).await;
                            }
                            selected_ids.set(HashSet::new());
                            refresh_downloads(downloads).await;
                        });
                    }
                >
                    <Icon paths=icons::TRASH/>
                    <span class="btn-label">{t!("download.toolbar.clear")}</span>
                </button>
            </div>
            </div>

            // ── Download list ─────────────────────────────────────────────
            <div class="tab-scroll">
                <Show
                    when=move || !downloads.get().is_empty()
                    fallback=|| view! { <div class="empty-state"><p>{t!("download.none")}</p></div> }
                >
                    <ul class="dl-list">
                        <For
                            each=move || {
                                let q = filter_name.get().to_lowercase();
                                let fs = filter_state.get();
                                let fc = filter_cat.get();
                                downloads.with(|v| {
                                    v.iter()
                                        .filter(|d| fs.matches(&d.state))
                                        .filter(|d| fc.matches(d.category_id))
                                        .filter(|d| {
                                            q.is_empty()
                                                || d.name
                                                    .as_deref()
                                                    .unwrap_or("")
                                                    .to_lowercase()
                                                    .contains(&q)
                                        })
                                        .map(|d| d.id)
                                        .collect::<Vec<i64>>()
                                })
                            }
                            key=|id| *id
                            children=move |id| view! {
                                <DownloadRow
                                    id=id
                                    downloads=downloads
                                    categories=categories
                                    selected_ids=selected_ids
                                    on_select=on_row_click
                                />
                            }
                        />
                    </ul>
                </Show>
            </div>

            // ── Filter + stats bar ────────────────────────────────────────
            <StatusBar dl_speed=dl_speed ul_speed=ul_speed temp_limit=temp_limit>
                <select
                    class="dl-filter-select"
                    prop:value=move || filter_state.get().as_key()
                    on:change=move |e| {
                        let fs = FilterState::from_key(&event_target_value(&e));
                        filter_state.set(fs);
                        save_filter(FILTER_STATE_KEY, fs.as_key());
                    }
                >
                    <option value="all">{t!("download.filter.all")}</option>
                    <option value="active">{t!("download.filter.active")}</option>
                    <option value="downloading">{t!("download.filter.downloading")}</option>
                    <option value="paused">{t!("download.filter.paused")}</option>
                    <option value="completed">{t!("download.filter.completed")}</option>
                    <option value="history">{t!("download.filter.history")}</option>
                </select>
                <Show when=move || !categories.get().is_empty()>
                    <select
                        class="dl-filter-select"
                        prop:value=move || filter_cat.get().to_value()
                        on:change=move |e| {
                            let fc = CatFilter::from_value(&event_target_value(&e));
                            filter_cat.set(fc);
                            save_filter(FILTER_CAT_KEY, &fc.to_value());
                        }
                    >
                        <option value="">{t!("download.filter.all_categories")}</option>
                        <option value="none">{t!("download.filter.uncategorized")}</option>
                        <For each=move || categories.get() key=|c| c.id let:c>
                            <option value=c.id.to_string()>{c.name.clone()}</option>
                        </For>
                    </select>
                </Show>
                <input
                    type="text"
                    class="dl-filter-input"
                    placeholder=t!("download.filter.placeholder")
                    prop:value=move || filter_name.get()
                    on:input=move |e| filter_name.set(event_target_value(&e))
                />
                {move || {
                    let n = downloads.with(|v| v.iter().filter(|d| is_streamed_state(&d.state)).count());
                    if n > 0 {
                        view! {
                            <span class="dl-active-count">
                                {t!("download.filter.active_count", n = n)}
                            </span>
                        }.into_any()
                    } else {
                        view! {
                            <span class="dl-active-count dl-active-none">
                                {t!("download.filter.none_active")}
                            </span>
                        }.into_any()
                    }
                }}
            </StatusBar>
        </div>

        // ── Add modal ─────────────────────────────────────────────────────
        <Show when=move || add_open.get()>
            <AddModal
                downloads=downloads
                categories=categories
                on_close=move || add_open.set(false)
            />
        </Show>

        // ── Info overlay ──────────────────────────────────────────────────
        {move || detail.get().map(|d| view! {
            <DownloadInfoOverlay
                detail=d
                categories=categories
                downloads=downloads
                on_close=move || detail.set(None)
            />
        })}

        // ── Bulk-edit modal (multi-selection) ──────────────────────────────
        <Show when=move || bulk_open.get()>
            <BulkEditOverlay
                selected_ids=selected_ids
                categories=categories
                downloads=downloads
                on_close=move || bulk_open.set(false)
            />
        </Show>
    }
}

// ── Download row ──────────────────────────────────────────────────────────────

/// Compute the progress percentage [0, 100] for a download, or None when the
/// total size is unknown.
fn pct_for(dl: &DownloadResponse) -> Option<f64> {
    dl.size.map(|total| {
        if total == 0 {
            0.0_f64
        } else {
            (dl.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        }
    })
}

#[component]
fn DownloadRow(
    id: i64,
    downloads: RwSignal<Vec<DownloadResponse>>,
    categories: RwSignal<Vec<Category>>,
    selected_ids: RwSignal<HashSet<i64>>,
    on_select: Callback<(i64, bool, bool)>,
) -> impl IntoView {
    // Each reactive prop reads the signal directly and locates the row by id.
    // We deliberately avoid wrapping this in a per-row Memo: Memos inside a
    // <For> child are easy to mis-dispose when the parent reorders, leaving
    // orphan DOM nodes that keep showing stale data (the "ghost row" bug).
    let with_row = move |f: &dyn Fn(&DownloadResponse) -> String| -> String {
        downloads.with(|v| v.iter().find(|d| d.id == id).map(f).unwrap_or_default())
    };

    let name = move || {
        with_row(&|d| {
            d.name
                .clone()
                .unwrap_or_else(|| format!("{}…", &d.root_hash[..16]))
        })
    };

    let size_label = move || {
        with_row(&|d| {
            let p = pct_for(d);
            match (d.size, p) {
                (Some(total), Some(p)) if p < 100.0 => format!(
                    "{} / {} — {:.1}%",
                    format_size(d.bytes_done),
                    format_size(total),
                    p
                ),
                (Some(total), _) => format_size(total),
                _ => format_size(d.bytes_done),
            }
        })
    };

    let show_bar = move || {
        downloads.with(|v| {
            v.iter()
                .find(|d| d.id == id)
                .map(|d| !is_terminal(&d.state) && pct_for(d).is_some())
                .unwrap_or(false)
        })
    };
    let bar_width = move || {
        let p = downloads.with(|v| {
            v.iter()
                .find(|d| d.id == id)
                .and_then(pct_for)
                .unwrap_or(0.0)
        });
        format!("width:{p:.1}%")
    };

    let state_class = move || {
        downloads.with(|v| {
            v.iter()
                .find(|d| d.id == id)
                .map(|d| state_css(&d.state))
                .unwrap_or("")
        })
    };
    let state_text = move || {
        downloads.with(|v| {
            v.iter()
                .find(|d| d.id == id)
                .map(|d| state_label(&d.state))
                .unwrap_or_default()
        })
    };
    let has_error = move || {
        downloads.with(|v| {
            v.iter()
                .find(|d| d.id == id)
                .map(|d| d.error.is_some())
                .unwrap_or(false)
        })
    };
    let error_text =
        move || downloads.with(|v| v.iter().find(|d| d.id == id).and_then(|d| d.error.clone()));

    // Live transfer info shown under the bar, right-aligned opposite the state
    // pill. State-sensitive: the rate (+ ETA) headlines while transferring;
    // otherwise we fill the slot with whatever live signal fits the state — the
    // eMule upload-queue rank while waiting for a slot, or the source count
    // while still locating peers. Empty string = nothing to show (hidden).
    let live_info = move || -> String {
        with_row(&|d| {
            // Strictly state-driven: a paused row (or one mid-pause, before the
            // engine clears its live stats) can still carry a stale speed/rank
            // from the last sample, so each figure is shown only for the state it
            // belongs to — never for paused or terminal downloads.
            match d.state {
                DownloadState::Downloading => {
                    if let Some(bps) = d.speed_bps.filter(|&b| b > 0) {
                        let mut s = format_speed(bps);
                        if let Some(eta) = d.eta_secs.filter(|&e| e > 0) {
                            s.push_str(" · ");
                            s.push_str(&format_eta(eta));
                        }
                        return s;
                    }
                    String::new()
                }
                DownloadState::Queued => {
                    if let Some(rank) = d.best_queue_rank {
                        return t!("download.queue_rank", rank = rank).to_string();
                    }
                    if let Some(n) = d.sources_total.filter(|&n| n > 0) {
                        return t!("download.sources_count", n = n).to_string();
                    }
                    String::new()
                }
                DownloadState::FindingProviders | DownloadState::Stalled => {
                    if let Some(n) = d.sources_total.filter(|&n| n > 0) {
                        return format!("{n} source{}", if n == 1 { "" } else { "s" });
                    }
                    String::new()
                }
                _ => String::new(),
            }
        })
    };

    // The download's category as (name, optional colour), resolved live so it
    // reflects edits and reassignments. None when unassigned or since deleted.
    let category = move || -> Option<(String, Option<String>)> {
        let cid = downloads.with(|v| v.iter().find(|d| d.id == id).and_then(|d| d.category_id))?;
        categories.with(|cs| {
            cs.iter()
                .find(|c| c.id == cid)
                .map(|c| (c.name.clone(), c.color.clone()))
        })
    };

    view! {
        <li
            class=move || if selected_ids.with(|s| s.contains(&id)) {
                "dl-row dl-row-selected"
            } else {
                "dl-row"
            }
            on:click=move |ev| {
                // On a touchscreen there are no modifiers, so a plain tap is
                // treated as an additive toggle to allow building a selection.
                let additive = ev.ctrl_key() || ev.meta_key() || crate::platform::coarse_pointer();
                on_select.run((id, additive, ev.shift_key()));
            }
        >
            <div class="dl-top">
                <span class="dl-name">{name}</span>
                {move || {
                    // At-a-glance priority marker; Medium (default) shows nothing.
                    let prio = downloads
                        .with(|v| v.iter().find(|d| d.id == id).map(|d| d.priority))
                        .unwrap_or_default();
                    let (cls, glyph, label): (&str, &str, String) = match prio {
                        DownloadPriority::High => (
                            "dl-prio dl-prio-high",
                            "\u{25B2}",
                            t!("download.priority.high").to_string(),
                        ),
                        DownloadPriority::Low => (
                            "dl-prio dl-prio-low",
                            "\u{25BC}",
                            t!("download.priority.low").to_string(),
                        ),
                        DownloadPriority::Medium => ("", "", String::new()),
                    };
                    (!glyph.is_empty()).then(|| view! {
                        <span class=cls title=label>{glyph}</span>
                    })
                }}
                {move || category().map(|(cname, color)| {
                    // Coloured badge: background = category colour, text picked
                    // for contrast. A colourless category falls back to the
                    // shared neutral grey, matching the Settings colour picker.
                    let c = color.unwrap_or_else(|| NEUTRAL_CATEGORY_COLOR.to_string());
                    let style = format!("background:{c};color:{}", contrast_text(&c));
                    view! { <span class="dl-cat-badge" style=style>{cname}</span> }
                })}
                <span class="dl-size">{size_label}</span>
            </div>

            <Show when=show_bar fallback=|| ()>
                <div class="dl-bar-track">
                    <div class="dl-bar-fill" style=bar_width/>
                </div>
            </Show>

            <div class="dl-bottom">
                <span class=state_class>{state_text}</span>
                <Show when=has_error fallback=|| ()>
                    <span class="dl-error">{error_text}</span>
                </Show>
                <Show when=move || !live_info().is_empty() fallback=|| ()>
                    <span class="dl-live">{live_info}</span>
                </Show>
            </div>
        </li>
    }
}

// ── Add modal ─────────────────────────────────────────────────────────────────

#[component]
fn AddModal(
    downloads: RwSignal<Vec<DownloadResponse>>,
    categories: RwSignal<Vec<Category>>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let text = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    // Lines the daemon couldn't accept; shown so the user can fix them.
    let rejected: RwSignal<Vec<String>> = RwSignal::new(vec![]);
    // Messages for links the user already has ("…is in your shared files", etc.).
    let duplicates: RwSignal<Vec<String>> = RwSignal::new(vec![]);
    // Selected category id; None = "Auto / none" (let keyword auto-match decide).
    let selected_cat: RwSignal<Option<i64>> = RwSignal::new(None);

    let submit = move || {
        let t = text.get();
        if t.trim().is_empty() {
            return;
        }
        busy.set(true);
        let cat = selected_cat.get_untracked();
        spawn_local(async move {
            let out = api_add_links(t, downloads, cat).await;
            busy.set(false);
            duplicates.set(out.duplicates.clone());
            if out.rejected.is_empty() && out.duplicates.is_empty() {
                on_close();
            } else {
                // The valid links went through; keep only the invalid ones in the
                // box so the user can correct and retry. Duplicates are shown as
                // an informational notice (nothing to fix).
                text.set(out.rejected.join("\n"));
                rejected.set(out.rejected);
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("download.add_modal.title")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("download.add_modal.hint")}
                    </p>
                    <textarea
                        class="link-textarea"
                        placeholder="rucio:<hash>?name=…&size=…\ned2k://|file|…"
                        prop:value=move || text.get()
                        on:input=move |e| {
                            text.set(event_target_value(&e));
                            // Editing clears the previous notices.
                            if !rejected.get_untracked().is_empty() {
                                rejected.set(vec![]);
                            }
                            if !duplicates.get_untracked().is_empty() {
                                duplicates.set(vec![]);
                            }
                        }
                        on:keydown=move |e| {
                            if e.key() == "Enter" && e.ctrl_key() {
                                submit();
                            }
                        }
                        rows="6"
                    />
                    {move || {
                        let r = rejected.get();
                        (!r.is_empty()).then(|| view! {
                            <p class="error-msg">
                                {t!("download.add_modal.rejected", n = r.len())}
                            </p>
                        })
                    }}
                    {move || {
                        let d = duplicates.get();
                        (!d.is_empty()).then(|| view! {
                            <ul class="dup-notice">
                                <For
                                    each=move || duplicates.get()
                                    key=|m| m.clone()
                                    children=move |m| view! { <li>{m}</li> }
                                />
                            </ul>
                        })
                    }}
                    <Show when=move || !categories.get().is_empty()>
                        <div class="add-cat-row">
                            <label class="config-label">{t!("download.add_modal.category")}</label>
                            <select
                                class="dl-filter-select"
                                on:change=move |e| {
                                    let v = event_target_value(&e);
                                    selected_cat.set(v.parse::<i64>().ok());
                                }
                            >
                                <option value="" selected=true>{t!("download.add_modal.auto_none")}</option>
                                <For each=move || categories.get() key=|c| c.id let:c>
                                    <option value=c.id.to_string()>{c.name.clone()}</option>
                                </For>
                            </select>
                        </div>
                    </Show>
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || text.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { t!("download.add_modal.adding") } else { t!("download.add_modal.add_btn") }}
                    </button>
                </div>
            </div>
        </div>
    }
}

// ── Bulk-edit modal ─────────────────────────────────────────────────────────

/// Edit category and/or priority for several selected downloads at once. Each
/// field defaults to "leave unchanged", so applying only touches the dimensions
/// the user actually picked — never clobbering the rest.
#[component]
fn BulkEditOverlay(
    selected_ids: RwSignal<HashSet<i64>>,
    categories: RwSignal<Vec<Category>>,
    downloads: RwSignal<Vec<DownloadResponse>>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    // Snapshot the selection at open time so the applied set is stable even if
    // the live list refreshes underneath us.
    let ids: Vec<i64> = selected_ids.get_untracked().into_iter().collect();
    let count = ids.len();

    // `None` = leave unchanged. For category the inner Option distinguishes
    // "clear to global" (Some(None)) from "set to id" (Some(Some(id))).
    let cat_choice: RwSignal<Option<Option<i64>>> = RwSignal::new(None);
    let prio_choice: RwSignal<Option<DownloadPriority>> = RwSignal::new(None);
    let busy = RwSignal::new(false);

    let apply = move |_| {
        let cat = cat_choice.get_untracked();
        let prio = prio_choice.get_untracked();
        // Nothing chosen → just close, no requests.
        if cat.is_none() && prio.is_none() {
            on_close();
            return;
        }
        let ids = ids.clone();
        busy.set(true);
        spawn_local(async move {
            for id in ids {
                if let Some(catopt) = cat {
                    let body = serde_json::json!({ "category_id": catopt });
                    if let Ok(req) = gloo_net::http::Request::put(&crate::api::api(&format!(
                        "/api/v1/downloads/{id}/category"
                    )))
                    .json(&body)
                    {
                        let _ = req.send().await;
                    }
                }
                if let Some(p) = prio {
                    let body = serde_json::json!({ "priority": p.as_str() });
                    if let Ok(req) = gloo_net::http::Request::put(&crate::api::api(&format!(
                        "/api/v1/downloads/{id}/priority"
                    )))
                    .json(&body)
                    {
                        let _ = req.send().await;
                    }
                }
            }
            refresh_downloads(downloads).await;
            busy.set(false);
            on_close();
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("download.bulk.title", n = count)}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">{t!("download.bulk.hint")}</p>

                    <div class="bulk-fields">
                        <Show when=move || !categories.get().is_empty()>
                            <div class="bulk-row">
                                <label class="config-label">{t!("download.detail.category")}</label>
                                <select
                                    class="dl-filter-select"
                                    on:change=move |e| {
                                        let v = event_target_value(&e);
                                        let choice = match v.as_str() {
                                            "" => None,
                                            "none" => Some(None),
                                            s => s.parse::<i64>().ok().map(Some),
                                        };
                                        cat_choice.set(choice);
                                    }
                                >
                                    <option value="" selected=true>{t!("download.bulk.unchanged")}</option>
                                    <option value="none">{t!("download.detail.category_none")}</option>
                                    <For each=move || categories.get() key=|c| c.id let:c>
                                        <option value=c.id.to_string()>{c.name.clone()}</option>
                                    </For>
                                </select>
                            </div>
                        </Show>

                        <div class="bulk-row">
                            <label class="config-label">{t!("download.detail.priority")}</label>
                            <select
                                class="dl-filter-select"
                                on:change=move |e| {
                                    let choice = match event_target_value(&e).as_str() {
                                        "low" => Some(DownloadPriority::Low),
                                        "medium" => Some(DownloadPriority::Medium),
                                        "high" => Some(DownloadPriority::High),
                                        _ => None,
                                    };
                                    prio_choice.set(choice);
                                }
                            >
                                <option value="" selected=true>{t!("download.bulk.unchanged")}</option>
                                <option value="high">{t!("download.priority.high")}</option>
                                <option value="medium">{t!("download.priority.medium")}</option>
                                <option value="low">{t!("download.priority.low")}</option>
                            </select>
                        </div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get()
                        on:click=apply
                    >
                        {move || if busy.get() { t!("download.bulk.applying") } else { t!("download.bulk.apply") }}
                    </button>
                </div>
            </div>
        </div>
    }
}

// ── Info overlay ──────────────────────────────────────────────────────────────

#[component]
fn DownloadInfoOverlay(
    detail: DownloadDetailResponse,
    categories: RwSignal<Vec<Category>>,
    downloads: RwSignal<Vec<DownloadResponse>>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let id = detail.id;
    // Current category, editable: changing it PUTs and refreshes the list badge.
    let cur_cat: RwSignal<Option<i64>> = RwSignal::new(detail.category_id);
    // Current priority, editable: changing it PUTs and refreshes the list.
    let cur_prio: RwSignal<DownloadPriority> = RwSignal::new(detail.priority);
    let name = detail
        .name
        .clone()
        .unwrap_or_else(|| format!("{}…", &detail.root_hash[..16]));
    // Title is reactive so a successful rename updates it without reopening.
    let display_name = RwSignal::new(name);

    let pct = detail.size.map(|total| {
        if total == 0 {
            0.0
        } else {
            (detail.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        }
    });

    // Renaming changes the name the file is saved as on completion; only allowed
    // while the download is unfinished (a completed file already belongs to the
    // user). Pre-fill with the current name.
    let renamable = !is_terminal(&detail.state);
    let name_input = RwSignal::new(detail.name.clone().unwrap_or_default());
    let saving = RwSignal::new(false);

    // Per-peer sources (libp2p), a snapshot at the moment the panel was opened.
    let peers = detail.peers;
    let queued_sources = detail.queued_sources;
    let best_queue_rank = detail.best_queue_rank;

    view! {
        <div class="overlay-backdrop" on:click=move |_| on_close()>
            <div class="overlay overlay-wide" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">{move || display_name.get()}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <dl class="panel-dl">
                        <dt>{t!("download.detail.state")}</dt>
                        <dd>
                            <span class=state_css(&detail.state)>
                                {state_label(&detail.state)}
                            </span>
                        </dd>

                        <dt>{t!("download.detail.kind")}</dt>
                        <dd>{detail.kind}</dd>

                        <dt>{t!("download.detail.category")}</dt>
                        <dd>
                            <select
                                class="dl-filter-select"
                                prop:value=move || cur_cat.get().map(|c| c.to_string()).unwrap_or_default()
                                on:change=move |e| {
                                    let cat = event_target_value(&e).parse::<i64>().ok();
                                    cur_cat.set(cat);
                                    spawn_local(async move {
                                        let body = serde_json::json!({ "category_id": cat });
                                        if let Ok(req) = gloo_net::http::Request::put(
                                            &crate::api::api(&format!("/api/v1/downloads/{id}/category")),
                                        ).json(&body)
                                            && req.send().await.map(|r| r.ok()).unwrap_or(false)
                                        {
                                            // Reflect the new badge in the list.
                                            refresh_downloads(downloads).await;
                                        }
                                    });
                                }
                            >
                                <option value="">{t!("download.detail.category_none")}</option>
                                <For each=move || categories.get() key=|c| c.id let:c>
                                    <option value=c.id.to_string()>{c.name.clone()}</option>
                                </For>
                            </select>
                        </dd>

                        <dt>{t!("download.detail.priority")}</dt>
                        <dd>
                            <select
                                class="dl-filter-select"
                                prop:value=move || cur_prio.get().as_str().to_string()
                                on:change=move |e| {
                                    let prio = match event_target_value(&e).as_str() {
                                        "low" => DownloadPriority::Low,
                                        "high" => DownloadPriority::High,
                                        _ => DownloadPriority::Medium,
                                    };
                                    cur_prio.set(prio);
                                    spawn_local(async move {
                                        let body = serde_json::json!({ "priority": prio.as_str() });
                                        if let Ok(req) = gloo_net::http::Request::put(
                                            &crate::api::api(&format!("/api/v1/downloads/{id}/priority")),
                                        ).json(&body)
                                            && req.send().await.map(|r| r.ok()).unwrap_or(false)
                                        {
                                            refresh_downloads(downloads).await;
                                        }
                                    });
                                }
                            >
                                <option value="high">{t!("download.priority.high")}</option>
                                <option value="medium">{t!("download.priority.medium")}</option>
                                <option value="low">{t!("download.priority.low")}</option>
                            </select>
                        </dd>

                        {pct.map(|p| view! {
                            <dt>{t!("download.detail.progress")}</dt>
                            <dd>{format!("{p:.1}%")}</dd>
                        })}

                        {detail.size.map(|s| view! {
                            <dt>{t!("download.detail.size")}</dt>
                            <dd>{format_size(s)}</dd>
                        })}

                        {detail.speed_bps.filter(|&s| s > 0).map(|s| view! {
                            <dt>{t!("download.detail.speed")}</dt>
                            <dd>{format_speed(s)}</dd>
                        })}

                        {detail.eta_secs.map(|e| view! {
                            <dt>{t!("download.detail.eta")}</dt>
                            <dd>{format_eta(e)}</dd>
                        })}

                        {detail.sources_active.zip(detail.sources_total).map(|(a, t)| view! {
                            <dt>{t!("download.detail.sources")}</dt>
                            <dd>{t!("download.detail.sources_val", active = a, known = t)}</dd>
                        })}

                        {queued_sources.map(|n| {
                            let label = match best_queue_rank {
                                Some(r) => t!("download.detail.queued_rank", n = n, rank = r).to_string(),
                                None => t!("download.detail.queued_n", n = n).to_string(),
                            };
                            view! {
                                <dt>{t!("download.detail.queued")}</dt>
                                <dd>{label}</dd>
                            }
                        })}

                        {detail.dest_path.map(|p| view! {
                            <dt>{t!("download.detail.saved_to")}</dt>
                            <dd class="mono">{p}</dd>
                        })}

                        {detail.error.map(|e| view! {
                            <dt>{t!("download.detail.error")}</dt>
                            <dd class="dl-error">{e}</dd>
                        })}

                        <dt>{t!("download.detail.hash")}</dt>
                        <dd class="mono">{detail.root_hash}</dd>

                        {detail.link.map(|l| view! {
                            <dt>{t!("download.detail.link")}</dt>
                            <dd class="mono">{l}</dd>
                        })}
                    </dl>

                    {(!peers.is_empty()).then(|| view! {
                        <p class="section-label">{t!("download.detail.downloading_from")}</p>
                        <ul class="peer-list">
                            {peers.into_iter().map(|p| {
                                let who = p.address.clone().unwrap_or_else(|| p.peer_id.clone());
                                let rate = format_speed(p.rate_bps);
                                let meta = t!(
                                    "download.detail.peer_meta",
                                    bytes = format_size(p.bytes_downloaded),
                                    n = p.chunks_in_flight
                                ).to_string();
                                view! {
                                    <li class="peer-item">
                                        <div class="peer-head">
                                            <span class="dl-peer-rate">
                                                {if rate.is_empty() { t!("download.detail.idle").to_string() } else { rate }}
                                            </span>
                                            <span class="mono peer-id" title=p.peer_id>{who}</span>
                                        </div>
                                        <span class="peer-addr">{meta}</span>
                                    </li>
                                }
                            }).collect_view()}
                        </ul>
                    })}

                    {renamable.then(|| view! {
                        <div class="dl-rename" style="display:flex; gap:8px; margin:14px 0;">
                            <input
                                class="config-input"
                                style="flex:1;"
                                type="text"
                                placeholder=t!("download.detail.rename_placeholder")
                                prop:value=move || name_input.get()
                                on:input=move |e| name_input.set(event_target_value(&e))
                            />
                            <button
                                class="btn-sm btn-primary"
                                style="min-width: 5.5rem;"
                                disabled=move || saving.get()
                                on:click=move |_| {
                                    let new = name_input.get_untracked().trim().to_string();
                                    if new.is_empty() || saving.get_untracked() {
                                        return;
                                    }
                                    saving.set(true);
                                    spawn_local(async move {
                                        if api_rename(id, new.clone()).await {
                                            // Reflect the new name in the list row.
                                            // Paused (and other non-active) downloads
                                            // aren't part of the WebSocket progress
                                            // stream, so the row would otherwise keep
                                            // the old name until a full refresh.
                                            downloads.update(|list| {
                                                if let Some(d) =
                                                    list.iter_mut().find(|d| d.id == id)
                                                {
                                                    d.name = Some(new.clone());
                                                }
                                            });
                                            // Keep the modal open; reflect the new
                                            // name in the title and input.
                                            display_name.set(new);
                                        }
                                        saving.set(false);
                                    });
                                }
                            >
                                {move || if saving.get() { t!("download.detail.renaming") } else { t!("download.detail.rename_btn") }}
                            </button>
                        </div>
                    })}

                    <div class="piece-map-wrap">
                        <span class="section-label">{t!("download.detail.pieces")}</span>
                        <PieceMap id=id/>
                    </div>
                </div>
            </div>
        </div>
    }
}

// ── Piece map ───────────────────────────────────────────────────────────────

/// Pick the colour class for a contiguous group of pieces. Priority: any
/// in-flight → in-flight; all done → done; some done → partial; all missing →
/// missing (no provider has them); else pending.
fn segment_class(slice: &[PieceState]) -> &'static str {
    let mut done = 0usize;
    let mut in_flight = 0usize;
    let mut missing = 0usize;
    for s in slice {
        match s {
            PieceState::Done => done += 1,
            PieceState::InFlight => in_flight += 1,
            PieceState::Missing => missing += 1,
            PieceState::Pending => {}
        }
    }
    if in_flight > 0 {
        "piece-seg piece-inflight"
    } else if done == slice.len() {
        "piece-seg piece-done"
    } else if done > 0 {
        "piece-seg piece-partial"
    } else if missing == slice.len() {
        "piece-seg piece-missing"
    } else {
        "piece-seg piece-pending"
    }
}

/// A block-style progress bar that polls `/pieces` while it is mounted.
/// Pieces are grouped into at most `MAX_SEGMENTS` coloured segments so the bar
/// stays legible regardless of the (potentially thousands of) piece count.
#[component]
fn PieceMap(id: i64) -> impl IntoView {
    const MAX_SEGMENTS: usize = 240;

    let states: RwSignal<Vec<PieceState>> = RwSignal::new(Vec::new());
    // Fraction of the file reachable across the swarm; None until probed.
    let availability: RwSignal<Option<f64>> = RwSignal::new(None);

    // Use Rc<Cell> instead of RwSignal for the liveness flag.  When the
    // overlay closes, Leptos first runs on_cleanup callbacks and then frees
    // all reactive nodes in the scope (including any signals we own).  If
    // the fetch is in flight at that moment, resuming it and calling
    // states.set() on a freed node would panic with "unreachable executed".
    // Rc<Cell<bool>> lives outside the reactive graph so on_cleanup can set
    // it to false before the scope is freed; we check it after every await
    // before touching any reactive signal.
    // Arc<AtomicBool> rather than RwSignal: on_cleanup runs before Leptos
    // frees the reactive scope, so if a fetch is in flight the future would
    // resume and try to write into already-freed signal nodes → panic.
    // AtomicBool lives outside the graph; we check it after every await
    // before touching any reactive signal.  Arc (Send+Sync) is needed
    // because Leptos 0.8 spawn_local and on_cleanup require Send.
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

    spawn_local(async move {
        loop {
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            if let Some(p) = api_fetch_pieces(id).await {
                // Re-check after the await: component may have unmounted
                // while the HTTP request was in flight.
                if alive.load(Ordering::Relaxed) {
                    availability.set(p.availability());
                    states.set(p.piece_states());
                }
            }
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_millis(1500)).await;
        }
    });

    view! {
        <Show
            when=move || !states.get().is_empty()
            fallback=|| view! { <div class="piece-map-empty">{t!("download.piece.none")}</div> }
        >
            <div class="piece-map">
                {move || {
                    let st = states.get();
                    let total = st.len();
                    let n = total.clamp(1, MAX_SEGMENTS);
                    (0..n)
                        .map(|seg| {
                            let start = seg * total / n;
                            let end = ((seg + 1) * total / n).max(start + 1).min(total);
                            let cls = segment_class(&st[start..end]);
                            view! { <div class=cls/> }
                        })
                        .collect_view()
                }}
            </div>
            {move || availability.get().map(|frac| {
                let pct = (frac * 100.0).floor() as u32;
                let (cls, text) = if frac >= 0.999 {
                    ("piece-avail piece-avail-full", t!("download.piece.full").to_string())
                } else {
                    ("piece-avail piece-avail-partial", t!("download.piece.partial", pct = pct, missing = 100 - pct).to_string())
                };
                view! { <div class=cls>{text}</div> }
            })}
        </Show>
    }
}
