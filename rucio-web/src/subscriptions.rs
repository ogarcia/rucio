//! Subscriptions tab: mirror other nodes' pinned content (cooperative pinning).
//!
//! Lists subscriptions with a used/quota storage meter, lets the user subscribe
//! to a peer (a PeerId or a `rucio-peer:` link) within a disk quota, copy this
//! node's own shareable link, and unsubscribe (which evicts content nobody else
//! wants).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;
use rust_i18n::t;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{
    MirrorFile, StatusResponse, Subscription, SubscriptionFilesResponse, SubscriptionsResponse,
    format_size,
};

// ── API ─────────────────────────────────────────────────────────────────────

async fn api_list() -> Option<Vec<Subscription>> {
    gloo_net::http::Request::get("/api/v1/subscriptions")
        .send()
        .await
        .ok()?
        .json::<SubscriptionsResponse>()
        .await
        .ok()
        .map(|r| r.subscriptions)
}

/// Subscribe to a peer with a byte quota. Returns `Err(message)` on failure.
async fn api_add(peer: String, quota_bytes: u64) -> Result<(), String> {
    let body = serde_json::json!({ "peer": peer, "quota_bytes": quota_bytes });
    let req = gloo_net::http::Request::post("/api/v1/subscriptions")
        .json(&body)
        .map_err(|e| e.to_string())?;
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.ok() {
        Ok(())
    } else if resp.status() == 400 {
        Err(t!("sub.err_invalid_peer").to_string())
    } else {
        Err(format!("HTTP {}", resp.status()))
    }
}

/// Unsubscribe. `keep = true` retains the mirrored content (it becomes a share
/// you own); `false` frees the space by evicting mirror-only content.
async fn api_remove(peer_id: &str, keep: bool) {
    let url = format!("/api/v1/subscriptions/{peer_id}?keep={keep}");
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// Set which collections of a peer to mirror. `follow_all` mirrors everything;
/// otherwise only `collections` ("" = the peer's uncollected pins). `keep`
/// retains content that the new (narrower) scope drops instead of evicting it.
async fn api_set_collections(
    peer_id: &str,
    follow_all: bool,
    collections: Vec<String>,
    keep: bool,
) {
    let url = format!("/api/v1/subscriptions/{peer_id}/collections");
    let body = serde_json::json!({
        "follow_all": follow_all, "collections": collections, "keep": keep,
    });
    if let Ok(req) = gloo_net::http::Request::put(&url).json(&body) {
        let _ = req.send().await;
    }
}

/// Re-fetch a single subscription (latest stats + available collections).
async fn api_get(peer_id: &str) -> Option<Subscription> {
    let url = format!("/api/v1/subscriptions/{peer_id}");
    gloo_net::http::Request::get(&url)
        .send()
        .await
        .ok()?
        .json::<Subscription>()
        .await
        .ok()
}

/// Ask the daemon to pull this peer's pin-set now (best-effort, asynchronous).
async fn api_sync(peer_id: &str) {
    let url = format!("/api/v1/subscriptions/{peer_id}/sync");
    let _ = gloo_net::http::Request::post(&url).send().await;
}

/// Bytes that unsubscribing would actually free. `Some(0)` means nothing is at
/// stake (so the keep/free prompt can be skipped); `None` on error (prompt to
/// be safe).
async fn api_evictable(peer_id: &str) -> Option<u64> {
    let url = format!("/api/v1/subscriptions/{peer_id}/evictable");
    let resp = gloo_net::http::Request::get(&url).send().await.ok()?;
    let v = resp.json::<serde_json::Value>().await.ok()?;
    v.get("bytes").and_then(|b| b.as_u64())
}

/// Re-request a mirror file the user previously cancelled.
async fn api_refetch(peer_id: &str, hash: &str) {
    let url = format!("/api/v1/subscriptions/{peer_id}/files/{hash}/refetch");
    let _ = gloo_net::http::Request::post(&url).send().await;
}

/// The mirror files of a subscription, with their resolved state.
async fn api_files(peer_id: &str) -> Vec<MirrorFile> {
    let url = format!("/api/v1/subscriptions/{peer_id}/files");
    match gloo_net::http::Request::get(&url).send().await {
        Ok(resp) => resp
            .json::<SubscriptionFilesResponse>()
            .await
            .map(|r| r.files)
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Fetch this node's own shareable `rucio-peer:` link from the status endpoint.
async fn api_my_link() -> Option<String> {
    let status = gloo_net::http::Request::get("/api/v1/status")
        .send()
        .await
        .ok()?
        .json::<StatusResponse>()
        .await
        .ok()?;
    Some(format!("rucio-peer:{}", status.peer_id))
}

fn copy_to_clipboard(text: &str) {
    if let Some(win) = web_sys::window() {
        let _ = win.navigator().clipboard().write_text(text);
    }
}

/// Convert a quota input (value + unit) into bytes. Base 1024.
fn quota_to_bytes(value: f64, unit: &str) -> u64 {
    let mult: u64 = match unit {
        "MB" => 1024 * 1024,
        "TB" => 1024u64 * 1024 * 1024 * 1024,
        _ => 1024 * 1024 * 1024, // GB
    };
    (value * mult as f64) as u64
}

/// Split a byte quota into a (value, unit) pair for the editor, picking the
/// largest unit that yields a value ≥ 1 and trimming trailing zeros.
fn split_quota(bytes: u64) -> (String, &'static str) {
    const TB: f64 = (1u64 << 40) as f64;
    const GB: f64 = (1u64 << 30) as f64;
    const MB: f64 = (1u64 << 20) as f64;
    let b = bytes as f64;
    let (v, u) = if b >= TB {
        (b / TB, "TB")
    } else if b >= GB {
        (b / GB, "GB")
    } else {
        (b / MB, "MB")
    };
    let s = format!("{v:.2}");
    let s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    (s, u)
}

// ── Component ─────────────────────────────────────────────────────────────────

#[component]
pub fn SubscriptionsTab(
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    let subs: RwSignal<Vec<Subscription>> = RwSignal::new(vec![]);
    let add_open: RwSignal<bool> = RwSignal::new(false);
    let copied: RwSignal<bool> = RwSignal::new(false);
    // The subscription whose info modal is open (None = closed).
    let info_for: RwSignal<Option<Subscription>> = RwSignal::new(None);
    // The peer whose unsubscribe (keep/free) modal is open (None = closed).
    let unsub_for: RwSignal<Option<String>> = RwSignal::new(None);

    let reload = move || {
        spawn_local(async move {
            if let Some(s) = api_list().await {
                subs.set(s);
            }
        });
    };
    reload();

    let copy_link = move |_| {
        spawn_local(async move {
            if let Some(link) = api_my_link().await {
                copy_to_clipboard(&link);
                copied.set(true);
            }
        });
    };

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
                <div class="dl-toolbar">
                    <button
                        class="toolbar-btn"
                        title=t!("sub.add_title")
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PLUS/>
                        <span class="btn-label">{t!("sub.subscribe")}</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title=t!("sub.copy_link_title")
                        on:click=copy_link
                    >
                        <Icon paths=icons::COPY/>
                        <span class="btn-label">
                            {move || if copied.get() { t!("sub.copied") } else { t!("sub.copy_link") }}
                        </span>
                    </button>
                </div>
            </div>

            <div class="tab-scroll">
                <Show
                    when=move || !subs.get().is_empty()
                    fallback=|| view! {
                        <div class="empty-state empty-state-sm">
                            <p>{t!("sub.none")}</p>
                            <p class="empty-hint">
                                {t!("sub.empty_hint")}
                            </p>
                        </div>
                    }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || subs.get()
                            // Key on the displayed fields, not just the peer id:
                            // <For> never re-renders an existing key, so keying by
                            // peer id alone froze a row's meter/counts (and the
                            // snapshot the info button captures) until a full page
                            // reload. subs only refreshes on actions, so rebuilding
                            // a changed row costs nothing visible.
                            key=|s| {
                                format!(
                                    "{}|{}|{}|{}|{}|{}|{}|{}|{}",
                                    s.peer_id,
                                    s.quota_bytes,
                                    s.used_bytes,
                                    s.present_bytes,
                                    s.wanted_count,
                                    s.present_count,
                                    s.skipped_count,
                                    s.last_synced_at,
                                    s.follow_all,
                                )
                            }
                            children=move |s| {
                                let peer_rm = s.peer_id.clone();
                                let peer_full = s.peer_id.clone();
                                let peer_title = s.peer_id.clone();
                                let sub_info = s.clone();
                                // Two-tone meter: lighter = committed (selected within
                                // quota), solid = actually present on disk.
                                let committed_pct = if s.quota_bytes > 0 {
                                    (s.used_bytes as f64 / s.quota_bytes as f64 * 100.0).clamp(0.0, 100.0)
                                } else {
                                    0.0
                                };
                                let present_pct = if s.quota_bytes > 0 {
                                    (s.present_bytes as f64 / s.quota_bytes as f64 * 100.0).clamp(0.0, 100.0)
                                } else {
                                    0.0
                                };
                                let meter_text = format!(
                                    "{} / {}",
                                    format_size(s.present_bytes),
                                    format_size(s.quota_bytes)
                                );
                                // Genuinely mirrored vs still fetching — don't conflate them.
                                let fetching = s.wanted_count.saturating_sub(s.present_count);
                                let mut parts = vec![t!("sub.mirrored", n = s.present_count).to_string()];
                                if fetching > 0 {
                                    parts.push(t!("sub.fetching", n = fetching).to_string());
                                }
                                if s.skipped_count > 0 {
                                    parts.push(t!("sub.over_quota", n = s.skipped_count).to_string());
                                }
                                // Only the positive "synced" marker, never "not synced yet"
                                // (which reads like a failure when it's just pending).
                                if s.last_synced_at != 0 {
                                    parts.push(t!("sub.synced").to_string());
                                }
                                let meta = format!("{meter_text} · {}", parts.join(" · "));
                                view! {
                                    <li class="share-dir-row static-row">
                                        <span class="share-dir-icon"><Icon paths=icons::NETWORK/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path" title=peer_title>{peer_full}</span>
                                            <div class="dl-bar-track sub-meter">
                                                <div
                                                    class="sub-meter-committed"
                                                    style=move || format!("width:{committed_pct:.1}%")
                                                ></div>
                                                <div
                                                    class="dl-bar-fill"
                                                    style=move || format!("width:{present_pct:.1}%")
                                                ></div>
                                            </div>
                                            <span class="share-dir-meta">{meta}</span>
                                        </div>
                                        <button
                                            class="icon-btn"
                                            title=t!("sub.details_title")
                                            on:click=move |_| info_for.set(Some(sub_info.clone()))
                                        >
                                            <Icon paths=icons::INFO_CIRCLE/>
                                        </button>
                                        <button
                                            class="icon-btn icon-btn-danger"
                                            title=t!("sub.unsub_title")
                                            on:click=move |_| {
                                                let p = peer_rm.clone();
                                                spawn_local(async move {
                                                    // Skip the keep/free prompt when nothing would
                                                    // actually be freed (content outside pin_dir,
                                                    // pinned, or wanted elsewhere): just leave.
                                                    if api_evictable(&p).await == Some(0) {
                                                        api_remove(&p, true).await;
                                                        if let Some(s) = api_list().await {
                                                            subs.set(s);
                                                        }
                                                    } else {
                                                        unsub_for.set(Some(p));
                                                    }
                                                });
                                            }
                                        >
                                            <Icon paths=icons::TRASH/>
                                        </button>
                                    </li>
                                }
                            }
                        />
                    </ul>
                </Show>
            </div>

            <StatusBar dl_speed=dl_speed ul_speed=ul_speed temp_limit=temp_limit>
                {move || {
                    let n = subs.get().len();
                    if n == 0 {
                        view! { <span class="dl-active-count dl-active-none">{t!("sub.none")}</span> }
                            .into_any()
                    } else {
                        view! { <span class="dl-active-count">{t!("sub.count", n = n)}</span> }
                            .into_any()
                    }
                }}
            </StatusBar>
        </div>

        <Show when=move || add_open.get()>
            <AddSubscriptionModal
                on_added=move || reload()
                on_close=move || add_open.set(false)
            />
        </Show>

        <Show when=move || info_for.get().is_some()>
            <SubscriptionInfoModal
                sub=info_for.get().unwrap()
                on_saved=move || reload()
                on_close=move || info_for.set(None)
            />
        </Show>

        <Show when=move || unsub_for.get().is_some()>
            <UnsubscribeModal
                peer=unsub_for.get().unwrap()
                on_done=move || reload()
                on_close=move || unsub_for.set(None)
            />
        </Show>
    }
}

// ── Unsubscribe modal ───────────────────────────────────────────────────────

/// Asks whether to keep or free the content mirrored from a peer when
/// unsubscribing. "Keep" turns it into permanent shares you own; "Free" evicts
/// the mirror-only content nobody else wants.
#[component]
fn UnsubscribeModal(
    peer: String,
    on_done: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let peer = StoredValue::new(peer);
    let busy = RwSignal::new(false);

    let go = move |keep: bool| {
        busy.set(true);
        spawn_local(async move {
            api_remove(&peer.get_value(), keep).await;
            on_done();
            on_close();
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("sub.unsub_title")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("sub.unsub_hint")}
                    </p>
                    <ul class="unsub-choices">
                        <li>
                            <strong>{t!("sub.keep")}</strong>
                            {t!("sub.keep_desc")}
                        </li>
                        <li>
                            <strong>{t!("sub.free")}</strong>
                            {t!("sub.free_desc")}
                        </li>
                    </ul>
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-danger"
                        disabled=move || busy.get()
                        on:click=move |_| go(false)
                    >
                        {t!("sub.free")}
                    </button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get()
                        on:click=move |_| go(true)
                    >
                        {t!("sub.keep")}
                    </button>
                </div>
            </div>
        </div>
    }
}

// ── Add-subscription modal ──────────────────────────────────────────────────

#[component]
fn AddSubscriptionModal(
    on_added: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let peer = RwSignal::new(String::new());
    let quota = RwSignal::new(String::new());
    let unit = RwSignal::new("GB".to_string());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    let submit = move || {
        let p = peer.get().trim().to_string();
        let q: f64 = quota.get().trim().parse().unwrap_or(0.0);
        if p.is_empty() || q <= 0.0 {
            error.set(Some(t!("sub.err_need_peer_quota").to_string()));
            return;
        }
        let quota_bytes = quota_to_bytes(q, &unit.get());
        busy.set(true);
        error.set(None);
        spawn_local(async move {
            match api_add(p, quota_bytes).await {
                Ok(()) => {
                    on_added();
                    on_close();
                }
                Err(msg) => {
                    error.set(Some(msg));
                    busy.set(false);
                }
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("sub.add_modal_title")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("sub.add_hint")}
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        placeholder="rucio-peer:12D3KooW…"
                        prop:value=move || peer.get()
                        on:input=move |e| peer.set(event_target_value(&e))
                    />
                    <div class="sub-quota-row">
                        <input
                            class="search-input"
                            type="number"
                            min="0"
                            step="any"
                            placeholder=t!("sub.quota_placeholder")
                            prop:value=move || quota.get()
                            on:input=move |e| quota.set(event_target_value(&e))
                            on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                        />
                        <select
                            class="config-input sub-unit"
                            on:change=move |e| unit.set(event_target_value(&e))
                        >
                            <option value="MB">"MB"</option>
                            <option value="GB" selected=true>"GB"</option>
                            <option value="TB">"TB"</option>
                        </select>
                    </div>
                    {move || error.get().map(|e| view! { <p class="error-msg">{e}</p> })}
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || peer.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { t!("sub.subscribing") } else { t!("sub.subscribe") }}
                    </button>
                </div>
            </div>
        </div>
    }
}

// ── Subscription info modal ─────────────────────────────────────────────────

#[component]
fn SubscriptionInfoModal(
    sub: Subscription,
    // Send + Sync: `do_save_scope` (which captures these) is referenced from a
    // reactive `<Show>` fallback, whose ViewFn must be Send + Sync.
    on_saved: impl Fn() + Copy + Send + Sync + 'static,
    on_close: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let files: RwSignal<Vec<MirrorFile>> = RwSignal::new(vec![]);
    let loaded = RwSignal::new(false);
    // Poll the mirror file list while the modal is open so the rows stay live —
    // states advance (pending → fetching → present) and a scope change's
    // asynchronously-synced files appear on the next tick. The `alive` flag
    // (checked after every await) stops the loop and prevents writing into freed
    // signals once the modal closes.
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));
    {
        let peer = sub.peer_id.clone();
        spawn_local(async move {
            loop {
                if !alive.load(Ordering::Relaxed) {
                    break;
                }
                let f = api_files(&peer).await;
                if !alive.load(Ordering::Relaxed) {
                    break;
                }
                files.set(f);
                loaded.set(true);
                sleep(Duration::from_millis(1500)).await;
            }
        });
    }

    let peer_id = sub.peer_id.clone();
    // Live subscription data (stats + available collections), refreshable.
    let info: RwSignal<Subscription> = RwSignal::new(sub.clone());
    let refreshing = RwSignal::new(false);
    let usage = move || {
        let s = info.get();
        t!(
            "sub.usage",
            disk = format_size(s.present_bytes),
            committed = format_size(s.used_bytes),
            quota = format_size(s.quota_bytes)
        )
        .to_string()
    };
    let summary = move || {
        let s = info.get();
        let fetching = s.wanted_count.saturating_sub(s.present_count);
        t!(
            "sub.summary",
            mirrored = s.present_count,
            fetching = fetching,
            over = s.skipped_count
        )
        .to_string()
    };
    let peer_refresh = StoredValue::new(sub.peer_id.clone());
    let do_refresh = move || {
        if refreshing.get() {
            return;
        }
        refreshing.set(true);
        let peer = peer_refresh.get_value();
        spawn_local(async move {
            // Kick a pull, give the async round-trip a moment, then re-read.
            api_sync(&peer).await;
            sleep(Duration::from_millis(1200)).await;
            if let Some(s) = api_get(&peer).await {
                info.set(s);
            }
            files.set(api_files(&peer).await);
            refreshing.set(false);
        });
    };

    // Quota editor, prefilled from the current quota. Copy-able peer id via a
    // StoredValue so the save closure can be reused in two handlers.
    let peer_sv = StoredValue::new(sub.peer_id.clone());
    let (init_val, init_unit) = split_quota(sub.quota_bytes);
    let quota = RwSignal::new(init_val);
    let unit = RwSignal::new(init_unit.to_string());
    let saving = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);
    // Transient "applied" confirmations; the modal stays open after a save.
    let quota_msg: RwSignal<Option<String>> = RwSignal::new(None);
    let scope_msg: RwSignal<Option<String>> = RwSignal::new(None);
    let (is_mb, is_gb, is_tb) = (init_unit == "MB", init_unit == "GB", init_unit == "TB");

    // Collection scope editor.
    let follow_all = RwSignal::new(sub.follow_all);
    let selected: RwSignal<std::collections::HashSet<String>> =
        RwSignal::new(sub.followed_collections.iter().cloned().collect());
    let scope_saving = RwSignal::new(false);
    let peer_scope = StoredValue::new(sub.peer_id.clone());
    // The collections in scope right now, to detect a narrowing on save.
    let before_scope: std::collections::HashSet<String> = if sub.follow_all {
        sub.available_collections.iter().cloned().collect()
    } else {
        sub.followed_collections.iter().cloned().collect()
    };
    let before = StoredValue::new(before_scope);
    // When the new scope drops collections, ask keep vs free before applying.
    let narrow_confirm = RwSignal::new(false);

    // The `sub` that opened us comes from the list row, which can be stale (the
    // <For> is keyed by peer id, so a row's captured snapshot isn't refreshed in
    // place). Fetch the authoritative current subscription on open and re-seed
    // the scope editor from it, so the checkboxes always reflect reality — no
    // page refresh needed. Done once, before the user can edit.
    {
        let peer = sub.peer_id.clone();
        spawn_local(async move {
            if let Some(s) = api_get(&peer).await {
                follow_all.set(s.follow_all);
                selected.set(s.followed_collections.iter().cloned().collect());
                before.set_value(if s.follow_all {
                    s.available_collections.iter().cloned().collect()
                } else {
                    s.followed_collections.iter().cloned().collect()
                });
                info.set(s);
            }
        });
    }

    let do_save_scope = move |keep: bool| {
        let fa = follow_all.get();
        let cols: Vec<String> = selected.get().into_iter().collect();
        let peer = peer_scope.get_value();
        // The scope we apply becomes the new baseline for narrowing detection,
        // so a second edit in the same session compares against it.
        let new_before: std::collections::HashSet<String> = if fa {
            info.get().available_collections.into_iter().collect()
        } else {
            cols.iter().cloned().collect()
        };
        scope_saving.set(true);
        scope_msg.set(None);
        spawn_local(async move {
            api_set_collections(&peer, fa, cols, keep).await;
            on_saved();
            before.set_value(new_before);
            // Refresh the modal in place (keep it open) so stats/files reflect
            // the change; closing is the X / Close / click-outside.
            if let Some(s) = api_get(&peer).await {
                info.set(s);
            }
            files.set(api_files(&peer).await);
            scope_saving.set(false);
            narrow_confirm.set(false);
            scope_msg.set(Some(t!("sub.collections_updated").to_string()));
        });
    };
    let save_scope = move || {
        // Scope after this change: everything (follow_all) or the selected set.
        let after: std::collections::HashSet<String> = if follow_all.get() {
            info.get().available_collections.into_iter().collect()
        } else {
            selected.get()
        };
        // Growing the scope applies straight away (adding never evicts).
        let removes = before.get_value().iter().any(|c| !after.contains(c));
        if !removes {
            do_save_scope(false);
            return;
        }
        // It drops collections — but only ask if something would actually be
        // freed; otherwise apply keeping (a no-op for content outside pin_dir).
        let peer = peer_scope.get_value();
        scope_saving.set(true);
        spawn_local(async move {
            let nothing_to_free = api_evictable(&peer).await == Some(0);
            scope_saving.set(false);
            if nothing_to_free {
                do_save_scope(true);
            } else {
                narrow_confirm.set(true);
            }
        });
    };

    let save = move || {
        let q: f64 = quota.get().trim().parse().unwrap_or(0.0);
        if q <= 0.0 {
            error.set(Some(t!("sub.err_positive_quota").to_string()));
            return;
        }
        let quota_bytes = quota_to_bytes(q, &unit.get());
        let peer = peer_sv.get_value();
        saving.set(true);
        error.set(None);
        quota_msg.set(None);
        spawn_local(async move {
            match api_add(peer.clone(), quota_bytes).await {
                Ok(()) => {
                    on_saved();
                    // Keep the modal open; reflect the new quota in the meter.
                    if let Some(s) = api_get(&peer).await {
                        info.set(s);
                    }
                    saving.set(false);
                    quota_msg.set(Some(t!("sub.quota_updated").to_string()));
                }
                Err(msg) => {
                    error.set(Some(msg));
                    saving.set(false);
                }
            }
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal modal-wide" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("sub.info_title")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="sub-info-peer">{peer_id}</p>
                    <p class="sub-info-line">{usage}</p>
                    <p class="sub-info-line">{summary}</p>
                    <div class="sub-quota-row">
                        <input
                            class="search-input"
                            type="number"
                            min="0"
                            step="any"
                            prop:value=move || quota.get()
                            on:input=move |e| { quota.set(event_target_value(&e)); quota_msg.set(None); }
                            on:keydown=move |e| { if e.key() == "Enter" { save(); } }
                        />
                        <select
                            class="config-input sub-unit"
                            on:change=move |e| { unit.set(event_target_value(&e)); quota_msg.set(None); }
                        >
                            <option value="MB" selected=is_mb>"MB"</option>
                            <option value="GB" selected=is_gb>"GB"</option>
                            <option value="TB" selected=is_tb>"TB"</option>
                        </select>
                        <button
                            class="btn-sm btn-primary"
                            disabled=move || saving.get()
                            on:click=move |_| save()
                        >
                            {move || if saving.get() { t!("common.saving") } else { t!("sub.update_quota") }}
                        </button>
                    </div>
                    {move || error.get().map(|e| view! { <p class="error-msg">{e}</p> })}
                    {move || quota_msg.get().map(|m| view! { <p class="sub-applied">{m}</p> })}

                    // Collection scope: follow the whole peer, or pick which of
                    // their collections to mirror.
                    <div class="sub-scope">
                        <div class="sub-scope-head">
                            <label class="sub-scope-all">
                                <input
                                    type="checkbox"
                                    prop:checked=move || follow_all.get()
                                    on:change=move |e| { follow_all.set(event_target_checked(&e)); scope_msg.set(None); }
                                />
                                <span>{t!("sub.mirror_all")}</span>
                            </label>
                            <button
                                class="icon-btn sub-refresh"
                                class:is-refreshing=move || refreshing.get()
                                title=t!("sub.refresh_title")
                                disabled=move || refreshing.get()
                                on:click=move |_| do_refresh()
                            >
                                <Icon paths=icons::REFRESH/>
                            </button>
                        </div>
                        <Show when=move || !follow_all.get()>
                            {move || {
                                let avail = info.get().available_collections;
                                if avail.is_empty() {
                                    view! {
                                        <p class="empty-hint">
                                            {t!("sub.no_collections")}
                                        </p>
                                    }.into_any()
                                } else {
                                    view! {
                                        <div class="sub-scope-list">
                                            <For
                                                each=move || info.get().available_collections
                                                key=|c| c.clone()
                                                children=move |c| {
                                                    let label = if c.is_empty() {
                                                        t!("sub.no_collection").to_string()
                                                    } else {
                                                        c.clone()
                                                    };
                                                    let key = c.clone();
                                                    view! {
                                                        <label class="sub-scope-item">
                                                            <input
                                                                type="checkbox"
                                                                prop:checked=move || selected.get().contains(&key)
                                                                on:change=move |e| {
                                                                    let on = event_target_checked(&e);
                                                                    selected.update(|s| {
                                                                        if on { s.insert(c.clone()); }
                                                                        else { s.remove(&c); }
                                                                    });
                                                                    scope_msg.set(None);
                                                                }
                                                            />
                                                            <span>{label}</span>
                                                        </label>
                                                    }
                                                }
                                            />
                                        </div>
                                    }.into_any()
                                }
                            }}
                        </Show>
                        // Following nothing is a valid, deliberate state: stay
                        // subscribed (don't lose the peer) but mirror nothing.
                        <Show when=move || {
                            !follow_all.get()
                                && selected.get().is_empty()
                                && !info.get().available_collections.is_empty()
                        }>
                            <p class="empty-hint">
                                {t!("sub.no_selected")}
                            </p>
                        </Show>
                        <Show
                            when=move || !narrow_confirm.get()
                            fallback=move || view! {
                                <div class="sub-scope-confirm">
                                    <p class="modal-hint">
                                        {t!("sub.narrow_hint")}
                                    </p>
                                    <div class="sub-scope-confirm-btns">
                                        <button
                                            class="btn-sm"
                                            on:click=move |_| narrow_confirm.set(false)
                                        >{t!("sub.back")}</button>
                                        <button
                                            class="btn-sm btn-danger"
                                            disabled=move || scope_saving.get()
                                            on:click=move |_| do_save_scope(false)
                                        >{t!("sub.free")}</button>
                                        <button
                                            class="btn-sm btn-primary"
                                            disabled=move || scope_saving.get()
                                            on:click=move |_| do_save_scope(true)
                                        >{t!("sub.keep")}</button>
                                    </div>
                                </div>
                            }
                        >
                            <div class="sub-scope-save">
                                {move || scope_msg.get().map(|m| view! { <span class="sub-applied">{m}</span> })}
                                <button
                                    class="btn-sm btn-primary"
                                    disabled=move || scope_saving.get()
                                    on:click=move |_| save_scope()
                                >
                                    {move || if scope_saving.get() { t!("common.saving") } else { t!("sub.update_collections") }}
                                </button>
                            </div>
                        </Show>
                    </div>

                    <div class="sub-file-list">
                        <Show
                            when=move || loaded.get()
                            fallback=|| view! { <p class="empty-hint">{t!("sub.loading")}</p> }
                        >
                            <Show
                                when=move || !files.get().is_empty()
                                fallback=|| view! {
                                    <p class="empty-hint">{t!("sub.no_files")}</p>
                                }
                            >
                                <ul class="share-file-list">
                                    <For
                                        each=move || files.get()
                                        key=|f| f.root_hash.clone()
                                        children=move |f| {
                                            let st = f.state.clone();
                                            let label = match st.as_str() {
                                                "present" => t!("sub.state.present"),
                                                "fetching" => t!("sub.state.fetching"),
                                                "missing" => t!("sub.state.pending"),
                                                "skipped" => t!("sub.state.over_quota"),
                                                "cancelled" => t!("sub.state.cancelled"),
                                                other => std::borrow::Cow::Owned(other.to_string()),
                                            };
                                            let pill_class = format!("mirror-state mirror-state-{st}");
                                            let name = f
                                                .name
                                                .clone()
                                                .unwrap_or_else(|| f.root_hash.chars().take(16).collect());
                                            // A cancelled file can be re-requested; the poll then
                                            // shows it move to fetching.
                                            let is_cancelled = st == "cancelled";
                                            let hash = f.root_hash.clone();
                                            view! {
                                                <li class="share-file-row">
                                                    <span class="share-file-name">{name}</span>
                                                    <span class="share-file-size">{format_size(f.size)}</span>
                                                    <span class=pill_class>{label}</span>
                                                    <Show when=move || is_cancelled fallback=|| ()>
                                                        {
                                                            let hash = hash.clone();
                                                            view! {
                                                                <button
                                                                    class="icon-btn"
                                                                    title=t!("sub.refetch_title")
                                                                    on:click=move |_| {
                                                                        let h = hash.clone();
                                                                        let peer = peer_refresh.get_value();
                                                                        spawn_local(async move {
                                                                            api_refetch(&peer, &h).await;
                                                                        });
                                                                    }
                                                                >
                                                                    <Icon paths=icons::REFRESH/>
                                                                </button>
                                                            }
                                                        }
                                                    </Show>
                                                </li>
                                            }
                                        }
                                    />
                                </ul>
                            </Show>
                        </Show>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("sub.close")}</button>
                </div>
            </div>
        </div>
    }
}
