//! Subscriptions tab: mirror other nodes' pinned content (cooperative pinning).
//!
//! Lists subscriptions with a used/quota storage meter, lets the user subscribe
//! to a peer (a PeerId or a `rucio-peer:` link) within a disk quota, copy this
//! node's own shareable link, and unsubscribe (which evicts content nobody else
//! wants).

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{StatusResponse, Subscription, SubscriptionsResponse, format_size};

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
        Err(
            "Invalid peer id or quota — check the link, and note you can't \
             subscribe to your own node."
                .to_string(),
        )
    } else {
        Err(format!("HTTP {}", resp.status()))
    }
}

async fn api_remove(peer_id: &str) {
    let url = format!("/api/v1/subscriptions/{peer_id}");
    let _ = gloo_net::http::Request::delete(&url).send().await;
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

fn confirm(message: &str) -> bool {
    web_sys::window()
        .and_then(|w| w.confirm_with_message(message).ok())
        .unwrap_or(false)
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
                        title="Subscribe to a peer's pinned content (mirror it within a quota)"
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PLUS/>
                        <span class="btn-label">"Subscribe"</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title="Copy this node's link so others can mirror your pinned content"
                        on:click=copy_link
                    >
                        <Icon paths=icons::COPY/>
                        <span class="btn-label">
                            {move || if copied.get() { "Copied!" } else { "Copy my link" }}
                        </span>
                    </button>
                </div>
            </div>

            <div class="tab-scroll">
                <Show
                    when=move || !subs.get().is_empty()
                    fallback=|| view! {
                        <div class="empty-state empty-state-sm">
                            <p>"No subscriptions"</p>
                            <p class="empty-hint">
                                "Subscribe to a peer to mirror its pinned content within a disk
                                 quota, helping keep that content available."
                            </p>
                        </div>
                    }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || subs.get()
                            key=|s| s.peer_id.clone()
                            children=move |s| {
                                let peer_rm = s.peer_id.clone();
                                let peer_short: String = s.peer_id.chars().take(20).collect();
                                let pct = if s.quota_bytes > 0 {
                                    (s.used_bytes as f64 / s.quota_bytes as f64 * 100.0)
                                        .clamp(0.0, 100.0)
                                } else {
                                    0.0
                                };
                                let meter_text = format!(
                                    "{} / {}",
                                    format_size(s.used_bytes),
                                    format_size(s.quota_bytes)
                                );
                                let files = if s.skipped_count > 0 {
                                    format!(
                                        "{} mirrored (+{} over quota)",
                                        s.wanted_count, s.skipped_count
                                    )
                                } else {
                                    format!("{} mirrored", s.wanted_count)
                                };
                                let synced = if s.last_synced_at == 0 {
                                    "not synced yet".to_string()
                                } else {
                                    "synced".to_string()
                                };
                                view! {
                                    <li class="share-dir-row">
                                        <span class="share-dir-icon"><Icon paths=icons::NETWORK/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path">{peer_short}"…"</span>
                                            <div class="dl-bar-track sub-meter">
                                                <div
                                                    class="dl-bar-fill"
                                                    style=move || format!("width:{pct:.1}%")
                                                ></div>
                                            </div>
                                            <span class="share-dir-meta">
                                                {meter_text}" · "{files}" · "{synced}
                                            </span>
                                        </div>
                                        <button
                                            class="icon-btn icon-btn-danger"
                                            title="Unsubscribe (evicts mirrored content nobody else wants)"
                                            on:click=move |_| {
                                                let p = peer_rm.clone();
                                                if confirm(
                                                    "Unsubscribe from this peer?\n\nMirrored content that no other subscription wants — and that you haven't pinned — will be removed from disk.",
                                                ) {
                                                    spawn_local(async move {
                                                        api_remove(&p).await;
                                                        if let Some(s) = api_list().await {
                                                            subs.set(s);
                                                        }
                                                    });
                                                }
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
                        view! { <span class="dl-active-count dl-active-none">"No subscriptions"</span> }
                            .into_any()
                    } else {
                        view! { <span class="dl-active-count">{format!("{n} subscribed")}</span> }
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
            error.set(Some("Enter a peer link and a positive quota.".to_string()));
            return;
        }
        let mult: u64 = match unit.get().as_str() {
            "MB" => 1024 * 1024,
            "TB" => 1024u64 * 1024 * 1024 * 1024,
            _ => 1024 * 1024 * 1024, // GB
        };
        let quota_bytes = (q * mult as f64) as u64;
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
                    <span class="modal-title">"Subscribe to a peer"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Paste a peer's link (or PeerId). Its pinned content is mirrored on
                         this node, smallest files first, up to the quota you set."
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
                            placeholder="Quota"
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
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || peer.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { "Subscribing…" } else { "Subscribe" }}
                    </button>
                </div>
            </div>
        </div>
    }
}
