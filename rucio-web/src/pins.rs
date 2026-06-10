//! Pins tab: content kept available on this node on purpose (fetch-and-retain).
//!
//! Lists pinned items with their state (available / fetching / missing), lets
//! the user pin a `rucio:` magnet, and unpin (which only drops the intent — the
//! content stays on disk, per the daemon's no-auto-delete policy).

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{Pin, PinsResponse, format_size};

// ── API ─────────────────────────────────────────────────────────────────────

async fn api_list_pins() -> Option<Vec<Pin>> {
    gloo_net::http::Request::get("/api/v1/pins")
        .send()
        .await
        .ok()?
        .json::<PinsResponse>()
        .await
        .ok()
        .map(|r| r.pins)
}

/// Pin a magnet. Returns `Err(message)` on a request/validation failure.
async fn api_add_pin(magnet: String) -> Result<(), String> {
    let body = serde_json::json!({ "magnet": magnet });
    let req = gloo_net::http::Request::post("/api/v1/pins")
        .json(&body)
        .map_err(|e| e.to_string())?;
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.ok() {
        Ok(())
    } else {
        let msg = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
            .unwrap_or_else(|| format!("HTTP {}", resp.status()));
        Err(msg)
    }
}

async fn api_remove_pin(hash: &str) {
    let url = format!("/api/v1/pins/{hash}");
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

fn confirm(message: &str) -> bool {
    web_sys::window()
        .and_then(|w| w.confirm_with_message(message).ok())
        .unwrap_or(false)
}

// ── Component ─────────────────────────────────────────────────────────────────

#[component]
pub fn PinsTab(
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    let pins: RwSignal<Vec<Pin>> = RwSignal::new(vec![]);
    let add_open: RwSignal<bool> = RwSignal::new(false);

    let reload = move || {
        spawn_local(async move {
            if let Some(p) = api_list_pins().await {
                pins.set(p);
            }
        });
    };
    // Initial load.
    reload();

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
                <div class="dl-toolbar">
                    <button
                        class="toolbar-btn"
                        title="Pin a magnet (fetched if missing, then kept available)"
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PIN/>
                        <span class="btn-label">"Pin a magnet"</span>
                    </button>
                </div>
            </div>

            <div class="tab-scroll">
                <Show
                    when=move || !pins.get().is_empty()
                    fallback=|| view! {
                        <div class="empty-state empty-state-sm">
                            <p>"Nothing pinned"</p>
                            <p class="empty-hint">
                                "Pin a magnet to keep that content available on this node."
                            </p>
                        </div>
                    }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || pins.get()
                            key=|p| p.root_hash.clone()
                            children=move |p| {
                                let hash_rm = p.root_hash.clone();
                                let title = p
                                    .name
                                    .clone()
                                    .unwrap_or_else(|| p.root_hash.chars().take(16).collect());
                                let meta = {
                                    let size = p
                                        .size
                                        .map(format_size)
                                        .unwrap_or_else(|| "unknown size".to_string());
                                    let short: String = p.root_hash.chars().take(12).collect();
                                    format!("{size} · {short}…")
                                };
                                let state = p.state.clone();
                                let state_class = format!("pin-state pin-state-{state}");
                                view! {
                                    <li class="share-dir-row">
                                        <span class="share-dir-icon"><Icon paths=icons::PIN/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path">{title}</span>
                                            <span class="share-dir-meta">{meta}</span>
                                        </div>
                                        <span class=state_class>{state}</span>
                                        <button
                                            class="icon-btn icon-btn-danger"
                                            title="Unpin (content stays on disk)"
                                            on:click=move |_| {
                                                let h = hash_rm.clone();
                                                if confirm(
                                                    "Unpin this item?\n\nThe content stays on disk; this only stops keeping it on purpose.",
                                                ) {
                                                    spawn_local(async move {
                                                        api_remove_pin(&h).await;
                                                        if let Some(p) = api_list_pins().await {
                                                            pins.set(p);
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
                    let n = pins.get().len();
                    if n == 0 {
                        view! { <span class="dl-active-count dl-active-none">"No pins"</span> }
                            .into_any()
                    } else {
                        view! { <span class="dl-active-count">{format!("{n} pinned")}</span> }
                            .into_any()
                    }
                }}
            </StatusBar>
        </div>

        <Show when=move || add_open.get()>
            <AddPinModal
                on_added=move || reload()
                on_close=move || add_open.set(false)
            />
        </Show>
    }
}

// ── Add-pin modal ─────────────────────────────────────────────────────────────

#[component]
fn AddPinModal(
    on_added: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let magnet = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    let submit = move || {
        let m = magnet.get().trim().to_string();
        if m.is_empty() {
            return;
        }
        busy.set(true);
        error.set(None);
        spawn_local(async move {
            match api_add_pin(m).await {
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
                    <span class="modal-title">"Pin a magnet"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Paste a rucio: magnet. If you don't already have the content it is
                         fetched and then kept available (re-provided) on this node."
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        placeholder="rucio:<hash>?name=…&size=…"
                        prop:value=move || magnet.get()
                        on:input=move |e| magnet.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                    {move || error.get().map(|e| view! { <p class="error-msg">{e}</p> })}
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || magnet.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { "Pinning…" } else { "Pin" }}
                    </button>
                </div>
            </div>
        </div>
    }
}
