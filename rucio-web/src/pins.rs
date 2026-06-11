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

async fn api_list_pins() -> Option<PinsResponse> {
    gloo_net::http::Request::get("/api/v1/pins")
        .send()
        .await
        .ok()?
        .json::<PinsResponse>()
        .await
        .ok()
}

/// Pin a magnet into an optional collection. `Err(message)` on failure.
async fn api_add_pin(magnet: String, collection: Option<String>) -> Result<(), String> {
    let body = serde_json::json!({ "magnet": magnet, "collection": collection });
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

/// Re-file a pin under a different collection (None/empty = uncollected).
async fn api_set_pin_collection(hash: &str, collection: Option<String>) {
    let url = format!("/api/v1/pins/{hash}/collection");
    let body = serde_json::json!({ "collection": collection });
    if let Ok(req) = gloo_net::http::Request::put(&url).json(&body) {
        let _ = req.send().await;
    }
}

async fn api_remove_pin(hash: &str) {
    let url = format!("/api/v1/pins/{hash}");
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// Normalise a pin input into a `rucio:` magnet: a magnet is used as-is; a bare
/// 64-character hex root hash becomes `rucio:<hash>`. Anything else is returned
/// untouched and left for the daemon to reject.
fn resolve_pin_input(input: &str) -> String {
    let t = input.trim();
    if t.starts_with("rucio:") {
        t.to_string()
    } else if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!("rucio:{}", t.to_lowercase())
    } else {
        t.to_string()
    }
}

// ── Component ─────────────────────────────────────────────────────────────────

#[component]
pub fn PinsTab(
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    let pins: RwSignal<Vec<Pin>> = RwSignal::new(vec![]);
    let collections: RwSignal<Vec<String>> = RwSignal::new(vec![]);
    let add_open: RwSignal<bool> = RwSignal::new(false);
    // When set to (hash, current_collection), the change-collection modal is open.
    let edit_modal: RwSignal<Option<(String, Option<String>)>> = RwSignal::new(None);

    let reload = move || {
        spawn_local(async move {
            if let Some(r) = api_list_pins().await {
                pins.set(r.pins);
                collections.set(r.collections);
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
                        title="Pin content by magnet or root hash (fetched if missing, then kept available)"
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PIN/>
                        <span class="btn-label">"Pin content"</span>
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
                            // Key on the collection too, so re-filing a pin
                            // changes its key and the row (and its pill) rebuilds.
                            key=|p| (p.root_hash.clone(), p.collection.clone())
                            children=move |p| {
                                let hash_rm = p.root_hash.clone();
                                let hash_col = p.root_hash.clone();
                                let col_now = p.collection.clone();
                                let title = p
                                    .name
                                    .clone()
                                    .unwrap_or_else(|| p.root_hash.chars().take(16).collect());
                                let meta = {
                                    let size = p
                                        .size
                                        .map(format_size)
                                        .unwrap_or_else(|| "unknown size".to_string());
                                    // Full hash; the meta line truncates with an
                                    // ellipsis via CSS only when it doesn't fit.
                                    format!("{size} · {}", p.root_hash)
                                };
                                let state = p.state.clone();
                                let state_class = format!("pin-state pin-state-{state}");
                                let (pill_label, pill_class) = match &col_now {
                                    Some(c) if !c.is_empty() => {
                                        (c.clone(), "pin-collection-pill")
                                    }
                                    _ => (
                                        "+ collection".to_string(),
                                        "pin-collection-pill pin-collection-pill-empty",
                                    ),
                                };
                                view! {
                                    <li class="share-dir-row static-row">
                                        <span class="share-dir-icon"><Icon paths=icons::PIN/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path">{title}</span>
                                            <span class="share-dir-meta">{meta}</span>
                                        </div>
                                        <div class="pin-side">
                                            <button
                                                class=pill_class
                                                title="Change publishing collection — subscribers can follow specific collections of yours"
                                                on:click=move |_| {
                                                    edit_modal.set(Some((hash_col.clone(), col_now.clone())));
                                                }
                                            >
                                                {pill_label}
                                            </button>
                                            <span class=state_class>{state}</span>
                                        </div>
                                        <button
                                            class="icon-btn icon-btn-danger"
                                            title="Unpin (content stays on disk)"
                                            on:click=move |_| {
                                                // Unpinning is reversible and non-destructive
                                                // (the file stays on disk and shared), so no
                                                // confirmation — consistent with the Shares
                                                // list's Unpin toggle.
                                                let h = hash_rm.clone();
                                                spawn_local(async move {
                                                    api_remove_pin(&h).await;
                                                    if let Some(r) = api_list_pins().await {
                                                        pins.set(r.pins);
                                                        collections.set(r.collections);
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
                    <datalist id="pin-collections">
                        <For
                            each=move || collections.get()
                            key=|c| c.clone()
                            children=move |c| view! { <option value=c></option> }
                        />
                    </datalist>
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
                collections=collections
                on_added=move || reload()
                on_close=move || add_open.set(false)
            />
        </Show>

        <Show when=move || edit_modal.get().is_some()>
            {move || {
                let (hash, current) = edit_modal.get().unwrap();
                view! {
                    <SetCollectionModal
                        hash=hash
                        current=current
                        on_saved=move || reload()
                        on_close=move || edit_modal.set(None)
                    />
                }
            }}
        </Show>
    }
}

// ── Change-collection modal ─────────────────────────────────────────────────

/// Re-file an existing pin under a different collection (or clear it). Opened by
/// clicking a pin's collection pill; mirrors the collection control in the
/// add-pin and share-pin modals so the interaction is consistent.
#[component]
fn SetCollectionModal(
    hash: String,
    current: Option<String>,
    on_saved: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let collection = RwSignal::new(current.unwrap_or_default());
    let hash = StoredValue::new(hash);
    let busy = RwSignal::new(false);

    let submit = move || {
        let col = {
            let c = collection.get();
            (!c.trim().is_empty()).then(|| c.trim().to_string())
        };
        busy.set(true);
        spawn_local(async move {
            api_set_pin_collection(&hash.get_value(), col).await;
            on_saved();
            on_close();
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">"Change collection"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Subscribers can follow specific collections of yours. Leave it blank to
                         make this pin uncollected."
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        list="pin-collections"
                        placeholder="Collection — e.g. Manuals, Series"
                        prop:value=move || collection.get()
                        on:input=move |e| collection.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { "Saving…" } else { "Save" }}
                    </button>
                </div>
            </div>
        </div>
    }
}

// ── Add-pin modal ─────────────────────────────────────────────────────────────

#[component]
fn AddPinModal(
    collections: RwSignal<Vec<String>>,
    on_added: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let magnet = RwSignal::new(String::new());
    let collection = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    let submit = move || {
        let raw = magnet.get();
        if raw.trim().is_empty() {
            return;
        }
        let m = resolve_pin_input(&raw);
        let col = {
            let c = collection.get();
            (!c.trim().is_empty()).then(|| c.trim().to_string())
        };
        busy.set(true);
        error.set(None);
        spawn_local(async move {
            match api_add_pin(m, col).await {
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
                    <span class="modal-title">"Pin content"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Paste a rucio: magnet or a 64-character root hash. If you already have
                         the content it's simply marked as kept; if not, it's fetched from the
                         network and then kept available (re-provided) on this node."
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        placeholder="rucio:<hash>?name=… — or a 64-char hash"
                        prop:value=move || magnet.get()
                        on:input=move |e| magnet.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                    <input
                        class="search-input"
                        type="text"
                        list="pin-collections-modal"
                        placeholder="Collection (optional) — e.g. Manuals, Series"
                        prop:value=move || collection.get()
                        on:input=move |e| collection.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                    <datalist id="pin-collections-modal">
                        <For
                            each=move || collections.get()
                            key=|c| c.clone()
                            children=move |c| view! { <option value=c></option> }
                        />
                    </datalist>
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
