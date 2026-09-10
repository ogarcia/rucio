//! Pins tab: content kept available on this node on purpose (fetch-and-retain).
//!
//! Lists pinned items with their state (available / fetching / missing), lets
//! the user pin a `rucio:` magnet, and unpin (which only drops the intent — the
//! content stays on disk, per the daemon's no-auto-delete policy).
//!
//! Rows behave like the Downloads/Shares lists: click to select (ctrl/⌘ to
//! toggle, shift for a range, plain tap on touch), with the collection and
//! unpin actions living in the toolbar and acting on the whole selection. A
//! state/collection/name filter bar sits in the status bar.

use std::collections::HashSet;

use leptos::prelude::*;
use leptos::task::spawn_local;
use rust_i18n::t;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{Pin, PinsResponse, format_size};

// ── API ─────────────────────────────────────────────────────────────────────

async fn api_list_pins() -> Option<PinsResponse> {
    gloo_net::http::Request::get(&crate::api::api("/api/v1/pins"))
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
    let req = gloo_net::http::Request::post(&crate::api::api("/api/v1/pins"))
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
    let url = crate::api::api(&format!("/api/v1/pins/{hash}/collection"));
    let body = serde_json::json!({ "collection": collection });
    if let Ok(req) = gloo_net::http::Request::put(&url).json(&body) {
        let _ = req.send().await;
    }
}

async fn api_remove_pin(hash: &str) {
    let url = crate::api::api(&format!("/api/v1/pins/{hash}"));
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

// ── Filter ────────────────────────────────────────────────────────────────────

/// State filter for the pin list, mirroring the pin `state` string.
#[derive(Clone, Copy, PartialEq)]
enum PinFilter {
    All,
    Available,
    Fetching,
    Missing,
}

impl PinFilter {
    fn matches(self, state: &str) -> bool {
        match self {
            PinFilter::All => true,
            PinFilter::Available => state == "available",
            PinFilter::Fetching => state == "fetching",
            PinFilter::Missing => state == "missing",
        }
    }

    /// Stable key for the `<select>` value and localStorage.
    fn as_key(self) -> &'static str {
        match self {
            PinFilter::All => "all",
            PinFilter::Available => "available",
            PinFilter::Fetching => "fetching",
            PinFilter::Missing => "missing",
        }
    }

    /// Parse a key back; unknown values fall back to `All`.
    fn from_key(v: &str) -> Self {
        match v {
            "available" => PinFilter::Available,
            "fetching" => PinFilter::Fetching,
            "missing" => PinFilter::Missing,
            _ => PinFilter::All,
        }
    }
}

/// Collection filter. Collection names are arbitrary user text, so a control
/// char that trimmed input can never contain is used as the "uncollected"
/// sentinel in the `<select>` value (empty = all).
const COLL_NONE: &str = "\u{1}";

#[derive(Clone, PartialEq)]
enum CollFilter {
    All,
    Uncollected,
    Name(String),
}

impl CollFilter {
    fn matches(&self, coll: &Option<String>) -> bool {
        match self {
            CollFilter::All => true,
            CollFilter::Uncollected => coll.as_deref().unwrap_or("").is_empty(),
            CollFilter::Name(n) => coll.as_deref() == Some(n.as_str()),
        }
    }

    /// Parse the `<select>` value: "" = all, sentinel = uncollected, else a name.
    fn from_value(v: &str) -> Self {
        match v {
            "" => CollFilter::All,
            COLL_NONE => CollFilter::Uncollected,
            name => CollFilter::Name(name.to_string()),
        }
    }

    /// Inverse of [`CollFilter::from_value`], for the `<select>` value and
    /// localStorage.
    fn to_value(&self) -> String {
        match self {
            CollFilter::All => String::new(),
            CollFilter::Uncollected => COLL_NONE.to_string(),
            CollFilter::Name(n) => n.clone(),
        }
    }
}

// ── Filter persistence ──────────────────────────────────────────────────────

/// localStorage keys for the persisted pin filters (state + collection), kept
/// across reloads like the active tab. The name search stays transient.
const FILTER_STATE_KEY: &str = "rucio-pin-filter";
const FILTER_COLL_KEY: &str = "rucio-pin-coll";

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
    // Multi-selection: the set of selected root hashes, plus the anchor row used
    // as the pivot for shift+click range selection.
    let selected: RwSignal<HashSet<String>> = RwSignal::new(HashSet::new());
    let anchor: RwSignal<Option<String>> = RwSignal::new(None);
    // When set to (hashes, prefill), the set-collection modal is open. The
    // prefill is the current collection, carried only for a single selection.
    let coll_modal: RwSignal<Option<(Vec<String>, Option<String>)>> = RwSignal::new(None);

    // State and collection filters persist across reloads; the name search stays
    // transient. Restore them from localStorage.
    let filter_state: RwSignal<PinFilter> = RwSignal::new(
        load_filter(FILTER_STATE_KEY)
            .map(|s| PinFilter::from_key(&s))
            .unwrap_or(PinFilter::All),
    );
    let filter_coll: RwSignal<CollFilter> = RwSignal::new(
        load_filter(FILTER_COLL_KEY)
            .map(|s| CollFilter::from_value(&s))
            .unwrap_or(CollFilter::All),
    );
    let filter_name: RwSignal<String> = RwSignal::new(String::new());

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

    // If the selected collection is later removed (its last pin re-filed or
    // unpinned), fall back to "all" so the user isn't stranded on an empty view.
    Effect::new(move |loaded: Option<bool>| {
        let loaded = loaded.unwrap_or(false) || collections.with(|c| !c.is_empty());
        if loaded
            && let CollFilter::Name(n) = filter_coll.get_untracked()
            && collections.with(|c| !c.iter().any(|x| x == &n))
        {
            filter_coll.set(CollFilter::All);
            save_filter(FILTER_COLL_KEY, &CollFilter::All.to_value());
        }
        loaded
    });

    // The pins currently selected and still present in the list.
    let selected_pins = move || -> Vec<Pin> {
        pins.with(|v| {
            v.iter()
                .filter(|p| selected.with(|s| s.contains(&p.root_hash)))
                .cloned()
                .collect()
        })
    };
    // Both bulk actions need at least one selected pin present in the list.
    let has_selection = move || !selected_pins().is_empty();

    // Visible (filtered) hashes in display order — used by the list and by
    // shift+click to resolve the range between the anchor and the clicked row.
    let visible_hashes = move || {
        let fs = filter_state.get();
        let fc = filter_coll.get();
        let q = filter_name.get().to_lowercase();
        pins.with(|v| {
            v.iter()
                .filter(|p| fs.matches(&p.state))
                .filter(|p| fc.matches(&p.collection))
                .filter(|p| pin_matches_name(p, &q))
                .map(|p| p.root_hash.clone())
                .collect::<Vec<String>>()
        })
    };

    // Row click with modifier keys: plain = select only this row; ctrl/⌘ =
    // toggle this row; shift = select the range from the anchor to this row.
    let on_row_click = Callback::new(move |(hash, additive, range): (String, bool, bool)| {
        if range && let Some(a) = anchor.get_untracked() {
            let vis = visible_hashes();
            if let (Some(i1), Some(i2)) = (
                vis.iter().position(|x| x == &a),
                vis.iter().position(|x| x == &hash),
            ) {
                let (lo, hi) = if i1 <= i2 { (i1, i2) } else { (i2, i1) };
                selected.set(vis[lo..=hi].iter().cloned().collect());
                return;
            }
        }
        if additive {
            selected.update(|s| {
                if !s.insert(hash.clone()) {
                    s.remove(&hash);
                }
            });
        } else {
            selected.set(HashSet::from([hash.clone()]));
        }
        anchor.set(Some(hash));
    });

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
                <div class="dl-toolbar">
                    <button
                        class="toolbar-btn"
                        title=t!("pin.add_title")
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PIN/>
                        <span class="btn-label">{t!("pin.add")}</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title=t!("pin.collection_title")
                        disabled=move || !has_selection()
                        on:click=move |_| {
                            let sel = selected_pins();
                            if sel.is_empty() {
                                return;
                            }
                            let hashes: Vec<String> =
                                sel.iter().map(|p| p.root_hash.clone()).collect();
                            // Prefill the current value only when editing a single
                            // pin; a bulk edit starts blank and overwrites all.
                            let prefill = (sel.len() == 1)
                                .then(|| sel[0].collection.clone())
                                .flatten();
                            coll_modal.set(Some((hashes, prefill)));
                        }
                    >
                        <Icon paths=icons::FOLDER/>
                        <span class="btn-label">{t!("pin.collection")}</span>
                    </button>
                    <button
                        class="toolbar-btn toolbar-btn-danger"
                        title=t!("pin.unpin_title")
                        disabled=move || !has_selection()
                        on:click=move |_| {
                            // Unpinning is reversible and non-destructive (the file
                            // stays on disk and shared), so no confirmation — same
                            // as the Shares list's bulk Unpin.
                            let hashes: Vec<String> = selected_pins()
                                .iter()
                                .map(|p| p.root_hash.clone())
                                .collect();
                            if hashes.is_empty() {
                                return;
                            }
                            spawn_local(async move {
                                for h in &hashes {
                                    api_remove_pin(h).await;
                                }
                                selected.set(HashSet::new());
                                if let Some(r) = api_list_pins().await {
                                    pins.set(r.pins);
                                    collections.set(r.collections);
                                }
                            });
                        }
                    >
                        <Icon paths=icons::PINNED_OFF/>
                        <span class="btn-label">{t!("pin.unpin")}</span>
                    </button>
                </div>
            </div>

            <div class="tab-scroll">
                <Show
                    when=move || !pins.get().is_empty()
                    fallback=|| view! {
                        <div class="empty-state empty-state-sm">
                            <p>{t!("pin.empty")}</p>
                            <p class="empty-hint">
                                {t!("pin.empty_hint")}
                            </p>
                        </div>
                    }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || {
                                let fs = filter_state.get();
                                let fc = filter_coll.get();
                                let q = filter_name.get().to_lowercase();
                                pins.with(|v| {
                                    v.iter()
                                        .filter(|p| fs.matches(&p.state))
                                        .filter(|p| fc.matches(&p.collection))
                                        .filter(|p| pin_matches_name(p, &q))
                                        .cloned()
                                        .collect::<Vec<Pin>>()
                                })
                            }
                            // Key on the collection too, so re-filing a pin
                            // changes its key and the row (with its collection
                            // label) rebuilds.
                            key=|p| (p.root_hash.clone(), p.collection.clone())
                            children=move |p| {
                                let hash = p.root_hash.clone();
                                let title = p
                                    .name
                                    .clone()
                                    .unwrap_or_else(|| p.root_hash.chars().take(16).collect());
                                let meta = {
                                    let size = p
                                        .size
                                        .map(format_size)
                                        .unwrap_or_else(|| t!("pin.unknown_size").to_string());
                                    // Full hash; the meta line truncates with an
                                    // ellipsis via CSS only when it doesn't fit.
                                    format!("{size} · {}", p.root_hash)
                                };
                                let state = p.state.clone();
                                let state_class = format!("pin-state pin-state-{state}");
                                let state_label = match state.as_str() {
                                    "available" => t!("pin.state.available"),
                                    "fetching" => t!("pin.state.fetching"),
                                    "missing" => t!("pin.state.missing"),
                                    _ => std::borrow::Cow::Owned(state.clone()),
                                };
                                // Read-only collection tag (editing is in the
                                // toolbar); shown only when the pin is filed.
                                let coll_tag = p.collection.clone().filter(|c| !c.is_empty());
                                let row_hash = hash.clone();
                                view! {
                                    <li
                                        class=move || {
                                            let mut c = String::from("share-dir-row");
                                            if selected.with(|s| s.contains(&row_hash)) {
                                                c.push_str(" share-dir-selected");
                                            }
                                            c
                                        }
                                        on:click=move |ev| {
                                            // On a touchscreen there are no modifiers,
                                            // so a plain tap toggles (builds a set).
                                            let additive = ev.ctrl_key()
                                                || ev.meta_key()
                                                || crate::platform::coarse_pointer();
                                            on_row_click.run((hash.clone(), additive, ev.shift_key()));
                                        }
                                    >
                                        <span class="share-dir-icon"><Icon paths=icons::PIN/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path">{title}</span>
                                            <span class="share-dir-meta">{meta}</span>
                                        </div>
                                        <div class="pin-side">
                                            {coll_tag.map(|c| view! {
                                                <span class="pin-collection-tag">{c}</span>
                                            })}
                                            <span class=state_class>{state_label}</span>
                                        </div>
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
                // The filter controls are meaningless with an empty list, so
                // they only appear once there is something to filter.
                <Show when=move || !pins.get().is_empty()>
                    <select
                        class="dl-filter-select"
                        prop:value=move || filter_state.get().as_key()
                        on:change=move |e| {
                            let fs = PinFilter::from_key(&event_target_value(&e));
                            filter_state.set(fs);
                            save_filter(FILTER_STATE_KEY, fs.as_key());
                        }
                    >
                        <option value="all">{t!("pin.filter.all")}</option>
                        <option value="available">{t!("pin.filter.available")}</option>
                        <option value="fetching">{t!("pin.filter.fetching")}</option>
                        <option value="missing">{t!("pin.filter.missing")}</option>
                    </select>
                    <Show when=move || !collections.get().is_empty()>
                        <select
                            class="dl-filter-select"
                            prop:value=move || filter_coll.get().to_value()
                            on:change=move |e| {
                                let fc = CollFilter::from_value(&event_target_value(&e));
                                filter_coll.set(fc.clone());
                                save_filter(FILTER_COLL_KEY, &fc.to_value());
                            }
                        >
                            <option value="">{t!("pin.filter.all_collections")}</option>
                            <option value=COLL_NONE>{t!("pin.filter.uncollected")}</option>
                            <For each=move || collections.get() key=|c| c.clone() let:c>
                                {
                                    let val = c.clone();
                                    view! { <option value=val>{c}</option> }
                                }
                            </For>
                        </select>
                    </Show>
                    <input
                        type="text"
                        class="dl-filter-input"
                        placeholder=t!("pin.filter.placeholder")
                        prop:value=move || filter_name.get()
                        on:input=move |e| filter_name.set(event_target_value(&e))
                    />
                </Show>
                {move || {
                    let n = pins.get().len();
                    if n == 0 {
                        view! { <span class="dl-active-count dl-active-none">{t!("pin.none")}</span> }
                            .into_any()
                    } else {
                        view! { <span class="dl-active-count">{t!("pin.count", n = n)}</span> }
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

        <Show when=move || coll_modal.get().is_some()>
            {move || {
                let (hashes, current) = coll_modal.get().unwrap();
                view! {
                    <SetCollectionModal
                        hashes=hashes
                        current=current
                        on_saved=move || {
                            reload();
                            selected.set(HashSet::new());
                        }
                        on_close=move || coll_modal.set(None)
                    />
                }
            }}
        </Show>
    }
}

/// Case-insensitive match of a pin against a name/hash search (`q` already
/// lowercased). An empty query matches everything.
fn pin_matches_name(p: &Pin, q: &str) -> bool {
    if q.is_empty() {
        return true;
    }
    p.name.as_deref().unwrap_or("").to_lowercase().contains(q)
        || p.root_hash.to_lowercase().contains(q)
}

// ── Change-collection modal ─────────────────────────────────────────────────

/// Re-file the selected pins under a collection (or clear it). Opened from the
/// toolbar; mirrors the collection control in the add-pin and share-pin modals
/// so the interaction is consistent. Applies to one or many pins — a single
/// selection prefills the current value, a bulk edit starts blank and
/// overwrites every selected pin (blank = uncollected).
#[component]
fn SetCollectionModal(
    hashes: Vec<String>,
    current: Option<String>,
    on_saved: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    crate::overlays::close_on_escape(on_close);
    let collection = RwSignal::new(current.unwrap_or_default());
    let count = hashes.len();
    let hashes = StoredValue::new(hashes);
    let busy = RwSignal::new(false);

    let submit = move || {
        let col = {
            let c = collection.get();
            (!c.trim().is_empty()).then(|| c.trim().to_string())
        };
        busy.set(true);
        spawn_local(async move {
            for h in hashes.get_value() {
                api_set_pin_collection(&h, col.clone()).await;
            }
            on_saved();
            on_close();
        });
    };

    view! {
        <div class="modal-backdrop">
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">
                        {if count == 1 {
                            t!("pin.change_collection").to_string()
                        } else {
                            t!("pin.set_collection_n", n = count).to_string()
                        }}
                    </span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {if count == 1 {
                            t!("pin.collection_hint").to_string()
                        } else {
                            t!("pin.collection_hint_bulk", n = count).to_string()
                        }}
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        list="pin-collections"
                        placeholder=t!("pin.collection_placeholder")
                        prop:value=move || collection.get()
                        on:input=move |e| collection.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { t!("common.saving") } else { t!("common.save") }}
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
    crate::overlays::close_on_escape(on_close);
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
        <div class="modal-backdrop">
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">{t!("pin.add")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("pin.add_hint")}
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        placeholder=t!("pin.magnet_placeholder")
                        prop:value=move || magnet.get()
                        on:input=move |e| magnet.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                    <input
                        class="search-input"
                        type="text"
                        list="pin-collections-modal"
                        placeholder=t!("pin.collection_optional_placeholder")
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
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || magnet.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { t!("pin.pinning") } else { t!("pin.pin_btn") }}
                    </button>
                </div>
            </div>
        </div>
    }
}
