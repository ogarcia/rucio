use std::collections::HashSet;
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
    AddShareResponse, EmuleStatusResponse, PinsResponse, ShareFile, SharedDir, SharedDirKind,
    SharedDirsResponse, SharesFilesResponse, format_size,
};

// ── API ─────────────────────────────────────────────────────────────────────

async fn api_list_dirs() -> Option<Vec<SharedDir>> {
    gloo_net::http::Request::get(&crate::api::api("/api/v1/shares"))
        .send()
        .await
        .ok()?
        .json::<SharedDirsResponse>()
        .await
        .ok()
        .map(|r| r.dirs)
}

/// Fetch one page of shared files, filtered server-side by name (`q`) and/or
/// directory (`dir`). Returns the page plus the total number of matches.
async fn api_list_files_page(
    q: &str,
    dir: Option<&str>,
    offset: u32,
    limit: u32,
) -> Option<(Vec<ShareFile>, u64)> {
    let mut url = crate::api::api(&format!(
        "/api/v1/shares/files?limit={limit}&offset={offset}"
    ));
    if !q.is_empty() {
        url.push_str("&q=");
        url.push_str(&urlencoding_encode(q));
    }
    if let Some(d) = dir {
        url.push_str("&dir=");
        url.push_str(&urlencoding_encode(d));
    }
    gloo_net::http::Request::get(&url)
        .send()
        .await
        .ok()?
        .json::<SharesFilesResponse>()
        .await
        .ok()
        .map(|r| (r.shares, r.total))
}

/// POST a directory to share. Returns the queued count and any read errors, or
/// `Err(message)` on a request/validation failure.
async fn api_add_dir(path: String) -> Result<AddShareResponse, String> {
    let body = serde_json::json!({ "path": path });
    let req = gloo_net::http::Request::post(&crate::api::api("/api/v1/shares"))
        .json(&body)
        .map_err(|e| e.to_string())?;
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.ok() {
        resp.json::<AddShareResponse>()
            .await
            .map_err(|e| e.to_string())
    } else {
        // The daemon returns { "error": "…" } on 400.
        let msg = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
            .unwrap_or_else(|| format!("HTTP {}", resp.status()));
        Err(msg)
    }
}

async fn api_remove_dir(path: &str) {
    let url = crate::api::api(&format!("/api/v1/shares?path={}", urlencoding_encode(path)));
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// Pin a shared file by its magnet, into an optional collection. Since the file
/// is already present, this never re-fetches — it just marks it as deliberately
/// kept, which also publishes it (under `collection`) in this node's pin-set for
/// subscribers.
async fn api_pin_magnet(magnet: String, collection: Option<String>) {
    let body = serde_json::json!({ "magnet": magnet, "collection": collection });
    if let Ok(req) = gloo_net::http::Request::post(&crate::api::api("/api/v1/pins")).json(&body) {
        let _ = req.send().await;
    }
}

/// Whether eMule is actually active on this daemon — compiled in *and* enabled
/// in the config. Only then is the ed2k backfill running and generating links,
/// so the info overlay offers an eMule link only in this case (`runtime_enabled`
/// implies `feature_enabled`, so it covers both "no eMule build" and "eMule
/// disabled by config").
async fn api_emule_active() -> bool {
    let Ok(resp) = gloo_net::http::Request::get(&crate::api::api("/api/v1/emule/status"))
        .send()
        .await
    else {
        return false;
    };
    resp.json::<EmuleStatusResponse>()
        .await
        .map(|s| s.runtime_enabled)
        .unwrap_or(false)
}

/// Unpin a root hash (drops the pin intent; the file stays shared).
async fn api_unpin(hash: &str) {
    let url = crate::api::api(&format!("/api/v1/pins/{hash}"));
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// The set of currently-pinned root hashes (hex) plus the distinct collection
/// labels in use, so the shared-files list can show what's pinned, offer to
/// unpin, and suggest existing collections when pinning.
async fn api_list_pinned() -> (HashSet<String>, Vec<String>) {
    let Ok(resp) = gloo_net::http::Request::get(&crate::api::api("/api/v1/pins"))
        .send()
        .await
    else {
        return (HashSet::new(), Vec::new());
    };
    match resp.json::<PinsResponse>().await {
        Ok(r) => (
            r.pins.into_iter().map(|p| p.root_hash).collect(),
            r.collections,
        ),
        Err(_) => (HashSet::new(), Vec::new()),
    }
}

/// Minimal percent-encoding for the `path` query value (spaces and reserved
/// chars). `js_sys`'s `encodeURIComponent` keeps it dependency-free.
fn urlencoding_encode(s: &str) -> String {
    js_sys::encode_uri_component(s).into()
}

fn copy_to_clipboard(text: &str) {
    if let Some(win) = web_sys::window() {
        // Fire-and-forget: the returned Promise resolves asynchronously and we
        // don't need to await it.
        //
        // NOTE: the Clipboard API only works in a *secure context*. localhost
        // counts as secure, as does any HTTPS origin, so copying works when the
        // panel is served locally or behind a TLS reverse proxy (the expected
        // setup when exposed to the internet). Over plain HTTP to a LAN IP the
        // browser blocks it and this silently no-ops — an accepted limitation.
        let _ = win.navigator().clipboard().write_text(text);
    }
}

/// Last path component, for a compact folder label.
fn basename(p: &str) -> &str {
    let trimmed = p.trim_end_matches('/');
    trimmed.rsplit('/').next().unwrap_or(trimmed)
}

fn confirm(message: &str) -> bool {
    web_sys::window()
        .and_then(|w| w.confirm_with_message(message).ok())
        .unwrap_or(false)
}

// ── Tab ───────────────────────────────────────────────────────────────────────

/// Files fetched per page (and on the initial load). The server caps this.
const PAGE_SIZE: u32 = 200;

/// `(root_hash, magnet)` pairs handed to the pin modal — every selected file to
/// pin under one chosen collection.
type PinTargets = Vec<(String, String)>;

#[component]
pub fn SharesTab(
    indexing: RwSignal<usize>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    let dirs: RwSignal<Vec<SharedDir>> = RwSignal::new(vec![]);
    // The files loaded so far (one or more appended pages). The full set is never
    // loaded at once: filtering and paging happen on the server so this scales.
    let files: RwSignal<Vec<ShareFile>> = RwSignal::new(vec![]);
    // Total files matching the current filter (server-side), to show progress
    // and decide whether a "Load more" page remains.
    let total: RwSignal<u64> = RwSignal::new(0);
    let loading: RwSignal<bool> = RwSignal::new(false);
    let filter: RwSignal<String> = RwSignal::new(String::new());
    let add_open: RwSignal<bool> = RwSignal::new(false);
    // When set, the file list is restricted to this directory. Toggled by
    // clicking a folder row.
    let selected_dir: RwSignal<Option<String>> = RwSignal::new(None);
    // Multi-selection over the loaded files (by root_hash), plus the anchor row
    // for shift+click range selection — the same model as the downloads list.
    // Per-file actions (info, pin, unpin) act on the selection from the toolbar,
    // so the rows themselves carry no buttons (keeps them readable on mobile).
    let selected: RwSignal<HashSet<String>> = RwSignal::new(HashSet::new());
    let anchor: RwSignal<Option<String>> = RwSignal::new(None);
    // The file shown in the info overlay (the single selected one), or None.
    let detail: RwSignal<Option<ShareFile>> = RwSignal::new(None);
    // Whether eMule is active (compiled in and enabled), resolved once below —
    // the info overlay only offers an eMule link when it is.
    let emule_active: RwSignal<bool> = RwSignal::new(false);
    // Set of currently-pinned hashes, so rows show a pin marker and the toolbar
    // knows whether Pin/Unpin apply to the selection.
    let pinned_set: RwSignal<HashSet<String>> = RwSignal::new(HashSet::new());
    // Distinct collection labels in use, suggested when pinning.
    let pin_collections: RwSignal<Vec<String>> = RwSignal::new(Vec::new());
    // When set, the "choose a collection" pin modal is open for these
    // (hash, magnet) targets.
    let pin_modal: RwSignal<Option<PinTargets>> = RwSignal::new(None);
    // Bumped on every load; a response is applied only if its generation is
    // still current, so a reset (new filter/dir) discards an in-flight page.
    let load_gen: RwSignal<u32> = RwSignal::new(0);

    // Load a page from the server. `reset` starts a fresh result set (offset 0,
    // replacing the list); otherwise it appends the next page.
    let load_files = Callback::new(move |reset: bool| {
        if reset {
            // A fresh result set replaces the list; drop any prior selection so
            // the toolbar can't act on rows that are no longer shown.
            selected.set(HashSet::new());
            anchor.set(None);
        }
        let q = filter.get_untracked();
        let dir = selected_dir.get_untracked();
        let offset = if reset {
            0
        } else {
            files.with_untracked(|f| f.len() as u32)
        };
        let generation = load_gen.get_untracked() + 1;
        load_gen.set(generation);
        loading.set(true);
        spawn_local(async move {
            let res = api_list_files_page(&q, dir.as_deref(), offset, PAGE_SIZE).await;
            // A newer load started while this was in flight — drop this result.
            if load_gen.get_untracked() != generation {
                return;
            }
            if let Some((page, tot)) = res {
                if reset {
                    files.set(page);
                } else {
                    files.update(|f| f.extend(page));
                }
                total.set(tot);
            }
            loading.set(false);
        });
    });

    let reload_dirs = move || {
        spawn_local(async move {
            if let Some(d) = api_list_dirs().await {
                dirs.set(d);
            }
        });
    };

    let reload_pins = move || {
        spawn_local(async move {
            let (set, cols) = api_list_pinned().await;
            pinned_set.set(set);
            pin_collections.set(cols);
        });
    };

    // Initial load, then reload whenever a round of indexing finishes (so files
    // indexed after adding a directory show up without a manual refresh).
    Effect::new(move |prev: Option<usize>| {
        let cur = indexing.get();
        let finished = matches!(prev, Some(p) if p > 0) && cur == 0;
        if prev.is_none() || finished {
            reload_dirs();
            reload_pins();
            load_files.run(true);
        }
        cur
    });

    // Selecting/clearing a folder re-queries from the server (skip the initial
    // run — the indexing effect above already does the first load).
    Effect::new(move |prev: Option<Option<String>>| {
        let dir = selected_dir.get();
        if prev.is_some() {
            load_files.run(true);
        }
        dir
    });

    // Debounced search: re-query 300 ms after the user stops typing, and only if
    // the text actually changed (skip the initial run).
    Effect::new(move |prev: Option<String>| {
        let q = filter.get();
        if prev.as_ref().is_some_and(|p| p != &q) {
            let generation = load_gen.get_untracked() + 1;
            load_gen.set(generation);
            spawn_local(async move {
                sleep(Duration::from_millis(300)).await;
                // Still the latest keystroke?
                if load_gen.get_untracked() == generation {
                    load_files.run(true);
                }
            });
        }
        q
    });

    // Row click with modifiers: plain = select only this row; ctrl/⌘ = toggle
    // this row; shift = select the range from the anchor to this row.
    let on_row_click = Callback::new(move |(hash, additive, range): (String, bool, bool)| {
        if range && let Some(a) = anchor.get_untracked() {
            let vis: Vec<String> =
                files.with_untracked(|f| f.iter().map(|x| x.root_hash.clone()).collect());
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

    // The currently-selected files still present in the loaded list.
    let selected_files = move || -> Vec<ShareFile> {
        files.with(|fs| {
            fs.iter()
                .filter(|f| selected.with(|s| s.contains(&f.root_hash)))
                .cloned()
                .collect()
        })
    };

    // Info is single-item; Pin/Unpin act on whichever selected rows qualify.
    // All three test the files actually present in the list, so a selection
    // left over from another directory (no longer shown) disables them.
    let can_info = move || selected_files().len() == 1;
    let can_pin = move || {
        selected_files()
            .iter()
            .any(|f| !pinned_set.get().contains(&f.root_hash))
    };
    let can_unpin = move || {
        selected_files()
            .iter()
            .any(|f| pinned_set.get().contains(&f.root_hash))
    };

    // Open the info overlay for the single selected file.
    let open_info = move || {
        if let [f] = selected_files().as_slice() {
            detail.set(Some(f.clone()));
        }
    };

    // Clicking Pin opens the collection modal for every selected file that
    // isn't pinned yet; the actual pin happens on confirm (see `do_pin`).
    let on_pin = move || {
        let targets: Vec<(String, String)> = selected_files()
            .into_iter()
            .filter(|f| !pinned_set.get_untracked().contains(&f.root_hash))
            .map(|f| (f.root_hash, f.magnet))
            .collect();
        if !targets.is_empty() {
            pin_modal.set(Some(targets));
        }
    };

    // Pin every target under `collection` (optimistic), then refresh the pin set
    // and the collection suggestions.
    let do_pin = Callback::new(move |(targets, collection): (PinTargets, Option<String>)| {
        pinned_set.update(|s| {
            for (h, _) in &targets {
                s.insert(h.clone());
            }
        });
        pin_modal.set(None);
        spawn_local(async move {
            for (_, magnet) in targets {
                api_pin_magnet(magnet, collection.clone()).await;
            }
            let (set, cols) = api_list_pinned().await;
            pin_collections.set(cols);
            pinned_set.set(set);
        });
    });

    // Unpin every selected file that is pinned (optimistic), then refresh.
    let on_unpin = move || {
        let hashes: Vec<String> = selected_files()
            .into_iter()
            .filter(|f| pinned_set.get_untracked().contains(&f.root_hash))
            .map(|f| f.root_hash)
            .collect();
        pinned_set.update(|s| {
            for h in &hashes {
                s.remove(h);
            }
        });
        spawn_local(async move {
            for h in &hashes {
                api_unpin(h).await;
            }
            let (set, cols) = api_list_pinned().await;
            pin_collections.set(cols);
            pinned_set.set(set);
        });
    };

    // Resolve eMule support once; gates the eMule link in the info overlay.
    spawn_local(async move { emule_active.set(api_emule_active().await) });

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
                <div class="dl-toolbar">
                    <button
                        class="toolbar-btn"
                        title=t!("share.add_title")
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PLUS/>
                        <span class="btn-label">{t!("share.add")}</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title=t!("share.info_title")
                        disabled=move || !can_info()
                        on:click=move |_| open_info()
                    >
                        <Icon paths=icons::INFO_CIRCLE/>
                        <span class="btn-label">{t!("share.info")}</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title=t!("share.pin_title")
                        disabled=move || !can_pin()
                        on:click=move |_| on_pin()
                    >
                        <Icon paths=icons::PIN/>
                        <span class="btn-label">{t!("share.pin")}</span>
                    </button>
                    <button
                        class="toolbar-btn"
                        title=t!("share.unpin_title")
                        disabled=move || !can_unpin()
                        on:click=move |_| on_unpin()
                    >
                        <Icon paths=icons::PINNED_OFF/>
                        <span class="btn-label">{t!("share.unpin")}</span>
                    </button>
                    {move || {
                        let n = indexing.get();
                        (n > 0).then(|| view! {
                            <span class="share-indexing">
                                <span class="spinner"></span>
                                {t!("share.indexing", n = n)}
                            </span>
                        })
                    }}
                </div>
            </div>

            <div class="tab-scroll">
                // ── Watched directories ───────────────────────────────────
                <div class="share-section-label">{t!("share.folders")}</div>
                <Show
                    when=move || !dirs.get().is_empty()
                    fallback=|| view! { <div class="empty-state empty-state-sm"><p>{t!("share.no_folders")}</p></div> }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || dirs.get()
                            key=|d| d.path.clone()
                            children=move |d| {
                                let path_click = d.path.clone();
                                let path_class = d.path.clone();
                                let path_rm = d.path.clone();
                                let kind = d.kind;
                                view! {
                                    <li
                                        class=move || if selected_dir.get().as_deref() == Some(path_class.as_str()) {
                                            "share-dir-row share-dir-selected"
                                        } else {
                                            "share-dir-row"
                                        }
                                        title=t!("share.dir_click_title")
                                        on:click=move |_| {
                                            selected_dir.update(|cur| {
                                                if cur.as_deref() == Some(path_click.as_str()) {
                                                    *cur = None;
                                                } else {
                                                    *cur = Some(path_click.clone());
                                                }
                                            });
                                        }
                                    >
                                        <span class="share-dir-icon"><Icon paths=icons::FOLDER/></span>
                                        <div class="share-dir-main">
                                            <span class="share-dir-path">{d.path.clone()}</span>
                                            <span class="share-dir-meta">
                                                {t!("share.dir_meta", count = d.file_count, size = format_size(d.total_size))}
                                            </span>
                                        </div>
                                        {if kind == SharedDirKind::User {
                                            view! {
                                                <button
                                                    class="icon-btn icon-btn-danger"
                                                    title=t!("share.unshare_title")
                                                    on:click=move |ev| {
                                                        // Don't let the row's select toggle fire too.
                                                        ev.stop_propagation();
                                                        let p = path_rm.clone();
                                                        if confirm(&t!("share.unshare_confirm", path = p.clone())) {
                                                            spawn_local(async move {
                                                                api_remove_dir(&p).await;
                                                                // Clear the filter if it pointed here.
                                                                if selected_dir.get_untracked().as_deref() == Some(p.as_str()) {
                                                                    selected_dir.set(None);
                                                                }
                                                                if let Some(d) = api_list_dirs().await { dirs.set(d); }
                                                                load_files.run(true);
                                                            });
                                                        }
                                                    }
                                                >
                                                    <Icon paths=icons::TRASH/>
                                                </button>
                                            }.into_any()
                                        } else {
                                            let (label, badge_class, title): (_, &str, _) = match kind {
                                                SharedDirKind::Pins => (
                                                    t!("share.kind.pins"),
                                                    "share-badge share-badge-pins",
                                                    t!("share.kind.pins_title"),
                                                ),
                                                SharedDirKind::Category => (
                                                    t!("share.kind.category"),
                                                    "share-badge share-badge-category",
                                                    t!("share.kind.category_title"),
                                                ),
                                                SharedDirKind::Config => (
                                                    t!("share.kind.config"),
                                                    "share-badge share-badge-config",
                                                    t!("share.kind.config_title"),
                                                ),
                                                // Downloads (and the User case, unreachable here).
                                                _ => (
                                                    t!("share.kind.downloads"),
                                                    "share-badge share-badge-downloads",
                                                    t!("share.kind.downloads_title"),
                                                ),
                                            };
                                            view! {
                                                <span class=badge_class title=title>
                                                    {label}
                                                </span>
                                            }.into_any()
                                        }}
                                    </li>
                                }
                            }
                        />
                    </ul>
                </Show>

                // ── Shared files ──────────────────────────────────────────
                <div class="share-files-header">
                    <span class="share-section-label">
                        {move || match selected_dir.get() {
                            Some(d) => t!("share.files_in", name = basename(&d)).to_string(),
                            None => t!("share.shared_files").to_string(),
                        }}
                    </span>
                    {move || selected_dir.get().map(|_| view! {
                        <button
                            class="share-clear-filter"
                            title=t!("share.clear_title")
                            on:click=move |_| selected_dir.set(None)
                        >
                            <Icon paths=icons::X/>
                            {t!("share.clear")}
                        </button>
                    })}
                    <input
                        type="text"
                        class="dl-filter-input"
                        placeholder=t!("share.filter_placeholder")
                        prop:value=move || filter.get()
                        on:input=move |e| filter.set(event_target_value(&e))
                    />
                </div>
                <Show
                    when=move || !files.get().is_empty()
                    fallback=move || view! {
                        <div class="empty-state empty-state-sm">
                            <p>{move || if loading.get() {
                                t!("share.loading")
                            } else if !filter.get().is_empty() {
                                t!("share.no_match")
                            } else {
                                t!("share.no_files")
                            }}</p>
                        </div>
                    }
                >
                    <ul class="share-file-list">
                        <For
                            each=move || files.get()
                            key=|f| f.root_hash.clone()
                            children=move |f| {
                                let hash = f.root_hash.clone();
                                let hash_sel = f.root_hash.clone();
                                let hash_pin = f.root_hash.clone();
                                // Selected rows highlight; pinned rows show a marker.
                                let row_class = move || if selected.get().contains(&hash) {
                                    "share-file-row share-file-selected"
                                } else {
                                    "share-file-row"
                                };
                                view! {
                                    <li
                                        class=row_class
                                        on:click=move |ev| {
                                            // On a touchscreen there are no modifiers, so a
                                            // plain tap is treated as an additive toggle to
                                            // allow building a selection.
                                            let additive = ev.ctrl_key()
                                                || ev.meta_key()
                                                || crate::platform::coarse_pointer();
                                            on_row_click.run((
                                                hash_sel.clone(),
                                                additive,
                                                ev.shift_key(),
                                            ));
                                        }
                                    >
                                        <Show when=move || pinned_set.get().contains(&hash_pin) fallback=|| ()>
                                            <span class="share-file-pin" title=t!("share.pinned_marker_title")>
                                                <Icon paths=icons::PIN/>
                                            </span>
                                        </Show>
                                        <span class="share-file-name" title=f.path.clone()>{f.name.clone()}</span>
                                        <span class="share-file-size">{format_size(f.size)}</span>
                                    </li>
                                }
                            }
                        />
                    </ul>
                    // Load the next page on demand (server-side paging).
                    <Show when=move || (files.get().len() as u64) < total.get() fallback=|| ()>
                        <button
                            class="share-load-more"
                            disabled=move || loading.get()
                            on:click=move |_| load_files.run(false)
                        >
                            {move || if loading.get() {
                                t!("share.loading").to_string()
                            } else {
                                t!("share.load_more", shown = files.get().len(), total = total.get()).to_string()
                            }}
                        </button>
                    </Show>
                </Show>
            </div>

            // ── Status bar: shared file count (left) + global meters (right) ─
            <StatusBar dl_speed=dl_speed ul_speed=ul_speed temp_limit=temp_limit>
                {move || {
                    let tot = total.get();
                    if tot == 0 {
                        return view! {
                            <span class="dl-active-count dl-active-none">{t!("share.none")}</span>
                        }.into_any();
                    }
                    let shown = files.get().len() as u64;
                    let label = if shown >= tot {
                        t!("share.count", n = tot).to_string()
                    } else {
                        t!("share.count_partial", shown = shown, total = tot).to_string()
                    };
                    view! { <span class="dl-active-count">{label}</span> }.into_any()
                }}
            </StatusBar>
        </div>

        <Show when=move || add_open.get()>
            <AddDirModal
                on_added=move || { reload_dirs(); load_files.run(true); }
                on_close=move || add_open.set(false)
            />
        </Show>

        <Show when=move || pin_modal.get().is_some()>
            {move || {
                let targets = pin_modal.get().unwrap();
                view! {
                    <PinCollectionModal
                        targets=targets
                        collections=pin_collections
                        on_pin=do_pin
                        on_close=move || pin_modal.set(None)
                    />
                }
            }}
        </Show>

        <Show when=move || detail.get().is_some()>
            {move || {
                let file = detail.get().unwrap();
                view! {
                    <ShareInfoOverlay
                        file=file
                        emule_active=emule_active.get()
                        on_close=move || detail.set(None)
                    />
                }
            }}
        </Show>
    }
}

// ── Pin-to-collection modal ─────────────────────────────────────────────────

/// Asks for an optional collection before pinning one or more shared files.
/// Empty = pin uncollected. Suggests existing collections via a datalist.
#[component]
fn PinCollectionModal(
    /// The (root_hash, magnet) pairs to pin, all under the chosen collection.
    targets: PinTargets,
    collections: RwSignal<Vec<String>>,
    on_pin: Callback<(PinTargets, Option<String>)>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let collection = RwSignal::new(String::new());
    let count = targets.len();
    let targets = StoredValue::new(targets);

    let confirm = move || {
        let col = {
            let c = collection.get();
            (!c.trim().is_empty()).then(|| c.trim().to_string())
        };
        on_pin.run((targets.get_value(), col));
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">
                        {if count > 1 {
                            t!("share.pin_modal_title_n", n = count).to_string()
                        } else {
                            t!("share.pin_modal_title").to_string()
                        }}
                    </span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("share.pin_modal_hint")}
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        list="share-pin-collections"
                        placeholder=t!("share.pin_collection_placeholder")
                        prop:value=move || collection.get()
                        on:input=move |e| collection.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { confirm(); } }
                    />
                    <datalist id="share-pin-collections">
                        <For
                            each=move || collections.get()
                            key=|c| c.clone()
                            children=move |c| view! { <option value=c></option> }
                        />
                    </datalist>
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button class="btn-sm btn-primary" on:click=move |_| confirm()>{t!("share.pin")}</button>
                </div>
            </div>
        </div>
    }
}

// ── File info overlay ────────────────────────────────────────────────────────

/// Read-only detail panel for a single shared file: its metadata plus the
/// shareable links — the Rucio magnet always, and the eMule `ed2k://` link once
/// the file has been hashed for eMule (absent until then). Each link has its
/// own copy button.
#[component]
fn ShareInfoOverlay(
    file: ShareFile,
    /// Whether eMule is active (compiled in and enabled). When false no eMule
    /// link is or will be generated, so that row is hidden and the section
    /// header stays singular.
    emule_active: bool,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    // Which link was just copied ("rucio" | "ed2k"), for the transient hint.
    let copied: RwSignal<Option<&'static str>> = RwSignal::new(None);
    // Guard the reset timer from firing after the overlay is closed/unmounted.
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

    let copy = Callback::new(move |(which, text): (&'static str, String)| {
        copy_to_clipboard(&text);
        copied.set(Some(which));
        let alive = alive.clone();
        spawn_local(async move {
            sleep(Duration::from_millis(1400)).await;
            if alive.load(Ordering::Relaxed) {
                copied.set(None);
            }
        });
    });

    let magnet = file.magnet.clone();
    let ed2k = file.ed2k.clone();

    view! {
        <div class="overlay-backdrop" on:click=move |_| on_close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">{file.name.clone()}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <dl class="panel-dl">
                        <dt>{t!("share.detail.path")}</dt>
                        <dd class="mono">{file.path.clone()}</dd>

                        <dt>{t!("share.detail.size")}</dt>
                        <dd>{format_size(file.size)}</dd>

                        {file.mime_type.clone().map(|m| view! {
                            <dt>{t!("share.detail.mime")}</dt>
                            <dd>{m}</dd>
                        })}

                        {(file.chunk_count > 0).then(|| view! {
                            <dt>{t!("share.detail.chunks")}</dt>
                            <dd>{file.chunk_count.to_string()}</dd>
                        })}

                        <dt>{t!("share.detail.hash")}</dt>
                        <dd class="mono">{file.root_hash.clone()}</dd>
                    </dl>

                    <p class="section-label">
                        {if emule_active { t!("share.detail.links") } else { t!("share.detail.link") }}
                    </p>

                    <div class="share-link-row">
                        <span class="share-link-label">{t!("share.detail.rucio_link")}</span>
                        <code class="mono share-link-value">{magnet.clone()}</code>
                        <button
                            class="btn-sm share-copy-btn"
                            on:click={let m = magnet.clone(); move |_| copy.run(("rucio", m.clone()))}
                        >
                            <Icon paths=icons::COPY/>
                            {move || if copied.get() == Some("rucio") { t!("share.copied") } else { t!("share.detail.copy") }}
                        </button>
                    </div>

                    // The eMule row only exists in a build with eMule support; the
                    // link itself appears once the file has been hashed for it.
                    {emule_active.then(|| match ed2k {
                        Some(link) => view! {
                            <div class="share-link-row">
                                <span class="share-link-label">{t!("share.detail.emule_link")}</span>
                                <code class="mono share-link-value">{link.clone()}</code>
                                <button
                                    class="btn-sm share-copy-btn"
                                    on:click={let l = link.clone(); move |_| copy.run(("ed2k", l.clone()))}
                                >
                                    <Icon paths=icons::COPY/>
                                    {move || if copied.get() == Some("ed2k") { t!("share.copied") } else { t!("share.detail.copy") }}
                                </button>
                            </div>
                        }.into_any(),
                        None => view! {
                            <div class="share-link-row">
                                <span class="share-link-label">{t!("share.detail.emule_link")}</span>
                                <span class="share-link-pending" title=t!("share.detail.emule_pending_title")>
                                    {t!("share.detail.emule_pending")}
                                </span>
                            </div>
                        }.into_any(),
                    })}
                </div>
            </div>
        </div>
    }
}

// ── Add-directory modal ─────────────────────────────────────────────────────

#[component]
fn AddDirModal(
    on_added: impl Fn() + Copy + 'static,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let path = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    let submit = move || {
        let p = path.get().trim().to_string();
        if p.is_empty() {
            return;
        }
        busy.set(true);
        error.set(None);
        spawn_local(async move {
            match api_add_dir(p).await {
                Ok(resp) => {
                    on_added();
                    if resp.errors.is_empty() {
                        on_close();
                    } else {
                        // Some files were queued but a few paths couldn't be
                        // read — keep the modal open and report them.
                        error.set(Some(
                            t!(
                                "share.add_partial",
                                queued = resp.queued,
                                count = resp.errors.len(),
                                list = resp.errors.join("\n")
                            )
                            .to_string(),
                        ));
                        busy.set(false);
                    }
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
                    <span class="modal-title">{t!("share.add_title")}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        {t!("share.add_dir_hint")}
                    </p>
                    <input
                        class="search-input"
                        type="text"
                        placeholder="/home/user/Media"
                        prop:value=move || path.get()
                        on:input=move |e| path.set(event_target_value(&e))
                        on:keydown=move |e| { if e.key() == "Enter" { submit(); } }
                    />
                    {move || error.get().map(|e| view! { <p class="error-msg">{e}</p> })}
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>{t!("common.cancel")}</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || path.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { t!("share.adding") } else { t!("share.share_btn") }}
                    </button>
                </div>
            </div>
        </div>
    }
}
