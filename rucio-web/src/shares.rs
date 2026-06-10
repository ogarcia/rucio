use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::statusbar::StatusBar;
use crate::types::{
    AddShareResponse, PinsResponse, ShareFile, SharedDir, SharedDirsResponse, SharesFilesResponse,
    format_size,
};

// ── API ─────────────────────────────────────────────────────────────────────

async fn api_list_dirs() -> Option<Vec<SharedDir>> {
    gloo_net::http::Request::get("/api/v1/shares")
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
    let mut url = format!("/api/v1/shares/files?limit={limit}&offset={offset}");
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
    let req = gloo_net::http::Request::post("/api/v1/shares")
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
    let url = format!("/api/v1/shares?path={}", urlencoding_encode(path));
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// Pin a shared file by its magnet (records the pin intent). Since the file is
/// already present, this never re-fetches — it just marks it as deliberately
/// kept, which also publishes it in this node's pin-set for subscribers.
async fn api_pin_magnet(magnet: String) {
    let body = serde_json::json!({ "magnet": magnet });
    if let Ok(req) = gloo_net::http::Request::post("/api/v1/pins").json(&body) {
        let _ = req.send().await;
    }
}

/// Unpin a root hash (drops the pin intent; the file stays shared).
async fn api_unpin(hash: &str) {
    let url = format!("/api/v1/pins/{hash}");
    let _ = gloo_net::http::Request::delete(&url).send().await;
}

/// The set of currently-pinned root hashes (hex), so the shared-files list can
/// show which files are pinned and offer to unpin them.
async fn api_list_pinned() -> HashSet<String> {
    let Ok(resp) = gloo_net::http::Request::get("/api/v1/pins").send().await else {
        return HashSet::new();
    };
    match resp.json::<PinsResponse>().await {
        Ok(r) => r.pins.into_iter().map(|p| p.root_hash).collect(),
        Err(_) => HashSet::new(),
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
    // root_hash of the file whose magnet was just copied (for the "Copied!" hint).
    let copied: RwSignal<Option<String>> = RwSignal::new(None);
    // root_hash of the file just pinned (for the transient "Pinned!" hint).
    let pinned: RwSignal<Option<String>> = RwSignal::new(None);
    // Set of currently-pinned hashes, so each file row shows Pin vs Unpin.
    let pinned_set: RwSignal<HashSet<String>> = RwSignal::new(HashSet::new());
    // Bumped on every load; a response is applied only if its generation is
    // still current, so a reset (new filter/dir) discards an in-flight page.
    let load_gen: RwSignal<u32> = RwSignal::new(0);

    // Load a page from the server. `reset` starts a fresh result set (offset 0,
    // replacing the list); otherwise it appends the next page.
    let load_files = Callback::new(move |reset: bool| {
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
            pinned_set.set(api_list_pinned().await);
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

    // Stays alive across this component's lifetime; used to guard the copied
    // reset timer from firing after the tab is unmounted.
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

    let alive_pin = alive.clone();
    let on_copy = Callback::new(move |(hash, magnet): (String, String)| {
        copy_to_clipboard(&magnet);
        copied.set(Some(hash));
        let alive = alive.clone();
        spawn_local(async move {
            sleep(Duration::from_millis(1400)).await;
            if alive.load(Ordering::Relaxed) {
                copied.set(None);
            }
        });
    });

    let on_pin = Callback::new(move |(hash, magnet): (String, String)| {
        // Optimistic: mark pinned now so the button flips to "Unpin" after the
        // transient "Pinned!" hint fades.
        pinned_set.update(|s| {
            s.insert(hash.clone());
        });
        pinned.set(Some(hash));
        let alive = alive_pin.clone();
        spawn_local(async move {
            api_pin_magnet(magnet).await;
            sleep(Duration::from_millis(1400)).await;
            if alive.load(Ordering::Relaxed) {
                pinned.set(None);
            }
        });
    });

    let on_unpin = Callback::new(move |hash: String| {
        pinned_set.update(|s| {
            s.remove(&hash);
        });
        spawn_local(async move {
            api_unpin(&hash).await;
        });
    });

    view! {
        <div class="tab-content">
            <div class="tab-toolbar">
                <div class="dl-toolbar">
                    <button
                        class="toolbar-btn"
                        title="Share a directory"
                        on:click=move |_| add_open.set(true)
                    >
                        <Icon paths=icons::PLUS/>
                        <span class="btn-label">"Add directory"</span>
                    </button>
                    {move || {
                        let n = indexing.get();
                        (n > 0).then(|| view! {
                            <span class="share-indexing">
                                <span class="spinner"></span>
                                {format!("{n} indexing…")}
                            </span>
                        })
                    }}
                </div>
            </div>

            <div class="tab-scroll">
                // ── Watched directories ───────────────────────────────────
                <div class="share-section-label">"Folders"</div>
                <Show
                    when=move || !dirs.get().is_empty()
                    fallback=|| view! { <div class="empty-state empty-state-sm"><p>"No shared folders"</p></div> }
                >
                    <ul class="share-dir-list">
                        <For
                            each=move || dirs.get()
                            key=|d| d.path.clone()
                            children=move |d| {
                                let path_click = d.path.clone();
                                let path_class = d.path.clone();
                                let path_rm = d.path.clone();
                                let protected = d.protected;
                                view! {
                                    <li
                                        class=move || if selected_dir.get().as_deref() == Some(path_class.as_str()) {
                                            "share-dir-row share-dir-selected"
                                        } else {
                                            "share-dir-row"
                                        }
                                        title="Click to show only this folder's files"
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
                                                {format!("{} file(s) · {}", d.file_count, format_size(d.total_size))}
                                            </span>
                                        </div>
                                        {if protected {
                                            view! {
                                                <span class="share-badge" title="The download directory is always shared">
                                                    "Downloads"
                                                </span>
                                            }.into_any()
                                        } else {
                                            view! {
                                                <button
                                                    class="icon-btn icon-btn-danger"
                                                    title="Stop sharing this folder (files stay on disk)"
                                                    on:click=move |ev| {
                                                        // Don't let the row's select toggle fire too.
                                                        ev.stop_propagation();
                                                        let p = path_rm.clone();
                                                        if confirm(&format!(
                                                            "Stop sharing this folder?\n{p}\n\nFiles stay on disk; re-adding will re-index them."
                                                        )) {
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
                            Some(d) => format!("Files in {}", basename(&d)),
                            None => "Shared files".to_string(),
                        }}
                    </span>
                    {move || selected_dir.get().map(|_| view! {
                        <button
                            class="share-clear-filter"
                            title="Show files from all folders"
                            on:click=move |_| selected_dir.set(None)
                        >
                            <Icon paths=icons::X/>
                            "Clear"
                        </button>
                    })}
                    <input
                        type="text"
                        class="dl-filter-input"
                        placeholder="Filter…"
                        prop:value=move || filter.get()
                        on:input=move |e| filter.set(event_target_value(&e))
                    />
                </div>
                <Show
                    when=move || !files.get().is_empty()
                    fallback=move || view! {
                        <div class="empty-state empty-state-sm">
                            <p>{move || if loading.get() {
                                "Loading…"
                            } else if !filter.get().is_empty() {
                                "No files match"
                            } else {
                                "No shared files yet"
                            }}</p>
                        </div>
                    }
                >
                    <ul class="share-file-list">
                        <For
                            each=move || files.get()
                            key=|f| f.root_hash.clone()
                            children=move |f| {
                                let hash_copy = f.root_hash.clone();
                                let magnet_copy = f.magnet.clone();
                                let hash_btn = f.root_hash.clone();
                                let magnet_btn = f.magnet.clone();
                                let is_copied = {
                                    let hash = f.root_hash.clone();
                                    move || copied.get().as_deref() == Some(hash.as_str())
                                };
                                // Per-reactive-block hash clones (the signals are Copy).
                                let h_title = f.root_hash.clone();
                                let h_icon = f.root_hash.clone();
                                let h_label = f.root_hash.clone();
                                view! {
                                    <li class="share-file-row">
                                        <span class="share-file-name" title=f.path.clone()>{f.name.clone()}</span>
                                        <span class="share-file-size">{format_size(f.size)}</span>
                                        <button
                                            class="btn-sm share-copy-btn"
                                            title=move || if pinned_set.get().contains(&h_title) {
                                                "Unpin this file (stops keeping it on purpose; the file stays shared)"
                                            } else {
                                                "Pin this file (keep it available on purpose; publishes it in your pin-set)"
                                            }
                                            on:click=move |_| {
                                                if pinned_set.get_untracked().contains(&hash_btn) {
                                                    on_unpin.run(hash_btn.clone());
                                                } else {
                                                    on_pin.run((hash_btn.clone(), magnet_btn.clone()));
                                                }
                                            }
                                        >
                                            {move || {
                                                let just = pinned.get().as_deref() == Some(h_icon.as_str());
                                                let pinned_now = pinned_set.get().contains(&h_icon);
                                                if pinned_now && !just {
                                                    view! { <Icon paths=icons::PINNED_OFF/> }.into_any()
                                                } else {
                                                    view! { <Icon paths=icons::PIN/> }.into_any()
                                                }
                                            }}
                                            {move || {
                                                let just = pinned.get().as_deref() == Some(h_label.as_str());
                                                if just {
                                                    "Pinned!"
                                                } else if pinned_set.get().contains(&h_label) {
                                                    "Unpin"
                                                } else {
                                                    "Pin"
                                                }
                                            }}
                                        </button>
                                        <button
                                            class="btn-sm share-copy-btn"
                                            title="Copy magnet link"
                                            on:click=move |_| on_copy.run((hash_copy.clone(), magnet_copy.clone()))
                                        >
                                            <Icon paths=icons::COPY/>
                                            {move || if is_copied() { "Copied!" } else { "Magnet" }}
                                        </button>
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
                                "Loading…".to_string()
                            } else {
                                format!("Load more ({} of {})", files.get().len(), total.get())
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
                            <span class="dl-active-count dl-active-none">"No shared files"</span>
                        }.into_any();
                    }
                    let shown = files.get().len() as u64;
                    let label = if shown >= tot {
                        format!("{tot} file(s)")
                    } else {
                        format!("{shown} of {tot} file(s)")
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
                        error.set(Some(format!(
                            "Shared {} file(s); {} path(s) could not be read:\n{}",
                            resp.queued,
                            resp.errors.len(),
                            resp.errors.join("\n"),
                        )));
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
                    <span class="modal-title">"Share a directory"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Enter the absolute path of a directory on the daemon host. All files
                         under it are indexed and shared. Individual files aren't accepted."
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
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || path.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { "Adding…" } else { "Share" }}
                    </button>
                </div>
            </div>
        </div>
    }
}
