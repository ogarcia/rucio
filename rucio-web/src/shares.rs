use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{
    AddShareResponse, ShareFile, SharedDir, SharedDirsResponse, SharesFilesResponse, format_size,
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

async fn api_list_files() -> Option<Vec<ShareFile>> {
    gloo_net::http::Request::get("/api/v1/shares/files")
        .send()
        .await
        .ok()?
        .json::<SharesFilesResponse>()
        .await
        .ok()
        .map(|r| r.shares)
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

/// Minimal percent-encoding for the `path` query value (spaces and reserved
/// chars). `js_sys`'s `encodeURIComponent` keeps it dependency-free.
fn urlencoding_encode(s: &str) -> String {
    js_sys::encode_uri_component(s).into()
}

fn copy_to_clipboard(text: &str) {
    if let Some(win) = web_sys::window() {
        // Fire-and-forget: the returned Promise resolves asynchronously and we
        // don't need to await it. Works in secure contexts (localhost counts).
        let _ = win.navigator().clipboard().write_text(text);
    }
}

fn confirm(message: &str) -> bool {
    web_sys::window()
        .and_then(|w| w.confirm_with_message(message).ok())
        .unwrap_or(false)
}

// ── Tab ───────────────────────────────────────────────────────────────────────

#[component]
pub fn SharesTab(indexing: RwSignal<usize>) -> impl IntoView {
    let dirs: RwSignal<Vec<SharedDir>> = RwSignal::new(vec![]);
    let files: RwSignal<Vec<ShareFile>> = RwSignal::new(vec![]);
    let filter: RwSignal<String> = RwSignal::new(String::new());
    let add_open: RwSignal<bool> = RwSignal::new(false);
    // root_hash of the file whose magnet was just copied (for the "Copied!" hint).
    let copied: RwSignal<Option<String>> = RwSignal::new(None);

    let reload = move || {
        spawn_local(async move {
            if let Some(d) = api_list_dirs().await {
                dirs.set(d);
            }
            if let Some(f) = api_list_files().await {
                files.set(f);
            }
        });
    };

    // Initial load, then reload whenever a round of indexing finishes (so files
    // indexed after adding a directory show up without a manual refresh).
    Effect::new(move |prev: Option<usize>| {
        let cur = indexing.get();
        let finished = matches!(prev, Some(p) if p > 0) && cur == 0;
        if prev.is_none() || finished {
            reload();
        }
        cur
    });

    // Stays alive across this component's lifetime; used to guard the copied
    // reset timer from firing after the tab is unmounted.
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

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

    let visible_files = move || {
        let q = filter.get().to_lowercase();
        files.with(|v| {
            v.iter()
                .filter(|f| q.is_empty() || f.name.to_lowercase().contains(&q))
                .cloned()
                .collect::<Vec<_>>()
        })
    };

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
                                let path = d.path.clone();
                                let protected = d.protected;
                                view! {
                                    <li class="share-dir-row">
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
                                                    on:click=move |_| {
                                                        let p = path.clone();
                                                        if confirm(&format!(
                                                            "Stop sharing this folder?\n{p}\n\nFiles stay on disk; re-adding will re-index them."
                                                        )) {
                                                            spawn_local(async move {
                                                                api_remove_dir(&p).await;
                                                                if let Some(d) = api_list_dirs().await { dirs.set(d); }
                                                                if let Some(f) = api_list_files().await { files.set(f); }
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
                    <span class="share-section-label">"Shared files"</span>
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
                    fallback=|| view! { <div class="empty-state empty-state-sm"><p>"No shared files yet"</p></div> }
                >
                    <ul class="share-file-list">
                        <For
                            each=move || visible_files()
                            key=|f| f.root_hash.clone()
                            children=move |f| {
                                let hash = f.root_hash.clone();
                                let magnet = f.magnet.clone();
                                let is_copied = {
                                    let hash = hash.clone();
                                    move || copied.get().as_deref() == Some(hash.as_str())
                                };
                                view! {
                                    <li class="share-file-row">
                                        <span class="share-file-name" title=f.path.clone()>{f.name.clone()}</span>
                                        <span class="share-file-size">{format_size(f.size)}</span>
                                        <button
                                            class="btn-sm share-copy-btn"
                                            title="Copy magnet link"
                                            on:click=move |_| on_copy.run((hash.clone(), magnet.clone()))
                                        >
                                            <Icon paths=icons::COPY/>
                                            {move || if is_copied() { "Copied!" } else { "Magnet" }}
                                        </button>
                                    </li>
                                }
                            }
                        />
                    </ul>
                </Show>
            </div>
        </div>

        <Show when=move || add_open.get()>
            <AddDirModal
                on_added=move || { reload(); }
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
