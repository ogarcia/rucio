use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{
    DownloadDetailResponse, DownloadResponse, DownloadState, format_eta, format_size, format_speed,
};

// ── State helpers ─────────────────────────────────────────────────────────────

fn state_label(s: &DownloadState) -> &'static str {
    match s {
        DownloadState::FindingProviders => "Finding peers",
        DownloadState::Queued => "Queued",
        DownloadState::Downloading => "Downloading",
        DownloadState::Stalled => "Stalled",
        DownloadState::Paused => "Paused",
        DownloadState::Completed => "Completed",
        DownloadState::Failed => "Failed",
        DownloadState::Cancelled => "Cancelled",
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
    if let Ok(resp) = gloo_net::http::Request::get("/api/v1/downloads")
        .send()
        .await
    {
        if let Ok(body) = resp.json::<crate::types::DownloadsResponse>().await {
            downloads.set(body.downloads);
        }
    }
}

async fn api_cancel(id: i64) {
    let _ = gloo_net::http::Request::post(&format!("/api/v1/downloads/{id}/cancel"))
        .send()
        .await;
}

async fn api_remove(id: i64) {
    let _ = gloo_net::http::Request::delete(&format!("/api/v1/downloads/{id}"))
        .send()
        .await;
}

async fn api_pause(id: i64) {
    let _ = gloo_net::http::Request::post(&format!("/api/v1/downloads/{id}/pause"))
        .send()
        .await;
}

async fn api_resume(id: i64) {
    let _ = gloo_net::http::Request::post(&format!("/api/v1/downloads/{id}/resume"))
        .send()
        .await;
}

async fn api_fetch_detail(id: i64) -> Option<DownloadDetailResponse> {
    gloo_net::http::Request::get(&format!("/api/v1/downloads/{id}"))
        .send()
        .await
        .ok()?
        .json::<DownloadDetailResponse>()
        .await
        .ok()
}

async fn api_add_links(text: String, downloads: RwSignal<Vec<DownloadResponse>>) {
    for line in text.lines() {
        let link = line.trim();
        if link.is_empty() {
            continue;
        }
        if link.starts_with("ed2k://") {
            let body = serde_json::json!({ "link": link });
            if let Ok(req) = gloo_net::http::Request::post("/api/v1/downloads/ed2k").json(&body) {
                let _ = req.send().await;
            }
        } else {
            let body = serde_json::json!({ "magnet": link, "providers": [] });
            if let Ok(req) = gloo_net::http::Request::post("/api/v1/downloads").json(&body) {
                let _ = req.send().await;
            }
        }
    }
    refresh_downloads(downloads).await;
}

// ── Tab ───────────────────────────────────────────────────────────────────────

#[component]
pub fn DownloadsTab(downloads: RwSignal<Vec<DownloadResponse>>) -> impl IntoView {
    let selected_id: RwSignal<Option<i64>> = RwSignal::new(None);
    let add_open: RwSignal<bool> = RwSignal::new(false);
    let detail: RwSignal<Option<DownloadDetailResponse>> = RwSignal::new(None);

    // The DownloadResponse for the currently selected row.
    let selected_dl = move || {
        let id = selected_id.get()?;
        downloads.get().into_iter().find(|d| d.id == id)
    };

    let can_cancel = move || {
        selected_dl()
            .map(|d| !is_terminal(&d.state))
            .unwrap_or(false)
    };
    let can_remove = move || {
        selected_dl()
            .map(|d| is_terminal(&d.state))
            .unwrap_or(false)
    };
    let can_info = move || selected_id.get().is_some();
    // The selected download is paused → the toggle offers "Resume".
    let is_paused = move || {
        selected_dl()
            .map(|d| d.state == DownloadState::Paused)
            .unwrap_or(false)
    };
    // The pause/resume toggle is enabled for active *or* paused downloads.
    let can_pause_toggle = move || {
        selected_dl()
            .map(|d| is_pausable(&d.state) || d.state == DownloadState::Paused)
            .unwrap_or(false)
    };

    view! {
        <div class="tab-content">
            // ── Toolbar ───────────────────────────────────────────────────
            <div class="tab-toolbar">
            <div class="dl-toolbar">
                <button
                    class="toolbar-btn"
                    title="Add downloads"
                    on:click=move |_| add_open.set(true)
                >
                    <Icon paths=icons::PLUS/>
                    <span class="btn-label">"Add"</span>
                </button>
                <button
                    class="toolbar-btn"
                    title="Show download details"
                    disabled=move || !can_info()
                    on:click=move |_| {
                        if let Some(id) = selected_id.get() {
                            spawn_local(async move {
                                if let Some(d) = api_fetch_detail(id).await {
                                    detail.set(Some(d));
                                }
                            });
                        }
                    }
                >
                    <Icon paths=icons::INFO_CIRCLE/>
                    <span class="btn-label">"Info"</span>
                </button>
                <button
                    class="toolbar-btn"
                    title=move || if is_paused() { "Resume download" } else { "Pause download" }
                    disabled=move || !can_pause_toggle()
                    on:click=move |_| {
                        if let Some(d) = selected_dl() {
                            let id = d.id;
                            let paused = d.state == DownloadState::Paused;
                            spawn_local(async move {
                                if paused {
                                    api_resume(id).await;
                                } else {
                                    api_pause(id).await;
                                }
                                refresh_downloads(downloads).await;
                            });
                        }
                    }
                >
                    <Show when=is_paused fallback=|| view! { <Icon paths=icons::PLAYER_PAUSE/> }>
                        <Icon paths=icons::PLAYER_PLAY/>
                    </Show>
                    <span class="btn-label">
                        {move || if is_paused() { "Resume" } else { "Pause" }}
                    </span>
                </button>
                <button
                    class="toolbar-btn toolbar-btn-danger"
                    title="Cancel download"
                    disabled=move || !can_cancel()
                    on:click=move |_| {
                        if let Some(id) = selected_id.get() {
                            spawn_local(async move {
                                api_cancel(id).await;
                                selected_id.set(None);
                                refresh_downloads(downloads).await;
                            });
                        }
                    }
                >
                    <Icon paths=icons::CIRCLE_X/>
                    <span class="btn-label">"Cancel"</span>
                </button>
                <button
                    class="toolbar-btn"
                    title="Remove from history"
                    disabled=move || !can_remove()
                    on:click=move |_| {
                        if let Some(id) = selected_id.get() {
                            spawn_local(async move {
                                api_remove(id).await;
                                selected_id.set(None);
                                refresh_downloads(downloads).await;
                            });
                        }
                    }
                >
                    <Icon paths=icons::TRASH/>
                    <span class="btn-label">"Remove"</span>
                </button>
            </div>
            </div>

            // ── Download list ─────────────────────────────────────────────
            <div class="tab-scroll">
                <Show
                    when=move || !downloads.get().is_empty()
                    fallback=|| view! { <div class="empty-state"><p>"No downloads"</p></div> }
                >
                    <ul class="dl-list">
                        // Iterate IDs only so <For> never sees key changes on progress
                        // updates. DownloadRow reads the row's fields reactively
                        // so progress updates patch text/style nodes in place
                        // instead of re-instantiating the whole row.
                        <For
                            each=move || {
                                downloads.with(|v| v.iter().map(|d| d.id).collect::<Vec<i64>>())
                            }
                            key=|id| *id
                            children=move |id| view! {
                                <DownloadRow
                                    id=id
                                    downloads=downloads
                                    selected_id=selected_id
                                />
                            }
                        />
                    </ul>
                </Show>
            </div>
        </div>

        // ── Add modal ─────────────────────────────────────────────────────
        <Show when=move || add_open.get()>
            <AddModal
                downloads=downloads
                on_close=move || add_open.set(false)
            />
        </Show>

        // ── Info overlay ──────────────────────────────────────────────────
        {move || detail.get().map(|d| view! {
            <DownloadInfoOverlay
                detail=d
                on_close=move || detail.set(None)
            />
        })}
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
    selected_id: RwSignal<Option<i64>>,
) -> impl IntoView {
    // One memo per row backed by PartialEq, so closures below only re-run when
    // *this* row's data actually changes — not on every WS tick for the list.
    let dl = Memo::new(move |_| downloads.with(|v| v.iter().find(|d| d.id == id).cloned()));

    let name = move || {
        dl.with(|opt| {
            opt.as_ref()
                .map(|d| {
                    d.name
                        .clone()
                        .unwrap_or_else(|| format!("{}…", &d.root_hash[..16]))
                })
                .unwrap_or_default()
        })
    };

    let size_label = move || {
        dl.with(|opt| {
            opt.as_ref()
                .map(|d| {
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
                .unwrap_or_default()
        })
    };

    let show_bar = move || {
        dl.with(|opt| {
            opt.as_ref()
                .map(|d| !is_terminal(&d.state) && pct_for(d).is_some())
                .unwrap_or(false)
        })
    };
    let bar_width = move || {
        let p = dl.with(|opt| opt.as_ref().and_then(pct_for).unwrap_or(0.0));
        format!("width:{p:.1}%")
    };

    let state_class =
        move || dl.with(|opt| opt.as_ref().map(|d| state_css(&d.state)).unwrap_or(""));
    let state_text =
        move || dl.with(|opt| opt.as_ref().map(|d| state_label(&d.state)).unwrap_or(""));
    let has_error = move || dl.with(|opt| opt.as_ref().map(|d| d.error.is_some()).unwrap_or(false));
    let error_text = move || dl.with(|opt| opt.as_ref().and_then(|d| d.error.clone()));

    view! {
        <li
            class=move || if selected_id.get() == Some(id) {
                "dl-row dl-row-selected"
            } else {
                "dl-row"
            }
            on:click=move |_| {
                selected_id.update(|s| {
                    *s = if *s == Some(id) { None } else { Some(id) };
                });
            }
        >
            <div class="dl-top">
                <span class="dl-name">{name}</span>
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
            </div>
        </li>
    }
}

// ── Add modal ─────────────────────────────────────────────────────────────────

#[component]
fn AddModal(
    downloads: RwSignal<Vec<DownloadResponse>>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let text = RwSignal::new(String::new());
    let busy = RwSignal::new(false);

    let submit = move || {
        let t = text.get();
        if t.trim().is_empty() {
            return;
        }
        busy.set(true);
        spawn_local(async move {
            api_add_links(t, downloads).await;
            busy.set(false);
            on_close();
        });
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">"Add downloads"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>"✕"</button>
                </div>
                <div class="modal-body">
                    <p class="modal-hint">
                        "Paste one link per line. Supported: "
                        <code>"rucio:"</code>" magnets and "<code>"ed2k://"</code>" links."
                    </p>
                    <textarea
                        class="link-textarea"
                        placeholder="rucio:?xt=urn:blake3:…\ned2k://|file|…"
                        prop:value=move || text.get()
                        on:input=move |e| text.set(event_target_value(&e))
                        on:keydown=move |e| {
                            if e.key() == "Enter" && e.ctrl_key() {
                                submit();
                            }
                        }
                        rows="6"
                    />
                </div>
                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || busy.get() || text.get().trim().is_empty()
                        on:click=move |_| submit()
                    >
                        {move || if busy.get() { "Adding…" } else { "Add" }}
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
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let name = detail
        .name
        .clone()
        .unwrap_or_else(|| format!("{}…", &detail.root_hash[..16]));

    let pct = detail.size.map(|total| {
        if total == 0 {
            0.0
        } else {
            (detail.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        }
    });

    view! {
        <div class="overlay-backdrop" on:click=move |_| on_close()>
            <div class="overlay overlay-wide" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">{name}</span>
                    <button class="overlay-close" on:click=move |_| on_close()>"✕"</button>
                </div>
                <div class="overlay-body">
                    <dl class="panel-dl">
                        <dt>"State"</dt>
                        <dd>
                            <span class=state_css(&detail.state)>
                                {state_label(&detail.state)}
                            </span>
                        </dd>

                        <dt>"Kind"</dt>
                        <dd>{detail.kind}</dd>

                        {pct.map(|p| view! {
                            <dt>"Progress"</dt>
                            <dd>{format!("{p:.1}%")}</dd>
                        })}

                        {detail.size.map(|s| view! {
                            <dt>"Size"</dt>
                            <dd>{format_size(s)}</dd>
                        })}

                        {detail.speed_bps.filter(|&s| s > 0).map(|s| view! {
                            <dt>"Speed"</dt>
                            <dd>{format_speed(s)}</dd>
                        })}

                        {detail.eta_secs.map(|e| view! {
                            <dt>"ETA"</dt>
                            <dd>{format_eta(e)}</dd>
                        })}

                        {detail.sources_active.zip(detail.sources_total).map(|(a, t)| view! {
                            <dt>"Sources"</dt>
                            <dd>{format!("{a} active / {t} known")}</dd>
                        })}

                        {detail.dest_path.map(|p| view! {
                            <dt>"Saved to"</dt>
                            <dd class="mono">{p}</dd>
                        })}

                        {detail.error.map(|e| view! {
                            <dt>"Error"</dt>
                            <dd class="dl-error">{e}</dd>
                        })}

                        <dt>"Hash"</dt>
                        <dd class="mono">{detail.root_hash}</dd>

                        {detail.link.map(|l| view! {
                            <dt>"Link"</dt>
                            <dd class="mono">{l}</dd>
                        })}
                    </dl>
                </div>
            </div>
        </div>
    }
}
