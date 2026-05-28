use leptos::prelude::*;
use leptos::task::spawn_local;

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
        _ => "dl-state dl-state-neutral",
    }
}

fn is_terminal(s: &DownloadState) -> bool {
    matches!(
        s,
        DownloadState::Completed | DownloadState::Failed | DownloadState::Cancelled
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
    let _ = gloo_net::http::Request::delete(&format!("/api/v1/downloads/{id}"))
        .send()
        .await;
}

async fn api_remove(id: i64) {
    let _ = gloo_net::http::Request::delete(&format!("/api/v1/downloads/{id}/history"))
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

    view! {
        <div class="tab-content">
            // ── Toolbar ───────────────────────────────────────────────────
            <div class="dl-toolbar">
                <button class="toolbar-btn" on:click=move |_| add_open.set(true)>
                    "Add"
                </button>
                <button
                    class="toolbar-btn"
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
                    "Info"
                </button>
                // Pause — not yet supported by the daemon.
                <button class="toolbar-btn" disabled=true title="Not yet supported by the daemon">
                    "Pause"
                </button>
                <button
                    class="toolbar-btn toolbar-btn-danger"
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
                    "Cancel"
                </button>
                <button
                    class="toolbar-btn"
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
                    "Remove"
                </button>
            </div>

            // ── Download list ─────────────────────────────────────────────
            {move || {
                let list = downloads.get();
                if list.is_empty() {
                    view! {
                        <div class="empty-state">
                            <p>"No downloads"</p>
                        </div>
                    }
                    .into_any()
                } else {
                    view! {
                        <ul class="dl-list">
                            {list
                                .into_iter()
                                .map(|dl| view! {
                                    <DownloadRow dl=dl selected_id=selected_id/>
                                })
                                .collect_view()}
                        </ul>
                    }
                    .into_any()
                }
            }}
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

#[component]
fn DownloadRow(dl: DownloadResponse, selected_id: RwSignal<Option<i64>>) -> impl IntoView {
    let id = dl.id;
    let name = dl
        .name
        .clone()
        .unwrap_or_else(|| format!("{}…", &dl.root_hash[..16]));

    let pct = dl.size.map(|total| {
        if total == 0 {
            0.0_f64
        } else {
            (dl.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        }
    });

    let size_label = match (dl.size, pct) {
        (Some(total), Some(p)) if p < 100.0 => format!(
            "{} / {} — {:.1}%",
            format_size(dl.bytes_done),
            format_size(total),
            p
        ),
        (Some(total), _) => format_size(total),
        _ => format_size(dl.bytes_done),
    };

    let terminal = is_terminal(&dl.state);

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

            {pct.filter(|_| !terminal).map(|p| view! {
                <div class="dl-bar-track">
                    <div class="dl-bar-fill" style=format!("width:{p:.1}%")/>
                </div>
            })}

            <div class="dl-bottom">
                <span class=state_css(&dl.state)>{state_label(&dl.state)}</span>
                {dl.error.map(|e| view! { <span class="dl-error">{e}</span> })}
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
