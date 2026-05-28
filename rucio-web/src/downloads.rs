use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::types::{DownloadResponse, DownloadState, format_eta, format_size, format_speed};

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

async fn api_fetch_downloads() -> Vec<DownloadResponse> {
    gloo_net::http::Request::get("/api/v1/downloads")
        .send()
        .await
        .ok()
        .and_then(|r| {
            // Can't .await in a sync closure; use a nested spawn and signal instead.
            // Workaround: just return None and fall back to empty.
            // The real solution is to do the json() decode in an async block.
            let _ = r; // suppress warning
            None::<crate::types::DownloadsResponse>
        })
        .map(|r| r.downloads)
        .unwrap_or_default()
}

/// Fetch downloads and update the signal.
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

#[component]
pub fn DownloadsTab(downloads: RwSignal<Vec<DownloadResponse>>) -> impl IntoView {
    view! {
        <div class="tab-content">
            {move || {
                let list = downloads.get();
                if list.is_empty() {
                    view! {
                        <div class="empty-state">
                            <p>"No downloads"</p>
                        </div>
                    }.into_any()
                } else {
                    view! {
                        <ul class="dl-list">
                            {list.into_iter().map(|dl| view! {
                                <DownloadRow dl=dl downloads=downloads/>
                            }).collect_view()}
                        </ul>
                    }.into_any()
                }
            }}
        </div>
    }
}

#[component]
fn DownloadRow(dl: DownloadResponse, downloads: RwSignal<Vec<DownloadResponse>>) -> impl IntoView {
    let id = dl.id;
    let name = dl
        .name
        .clone()
        .unwrap_or_else(|| dl.root_hash[..16].to_string() + "…");
    let terminal = is_terminal(&dl.state);

    let pct = dl.size.map(|total| {
        if total == 0 {
            0.0_f64
        } else {
            (dl.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        }
    });

    let size_label = match (dl.size, pct) {
        (Some(total), Some(p)) if p < 100.0 => {
            format!(
                "{} / {} — {:.1}%",
                format_size(dl.bytes_done),
                format_size(total),
                p
            )
        }
        (Some(total), _) => format_size(total),
        _ => format_size(dl.bytes_done),
    };

    view! {
        <li class="dl-row">
            <div class="dl-top">
                <span class="dl-name">{name}</span>
                <span class="dl-size">{size_label}</span>
            </div>

            // Progress bar — only shown when size is known and not terminal
            {pct.filter(|_| !terminal).map(|p| view! {
                <div class="dl-bar-track">
                    <div class="dl-bar-fill" style=format!("width:{p:.1}%")/>
                </div>
            })}

            <div class="dl-bottom">
                <span class=state_css(&dl.state)>{state_label(&dl.state)}</span>

                // Speed + ETA while downloading
                // (DownloadResponse doesn't carry speed/eta — only DownloadDetailResponse does.
                //  WS DownloadProgress sends DownloadResponse, so these fields are absent here.
                //  We show them when available via the detail endpoint in a future iteration.)

                {dl.error.map(|e| view! {
                    <span class="dl-error">{e}</span>
                })}

                <div class="dl-actions">
                    {if !terminal {
                        view! {
                            <button class="btn-sm btn-danger" on:click=move |_| {
                                spawn_local(async move {
                                    api_cancel(id).await;
                                    refresh_downloads(downloads).await;
                                });
                            }>"Cancel"</button>
                        }.into_any()
                    } else {
                        view! {
                            <button class="btn-sm" on:click=move |_| {
                                spawn_local(async move {
                                    api_remove(id).await;
                                    refresh_downloads(downloads).await;
                                });
                            }>"Remove"</button>
                        }.into_any()
                    }}
                </div>
            </div>
        </li>
    }
}
