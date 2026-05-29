use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{
    MetricsResponse, StatusResponse, class_badge, format_size, format_speed_full, format_uptime,
};

#[component]
pub fn NodeStatusPanel(
    status: RwSignal<Option<StatusResponse>>,
    active_panel: RwSignal<Option<super::Panel>>,
) -> impl IntoView {
    let close = move || active_panel.set(None);
    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"Node status"</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match status.get() {
                        None => view! { <p class="loading">"Loading..."</p> }.into_any(),
                        Some(s) => {
                            let (label, css) = class_badge(&s.class);
                            let uptime = format_uptime(s.uptime_secs);
                            view! {
                                <dl class="panel-dl">
                                    <dt>"Version"</dt>
                                    <dd>{s.version}</dd>
                                    <dt>"Class"</dt>
                                    <dd><span class=css>{label}</span></dd>
                                    <dt>"Peer ID"</dt>
                                    <dd class="mono">{s.peer_id}</dd>
                                    <dt>"Peers"</dt>
                                    <dd>{s.connected_peers.to_string()}</dd>
                                    <dt>"Uptime"</dt>
                                    <dd>{uptime}</dd>
                                    {s.external_ip.map(|ip| view! {
                                        <dt>"External IP"</dt>
                                        <dd class="mono">{ip}</dd>
                                    })}
                                </dl>
                            }.into_any()
                        }
                    }}
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn StatsPanel(active_panel: RwSignal<Option<super::Panel>>) -> impl IntoView {
    let close = move || active_panel.set(None);
    let metrics: RwSignal<Option<MetricsResponse>> = RwSignal::new(None);

    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

    spawn_local(async move {
        loop {
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(resp) = gloo_net::http::Request::get("/api/v1/metrics").send().await {
                if let Ok(m) = resp.json::<MetricsResponse>().await {
                    if alive.load(Ordering::Relaxed) {
                        metrics.set(Some(m));
                    }
                }
            }
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
    });

    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"Statistics"</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match metrics.get() {
                        None => view! { <p class="loading">"Loading…"</p> }.into_any(),
                        Some(m) => {
                            let s = &m.session;
                            let t = &m.total;
                            view! {
                                <p class="section-label">"This session"</p>
                                <dl class="panel-dl">
                                    <dt>"Uptime"</dt>
                                    <dd>{format_uptime(s.uptime_secs())}</dd>
                                    <dt>"↓ speed"</dt>
                                    <dd>{format_speed_full(s.download_speed)}</dd>
                                    <dt>"↑ speed"</dt>
                                    <dd>{format_speed_full(s.upload_speed)}</dd>
                                    <dt>"↓ downloaded"</dt>
                                    <dd>{format_size(s.downloaded_bytes)}</dd>
                                    <dt>"↑ uploaded"</dt>
                                    <dd>{format_size(s.uploaded_bytes)}</dd>
                                    <dt>"Chunks received"</dt>
                                    <dd>{s.chunks_received.to_string()}</dd>
                                    <dt>"Chunks served"</dt>
                                    <dd>{s.chunks_served.to_string()}</dd>
                                    {(s.chunks_rejected > 0).then(|| view! {
                                        <dt>"Chunks rejected"</dt>
                                        <dd class="dl-error">{s.chunks_rejected.to_string()}</dd>
                                    })}
                                </dl>

                                <p class="section-label">"All-time total"</p>
                                <dl class="panel-dl">
                                    <dt>"↓ downloaded"</dt>
                                    <dd>{format_size(t.downloaded_bytes)}</dd>
                                    <dt>"↑ uploaded"</dt>
                                    <dd>{format_size(t.uploaded_bytes)}</dd>
                                    <dt>"Chunks received"</dt>
                                    <dd>{t.chunks_received.to_string()}</dd>
                                    <dt>"Chunks served"</dt>
                                    <dd>{t.chunks_served.to_string()}</dd>
                                    {(t.chunks_rejected > 0).then(|| view! {
                                        <dt>"Chunks rejected"</dt>
                                        <dd class="dl-error">{t.chunks_rejected.to_string()}</dd>
                                    })}
                                </dl>
                            }.into_any()
                        }
                    }}
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn AddressesPanel(
    status: RwSignal<Option<StatusResponse>>,
    active_panel: RwSignal<Option<super::Panel>>,
) -> impl IntoView {
    let close = move || active_panel.set(None);
    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"Addresses"</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match status.get() {
                        None => view! { <p class="loading">"Loading..."</p> }.into_any(),
                        Some(s) => view! {
                            <p class="section-label">"Listen"</p>
                            <ul class="addr-list">
                                {s.listen_addrs.into_iter()
                                    .map(|a| view! { <li>{a}</li> })
                                    .collect_view()}
                            </ul>
                            <p class="section-label">"Observed"</p>
                            <ul class="addr-list">
                                {if s.observed_addrs.is_empty() {
                                    view! { <li class="muted">"None yet"</li> }.into_any()
                                } else {
                                    s.observed_addrs.into_iter()
                                        .map(|a| view! { <li>{a}</li> })
                                        .collect_view()
                                        .into_any()
                                }}
                            </ul>
                        }.into_any()
                    }}
                </div>
            </div>
        </div>
    }
}
