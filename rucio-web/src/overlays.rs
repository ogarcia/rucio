use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{
    EmuleConnectivity, EmuleStatusResponse, MetricsResponse, PeerInfo, PeersResponse,
    StatusResponse, class_badge, format_size, format_speed_full, format_uptime,
};

async fn api_fetch_emule_status() -> Option<EmuleStatusResponse> {
    gloo_net::http::Request::get("/api/v1/emule/status")
        .send()
        .await
        .ok()?
        .json::<EmuleStatusResponse>()
        .await
        .ok()
}

/// (label, css class) for the eMule connectivity badge, reusing node-class styles.
fn emule_conn_badge(c: EmuleConnectivity) -> (&'static str, &'static str) {
    match c {
        EmuleConnectivity::Open => ("Open", "badge badge-high"),
        EmuleConnectivity::Firewalled => ("Firewalled", "badge badge-low"),
        EmuleConnectivity::Unknown => ("Unknown", "badge badge-unknown"),
    }
}

#[component]
pub fn NodeStatusPanel(
    status: RwSignal<Option<StatusResponse>>,
    active_panel: RwSignal<Option<super::Panel>>,
) -> impl IntoView {
    let close = move || active_panel.set(None);

    // Fetch the eMule/Kad2 status once when the panel opens. Guard the signal
    // write with `alive` so a late response after the modal closes can't write
    // to a disposed scope.
    let emule: RwSignal<Option<EmuleStatusResponse>> = RwSignal::new(None);
    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));
    spawn_local(async move {
        if let Some(s) = api_fetch_emule_status().await
            && alive.load(Ordering::Relaxed)
        {
            emule.set(Some(s));
        }
    });

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
                                // Only label this section when the eMule section
                                // is also shown, so a single-section panel stays
                                // clean but a two-section one reads consistently.
                                {move || emule.get()
                                    .filter(|e| e.feature_enabled && e.runtime_enabled)
                                    .map(|_| view! {
                                        <p class="section-label">"Rucio / libp2p"</p>
                                    })}
                                <dl class="panel-dl">
                                    <dt>"Version"</dt>
                                    <dd>{s.version}</dd>
                                    <dt>"Class"</dt>
                                    <dd><span class=css>{label}</span></dd>
                                    <dt>"Peer ID"</dt>
                                    <dd class="mono">{s.peer_id}</dd>
                                    <dt>"Peers"</dt>
                                    <dd>{s.connected_peers.to_string()}</dd>
                                    <dt>"Active downloads"</dt>
                                    <dd>{s.active_downloads.to_string()}</dd>
                                    <dt>"Active uploads"</dt>
                                    <dd>{s.active_uploads.to_string()}</dd>
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
                    // eMule / Kad2 section — only when the subsystem is available.
                    {move || emule.get()
                        .filter(|e| e.feature_enabled && e.runtime_enabled)
                        .map(|e| {
                            let (clabel, ccss) = emule_conn_badge(e.connectivity);
                            view! {
                                <p class="section-label">"eMule / Kad2"</p>
                                <dl class="panel-dl">
                                    <dt>"Connectivity"</dt>
                                    <dd><span class=ccss>{clabel}</span></dd>
                                    <dt>"Kad contacts"</dt>
                                    <dd>{e.connected_peers.to_string()}</dd>
                                    <dt>"nodes.dat"</dt>
                                    <dd>{
                                        if e.nodes_dat_present {
                                            format!("{} contacts", e.contacts)
                                        } else {
                                            "missing".to_string()
                                        }
                                    }</dd>
                                    {e.tcp_port.map(|p| view! {
                                        <dt>"TCP port"</dt>
                                        <dd>{p.to_string()}</dd>
                                    })}
                                    {e.udp_port.map(|p| view! {
                                        <dt>"UDP port"</dt>
                                        <dd>{p.to_string()}</dd>
                                    })}
                                    <dt>"Active downloads"</dt>
                                    <dd>{e.active_downloads.to_string()}</dd>
                                    <dt>"Upload slots"</dt>
                                    <dd>{format!("{} / {}", e.upload_slots_in_use, e.upload_slots_total)}</dd>
                                    <dt>"Inbound conns"</dt>
                                    <dd>{e.inbound_connections.to_string()}</dd>
                                    {e.external_ip.map(|ip| view! {
                                        <dt>"External IP"</dt>
                                        <dd class="mono">{ip}</dd>
                                    })}
                                </dl>
                            }
                        })}
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
            if let Ok(resp) = gloo_net::http::Request::get("/api/v1/metrics").send().await
                && let Ok(m) = resp.json::<MetricsResponse>().await
                && alive.load(Ordering::Relaxed)
            {
                metrics.set(Some(m));
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

/// Recently-seen peers (GET /api/v1/peers): a directory of known peers with
/// their connectivity class and addresses. Polled every few seconds while open.
#[component]
pub fn PeersPanel(active_panel: RwSignal<Option<super::Panel>>) -> impl IntoView {
    let close = move || active_panel.set(None);
    let peers: RwSignal<Option<Vec<PeerInfo>>> = RwSignal::new(None);

    let alive = Arc::new(AtomicBool::new(true));
    let alive_cleanup = alive.clone();
    on_cleanup(move || alive_cleanup.store(false, Ordering::Relaxed));

    spawn_local(async move {
        loop {
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(resp) = gloo_net::http::Request::get("/api/v1/peers").send().await
                && let Ok(p) = resp.json::<PeersResponse>().await
                && alive.load(Ordering::Relaxed)
            {
                peers.set(Some(p.peers));
            }
            if !alive.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"Peers"</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <p class="panel-note">
                        "Peers seen recently — may include some no longer connected. \
                         The live connected count is in Node status."
                    </p>
                    {move || match peers.get() {
                        None => view! { <p class="loading">"Loading..."</p> }.into_any(),
                        Some(list) if list.is_empty() => {
                            view! { <p class="muted">"No peers seen yet."</p> }.into_any()
                        }
                        Some(list) => {
                            let count = list.len();
                            view! {
                                <ul class="peer-list">
                                    {list.into_iter().map(|p| {
                                        let (label, css) = class_badge(&p.class);
                                        let peer_id_title = p.peer_id.clone();
                                        let addr = if p.addresses.is_empty() {
                                            "—".to_string()
                                        } else if p.addresses.len() == 1 {
                                            p.addresses[0].clone()
                                        } else {
                                            format!("{} addresses", p.addresses.len())
                                        };
                                        view! {
                                            <li class="peer-item">
                                                <div class="peer-head">
                                                    <span class=css>{label}</span>
                                                    <span class="mono peer-id" title=peer_id_title>
                                                        {p.peer_id}
                                                    </span>
                                                </div>
                                                <span class="peer-addr">{addr}</span>
                                            </li>
                                        }
                                    }).collect_view()}
                                </ul>
                                <p class="section-label">
                                    {format!("{count} known peer{}", if count == 1 { "" } else { "s" })}
                                </p>
                            }.into_any()
                        }
                    }}
                </div>
            </div>
        </div>
    }
}

/// Quick reference: version, repository and where to report issues.
#[component]
pub fn AboutPanel(active_panel: RwSignal<Option<super::Panel>>) -> impl IntoView {
    let close = move || active_panel.set(None);
    const REPO: &str = "https://github.com/ogarcia/rucio";

    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"About"</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body" style="text-align: center;">
                    <svg
                        viewBox="0 0 24 24" width="72" height="72"
                        fill="none" stroke="currentColor" stroke-width="2"
                        stroke-linecap="round" stroke-linejoin="round"
                        style="color: var(--accent); margin: 0.25rem auto 0.75rem; display: block;"
                        aria-hidden="true"
                        inner_html=icons::LOGO
                    ></svg>
                    <div style="font-size: 1.3rem; font-weight: 600;">"Rucio"</div>
                    <div style="color: var(--text-3); margin-top: 0.15rem;">
                        {format!("v{}", env!("CARGO_PKG_VERSION"))}
                    </div>
                    <p style="color: var(--text-2); margin: 0.9rem 0 1.1rem;">
                        "Peer-to-peer file sharing over libp2p, with eMule/Kad2 compatibility."
                    </p>
                    <div style="display: flex; flex-direction: column; gap: 0.6rem;">
                        <a
                            href=REPO target="_blank" rel="noopener noreferrer"
                            style="color: var(--accent);"
                        >"Source code on GitHub"</a>
                        <a
                            href=format!("{REPO}/issues/new")
                            target="_blank" rel="noopener noreferrer"
                            style="color: var(--accent);"
                        >"Report an issue"</a>
                    </div>
                </div>
            </div>
        </div>
    }
}
