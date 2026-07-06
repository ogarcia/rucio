use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gloo_timers::future::sleep;
use leptos::prelude::*;
use leptos::task::spawn_local;
use rust_i18n::t;

use crate::icons::{self, Icon};
use crate::types::{
    EmuleConnectivity, EmuleStatusResponse, MetricsResponse, PeerInfo, PeersResponse,
    StatusResponse, class_badge, format_ratio, format_size, format_speed_full, format_uptime,
    reachability_hint,
};

async fn api_fetch_emule_status() -> Option<EmuleStatusResponse> {
    gloo_net::http::Request::get(&crate::api::api("/api/v1/emule/status"))
        .send()
        .await
        .ok()?
        .json::<EmuleStatusResponse>()
        .await
        .ok()
}

/// (label, css class) for the eMule connectivity badge, reusing node-class styles.
fn emule_conn_badge(c: EmuleConnectivity) -> (String, &'static str) {
    match c {
        EmuleConnectivity::Open => (t!("overlay.emule.open").to_string(), "badge badge-high"),
        EmuleConnectivity::Firewalled => (
            t!("overlay.emule.firewalled").to_string(),
            "badge badge-low",
        ),
        EmuleConnectivity::Unknown => (
            t!("overlay.emule.unknown").to_string(),
            "badge badge-unknown",
        ),
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
                    <span class="overlay-title">{t!("overlay.node.title")}</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match status.get() {
                        None => view! { <p class="loading">{t!("overlay.loading")}</p> }.into_any(),
                        Some(s) => {
                            let (label, css) = class_badge(&s.class);
                            // Reachability detail is only useful while LowID.
                            let reach = reachability_hint(&s.class, &s.reachability);
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
                                    <dt>{t!("overlay.node.version")}</dt>
                                    <dd>{s.version}</dd>
                                    <dt>{t!("overlay.node.class")}</dt>
                                    <dd>
                                        <span class=css>{label}</span>
                                        {reach.map(|h| view! {
                                            <span class="muted reach-hint">{h}</span>
                                        })}
                                    </dd>
                                    <dt>{t!("overlay.node.peer_id")}</dt>
                                    <dd class="mono">{s.peer_id}</dd>
                                    <dt>{t!("overlay.node.peers")}</dt>
                                    <dd>{s.connected_peers.to_string()}</dd>
                                    <dt>{t!("overlay.node.active_downloads")}</dt>
                                    <dd>{s.active_downloads.to_string()}</dd>
                                    <dt>{t!("overlay.node.active_uploads")}</dt>
                                    <dd>{s.active_uploads.to_string()}</dd>
                                    <dt>{t!("overlay.node.uptime")}</dt>
                                    <dd>{uptime}</dd>
                                    {s.external_ip.map(|ip| view! {
                                        <dt>{t!("overlay.node.external_ip")}</dt>
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
                                    <dt>{t!("overlay.emule.connectivity")}</dt>
                                    <dd><span class=ccss>{clabel}</span></dd>
                                    <dt>{t!("overlay.emule.kad_contacts")}</dt>
                                    <dd>{e.connected_peers.to_string()}</dd>
                                    <dt>"nodes.dat"</dt>
                                    <dd>{
                                        if e.nodes_dat_present {
                                            t!("overlay.emule.contacts", n = e.contacts).to_string()
                                        } else {
                                            t!("overlay.emule.missing").to_string()
                                        }
                                    }</dd>
                                    {e.tcp_port.map(|p| view! {
                                        <dt>{t!("overlay.emule.tcp_port")}</dt>
                                        <dd>{p.to_string()}</dd>
                                    })}
                                    {e.udp_port.map(|p| view! {
                                        <dt>{t!("overlay.emule.udp_port")}</dt>
                                        <dd>{p.to_string()}</dd>
                                    })}
                                    <dt>{t!("overlay.emule.active_downloads")}</dt>
                                    <dd>{e.active_downloads.to_string()}</dd>
                                    <dt>{t!("overlay.emule.upload_slots")}</dt>
                                    <dd>{format!("{} / {}", e.upload_slots_in_use, e.upload_slots_total)}</dd>
                                    <dt>{t!("overlay.emule.inbound_conns")}</dt>
                                    <dd>{e.inbound_connections.to_string()}</dd>
                                    {e.external_ip.map(|ip| view! {
                                        <dt>{t!("overlay.emule.external_ip")}</dt>
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
            if let Ok(resp) = gloo_net::http::Request::get(&crate::api::api("/api/v1/metrics"))
                .send()
                .await
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
                    <span class="overlay-title">{t!("overlay.stats.title")}</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match metrics.get() {
                        None => view! { <p class="loading">{t!("overlay.loading")}</p> }.into_any(),
                        Some(m) => {
                            let s = &m.session;
                            let t = &m.total;
                            view! {
                                <p class="section-label">{t!("overlay.stats.session")}</p>
                                <dl class="panel-dl">
                                    <dt>{t!("overlay.stats.uptime")}</dt>
                                    <dd>{format_uptime(s.uptime_secs())}</dd>
                                    <dt>{t!("overlay.stats.speed_down")}</dt>
                                    <dd>{format_speed_full(s.download_speed)}</dd>
                                    <dt>{t!("overlay.stats.speed_up")}</dt>
                                    <dd>{format_speed_full(s.upload_speed)}</dd>
                                    <dt>{t!("overlay.stats.downloaded")}</dt>
                                    <dd>{format_size(s.downloaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.uploaded")}</dt>
                                    <dd>{format_size(s.uploaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.ratio")}</dt>
                                    <dd>{format_ratio(s.ratio, s.uploaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.chunks_received")}</dt>
                                    <dd>{s.chunks_received.to_string()}</dd>
                                    <dt>{t!("overlay.stats.chunks_served")}</dt>
                                    <dd>{s.chunks_served.to_string()}</dd>
                                    {(s.chunks_rejected > 0).then(|| view! {
                                        <dt>{t!("overlay.stats.chunks_rejected")}</dt>
                                        <dd class="dl-error">{s.chunks_rejected.to_string()}</dd>
                                    })}
                                </dl>

                                <p class="section-label">{t!("overlay.stats.total")}</p>
                                <dl class="panel-dl">
                                    <dt>{t!("overlay.stats.uptime")}</dt>
                                    <dd>{format_uptime(t.uptime_seconds)}</dd>
                                    <dt>{t!("overlay.stats.downloaded")}</dt>
                                    <dd>{format_size(t.downloaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.uploaded")}</dt>
                                    <dd>{format_size(t.uploaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.ratio")}</dt>
                                    <dd>{format_ratio(t.ratio, t.uploaded_bytes)}</dd>
                                    <dt>{t!("overlay.stats.chunks_received")}</dt>
                                    <dd>{t.chunks_received.to_string()}</dd>
                                    <dt>{t!("overlay.stats.chunks_served")}</dt>
                                    <dd>{t.chunks_served.to_string()}</dd>
                                    {(t.chunks_rejected > 0).then(|| view! {
                                        <dt>{t!("overlay.stats.chunks_rejected")}</dt>
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
                    <span class="overlay-title">{t!("overlay.addr.title")}</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    {move || match status.get() {
                        None => view! { <p class="loading">{t!("overlay.loading")}</p> }.into_any(),
                        Some(s) => view! {
                            <p class="section-label">{t!("overlay.addr.listen")}</p>
                            <ul class="addr-list">
                                {s.listen_addrs.into_iter()
                                    .map(|a| view! { <li>{a}</li> })
                                    .collect_view()}
                            </ul>
                            <p class="section-label">{t!("overlay.addr.observed")}</p>
                            <ul class="addr-list">
                                {if s.observed_addrs.is_empty() {
                                    view! { <li class="muted">{t!("overlay.addr.none_yet")}</li> }.into_any()
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
            if let Ok(resp) = gloo_net::http::Request::get(&crate::api::api("/api/v1/peers"))
                .send()
                .await
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
                    <span class="overlay-title">{t!("overlay.peers.title")}</span>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <p class="panel-note">
                        {t!("overlay.peers.note")}
                    </p>
                    {move || match peers.get() {
                        None => view! { <p class="loading">{t!("overlay.loading")}</p> }.into_any(),
                        Some(list) if list.is_empty() => {
                            view! { <p class="muted">{t!("overlay.peers.none")}</p> }.into_any()
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
                                            t!("overlay.peers.addresses", n = p.addresses.len()).to_string()
                                        };
                                        let agent = p.agent_version.map(|a| view! {
                                            <span class="peer-agent">{a}</span>
                                        });
                                        view! {
                                            <li class="peer-item">
                                                <div class="peer-head">
                                                    <span class=css>{label}</span>
                                                    <span class="mono peer-id" title=peer_id_title>
                                                        {p.peer_id}
                                                    </span>
                                                </div>
                                                {agent}
                                                <span class="peer-addr">{addr}</span>
                                            </li>
                                        }
                                    }).collect_view()}
                                </ul>
                                <p class="section-label">
                                    {t!("overlay.peers.known", n = count)}
                                </p>
                            }.into_any()
                        }
                    }}
                </div>
            </div>
        </div>
    }
}

/// Display version from the running daemon's `/api/v1/status`: `v0.36.0-dev`
/// alone, or `v0.36.0-dev (49e59a1)` when the daemon build baked in a git
/// commit hash. The daemon is the single source of truth; `commit` is empty
/// when git wasn't available at the daemon's build time.
fn version_string(status: &StatusResponse) -> String {
    if status.commit.is_empty() {
        format!("v{}", status.version)
    } else {
        format!("v{} ({})", status.version, status.commit)
    }
}

/// Quick reference: version, repository and where to report issues.
#[component]
pub fn AboutPanel(
    status: RwSignal<Option<StatusResponse>>,
    active_panel: RwSignal<Option<super::Panel>>,
) -> impl IntoView {
    let close = move || active_panel.set(None);
    const REPO: &str = "https://github.com/ogarcia/rucio";

    view! {
        <div class="overlay-backdrop" on:click=move |_| close()>
            <div class="overlay" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">{t!("overlay.about.title")}</span>
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
                        {move || status.get().map(|s| version_string(&s)).unwrap_or_default()}
                    </div>
                    <p style="color: var(--text-2); margin: 0.9rem 0 1.1rem;">
                        {t!("overlay.about.tagline")}
                    </p>
                    <div style="display: flex; flex-direction: column; gap: 0.6rem;">
                        <a
                            href=REPO target="_blank" rel="noopener noreferrer"
                            style="color: var(--accent);"
                        >{t!("overlay.about.source")}</a>
                        <a
                            href=format!("{REPO}/issues/new")
                            target="_blank" rel="noopener noreferrer"
                            style="color: var(--accent);"
                        >{t!("overlay.about.report")}</a>
                    </div>
                </div>
            </div>
        </div>
    }
}
