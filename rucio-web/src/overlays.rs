use leptos::prelude::*;

use crate::types::{StatusResponse, class_badge, format_uptime};

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
                    <button class="overlay-close" on:click=move |_| close()>"✕"</button>
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
                    <button class="overlay-close" on:click=move |_| close()>"✕"</button>
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
