use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
struct StatusResponse {
    peer_id: String,
    class: String,
    connected_peers: usize,
    listen_addrs: Vec<String>,
    observed_addrs: Vec<String>,
    uptime_secs: u64,
    version: String,
    #[serde(default)]
    external_ip: Option<String>,
}

async fn fetch_status() -> Result<StatusResponse, String> {
    gloo_net::http::Request::get("/api/v1/status")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json::<StatusResponse>()
        .await
        .map_err(|e| e.to_string())
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn class_badge(class: &str) -> (&'static str, &'static str) {
    match class {
        "HighId" => ("HighID", "badge badge-high"),
        "LowId" => ("LowID", "badge badge-low"),
        _ => ("Unknown", "badge badge-unknown"),
    }
}

#[component]
fn StatusCard(status: StatusResponse) -> impl IntoView {
    let (label, css) = class_badge(&status.class);
    let uptime = format_uptime(status.uptime_secs);

    view! {
        <div class="card">
            <p class="card-title">"Node"</p>
            <dl>
                <dt>"Version"</dt>
                <dd>{status.version}</dd>

                <dt>"Class"</dt>
                <dd><span class=css>{label}</span></dd>

                <dt>"Peer ID"</dt>
                <dd class="mono">{status.peer_id}</dd>

                <dt>"Peers"</dt>
                <dd>{status.connected_peers.to_string()}</dd>

                <dt>"Uptime"</dt>
                <dd>{uptime}</dd>

                {status.external_ip.map(|ip| view! {
                    <dt>"External IP"</dt>
                    <dd class="mono">{ip}</dd>
                })}
            </dl>
        </div>

        <div class="card">
            <p class="card-title">"Addresses"</p>

            <p class="card-subtitle">"Listen"</p>
            <ul class="addr-list">
                {status.listen_addrs.into_iter()
                    .map(|a| view! { <li>{a}</li> })
                    .collect_view()}
            </ul>

            <p class="card-subtitle">"Observed"</p>
            <ul class="addr-list">
                {if status.observed_addrs.is_empty() {
                    view! { <li class="muted">"None yet"</li> }.into_any()
                } else {
                    status.observed_addrs.into_iter()
                        .map(|a| view! { <li>{a}</li> })
                        .collect_view()
                        .into_any()
                }}
            </ul>
        </div>
    }
}

#[component]
fn App() -> impl IntoView {
    let status: RwSignal<Option<Result<StatusResponse, String>>> = RwSignal::new(None);

    // fetch is Copy-able because RwSignal: Copy
    let do_fetch = move || {
        spawn_local(async move {
            status.set(Some(fetch_status().await));
        });
    };

    do_fetch();

    view! {
        <div class="layout">
            <header class="topbar">
                <span class="brand">"Rucio"</span>
                <button class="btn" on:click=move |_| do_fetch()>"Refresh"</button>
            </header>
            <main class="content">
                {move || match status.get() {
                    None         => view! { <p class="loading">"Loading..."</p> }.into_any(),
                    Some(Ok(s))  => view! { <StatusCard status=s/> }.into_any(),
                    Some(Err(e)) => view! { <p class="error-msg">"Error: "{e}</p> }.into_any(),
                }}
            </main>
        </div>
    }
}

fn main() {
    mount_to_body(App);
}
