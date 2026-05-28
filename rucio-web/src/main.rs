use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::Deserialize;

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Downloads,
    Searches,
}

#[derive(Clone, Copy, PartialEq)]
enum Panel {
    NodeStatus,
    Addresses,
}

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

// ── API ──────────────────────────────────────────────────────────────────────

async fn fetch_status() -> Result<StatusResponse, String> {
    gloo_net::http::Request::get("/api/v1/status")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json::<StatusResponse>()
        .await
        .map_err(|e| e.to_string())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

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

// ── Overlay: Node status ─────────────────────────────────────────────────────

#[component]
fn NodeStatusPanel(
    status: RwSignal<Option<Result<StatusResponse, String>>>,
    active_panel: RwSignal<Option<Panel>>,
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
                        Some(Err(e)) => view! { <p class="error-msg">{e}</p> }.into_any(),
                        Some(Ok(s)) => {
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

// ── Overlay: Addresses ───────────────────────────────────────────────────────

#[component]
fn AddressesPanel(
    status: RwSignal<Option<Result<StatusResponse, String>>>,
    active_panel: RwSignal<Option<Panel>>,
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
                        Some(Err(e)) => view! { <p class="error-msg">{e}</p> }.into_any(),
                        Some(Ok(s)) => view! {
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

// ── Tab placeholders ─────────────────────────────────────────────────────────

#[component]
fn DownloadsTab() -> impl IntoView {
    view! {
        <div class="empty-state">
            <p>"No active downloads"</p>
        </div>
    }
}

#[component]
fn SearchesTab() -> impl IntoView {
    view! {
        <div class="empty-state">
            <p>"No recent searches"</p>
        </div>
    }
}

// ── App ──────────────────────────────────────────────────────────────────────

#[component]
fn App() -> impl IntoView {
    let active_tab: RwSignal<Tab> = RwSignal::new(Tab::Downloads);
    let menu_open: RwSignal<bool> = RwSignal::new(false);
    let active_panel: RwSignal<Option<Panel>> = RwSignal::new(None);
    let status: RwSignal<Option<Result<StatusResponse, String>>> = RwSignal::new(None);

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

                <nav class="tabs">
                    <button
                        class=move || if active_tab.get() == Tab::Downloads {
                            "tab active"
                        } else {
                            "tab"
                        }
                        on:click=move |_| active_tab.set(Tab::Downloads)
                    >
                        "Downloads"
                    </button>
                    <button
                        class=move || if active_tab.get() == Tab::Searches {
                            "tab active"
                        } else {
                            "tab"
                        }
                        on:click=move |_| active_tab.set(Tab::Searches)
                    >
                        "Searches"
                    </button>
                </nav>

                <div class="menu-wrap">
                    <button
                        class="menu-btn"
                        on:click=move |_| menu_open.update(|o| *o = !*o)
                    >
                        "≡"
                    </button>
                    <Show when=move || menu_open.get()>
                        <div class="dropdown">
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::NodeStatus));
                                menu_open.set(false);
                            }>
                                "Node status"
                            </button>
                            <button class="dropdown-item" on:click=move |_| {
                                active_panel.set(Some(Panel::Addresses));
                                menu_open.set(false);
                            }>
                                "Addresses"
                            </button>
                            <div class="dropdown-sep"/>
                            <button class="dropdown-item" on:click=move |_| {
                                menu_open.set(false);
                                do_fetch();
                            }>
                                "Refresh"
                            </button>
                        </div>
                    </Show>
                </div>
            </header>

            <main class="content">
                {move || match active_tab.get() {
                    Tab::Downloads => view! { <DownloadsTab/> }.into_any(),
                    Tab::Searches => view! { <SearchesTab/> }.into_any(),
                }}
            </main>
        </div>

        {move || active_panel.get().map(|panel| match panel {
            Panel::NodeStatus => view! {
                <NodeStatusPanel status=status active_panel=active_panel/>
            }.into_any(),
            Panel::Addresses => view! {
                <AddressesPanel status=status active_panel=active_panel/>
            }.into_any(),
        })}
    }
}

fn main() {
    mount_to_body(App);
}
