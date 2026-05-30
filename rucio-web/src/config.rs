use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{ConfigResponse, ConfigSnapshot};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConfigTab {
    Network,
    Storage,
    Emule,
}

async fn api_get_config() -> Option<ConfigResponse> {
    gloo_net::http::Request::get("/api/v1/config")
        .send()
        .await
        .ok()?
        .json::<ConfigResponse>()
        .await
        .ok()
}

async fn api_put_config(body: &ConfigResponse) -> bool {
    match gloo_net::http::Request::put("/api/v1/config").json(body) {
        Ok(req) => req.send().await.map(|r| r.ok()).unwrap_or(false),
        Err(_) => false,
    }
}

fn parse_kbps(s: &str) -> u64 {
    s.trim().parse().unwrap_or(0)
}

/// Split a textarea value into a list, dropping blank lines.
fn lines_to_vec(s: &str) -> Vec<String> {
    s.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

/// Full configuration modal with tabbed sections. The menu's quick-settings
/// signals are passed in so saving the limits here keeps them in sync.
#[component]
pub fn ConfigModal(
    base_up: RwSignal<u64>,
    base_down: RwSignal<u64>,
    temp_up: RwSignal<u64>,
    temp_down: RwSignal<u64>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let tab = RwSignal::new(ConfigTab::Network);
    // The on-disk snapshot we edit (pending if any, else current). Kept whole so
    // a save preserves sections this phase doesn't expose yet.
    let base: RwSignal<Option<ConfigSnapshot>> = RwSignal::new(None);
    let has_pending = RwSignal::new(false);
    let saving = RwSignal::new(false);
    let loaded = RwSignal::new(false);

    // Network field bindings (text inputs).
    let f_dl = RwSignal::new(String::new());
    let f_ul = RwSignal::new(String::new());
    let f_tdl = RwSignal::new(String::new());
    let f_tul = RwSignal::new(String::new());
    let f_boot = RwSignal::new(String::new());
    let f_listen = RwSignal::new(String::new());
    let f_tasks = RwSignal::new(String::new());

    // Load once on open.
    Effect::new(move |_| {
        spawn_local(async move {
            if let Some(resp) = api_get_config().await {
                has_pending.set(resp.pending.is_some());
                // Edit the on-disk state so we don't clobber pending changes.
                let snap = resp.pending.map(|b| *b).unwrap_or(resp.current);
                f_dl.set(snap.network.download_limit_kbps.to_string());
                f_ul.set(snap.network.upload_limit_kbps.to_string());
                f_tdl.set(snap.network.temp_download_limit_kbps.to_string());
                f_tul.set(snap.network.temp_upload_limit_kbps.to_string());
                f_boot.set(snap.network.bootstrap_peers.join("\n"));
                f_listen.set(snap.node.listen_addrs.join("\n"));
                f_tasks.set(snap.network.max_upload_tasks.to_string());
                base.set(Some(snap));
                loaded.set(true);
            }
        });
    });

    let save = move || {
        let Some(mut snap) = base.get_untracked() else {
            return;
        };
        snap.network.download_limit_kbps = parse_kbps(&f_dl.get_untracked());
        snap.network.upload_limit_kbps = parse_kbps(&f_ul.get_untracked());
        snap.network.temp_download_limit_kbps = parse_kbps(&f_tdl.get_untracked());
        snap.network.temp_upload_limit_kbps = parse_kbps(&f_tul.get_untracked());
        snap.network.max_upload_tasks = f_tasks
            .get_untracked()
            .trim()
            .parse()
            .unwrap_or(64usize)
            .max(1);
        snap.network.bootstrap_peers = lines_to_vec(&f_boot.get_untracked());
        snap.node.listen_addrs = lines_to_vec(&f_listen.get_untracked());

        // Capture the limits to mirror into the menu's quick-settings signals.
        let (dl, ul, tdl, tul) = (
            snap.network.download_limit_kbps,
            snap.network.upload_limit_kbps,
            snap.network.temp_download_limit_kbps,
            snap.network.temp_upload_limit_kbps,
        );
        saving.set(true);
        spawn_local(async move {
            let ok = api_put_config(&ConfigResponse {
                current: snap,
                pending: None,
            })
            .await;
            saving.set(false);
            if ok {
                base_down.set(dl);
                base_up.set(ul);
                temp_down.set(tdl);
                temp_up.set(tul);
                on_close();
            }
        });
    };

    let tab_class = move |t: ConfigTab| {
        if tab.get() == t {
            "config-tab config-tab-active"
        } else {
            "config-tab"
        }
    };

    view! {
        <div class="modal-backdrop" on:click=move |_| on_close()>
            <div class="modal modal-config" on:click=move |e| e.stop_propagation()>
                <div class="modal-header">
                    <span class="modal-title">"Configuration"</span>
                    <button class="overlay-close" on:click=move |_| on_close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>

                <div class="config-tabs">
                    <button class=move || tab_class(ConfigTab::Network)
                        on:click=move |_| tab.set(ConfigTab::Network)>"Network"</button>
                    <button class=move || tab_class(ConfigTab::Storage)
                        on:click=move |_| tab.set(ConfigTab::Storage)>"Storage"</button>
                    <button class=move || tab_class(ConfigTab::Emule)
                        on:click=move |_| tab.set(ConfigTab::Emule)>"eMule"</button>
                </div>

                <div class="modal-body">
                    <Show when=move || has_pending.get() fallback=|| ()>
                        <div class="config-pending">
                            <Icon paths=icons::HOURGLASS/>
                            <span>"There are saved changes waiting for a daemon restart."</span>
                        </div>
                    </Show>

                    {move || if !loaded.get() {
                        view! { <p class="loading">"Loading…"</p> }.into_any()
                    } else {
                        match tab.get() {
                            ConfigTab::Network => view! {
                                <div class="config-section">
                                    <p class="config-hint">"Speed limits in KB/s (0 = unlimited); applied immediately."</p>
                                    <div class="config-field">
                                        <label class="config-label">"Download limit"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_dl.get()
                                            on:input=move |e| f_dl.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Upload limit"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_ul.get()
                                            on:input=move |e| f_ul.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Temp download limit"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_tdl.get()
                                            on:input=move |e| f_tdl.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Temp upload limit"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_tul.get()
                                            on:input=move |e| f_tul.set(event_target_value(&e))/>
                                    </div>

                                    <p class="config-hint">"The fields below apply after a daemon restart."</p>
                                    <div class="config-field config-field-col">
                                        <label class="config-label">"Bootstrap peers (one per line)"</label>
                                        <textarea class="config-textarea" rows="3"
                                            prop:value=move || f_boot.get()
                                            on:input=move |e| f_boot.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field config-field-col">
                                        <label class="config-label">"Listen addresses (one per line)"</label>
                                        <textarea class="config-textarea" rows="2"
                                            prop:value=move || f_listen.get()
                                            on:input=move |e| f_listen.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Max upload tasks"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            prop:value=move || f_tasks.get()
                                            on:input=move |e| f_tasks.set(event_target_value(&e))/>
                                    </div>
                                </div>
                            }.into_any(),
                            ConfigTab::Storage => view! {
                                <div class="empty-state empty-state-sm"><p>"Storage settings — coming soon"</p></div>
                            }.into_any(),
                            ConfigTab::Emule => view! {
                                <div class="empty-state empty-state-sm"><p>"eMule settings — coming soon"</p></div>
                            }.into_any(),
                        }
                    }}
                </div>

                <div class="modal-footer">
                    <button class="btn-sm" on:click=move |_| on_close()>"Cancel"</button>
                    <button
                        class="btn-sm btn-primary"
                        disabled=move || saving.get() || !loaded.get()
                        on:click=move |_| save()
                    >
                        {move || if saving.get() { "Saving…" } else { "Save" }}
                    </button>
                </div>
            </div>
        </div>
    }
}
