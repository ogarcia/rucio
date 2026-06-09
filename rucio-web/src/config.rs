use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{
    ConfigResponse, ConfigSnapshot, EmuleStatusResponse, NotificationSettings, WebhookDef,
};
use crate::webhooks::{Row, WebhooksEditor, collect_defs, mint_row};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConfigTab {
    Network,
    Storage,
    Emule,
    Notifications,
}

async fn api_get_notif_settings() -> Option<NotificationSettings> {
    gloo_net::http::Request::get("/api/v1/config/notifications")
        .send()
        .await
        .ok()?
        .json::<NotificationSettings>()
        .await
        .ok()
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

async fn api_emule_status() -> Option<EmuleStatusResponse> {
    gloo_net::http::Request::get("/api/v1/emule/status")
        .send()
        .await
        .ok()?
        .json::<EmuleStatusResponse>()
        .await
        .ok()
}

fn parse_kbps(s: &str) -> u64 {
    s.trim().parse().unwrap_or(0)
}

/// CSS classes for a per-kind notification pill: reflects its on/off state and
/// greys it out (non-interactive) while the master switch is off.
fn notif_pill_class(on: bool, master_enabled: bool) -> &'static str {
    match (on, master_enabled) {
        (true, true) => "toggle-pill toggle-on toggle-clickable",
        (false, true) => "toggle-pill toggle-clickable",
        (true, false) => "toggle-pill toggle-on toggle-disabled",
        (false, false) => "toggle-pill toggle-disabled",
    }
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
/// `notif_enabled` is the master notification switch, shared with the header so
/// toggling it here hides/shows the bell live.
#[component]
pub fn ConfigModal(
    base_up: RwSignal<u64>,
    base_down: RwSignal<u64>,
    temp_up: RwSignal<u64>,
    temp_down: RwSignal<u64>,
    notif_enabled: RwSignal<bool>,
    on_close: impl Fn() + Copy + 'static,
) -> impl IntoView {
    let tab = RwSignal::new(ConfigTab::Network);
    // The on-disk snapshot we edit (pending if any, else current). Kept whole so
    // a save preserves sections this modal doesn't expose.
    let base: RwSignal<Option<ConfigSnapshot>> = RwSignal::new(None);
    let has_pending = RwSignal::new(false);
    let saving = RwSignal::new(false);
    let loaded = RwSignal::new(false);
    // Whether the daemon was built with eMule support; gates the eMule tab.
    let emule_available = RwSignal::new(false);

    // Network fields.
    let f_dl = RwSignal::new(String::new());
    let f_ul = RwSignal::new(String::new());
    let f_tdl = RwSignal::new(String::new());
    let f_tul = RwSignal::new(String::new());
    let f_boot = RwSignal::new(String::new());
    let f_listen = RwSignal::new(String::new());
    let f_tasks = RwSignal::new(String::new());
    let f_excl_boot = RwSignal::new(false);
    // Storage fields (database_path is read-only).
    let f_st_dl = RwSignal::new(String::new());
    let f_st_tmp = RwSignal::new(String::new());
    let f_st_db = RwSignal::new(String::new());
    // eMule fields.
    let f_em_enabled = RwSignal::new(false);
    let f_em_udp = RwSignal::new(String::new());
    let f_em_tcp = RwSignal::new(String::new());
    let f_em_extip = RwSignal::new(String::new());
    let f_em_temp = RwSignal::new(String::new());
    let f_em_slots = RwSignal::new(String::new());
    let f_em_upslots = RwSignal::new(String::new());
    let f_em_maxconc = RwSignal::new(String::new());
    let f_em_nick = RwSignal::new(String::new());
    let f_em_minspeed = RwSignal::new(String::new());
    // Notification toggles. Applied immediately on change (dedicated endpoint),
    // independent of the Save button which only persists the config above. The
    // master switch is the shared `notif_enabled` so the header bell reacts to
    // it live.
    let n_enabled = notif_enabled;
    let n_downloads = RwSignal::new(true);
    let n_system = RwSignal::new(true);
    // Webhook rows live here (not in WebhooksEditor) so they survive tab
    // switches and are persisted by this modal's Save button.
    let webhook_rows: RwSignal<Vec<Row>> = RwSignal::new(vec![]);
    let webhook_next_id = RwSignal::new(0usize);

    // Load once on open.
    Effect::new(move |_| {
        spawn_local(async move {
            if let Some(st) = api_emule_status().await {
                emule_available.set(st.feature_enabled);
            }
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
                f_excl_boot.set(snap.network.exclusive_bootstrap);
                f_st_dl.set(snap.storage.download_dir.clone());
                f_st_tmp.set(snap.storage.temp_dir.clone());
                f_st_db.set(snap.storage.database_path.clone());
                f_em_enabled.set(snap.emule.enabled);
                f_em_udp.set(snap.emule.udp_port.to_string());
                f_em_tcp.set(snap.emule.tcp_port.to_string());
                f_em_extip.set(snap.emule.external_ip.clone().unwrap_or_default());
                f_em_temp.set(snap.emule.temp_dir.clone());
                f_em_slots.set(snap.emule.download_slots_per_file.to_string());
                f_em_upslots.set(snap.emule.max_upload_slots.to_string());
                f_em_maxconc.set(snap.emule.max_concurrent_downloads.to_string());
                f_em_nick.set(snap.emule.nick.clone());
                f_em_minspeed.set(snap.emule.min_source_speed_kib_s.to_string());
                base.set(Some(snap));
                loaded.set(true);
            }
            if let Some(s) = api_get_notif_settings().await {
                n_enabled.set(s.enabled);
                n_downloads.set(s.downloads);
                n_system.set(s.system);
            }
            if let Ok(r) = gloo_net::http::Request::get("/api/v1/config/notifications/webhooks")
                .send()
                .await
                && let Ok(list) = r.json::<Vec<WebhookDef>>().await
            {
                webhook_rows.set(list.iter().map(|d| mint_row(webhook_next_id, d)).collect());
            }
        });
    });

    // Push the current notification toggles to the daemon (applied live there).
    let save_notif = move || {
        let body = NotificationSettings {
            enabled: n_enabled.get_untracked(),
            downloads: n_downloads.get_untracked(),
            system: n_system.get_untracked(),
        };
        spawn_local(async move {
            if let Ok(req) =
                gloo_net::http::Request::put("/api/v1/config/notifications").json(&body)
            {
                let _ = req.send().await;
            }
        });
    };

    let save = move || {
        let Some(mut snap) = base.get_untracked() else {
            return;
        };
        // Network.
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
        snap.network.exclusive_bootstrap = f_excl_boot.get_untracked();
        // Storage (database_path is read-only and left as loaded).
        snap.storage.download_dir = f_st_dl.get_untracked().trim().to_string();
        snap.storage.temp_dir = f_st_tmp.get_untracked().trim().to_string();
        // eMule. Numeric fields keep their previous value if left blank/invalid.
        snap.emule.enabled = f_em_enabled.get_untracked();
        if let Ok(p) = f_em_udp.get_untracked().trim().parse() {
            snap.emule.udp_port = p;
        }
        if let Ok(p) = f_em_tcp.get_untracked().trim().parse() {
            snap.emule.tcp_port = p;
        }
        let ext = f_em_extip.get_untracked().trim().to_string();
        snap.emule.external_ip = (!ext.is_empty()).then_some(ext);
        snap.emule.temp_dir = f_em_temp.get_untracked().trim().to_string();
        if let Ok(n) = f_em_slots.get_untracked().trim().parse::<usize>() {
            snap.emule.download_slots_per_file = n.clamp(1, 50);
        }
        if let Ok(n) = f_em_upslots.get_untracked().trim().parse::<usize>() {
            snap.emule.max_upload_slots = n.clamp(1, 50);
        }
        if let Ok(n) = f_em_maxconc.get_untracked().trim().parse::<usize>() {
            snap.emule.max_concurrent_downloads = n.clamp(1, 50);
        }
        snap.emule.nick = f_em_nick.get_untracked().trim().to_string();
        if let Ok(n) = f_em_minspeed.get_untracked().trim().parse::<u32>() {
            snap.emule.min_source_speed_kib_s = n;
        }

        // Mirror the limits into the menu's quick-settings signals.
        let (dl, ul, tdl, tul) = (
            snap.network.download_limit_kbps,
            snap.network.upload_limit_kbps,
            snap.network.temp_download_limit_kbps,
            snap.network.temp_upload_limit_kbps,
        );
        // Collect the webhook rows now; they're PUT to their own endpoint as
        // part of this single Save (no separate "Save webhooks" button).
        let webhooks = collect_defs(&webhook_rows.get_untracked());
        saving.set(true);
        spawn_local(async move {
            // Webhooks first so the config PUT (which reloads from disk) sees
            // them; then the main config.
            if let Ok(req) = gloo_net::http::Request::put("/api/v1/config/notifications/webhooks")
                .json(&webhooks)
            {
                let _ = req.send().await;
            }
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
    // eMule fields are read-only until eMule is enabled (the toggle itself stays
    // editable so you can turn it on).
    let em_locked = move || !f_em_enabled.get();

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
                    <Show when=move || emule_available.get() fallback=|| ()>
                        <button class=move || tab_class(ConfigTab::Emule)
                            on:click=move |_| tab.set(ConfigTab::Emule)>"eMule"</button>
                    </Show>
                    <button class=move || tab_class(ConfigTab::Notifications)
                        on:click=move |_| tab.set(ConfigTab::Notifications)>"Notifications"</button>
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
                                    <div class="config-field config-field-keep">
                                        <label class="config-label">"Use only my bootstrap peers"</label>
                                        <span
                                            class=move || if f_excl_boot.get() {
                                                "toggle-pill toggle-on toggle-clickable"
                                            } else {
                                                "toggle-pill toggle-clickable"
                                            }
                                            on:click=move |_| f_excl_boot.update(|v| *v = !*v)
                                        >
                                            {move || if f_excl_boot.get() { "On" } else { "Off" }}
                                        </span>
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
                                <div class="config-section">
                                    <p class="config-hint">"Directories apply after a daemon restart."</p>
                                    <div class="config-field">
                                        <label class="config-label">"Download directory"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_st_dl.get()
                                            on:input=move |e| f_st_dl.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Temp directory"</label>
                                        <input class="config-input" type="text"
                                            prop:value=move || f_st_tmp.get()
                                            on:input=move |e| f_st_tmp.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Database path (read-only)"</label>
                                        <input class="config-input" type="text" disabled=true
                                            prop:value=move || f_st_db.get()/>
                                    </div>
                                </div>
                            }.into_any(),
                            ConfigTab::Emule => view! {
                                <div class="config-section">
                                    <div class="config-field config-field-keep">
                                        <label class="config-label">"eMule enabled"</label>
                                        <span
                                            class=move || if f_em_enabled.get() {
                                                "toggle-pill toggle-on toggle-clickable"
                                            } else {
                                                "toggle-pill toggle-clickable"
                                            }
                                            on:click=move |_| f_em_enabled.update(|v| *v = !*v)
                                        >
                                            {move || if f_em_enabled.get() { "On" } else { "Off" }}
                                        </span>
                                    </div>
                                    <p class="config-hint">
                                        "Changes apply after a daemon restart. The fields below are read-only until eMule is enabled."
                                    </p>
                                    <div class="config-field">
                                        <label class="config-label">"Nickname"</label>
                                        <input class="config-input" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_nick.get()
                                            on:input=move |e| f_em_nick.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Temp directory"</label>
                                        <input class="config-input" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_temp.get()
                                            on:input=move |e| f_em_temp.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"External IP (blank = auto)"</label>
                                        <input class="config-input" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_extip.get()
                                            on:input=move |e| f_em_extip.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"TCP port"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_tcp.get()
                                            on:input=move |e| f_em_tcp.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"UDP port (Kad2)"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_udp.get()
                                            on:input=move |e| f_em_udp.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Download slots per file"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_slots.get()
                                            on:input=move |e| f_em_slots.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Max upload slots"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_upslots.get()
                                            on:input=move |e| f_em_upslots.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Max concurrent downloads"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_maxconc.get()
                                            on:input=move |e| f_em_maxconc.set(event_target_value(&e))/>
                                    </div>
                                    <div class="config-field">
                                        <label class="config-label">"Min source speed (KiB/s, 0 = off)"</label>
                                        <input class="config-input config-input-sm" type="text"
                                            disabled=em_locked
                                            prop:value=move || f_em_minspeed.get()
                                            on:input=move |e| f_em_minspeed.set(event_target_value(&e))/>
                                    </div>
                                </div>
                            }.into_any(),
                            ConfigTab::Notifications => view! {
                                <div class="config-section">
                                    <p class="config-hint">"Changes apply immediately. The per-type switches only matter while notifications are enabled."</p>
                                    <div class="config-field config-field-keep">
                                        <label class="config-label">"Enable notifications"</label>
                                        <span
                                            class=move || if n_enabled.get() {
                                                "toggle-pill toggle-on toggle-clickable"
                                            } else {
                                                "toggle-pill toggle-clickable"
                                            }
                                            on:click=move |_| {
                                                n_enabled.update(|v| *v = !*v);
                                                save_notif();
                                            }
                                        >
                                            {move || if n_enabled.get() { "On" } else { "Off" }}
                                        </span>
                                    </div>
                                    <div class="config-field config-field-keep">
                                        <label class="config-label">"Download notifications"</label>
                                        <span
                                            class=move || notif_pill_class(n_downloads.get(), n_enabled.get())
                                            on:click=move |_| {
                                                n_downloads.update(|v| *v = !*v);
                                                save_notif();
                                            }
                                        >
                                            {move || if n_downloads.get() { "On" } else { "Off" }}
                                        </span>
                                    </div>
                                    <div class="config-field config-field-keep">
                                        <label class="config-label">"System notifications"</label>
                                        <span
                                            class=move || notif_pill_class(n_system.get(), n_enabled.get())
                                            on:click=move |_| {
                                                n_system.update(|v| *v = !*v);
                                                save_notif();
                                            }
                                        >
                                            {move || if n_system.get() { "On" } else { "Off" }}
                                        </span>
                                    </div>
                                </div>
                                <WebhooksEditor rows=webhook_rows next_id=webhook_next_id/>
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
