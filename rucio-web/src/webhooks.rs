//! Webhooks editor for the Notifications settings tab.
//!
//! Each row keeps its fields in their own signals (and a stable id) so editing
//! one input never re-renders — and so the custom-format fields can appear and
//! disappear reactively. "Save webhooks" PUTs the whole list.

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{NotificationKind, WebhookDef};

/// One editable webhook row. `RwSignal` is `Copy`, so the whole struct is.
#[derive(Clone, Copy)]
struct Row {
    id: usize,
    url: RwSignal<String>,
    format: RwSignal<String>,
    on_download: RwSignal<bool>,
    on_system: RwSignal<bool>,
    secret: RwSignal<String>,
    template: RwSignal<String>,
    content_type: RwSignal<String>,
}

impl Row {
    fn new(id: usize, def: &WebhookDef) -> Self {
        let has = |k: NotificationKind| def.kinds.contains(&k);
        Row {
            id,
            url: RwSignal::new(def.url.clone()),
            format: RwSignal::new(if def.format.is_empty() {
                "generic".to_string()
            } else {
                def.format.clone()
            }),
            on_download: RwSignal::new(has(NotificationKind::Download)),
            on_system: RwSignal::new(has(NotificationKind::System)),
            secret: RwSignal::new(def.secret.clone().unwrap_or_default()),
            template: RwSignal::new(def.template.clone().unwrap_or_default()),
            content_type: RwSignal::new(def.content_type.clone().unwrap_or_default()),
        }
    }

    /// Build the wire definition from the row's signals.
    fn to_def(self) -> WebhookDef {
        let mut kinds = Vec::new();
        if self.on_download.get_untracked() {
            kinds.push(NotificationKind::Download);
        }
        if self.on_system.get_untracked() {
            kinds.push(NotificationKind::System);
        }
        let opt = |s: String| (!s.trim().is_empty()).then_some(s);
        let is_custom = self.format.get_untracked() == "custom";
        WebhookDef {
            url: self.url.get_untracked().trim().to_string(),
            format: self.format.get_untracked(),
            kinds,
            secret: opt(self.secret.get_untracked()),
            template: is_custom
                .then(|| self.template.get_untracked())
                .and_then(&opt),
            content_type: if is_custom {
                opt(self.content_type.get_untracked())
            } else {
                None
            },
        }
    }
}

const FORMATS: [&str; 6] = ["generic", "discord", "slack", "telegram", "ntfy", "custom"];

#[component]
pub fn WebhooksEditor() -> impl IntoView {
    let rows: RwSignal<Vec<Row>> = RwSignal::new(vec![]);
    let next_id = RwSignal::new(0usize);
    let saving = RwSignal::new(false);
    let saved = RwSignal::new(false);

    let mint = move |def: &WebhookDef| -> Row {
        let id = next_id.get_untracked();
        next_id.set(id + 1);
        Row::new(id, def)
    };

    // Load existing webhooks once.
    {
        Effect::new(move |_| {
            spawn_local(async move {
                if let Ok(r) = gloo_net::http::Request::get("/api/v1/notifications/webhooks")
                    .send()
                    .await
                    && let Ok(list) = r.json::<Vec<WebhookDef>>().await
                {
                    rows.set(list.iter().map(&mint).collect());
                }
            });
        });
    }

    let add = move |_| {
        let row = mint(&WebhookDef {
            format: "generic".to_string(),
            ..Default::default()
        });
        rows.update(|r| r.push(row));
        saved.set(false);
    };

    let save = move |_| {
        let defs: Vec<WebhookDef> = rows
            .get_untracked()
            .iter()
            .map(|r| r.to_def())
            .filter(|d| !d.url.is_empty())
            .collect();
        saving.set(true);
        spawn_local(async move {
            let ok =
                match gloo_net::http::Request::put("/api/v1/notifications/webhooks").json(&defs) {
                    Ok(req) => req.send().await.map(|r| r.ok()).unwrap_or(false),
                    Err(_) => false,
                };
            saving.set(false);
            saved.set(ok);
        });
    };

    view! {
        <div class="config-section">
            <p class="config-hint">
                "Outbound webhooks: every notification you receive is also POSTed to these. "
                "Delivery is best-effort."
            </p>

            <For each=move || rows.get() key=|r| r.id let:row>
                <div class="webhook-row">
                    <div class="webhook-row-head">
                        <select
                            class="config-input config-input-sm"
                            prop:value=move || row.format.get()
                            on:change=move |e| { row.format.set(event_target_value(&e)); saved.set(false); }
                        >
                            {FORMATS.iter().map(|f| view! {
                                <option value=*f>{*f}</option>
                            }).collect_view()}
                        </select>
                        <button
                            class="webhook-del"
                            title="Remove webhook"
                            on:click=move |_| { rows.update(|r| r.retain(|x| x.id != row.id)); saved.set(false); }
                        >
                            <Icon paths=icons::TRASH/>
                        </button>
                    </div>

                    <input
                        class="config-input"
                        type="text"
                        placeholder="https://… (URL to POST to)"
                        prop:value=move || row.url.get()
                        on:input=move |e| { row.url.set(event_target_value(&e)); saved.set(false); }
                    />

                    <div class="webhook-kinds">
                        <span class="config-label">"Kinds:"</span>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_download.get()
                                on:change=move |e| { row.on_download.set(event_target_checked(&e)); saved.set(false); }
                            />
                            "downloads"
                        </label>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_system.get()
                                on:change=move |e| { row.on_system.set(event_target_checked(&e)); saved.set(false); }
                            />
                            "system"
                        </label>
                        <span class="webhook-hint">"(none = all)"</span>
                    </div>

                    <input
                        class="config-input"
                        type="text"
                        placeholder="secret (optional, signs body as X-Rucio-Signature)"
                        prop:value=move || row.secret.get()
                        on:input=move |e| { row.secret.set(event_target_value(&e)); saved.set(false); }
                    />

                    <Show when=move || row.format.get() == "custom">
                        <textarea
                            class="config-textarea"
                            rows="2"
                            placeholder=r#"body template, e.g. {"text":"{title} — {body}"}"#
                            prop:value=move || row.template.get()
                            on:input=move |e| { row.template.set(event_target_value(&e)); saved.set(false); }
                        />
                        <input
                            class="config-input config-input-sm"
                            type="text"
                            placeholder="content-type (default application/json)"
                            prop:value=move || row.content_type.get()
                            on:input=move |e| { row.content_type.set(event_target_value(&e)); saved.set(false); }
                        />
                    </Show>
                </div>
            </For>

            <div class="webhook-actions">
                <button class="btn-sm" on:click=add>"Add webhook"</button>
                <button
                    class="btn-sm btn-primary"
                    disabled=move || saving.get()
                    on:click=save
                >
                    {move || if saving.get() {
                        "Saving…"
                    } else if saved.get() {
                        "Saved ✓"
                    } else {
                        "Save webhooks"
                    }}
                </button>
            </div>
        </div>
    }
}
