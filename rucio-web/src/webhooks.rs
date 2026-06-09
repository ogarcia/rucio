//! Webhooks editor for the Notifications settings tab.
//!
//! Each row keeps its fields in their own signals (and a stable id) so editing
//! one input never re-renders — and so the custom-format fields can appear and
//! disappear reactively. The row list (`rows`) is owned by the config modal, not
//! this component, because the modal is remounted on every tab switch: keeping
//! the state in the parent means switching away and back doesn't lose unsaved
//! edits, and the modal's single "Save" button can persist the webhooks together
//! with the rest of the configuration.

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{NotificationKind, WebhookDef, WebhookTestResult};

/// One editable webhook row. `RwSignal` is `Copy`, so the whole struct is.
#[derive(Clone, Copy)]
pub struct Row {
    id: usize,
    url: RwSignal<String>,
    format: RwSignal<String>,
    on_download: RwSignal<bool>,
    on_system: RwSignal<bool>,
    secret: RwSignal<String>,
    template: RwSignal<String>,
    content_type: RwSignal<String>,
    /// Latest test result line (empty until the user hits Test).
    test: RwSignal<String>,
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
            test: RwSignal::new(String::new()),
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

/// Mint a fresh row with a unique id, bumping the shared counter.
pub fn mint_row(next_id: RwSignal<usize>, def: &WebhookDef) -> Row {
    let id = next_id.get_untracked();
    next_id.set(id + 1);
    Row::new(id, def)
}

/// Collect the wire definitions from the rows, dropping any with a blank URL.
/// This is what the modal's Save button PUTs to `/config/notifications/webhooks`.
pub fn collect_defs(rows: &[Row]) -> Vec<WebhookDef> {
    rows.iter()
        .map(|r| r.to_def())
        .filter(|d| !d.url.is_empty())
        .collect()
}

/// POST the row's current definition to the test endpoint and show the result.
fn test_webhook(row: Row) {
    let def = row.to_def();
    if def.url.is_empty() {
        row.test.set("✗ enter a URL first".to_string());
        return;
    }
    row.test.set("Testing…".to_string());
    spawn_local(async move {
        let msg = match gloo_net::http::Request::post("/api/v1/config/notifications/webhooks/test")
            .json(&def)
        {
            Ok(req) => match req.send().await {
                Ok(resp) => match resp.json::<WebhookTestResult>().await {
                    Ok(r) if r.ok => "✓ Delivered".to_string(),
                    Ok(r) => format!("✗ {}", r.error.unwrap_or_else(|| "failed".to_string())),
                    Err(_) => "✗ bad response".to_string(),
                },
                Err(_) => "✗ request failed".to_string(),
            },
            Err(_) => "✗ invalid request".to_string(),
        };
        row.test.set(msg);
    });
}

const FORMATS: [&str; 6] = ["generic", "discord", "slack", "telegram", "ntfy", "custom"];

/// Display label for a format value (the value stays lowercase on the wire).
fn format_label(f: &str) -> &'static str {
    match f {
        "discord" => "Discord",
        "slack" => "Slack",
        "telegram" => "Telegram",
        "ntfy" => "ntfy",
        "custom" => "Custom",
        _ => "Generic",
    }
}

/// The webhook rows editor. State lives in the parent (`rows` / `next_id`) so it
/// survives tab switches and is saved by the modal's "Save" button — this
/// component only renders and mutates the row list, it never persists.
#[component]
pub fn WebhooksEditor(rows: RwSignal<Vec<Row>>, next_id: RwSignal<usize>) -> impl IntoView {
    let add = move |_| {
        let row = mint_row(
            next_id,
            &WebhookDef {
                format: "generic".to_string(),
                ..Default::default()
            },
        );
        rows.update(|r| r.push(row));
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
                            class="webhook-format"
                            prop:value=move || row.format.get()
                            on:change=move |e| row.format.set(event_target_value(&e))
                        >
                            {FORMATS.iter().map(|f| view! {
                                <option value=*f>{format_label(f)}</option>
                            }).collect_view()}
                        </select>
                        <button
                            class="btn-sm"
                            title="Send a test notification to this webhook"
                            on:click=move |_| test_webhook(row)
                        >"Test"</button>
                        <span class=move || {
                            let t = row.test.get();
                            if t.starts_with('\u{2713}') { "webhook-test webhook-test-ok" }
                            else if t.is_empty() { "webhook-test" }
                            else { "webhook-test webhook-test-err" }
                        }>{move || row.test.get()}</span>
                        <button
                            class="webhook-del"
                            title="Remove webhook"
                            on:click=move |_| rows.update(|r| r.retain(|x| x.id != row.id))
                        >
                            <Icon paths=icons::TRASH/>
                        </button>
                    </div>

                    <input
                        class="config-input"
                        type="text"
                        placeholder="https://… (URL to POST to)"
                        prop:value=move || row.url.get()
                        on:input=move |e| row.url.set(event_target_value(&e))
                    />

                    <div class="webhook-kinds">
                        <span class="config-label">"Kinds:"</span>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_download.get()
                                on:change=move |e| row.on_download.set(event_target_checked(&e))
                            />
                            "downloads"
                        </label>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_system.get()
                                on:change=move |e| row.on_system.set(event_target_checked(&e))
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
                        on:input=move |e| row.secret.set(event_target_value(&e))
                    />

                    <Show when=move || row.format.get() == "custom">
                        <textarea
                            class="config-textarea"
                            rows="2"
                            placeholder=r#"body template, e.g. {"text":"{title} — {body}"}"#
                            prop:value=move || row.template.get()
                            on:input=move |e| row.template.set(event_target_value(&e))
                        />
                        <input
                            class="config-input"
                            type="text"
                            placeholder="content-type (default application/json)"
                            prop:value=move || row.content_type.get()
                            on:input=move |e| row.content_type.set(event_target_value(&e))
                        />
                    </Show>
                </div>
            </For>

            <div class="webhook-actions">
                <button class="btn-sm" on:click=add>"Add webhook"</button>
            </div>
        </div>
    }
}
