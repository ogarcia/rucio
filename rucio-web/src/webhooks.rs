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
use rust_i18n::t;

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
        row.test.set(format!("✗ {}", t!("wh.test_need_url")));
        return;
    }
    row.test.set(t!("wh.testing").to_string());
    spawn_local(async move {
        let msg = match gloo_net::http::Request::post("/api/v1/config/notifications/webhooks/test")
            .json(&def)
        {
            Ok(req) => match req.send().await {
                Ok(resp) => match resp.json::<WebhookTestResult>().await {
                    Ok(r) if r.ok => format!("✓ {}", t!("wh.test_ok")),
                    Ok(r) => format!(
                        "✗ {}",
                        r.error.unwrap_or_else(|| t!("wh.test_failed").to_string())
                    ),
                    Err(_) => format!("✗ {}", t!("wh.test_bad_response")),
                },
                Err(_) => format!("✗ {}", t!("wh.test_request_failed")),
            },
            Err(_) => format!("✗ {}", t!("wh.test_invalid_request")),
        };
        row.test.set(msg);
    });
}

const FORMATS: [&str; 6] = ["generic", "discord", "slack", "telegram", "ntfy", "custom"];

/// Display label for a format value (the value stays lowercase on the wire).
/// Brand names are kept verbatim; only Generic/Custom are translated.
fn format_label(f: &str) -> std::borrow::Cow<'static, str> {
    match f {
        "discord" => "Discord".into(),
        "slack" => "Slack".into(),
        "telegram" => "Telegram".into(),
        "ntfy" => "ntfy".into(),
        "custom" => t!("wh.format_custom"),
        _ => t!("wh.format_generic"),
    }
}

/// The webhook rows editor. State lives in the parent (`rows` / `next_id`) so it
/// survives tab switches and is saved by the modal's "Save" button — this
/// component only renders and mutates the row list, it never persists.
#[component]
pub fn WebhooksEditor(
    rows: RwSignal<Vec<Row>>,
    next_id: RwSignal<usize>,
    // The modal's reactive owner; new rows' signals are created under it so they
    // outlive this editor being disposed on a tab switch (see CategoriesEditor).
    owner: Option<Owner>,
) -> impl IntoView {
    let add = move |_| {
        let push = || {
            rows.update(|r| {
                r.push(mint_row(
                    next_id,
                    &WebhookDef {
                        format: "generic".to_string(),
                        ..Default::default()
                    },
                ))
            })
        };
        match &owner {
            Some(o) => o.with(push),
            None => push(),
        }
    };

    view! {
        <div class="config-section">
            <p class="config-hint">
                {t!("wh.hint")}
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
                            title=t!("wh.test_title")
                            on:click=move |_| test_webhook(row)
                        >{t!("wh.test")}</button>
                        <span class=move || {
                            let t = row.test.get();
                            if t.starts_with('\u{2713}') { "webhook-test webhook-test-ok" }
                            else if t.is_empty() { "webhook-test" }
                            else { "webhook-test webhook-test-err" }
                        }>{move || row.test.get()}</span>
                        <button
                            class="webhook-del"
                            title=t!("wh.remove_title")
                            on:click=move |_| rows.update(|r| r.retain(|x| x.id != row.id))
                        >
                            <Icon paths=icons::TRASH/>
                        </button>
                    </div>

                    <input
                        class="config-input"
                        type="text"
                        placeholder=t!("wh.url_placeholder")
                        prop:value=move || row.url.get()
                        on:input=move |e| row.url.set(event_target_value(&e))
                    />

                    <div class="webhook-kinds">
                        <span class="config-label">{t!("wh.kinds")}</span>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_download.get()
                                on:change=move |e| row.on_download.set(event_target_checked(&e))
                            />
                            {t!("wh.kind_downloads")}
                        </label>
                        <label class="webhook-check">
                            <input
                                type="checkbox"
                                prop:checked=move || row.on_system.get()
                                on:change=move |e| row.on_system.set(event_target_checked(&e))
                            />
                            {t!("wh.kind_system")}
                        </label>
                        <span class="webhook-hint">{t!("wh.kinds_hint")}</span>
                    </div>

                    <input
                        class="config-input"
                        type="text"
                        placeholder=t!("wh.secret_placeholder")
                        prop:value=move || row.secret.get()
                        on:input=move |e| row.secret.set(event_target_value(&e))
                    />

                    <Show when=move || row.format.get() == "custom">
                        <textarea
                            class="config-textarea"
                            rows="2"
                            placeholder=t!("wh.template_placeholder")
                            prop:value=move || row.template.get()
                            on:input=move |e| row.template.set(event_target_value(&e))
                        />
                        <input
                            class="config-input"
                            type="text"
                            placeholder=t!("wh.content_type_placeholder")
                            prop:value=move || row.content_type.get()
                            on:input=move |e| row.content_type.set(event_target_value(&e))
                        />
                    </Show>
                </div>
            </For>

            <div class="webhook-actions">
                <button class="btn-sm" on:click=add>{t!("wh.add")}</button>
            </div>
        </div>
    }
}
