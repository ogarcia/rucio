//! Categories editor for the Settings → Categories tab.
//!
//! Categories are individual REST resources, but the UI saves them with the
//! config modal's single "Save" button (no per-row Save). The row state lives
//! in the modal (like the webhooks editor); this component only renders and
//! edits it. [`persist`] is what the modal's Save calls: it DELETEs removed
//! rows, then POSTs new / PUTs existing ones, writing any per-row error back to
//! that row's status, and returns whether everything succeeded.

use leptos::prelude::*;
use rust_i18n::t;

use crate::icons::{self, Icon};
use crate::types::{CategoriesResponse, Category, NEUTRAL_CATEGORY_COLOR};

/// Colour a freshly-added row's picker starts at (an existing colourless
/// category instead starts at [`NEUTRAL_CATEGORY_COLOR`]).
const DEFAULT_COLOR: &str = "#3b82f6";

/// One editable category row. `RwSignal` is `Copy`, so the whole struct is.
#[derive(Clone, Copy)]
pub struct Row {
    /// Stable local key for `<For>` (not the DB id).
    id: usize,
    /// `Some` once persisted; `None` for a not-yet-saved new row.
    cat_id: RwSignal<Option<i64>>,
    name: RwSignal<String>,
    dir: RwSignal<String>,
    color: RwSignal<String>,
    keywords: RwSignal<String>,
    /// Error line, set by [`persist`] when a save fails (empty otherwise).
    status: RwSignal<String>,
}

impl Row {
    fn new(id: usize, c: &Category) -> Self {
        Row {
            id,
            cat_id: RwSignal::new((c.id != 0).then_some(c.id)),
            name: RwSignal::new(c.name.clone()),
            dir: RwSignal::new(c.download_dir.clone().unwrap_or_default()),
            // A colourless category starts the picker at the shared neutral
            // grey, so it shows the same colour the list badge does.
            color: RwSignal::new(
                c.color
                    .clone()
                    .unwrap_or_else(|| NEUTRAL_CATEGORY_COLOR.to_string()),
            ),
            keywords: RwSignal::new(c.match_keywords.clone().unwrap_or_default()),
            status: RwSignal::new(String::new()),
        }
    }

    /// Build the wire body from the row's signals (blank → None).
    fn to_category(self) -> Category {
        let opt = |s: String| {
            let t = s.trim().to_string();
            (!t.is_empty()).then_some(t)
        };
        Category {
            id: self.cat_id.get_untracked().unwrap_or(0),
            name: self.name.get_untracked().trim().to_string(),
            download_dir: opt(self.dir.get_untracked()),
            // The picker always holds a value, so colour is always sent.
            color: opt(self.color.get_untracked()),
            match_keywords: opt(self.keywords.get_untracked()),
        }
    }
}

/// Mint a fresh row with a unique local key, bumping the shared counter.
pub fn mint_row(next_id: RwSignal<usize>, c: &Category) -> Row {
    let id = next_id.get_untracked();
    next_id.set(id + 1);
    Row::new(id, c)
}

/// A blank new row (colour pre-filled so the badge isn't black).
pub fn blank_row(next_id: RwSignal<usize>) -> Row {
    mint_row(
        next_id,
        &Category {
            color: Some(DEFAULT_COLOR.to_string()),
            ..Default::default()
        },
    )
}

/// Persist the categories: DELETE the ids in `deleted`, then POST new / PUT
/// existing rows. Per-row failures are written to that row's status and make
/// the result `false`; on full success the shared `categories` signal is
/// refreshed (for the list badges) and `true` is returned. Called by the
/// modal's Save button.
pub async fn persist(
    rows: RwSignal<Vec<Row>>,
    deleted: RwSignal<Vec<i64>>,
    categories: RwSignal<Vec<Category>>,
) -> bool {
    let mut all_ok = true;

    // Deletions first (a rename could otherwise clash with a still-present row).
    for id in deleted.get_untracked() {
        let ok = gloo_net::http::Request::delete(&format!("/api/v1/categories/{id}"))
            .send()
            .await
            .map(|r| r.ok())
            .unwrap_or(false);
        all_ok &= ok;
    }
    if all_ok {
        deleted.set(Vec::new());
    }

    for row in rows.get_untracked() {
        if row.name.get_untracked().trim().is_empty() {
            row.status.set(format!("✗ {}", t!("cat.err_name_required")));
            all_ok = false;
            continue;
        }
        let body = row.to_category();
        let req = match row.cat_id.get_untracked() {
            Some(id) => gloo_net::http::Request::put(&format!("/api/v1/categories/{id}")),
            None => gloo_net::http::Request::post("/api/v1/categories"),
        };
        match req.json(&body) {
            Ok(req) => match req.send().await {
                Ok(resp) if resp.ok() => {
                    if row.cat_id.get_untracked().is_none()
                        && let Ok(created) = resp.json::<Category>().await
                    {
                        row.cat_id.set(Some(created.id));
                    }
                    row.status.set(String::new());
                }
                Ok(resp) => {
                    let hint = match resp.status() {
                        409 => t!("cat.err_name_exists"),
                        400 => t!("cat.err_check_dir"),
                        _ => t!("cat.err_save_failed"),
                    };
                    row.status.set(format!("✗ {hint}"));
                    all_ok = false;
                }
                Err(_) => {
                    row.status
                        .set(format!("✗ {}", t!("cat.err_request_failed")));
                    all_ok = false;
                }
            },
            Err(_) => {
                row.status
                    .set(format!("✗ {}", t!("cat.err_invalid_request")));
                all_ok = false;
            }
        }
    }

    if all_ok
        && let Ok(r) = gloo_net::http::Request::get("/api/v1/categories")
            .send()
            .await
        && let Ok(s) = r.json::<CategoriesResponse>().await
    {
        categories.set(s.categories);
    }
    all_ok
}

/// The categories editor. State (`rows` / `next_id` / `deleted`) lives in the
/// config modal so it survives tab switches and is saved by the modal's Save
/// button — this component only renders and mutates it.
#[component]
pub fn CategoriesEditor(
    rows: RwSignal<Vec<Row>>,
    next_id: RwSignal<usize>,
    deleted: RwSignal<Vec<i64>>,
    // The modal's reactive owner. New rows' signals must be created under it (not
    // this editor's, which is disposed on every tab switch) or returning to this
    // tab would read disposed signals and panic.
    owner: Option<Owner>,
) -> impl IntoView {
    let add = move |_| {
        let push = || rows.update(|r| r.push(blank_row(next_id)));
        match &owner {
            Some(o) => o.with(push),
            None => push(),
        }
    };

    // Remove a row: queue its id for deletion (if persisted) and drop it.
    let remove = move |row: Row| {
        if let Some(id) = row.cat_id.get_untracked() {
            deleted.update(|d| d.push(id));
        }
        rows.update(|r| r.retain(|x| x.id != row.id));
    };

    view! {
        <div class="config-section">
            <p class="config-hint">
                {t!("cat.hint")}
            </p>

            <For each=move || rows.get() key=|r| r.id let:row>
                <div class="cat-row">
                    <div class="cat-row-head">
                        <input
                            type="color"
                            class="cat-color"
                            title=t!("cat.color_title")
                            prop:value=move || row.color.get()
                            on:input=move |e| row.color.set(event_target_value(&e))
                        />
                        <input
                            class="config-input cat-name"
                            type="text"
                            placeholder=t!("cat.name_placeholder")
                            prop:value=move || row.name.get()
                            on:input=move |e| row.name.set(event_target_value(&e))
                        />
                        <span class="cat-status cat-status-err">{move || row.status.get()}</span>
                        <button class="webhook-del" title=t!("cat.delete_title") on:click=move |_| remove(row)>
                            <Icon paths=icons::TRASH/>
                        </button>
                    </div>
                    <input
                        class="config-input"
                        type="text"
                        placeholder=t!("cat.dir_placeholder")
                        prop:value=move || row.dir.get()
                        on:input=move |e| row.dir.set(event_target_value(&e))
                    />
                    <input
                        class="config-input"
                        type="text"
                        placeholder=t!("cat.keywords_placeholder")
                        prop:value=move || row.keywords.get()
                        on:input=move |e| row.keywords.set(event_target_value(&e))
                    />
                </div>
            </For>

            <div class="webhook-actions">
                <button class="btn-sm" on:click=add>{t!("cat.add")}</button>
            </div>
        </div>
    }
}
