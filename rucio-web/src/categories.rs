//! Categories editor for the Settings → Categories tab.
//!
//! Unlike the webhooks editor (one blob saved with the modal's Save button),
//! categories are individual REST resources, so each row is saved/deleted on
//! its own against `/api/v1/categories` (POST/PUT/DELETE). After any change the
//! shared `categories` signal is refreshed so the download-list badges update.

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{CategoriesResponse, Category};

/// Colour a freshly-added (or colourless) row's picker starts at.
const DEFAULT_COLOR: &str = "#3b82f6";

/// One editable category row. `RwSignal` is `Copy`, so the whole struct is.
#[derive(Clone, Copy)]
struct Row {
    /// Stable local key for `<For>` (not the DB id).
    key: usize,
    /// `Some` once persisted; `None` for a not-yet-saved new row.
    id: RwSignal<Option<i64>>,
    name: RwSignal<String>,
    dir: RwSignal<String>,
    color: RwSignal<String>,
    keywords: RwSignal<String>,
    /// Status line (empty, "Saved ✓", or "✗ <error>").
    status: RwSignal<String>,
}

impl Row {
    fn new(key: usize, c: &Category) -> Self {
        Row {
            key,
            id: RwSignal::new((c.id != 0).then_some(c.id)),
            name: RwSignal::new(c.name.clone()),
            dir: RwSignal::new(c.download_dir.clone().unwrap_or_default()),
            color: RwSignal::new(c.color.clone().unwrap_or_else(|| DEFAULT_COLOR.to_string())),
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
            id: self.id.get_untracked().unwrap_or(0),
            name: self.name.get_untracked().trim().to_string(),
            download_dir: opt(self.dir.get_untracked()),
            // The picker always holds a value, so colour is always sent.
            color: opt(self.color.get_untracked()),
            match_keywords: opt(self.keywords.get_untracked()),
        }
    }
}

/// Reload the shared categories signal from the daemon (for the list badges).
async fn refresh_global(categories: RwSignal<Vec<Category>>) {
    if let Ok(r) = gloo_net::http::Request::get("/api/v1/categories")
        .send()
        .await
        && let Ok(s) = r.json::<CategoriesResponse>().await
    {
        categories.set(s.categories);
    }
}

#[component]
pub fn CategoriesEditor(categories: RwSignal<Vec<Category>>) -> impl IntoView {
    let rows: RwSignal<Vec<Row>> = RwSignal::new(vec![]);
    let next_key = RwSignal::new(0usize);

    let mint = move |c: &Category| -> Row {
        let key = next_key.get_untracked();
        next_key.set(key + 1);
        Row::new(key, c)
    };

    // Load existing categories into rows once.
    Effect::new(move |_| {
        spawn_local(async move {
            if let Ok(r) = gloo_net::http::Request::get("/api/v1/categories")
                .send()
                .await
                && let Ok(s) = r.json::<CategoriesResponse>().await
            {
                rows.set(s.iter_minted(mint));
                categories.set(s.categories);
            }
        });
    });

    let add = move |_| {
        let row = mint(&Category {
            color: Some(DEFAULT_COLOR.to_string()),
            ..Default::default()
        });
        rows.update(|r| r.push(row));
    };

    // Persist one row: POST when new, PUT when it already has an id.
    let save = move |row: Row| {
        if row.name.get_untracked().trim().is_empty() {
            row.status.set("✗ name required".to_string());
            return;
        }
        let body = row.to_category();
        row.status.set("Saving…".to_string());
        spawn_local(async move {
            let req = match row.id.get_untracked() {
                Some(id) => gloo_net::http::Request::put(&format!("/api/v1/categories/{id}")),
                None => gloo_net::http::Request::post("/api/v1/categories"),
            };
            match req.json(&body) {
                Ok(req) => match req.send().await {
                    Ok(resp) if resp.ok() => {
                        // A POST returns the created category — adopt its id so a
                        // second save updates instead of duplicating.
                        if row.id.get_untracked().is_none()
                            && let Ok(created) = resp.json::<Category>().await
                        {
                            row.id.set(Some(created.id));
                        }
                        row.status.set("Saved ✓".to_string());
                        refresh_global(categories).await;
                    }
                    Ok(resp) => {
                        let code = resp.status();
                        let hint = match code {
                            409 => " (name already exists)".to_string(),
                            400 => " (check the directory / colour)".to_string(),
                            _ => String::new(),
                        };
                        row.status.set(format!("✗ HTTP {code}{hint}"));
                    }
                    Err(_) => row.status.set("✗ request failed".to_string()),
                },
                Err(_) => row.status.set("✗ invalid request".to_string()),
            }
        });
    };

    let remove = move |row: Row| {
        match row.id.get_untracked() {
            // Persisted → DELETE, then drop the row and refresh badges.
            Some(id) => spawn_local(async move {
                if let Ok(resp) =
                    gloo_net::http::Request::delete(&format!("/api/v1/categories/{id}"))
                        .send()
                        .await
                    && resp.ok()
                {
                    rows.update(|r| r.retain(|x| x.key != row.key));
                    refresh_global(categories).await;
                }
            }),
            // Never saved → just drop the local row.
            None => rows.update(|r| r.retain(|x| x.key != row.key)),
        }
    };

    view! {
        <div class="config-section">
            <p class="config-hint">
                "Categories route downloads to their own folder and tag them with a "
                "coloured badge. A download with no explicit category is auto-filed by "
                "the match keywords (the first matching category wins)."
            </p>

            <For each=move || rows.get() key=|r| r.key let:row>
                <div class="cat-row">
                    <div class="cat-row-head">
                        <input
                            type="color"
                            class="cat-color"
                            title="Badge colour"
                            prop:value=move || row.color.get()
                            on:input=move |e| { row.color.set(event_target_value(&e)); row.status.set(String::new()); }
                        />
                        <input
                            class="config-input cat-name"
                            type="text"
                            placeholder="Category name"
                            prop:value=move || row.name.get()
                            on:input=move |e| { row.name.set(event_target_value(&e)); row.status.set(String::new()); }
                        />
                        <button class="btn-sm btn-primary" on:click=move |_| save(row)>"Save"</button>
                        <span class=move || {
                            let s = row.status.get();
                            if s.starts_with('\u{2713}') || s == "Saved ✓" { "cat-status cat-status-ok" }
                            else if s.is_empty() || s == "Saving…" { "cat-status" }
                            else { "cat-status cat-status-err" }
                        }>{move || row.status.get()}</span>
                        <button class="webhook-del" title="Delete category" on:click=move |_| remove(row)>
                            <Icon paths=icons::TRASH/>
                        </button>
                    </div>
                    <input
                        class="config-input"
                        type="text"
                        placeholder="download directory (absolute path; empty = global)"
                        prop:value=move || row.dir.get()
                        on:input=move |e| { row.dir.set(event_target_value(&e)); row.status.set(String::new()); }
                    />
                    <input
                        class="config-input"
                        type="text"
                        placeholder="auto-match keywords, e.g. 1080p|bluray (empty = none)"
                        prop:value=move || row.keywords.get()
                        on:input=move |e| { row.keywords.set(event_target_value(&e)); row.status.set(String::new()); }
                    />
                </div>
            </For>

            <div class="webhook-actions">
                <button class="btn-sm" on:click=add>"Add category"</button>
            </div>
        </div>
    }
}

impl CategoriesResponse {
    /// Build rows from the response, minting a stable key for each.
    fn iter_minted(&self, mut mint: impl FnMut(&Category) -> Row) -> Vec<Row> {
        self.categories.iter().map(&mut mint).collect()
    }
}
