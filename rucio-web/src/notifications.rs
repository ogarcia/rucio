//! The notification centre: a right-edge slideover listing recent
//! notifications, opened from the header bell. Reuses the shared overlay header
//! and body chrome inside a slide-in drawer.

use leptos::prelude::*;
use leptos::task::spawn_local;
use rust_i18n::t;

use crate::icons::{self, Icon};
use crate::types::{Notification, NotificationKind};

/// Human-readable "time since" for a Unix-seconds timestamp.
fn seen_ago(created_at: i64) -> String {
    let now = (js_sys::Date::now() / 1000.0) as i64;
    let d = (now - created_at).max(0);
    if d < 60 {
        t!("notif.just_now").to_string()
    } else if d < 3_600 {
        t!("notif.minutes_ago", n = d / 60).to_string()
    } else if d < 86_400 {
        t!("notif.hours_ago", n = d / 3_600).to_string()
    } else {
        t!("notif.days_ago", n = d / 86_400).to_string()
    }
}

/// SVG paths for a notification's kind icon.
fn kind_icon(kind: NotificationKind) -> &'static str {
    match kind {
        NotificationKind::Download => icons::DOWNLOAD,
        NotificationKind::System => icons::INFO_CIRCLE,
    }
}

/// Localise a notification's (title, body) from its `code` + `args`. Unknown or
/// absent codes fall back to the English `title`/`body` the daemon sent, so a
/// new daemon message still shows (just untranslated) against an old panel.
fn localize(n: &Notification) -> (String, String) {
    let arg = |k: &str| n.args.get(k).cloned().unwrap_or_default();
    match n.code.as_deref() {
        Some("indexing_complete") => (
            t!("notif.msg.indexing_complete.title").into_owned(),
            t!("notif.msg.indexing_complete.body").into_owned(),
        ),
        // The body is the file name itself (data, not translatable text).
        Some("download_complete") => (
            t!("notif.msg.download_complete.title").into_owned(),
            arg("name"),
        ),
        Some("download_save_failed") => (
            t!("notif.msg.download_save_failed.title").into_owned(),
            t!("notif.msg.download_save_failed.body", name = arg("name")).into_owned(),
        ),
        Some("update_available") => (
            t!("notif.msg.update_available.title", v = arg("v")).into_owned(),
            t!("notif.msg.update_available.body", v = arg("v")).into_owned(),
        ),
        _ => (n.title.clone(), n.body.clone()),
    }
}

/// The notification's click target, but only when `ref_key` is a web link
/// (the update notice carries the release URL). Other kinds use `ref_key` for
/// non-URL references (e.g. a blake3 hash), which must never become an `href`.
fn link_ref(ref_key: &Option<String>) -> Option<String> {
    ref_key
        .as_deref()
        .filter(|r| r.starts_with("https://") || r.starts_with("http://"))
        .map(str::to_string)
}

#[component]
pub fn NotificationsPanel(
    notifications: RwSignal<Vec<Notification>>,
    active_panel: RwSignal<Option<super::Panel>>,
) -> impl IntoView {
    let close = move || active_panel.set(None);

    let clear_all = move |_| {
        notifications.set(vec![]);
        spawn_local(async move {
            let _ = gloo_net::http::Request::delete(&crate::api::api("/api/v1/notifications"))
                .send()
                .await;
        });
    };

    let delete_one = move |id: i64| {
        notifications.update(|list| list.retain(|n| n.id != id));
        spawn_local(async move {
            let _ = gloo_net::http::Request::delete(&crate::api::api(&format!(
                "/api/v1/notifications/{id}"
            )))
            .send()
            .await;
        });
    };

    view! {
        <div class="notif-drawer-backdrop" on:click=move |_| close()>
            <div class="notif-drawer" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">{t!("nav.notifications")}</span>
                    <Show when=move || !notifications.with(|l| l.is_empty())>
                        <button class="notif-clear" on:click=clear_all>{t!("notif.clear_all")}</button>
                    </Show>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <Show
                        when=move || !notifications.with(|l| l.is_empty())
                        fallback=|| view! {
                            <p class="notif-empty">{t!("notif.empty")}</p>
                        }
                    >
                        <ul class="notif-list">
                            <For
                                each=move || notifications.get()
                                key=|n| n.id
                                children=move |n| {
                                    let id = n.id;
                                    let link = link_ref(&n.ref_key);
                                    let (title, body) = localize(&n);
                                    view! {
                                        <li class="notif-item">
                                            <span class="notif-icon">
                                                <Icon paths=kind_icon(n.kind)/>
                                            </span>
                                            <div class="notif-text">
                                                {match link {
                                                    // Linkable notice (e.g. the update notice):
                                                    // the title opens the release page.
                                                    Some(url) => view! {
                                                        <a
                                                            class="notif-title notif-title-link"
                                                            href=url target="_blank" rel="noopener noreferrer"
                                                        >{title}</a>
                                                    }.into_any(),
                                                    None => view! {
                                                        <div class="notif-title">{title}</div>
                                                    }.into_any(),
                                                }}
                                                <div class="notif-body">{body}</div>
                                                <div class="notif-time">{seen_ago(n.created_at)}</div>
                                            </div>
                                            <button
                                                class="notif-del"
                                                title=t!("notif.dismiss")
                                                on:click=move |_| delete_one(id)
                                            >
                                                <Icon paths=icons::TRASH/>
                                            </button>
                                        </li>
                                    }
                                }
                            />
                        </ul>
                    </Show>
                </div>
            </div>
        </div>
    }
}
