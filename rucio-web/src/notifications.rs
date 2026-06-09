//! The notification centre: a right-edge slideover listing recent
//! notifications, opened from the header bell. Reuses the shared overlay header
//! and body chrome inside a slide-in drawer.

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{Notification, NotificationKind};

/// Human-readable "time since" for a Unix-seconds timestamp.
fn seen_ago(created_at: i64) -> String {
    let now = (js_sys::Date::now() / 1000.0) as i64;
    let d = (now - created_at).max(0);
    if d < 60 {
        "just now".to_string()
    } else if d < 3_600 {
        format!("{}m ago", d / 60)
    } else if d < 86_400 {
        format!("{}h ago", d / 3_600)
    } else {
        format!("{}d ago", d / 86_400)
    }
}

/// SVG paths for a notification's kind icon.
fn kind_icon(kind: NotificationKind) -> &'static str {
    match kind {
        NotificationKind::Download => icons::DOWNLOAD,
        NotificationKind::System => icons::INFO_CIRCLE,
    }
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
            let _ = gloo_net::http::Request::delete("/api/v1/notifications")
                .send()
                .await;
        });
    };

    let delete_one = move |id: i64| {
        notifications.update(|list| list.retain(|n| n.id != id));
        spawn_local(async move {
            let _ = gloo_net::http::Request::delete(&format!("/api/v1/notifications/{id}"))
                .send()
                .await;
        });
    };

    view! {
        <div class="notif-drawer-backdrop" on:click=move |_| close()>
            <div class="notif-drawer" on:click=move |e| e.stop_propagation()>
                <div class="overlay-header">
                    <span class="overlay-title">"Notifications"</span>
                    <Show when=move || !notifications.with(|l| l.is_empty())>
                        <button class="notif-clear" on:click=clear_all>"Clear all"</button>
                    </Show>
                    <button class="overlay-close" on:click=move |_| close()>
                        <Icon paths=icons::X/>
                    </button>
                </div>
                <div class="overlay-body">
                    <Show
                        when=move || !notifications.with(|l| l.is_empty())
                        fallback=|| view! {
                            <p class="notif-empty">"No notifications"</p>
                        }
                    >
                        <ul class="notif-list">
                            <For
                                each=move || notifications.get()
                                key=|n| n.id
                                children=move |n| {
                                    let id = n.id;
                                    view! {
                                        <li class="notif-item">
                                            <span class="notif-icon">
                                                <Icon paths=kind_icon(n.kind)/>
                                            </span>
                                            <div class="notif-text">
                                                <div class="notif-title">{n.title.clone()}</div>
                                                <div class="notif-body">{n.body.clone()}</div>
                                                <div class="notif-time">{seen_ago(n.created_at)}</div>
                                            </div>
                                            <button
                                                class="notif-del"
                                                title="Dismiss"
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
