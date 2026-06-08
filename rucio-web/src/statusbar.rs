//! The persistent footer shared by every tab.
//!
//! Visual consistency across tabs: the right-hand side — aggregate transfer
//! speeds and the temporary speed-limit toggle — is global and identical
//! everywhere, so it lives here once. The left-hand side is tab-specific and
//! supplied as `children` (e.g. the downloads filter controls, an active
//! count, a result count).

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::icons::{self, Icon};
use crate::types::{TempLimitRequest, TempLimitStatus, format_speed};

/// Toggle the daemon's temporary speed limit; returns the resulting state.
async fn api_set_temp_limit(active: bool) -> Option<bool> {
    gloo_net::http::Request::put("/api/v1/config/temp-limit")
        .json(&TempLimitRequest { active })
        .ok()?
        .send()
        .await
        .ok()?
        .json::<TempLimitStatus>()
        .await
        .ok()
        .map(|s| s.active)
}

/// The footer rendered at the bottom of every tab. `children` is the
/// tab-specific left side; the global speed meters + slow-mode toggle are
/// appended on the right.
#[component]
pub fn StatusBar(
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
    children: Children,
) -> impl IntoView {
    view! {
        <div class="dl-statusbar">
            {children()}
            <div class="dl-status-right">
                <div class="dl-speeds">
                    {move || {
                        let dl = dl_speed.get();
                        let ul = ul_speed.get();
                        if dl > 0 || ul > 0 {
                            view! {
                                <span class="dl-speed dl-speed-down">
                                    "↓ " {format_speed(dl)}
                                </span>
                                <span class="dl-speed dl-speed-up">
                                    "↑ " {format_speed(ul)}
                                </span>
                            }.into_any()
                        } else {
                            view! { <span class="dl-speed-idle">"Idle"</span> }.into_any()
                        }
                    }}
                </div>
                // Temporary speed-limit toggle: caps upload/download to free
                // bandwidth (e.g. for gaming) until switched off again.
                <button
                    class=move || if temp_limit.get() {
                        "dl-limit-btn dl-limit-on"
                    } else {
                        "dl-limit-btn"
                    }
                    title=move || if temp_limit.get() {
                        "Temporary speed limit: on"
                    } else {
                        "Temporary speed limit: off"
                    }
                    on:click=move |_| {
                        let next = !temp_limit.get_untracked();
                        spawn_local(async move {
                            if let Some(active) = api_set_temp_limit(next).await {
                                temp_limit.set(active);
                            }
                        });
                    }
                >
                    {move || view! {
                        // Icon shows the action: an un-crossed hourglass when off
                        // (press to slow down), a crossed one when on (press to
                        // lift the limit). The highlight conveys the active state.
                        <Icon paths=if temp_limit.get() {
                            icons::HOURGLASS_OFF
                        } else {
                            icons::HOURGLASS
                        }/>
                    }}
                </button>
            </div>
        </div>
    }
}
