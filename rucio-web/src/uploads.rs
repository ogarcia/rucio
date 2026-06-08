//! Uploads tab: peers currently downloading files *from* this node.
//!
//! The upload-side mirror of the Downloads tab. The list is volatile and lives
//! entirely off the `UploadProgress` WebSocket stream: the daemon pushes a full
//! snapshot every second while any upload is in progress and one final empty
//! snapshot when the last upload ends, so the signal is always replaced
//! wholesale (no merge). Rows are small and change every tick, so the whole
//! list is re-rendered each update rather than keyed/diffed.

use leptos::prelude::*;

use crate::statusbar::StatusBar;
use crate::types::{ActiveUpload, UploadNetwork, format_size, format_speed};

#[component]
pub fn UploadsTab(
    uploads: RwSignal<Vec<ActiveUpload>>,
    dl_speed: RwSignal<u64>,
    ul_speed: RwSignal<u64>,
    temp_limit: RwSignal<bool>,
) -> impl IntoView {
    view! {
        <div class="tab-content">
            <div class="tab-scroll">
                <Show
                    when=move || !uploads.get().is_empty()
                    fallback=|| view! {
                        <div class="empty-state">
                            <p>"No one is downloading from you right now"</p>
                        </div>
                    }
                >
                    <ul class="ul-list">
                        {move || uploads
                            .get()
                            .into_iter()
                            .map(|u| view! { <UploadRow upload=u/> })
                            .collect_view()}
                    </ul>
                </Show>
            </div>

            // ── Status bar: active upload count (left) + global meters (right) ─
            <StatusBar dl_speed=dl_speed ul_speed=ul_speed temp_limit=temp_limit>
                {move || {
                    let n = uploads.get().len();
                    let count_class = if n > 0 {
                        "dl-active-count"
                    } else {
                        "dl-active-count dl-active-none"
                    };
                    let count_label = if n > 0 {
                        format!("{n} uploading")
                    } else {
                        "No active uploads".to_string()
                    };
                    view! { <span class=count_class>{count_label}</span> }
                }}
            </StatusBar>
        </div>
    }
}

#[component]
fn UploadRow(upload: ActiveUpload) -> impl IntoView {
    let (net_label, net_class) = match upload.network {
        UploadNetwork::Rucio => ("rucio", "ul-badge-rucio"),
        UploadNetwork::Emule => ("eMule", "ul-badge-emule"),
    };
    // Fall back to the hash when the file name is unknown (e.g. a rucio peer
    // pulling a share whose row we couldn't resolve a name for).
    let title = upload
        .file_name
        .clone()
        .unwrap_or_else(|| upload.file_hash.clone());
    let title_attr = title.clone();
    let rate = format_speed(upload.rate_bps);
    let sent = format_size(upload.bytes_sent);

    view! {
        <li class="ul-row">
            <span class=format!("ul-badge {net_class}")>{net_label}</span>
            <div class="ul-main">
                <span class="ul-name" title=title_attr>{title}</span>
                <span class="ul-peer" title="Remote peer">{upload.peer}</span>
            </div>
            <span class="ul-sent" title="Sent this session">{sent}</span>
            <span class="ul-rate">{if rate.is_empty() { "—".to_string() } else { rate }}</span>
        </li>
    }
}
