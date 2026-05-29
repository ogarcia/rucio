use leptos::prelude::*;

/// Renders a Tabler icon SVG.  Pass one of the path constants below.
#[component]
pub fn Icon(paths: &'static str) -> impl IntoView {
    view! {
        <svg
            class="icon"
            viewBox="0 0 24 24"
            stroke="currentColor"
            fill="none"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            aria-hidden="true"
            inner_html=paths
        ></svg>
    }
}

// ── Tabler icon paths ─────────────────────────────────────────────────────────

pub const PLUS: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 5l0 14"/><path d="M5 12l14 0"/>"#;

pub const INFO_CIRCLE: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M3 12a9 9 0 1 0 18 0a9 9 0 0 0 -18 0"/><path d="M12 9h.01"/><path d="M11 12h1v4h1"/>"#;

pub const PLAYER_PAUSE: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6 5m0 1a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v12a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1z"/><path d="M14 5m0 1a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v12a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1z"/>"#;

pub const PLAYER_PLAY: &str =
    r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M7 4v16l13 -8z"/>"#;

pub const CIRCLE_X: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 12m-9 0a9 9 0 1 0 18 0a9 9 0 1 0 -18 0"/><path d="M10 10l4 4m0 -4l-4 4"/>"#;

pub const X: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M18 6l-12 12"/><path d="M6 6l12 12"/>"#;

pub const TRASH: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M4 7l16 0"/><path d="M10 11l0 6"/><path d="M14 11l0 6"/><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2 -2l1 -12"/><path d="M9 7v-3a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v3"/>"#;

pub const NETWORK: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6 9a6 6 0 1 0 12 0a6 6 0 0 0 -12 0"/><path d="M12 3c1.333 .333 2 2.333 2 6s-.667 5.667 -2 6"/><path d="M12 3c-1.333 .333 -2 2.333 -2 6s.667 5.667 2 6"/><path d="M6 9h12"/><path d="M3 20h7"/><path d="M14 20h7"/><path d="M10 20a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"/><path d="M12 15v3"/>"#;

pub const NETWORK_OFF: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.528 6.536a6 6 0 0 0 7.942 7.933m2.247 -1.76a6 6 0 0 0 -8.427 -8.425"/><path d="M12 3c1.333 .333 2 2.333 2 6c0 .337 -.006 .66 -.017 .968m-.55 3.473c-.333 .884 -.81 1.403 -1.433 1.559"/><path d="M12 3c-.936 .234 -1.544 1.29 -1.822 3.167m-.16 3.838c.116 3.029 .776 4.695 1.982 4.995"/><path d="M6 9h3m4 0h5"/><path d="M3 20h7"/><path d="M14 20h7"/><path d="M10 20a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"/><path d="M12 15v3"/><path d="M3 3l18 18"/>"#;

pub const MENU: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M4 6l16 0"/><path d="M4 12l16 0"/><path d="M4 18l16 0"/>"#;

pub const HOURGLASS: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.5 7h11"/><path d="M6.5 17h11"/><path d="M6 20v-2a6 6 0 1 1 12 0v2a1 1 0 0 1 -1 1h-10a1 1 0 0 1 -1 -1z"/><path d="M6 4v2a6 6 0 1 0 12 0v-2a1 1 0 0 0 -1 -1h-10a1 1 0 0 0 -1 1z"/>"#;

pub const HOURGLASS_OFF: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.5 7h11"/><path d="M6.5 17h11"/><path d="M6 20v-2a6 6 0 1 1 12 0v2a1 1 0 0 1 -1 1h-10a1 1 0 0 1 -1 -1z"/><path d="M6 4v2a6 6 0 1 0 12 0v-2a1 1 0 0 0 -1 -1h-10a1 1 0 0 0 -1 1z"/><path d="M3 3l18 18"/>"#;

pub const CHART_BAR: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M3 13m0 1a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v5a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1z"/><path d="M9 9m0 1a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v9a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1z"/><path d="M15 5m0 1a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v13a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1z"/><path d="M4 20l14 0"/>"#;

pub const SUN: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 12m-4 0a4 4 0 1 0 8 0a4 4 0 1 0 -8 0"/><path d="M3 12h1m8 -9v1m8 8h1m-9 8v1m-6.4 -15.4l.7 .7m12.1 -.7l-.7 .7m0 11.4l.7 .7m-12.1 -.7l-.7 .7"/>"#;

pub const MOON: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 3c.132 0 .263 0 .393 0a7.5 7.5 0 0 0 7.92 12.446a9 9 0 1 1 -8.313 -12.454z"/>"#;

pub const DEVICE_DESKTOP: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M3 5a1 1 0 0 1 1 -1h16a1 1 0 0 1 1 1v10a1 1 0 0 1 -1 1h-16a1 1 0 0 1 -1 -1v-10z"/><path d="M7 20h10"/><path d="M9 16v4"/><path d="M15 16v4"/>"#;
