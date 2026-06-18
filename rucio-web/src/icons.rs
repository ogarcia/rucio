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

pub const REFRESH: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M20 11a8.1 8.1 0 0 0 -15.5 -2m-.5 -4v4h4"/><path d="M4 13a8.1 8.1 0 0 0 15.5 2m.5 4v-4h-4"/>"#;

pub const TRASH: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M4 7l16 0"/><path d="M10 11l0 6"/><path d="M14 11l0 6"/><path d="M5 7l1 12a2 2 0 0 0 2 2h8a2 2 0 0 0 2 -2l1 -12"/><path d="M9 7v-3a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v3"/>"#;

pub const NETWORK: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6 9a6 6 0 1 0 12 0a6 6 0 0 0 -12 0"/><path d="M12 3c1.333 .333 2 2.333 2 6s-.667 5.667 -2 6"/><path d="M12 3c-1.333 .333 -2 2.333 -2 6s.667 5.667 2 6"/><path d="M6 9h12"/><path d="M3 20h7"/><path d="M14 20h7"/><path d="M10 20a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"/><path d="M12 15v3"/>"#;

pub const NETWORK_OFF: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.528 6.536a6 6 0 0 0 7.942 7.933m2.247 -1.76a6 6 0 0 0 -8.427 -8.425"/><path d="M12 3c1.333 .333 2 2.333 2 6c0 .337 -.006 .66 -.017 .968m-.55 3.473c-.333 .884 -.81 1.403 -1.433 1.559"/><path d="M12 3c-.936 .234 -1.544 1.29 -1.822 3.167m-.16 3.838c.116 3.029 .776 4.695 1.982 4.995"/><path d="M6 9h3m4 0h5"/><path d="M3 20h7"/><path d="M14 20h7"/><path d="M10 20a2 2 0 1 0 4 0a2 2 0 0 0 -4 0"/><path d="M12 15v3"/><path d="M3 3l18 18"/>"#;

pub const MENU: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M4 6l16 0"/><path d="M4 12l16 0"/><path d="M4 18l16 0"/>"#;

/// The Rucio mark (same path as `favicon.svg`), used larger in the About dialog.
pub const LOGO: &str = r#"<path d="M19.3 12.4 C16.7 9.2 15.9 8.2 16.0 7.9 A16.3 16.3 0 0 0 17.1 6.3 C17.3 6.0 17.7 5.0 17.0 4.3 C16.4 3.7 15.7 3.7 15.1 4.3 S14.3 5.0 13.8 5.4 L13.2 4.3 C13.0 3.8 12.5 3.0 11.9 2.7 S10.5 3.0 10.5 4.3 A10.0 10.0 0 0 1 10.2 6.8 C10.1 7.1 9.9 7.6 5.3 17.1 L4.0 19.7 L9.9 19.7 C10.5 18.7 10.3 18.7 11.2 16.9 L11.8 15.4 L13.0 15.9 C14.4 16.5 15.7 17.0 17.1 17.6 A2.1 2.1 0 0 0 19.4 17.0 A3.5 3.5 0 0 0 19.3 12.4 Z"/>"#;

pub const FOLDER: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M5 4h4l3 3h7a2 2 0 0 1 2 2v8a2 2 0 0 1 -2 2h-14a2 2 0 0 1 -2 -2v-11a2 2 0 0 1 2 -2"/>"#;

pub const PIN: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M15 4.5l-4 4l-4 1.5l-1.5 1.5l7 7l1.5 -1.5l1.5 -4l4 -4"/><path d="M9 15l-4.5 4.5"/><path d="M14.5 4l5.5 5.5"/>"#;

pub const PINNED_OFF: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M3 3l18 18"/><path d="M15 4.5l-3.249 3.249m-2.57 1.433l-2.181 .818l-1.5 1.5l7 7l1.5 -1.5l.82 -2.186m1.43 -2.563l3.25 -3.251"/><path d="M9 15l-4.5 4.5"/><path d="M14.5 4l5.5 5.5"/>"#;

pub const COPY: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M7 7m0 2.667a2.667 2.667 0 0 1 2.667 -2.667h8.666a2.667 2.667 0 0 1 2.667 2.667v8.666a2.667 2.667 0 0 1 -2.667 2.667h-8.666a2.667 2.667 0 0 1 -2.667 -2.667z"/><path d="M4.012 16.737a2.005 2.005 0 0 1 -1.012 -1.737v-10c0 -1.1 .9 -2 2 -2h10c.75 0 1.158 .385 1.5 1"/>"#;

pub const CHEVRON_DOWN: &str =
    r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6 9l6 6l6 -6"/>"#;

pub const HOURGLASS: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.5 7h11"/><path d="M6.5 17h11"/><path d="M6 20v-2a6 6 0 1 1 12 0v2a1 1 0 0 1 -1 1h-10a1 1 0 0 1 -1 -1z"/><path d="M6 4v2a6 6 0 1 0 12 0v-2a1 1 0 0 0 -1 -1h-10a1 1 0 0 0 -1 1z"/>"#;

pub const HOURGLASS_OFF: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M6.5 7h11"/><path d="M6.5 17h11"/><path d="M6 20v-2a6 6 0 1 1 12 0v2a1 1 0 0 1 -1 1h-10a1 1 0 0 1 -1 -1z"/><path d="M6 4v2a6 6 0 1 0 12 0v-2a1 1 0 0 0 -1 -1h-10a1 1 0 0 0 -1 1z"/><path d="M3 3l18 18"/>"#;

pub const BELL: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M10 5a2 2 0 1 1 4 0a7 7 0 0 1 4 6v3a4 4 0 0 0 2 3h-16a4 4 0 0 0 2 -3v-3a7 7 0 0 1 4 -6"/><path d="M9 17v1a3 3 0 0 0 6 0v-1"/>"#;

pub const DOWNLOAD: &str = r#"<path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2 -2v-2"/><path d="M7 11l5 5l5 -5"/><path d="M12 4l0 12"/>"#;
