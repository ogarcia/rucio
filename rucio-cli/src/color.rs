//! Colour helpers for the CLI.
//!
//! All styling goes through this module so the colour scheme is defined in one
//! place.  Every function checks `supports_color` and returns a plain string
//! when the output is not a TTY or when `NO_COLOR` is set.

use owo_colors::Stream::Stdout;
use owo_colors::{OwoColorize as _, Style};
use rucio_core::api::downloads::DownloadState;
use rucio_core::protocol::node::NodeClass;
use rust_i18n::t;

// ---------------------------------------------------------------------------
// Node class
// ---------------------------------------------------------------------------

pub fn node_class(class: &NodeClass) -> String {
    match class {
        NodeClass::HighId => t!("node.class.high_id")
            .if_supports_color(Stdout, |t| t.style(Style::new().bold().green()))
            .to_string(),
        NodeClass::LowId => t!("node.class.low_id")
            .if_supports_color(Stdout, |t| t.style(Style::new().bold().yellow()))
            .to_string(),
        NodeClass::Unknown => t!("node.class.unknown")
            .if_supports_color(Stdout, |t| t.dimmed())
            .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Connectivity summary
// ---------------------------------------------------------------------------

pub fn online(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.green()).to_string()
}

pub fn offline(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.red()).to_string()
}

pub fn limited(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.yellow()).to_string()
}

// ---------------------------------------------------------------------------
// Download state
// ---------------------------------------------------------------------------

pub fn download_state(state: &DownloadState) -> String {
    match state {
        DownloadState::FindingProviders => t!("download.state.finding_providers")
            .if_supports_color(Stdout, |t| t.yellow())
            .to_string(),
        DownloadState::Queued => t!("download.state.queued")
            .if_supports_color(Stdout, |t| t.yellow())
            .to_string(),
        DownloadState::Downloading => t!("download.state.downloading")
            .if_supports_color(Stdout, |t| t.cyan())
            .to_string(),
        DownloadState::Stalled => t!("download.state.stalled")
            .if_supports_color(Stdout, |t| t.red())
            .to_string(),
        DownloadState::Paused => t!("download.state.paused")
            .if_supports_color(Stdout, |t| t.magenta())
            .to_string(),
        DownloadState::Completed => t!("download.state.completed")
            .if_supports_color(Stdout, |t| t.green())
            .to_string(),
        DownloadState::Failed => t!("download.state.failed")
            .if_supports_color(Stdout, |t| t.red())
            .to_string(),
        DownloadState::Cancelled => t!("download.state.cancelled")
            .if_supports_color(Stdout, |t| t.dimmed())
            .to_string(),
    }
}

/// Progress bar for a download.
/// `bytes_done` and `total` are both in bytes; `total == 0` means unknown.
pub fn progress_bar(bytes_done: u64, total: u64) -> String {
    if total == 0 {
        return "[-                  ] -".to_string();
    }
    let ratio = bytes_done as f64 / total as f64;
    let filled = (ratio * 20.0).round() as usize;
    let bar = format!(
        "[{}{}] {:.0}%",
        "#".repeat(filled),
        ".".repeat(20 - filled),
        ratio * 100.0,
    );
    if ratio >= 1.0 {
        bar.if_supports_color(Stdout, |t| t.green()).to_string()
    } else {
        bar.if_supports_color(Stdout, |t| t.cyan()).to_string()
    }
}

// ---------------------------------------------------------------------------
// Generic
// ---------------------------------------------------------------------------

/// Bold section header, e.g. `[node]`.
pub fn section(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.bold()).to_string()
}

/// Cyan — used for addresses, paths and magnet links.
pub fn value(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.cyan()).to_string()
}

/// Green — success messages.
pub fn success(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.green()).to_string()
}

/// Red bold — error messages (stderr).
pub fn error(s: &str) -> String {
    s.if_supports_color(Stdout, |t| t.style(Style::new().bold().red()))
        .to_string()
}

/// Source count: green if >1, yellow if exactly 1.
pub fn sources(n: usize) -> String {
    let s = n.to_string();
    if n > 1 {
        s.if_supports_color(Stdout, |t| t.green()).to_string()
    } else {
        s.if_supports_color(Stdout, |t| t.yellow()).to_string()
    }
}
