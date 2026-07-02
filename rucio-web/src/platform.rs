//! Runtime detection of the input device, used to adapt touch interactions.
//!
//! The multi-selection lists (downloads, shares) build their selection from
//! keyboard modifiers: ctrl/⌘+click toggles a row, shift+click extends a range.
//! Touch devices have no modifiers, so a plain tap would only ever select a
//! single row. On a coarse pointer we therefore treat a plain tap as an additive
//! toggle, letting the user build a selection tap by tap; desktop behaviour is
//! left untouched. Range selection stays keyboard-only by nature.

use std::sync::OnceLock;

static COARSE: OnceLock<bool> = OnceLock::new();

/// Whether the primary pointer is coarse (a touchscreen), detected once via the
/// `(pointer: coarse)` media query. Defaults to `false` if the query is
/// unavailable, so ambiguous environments keep the desktop interaction model.
pub fn coarse_pointer() -> bool {
    *COARSE.get_or_init(|| {
        web_sys::window()
            .and_then(|w| w.match_media("(pointer: coarse)").ok().flatten())
            .map(|m| m.matches())
            .unwrap_or(false)
    })
}
