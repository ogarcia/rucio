//! Shared multi-selection state for the desktop list views.
//!
//! Downloads, shares and pins all offer the same desktop multi-selection: click
//! a row to select it, ctrl/⌘-click to toggle one, shift-click to select the
//! range from the anchor to the clicked row. This module holds that state and
//! the one `click` routine they share, generic over the row id type.

use std::collections::HashSet;
use std::hash::Hash;

use leptos::prelude::*;

/// Multi-selection over rows keyed by `Id`.
///
/// Both fields are reactive signals, so `Selection` is `Copy` and can be handed
/// to child components and event closures just like a plain signal.
pub struct Selection<Id>
where
    Id: Send + Sync + 'static,
{
    /// The currently selected row ids.
    pub selected: RwSignal<HashSet<Id>>,
    /// Pivot row for shift range selection — the last row chosen without shift.
    /// `None` until a row anchors the range.
    pub anchor: RwSignal<Option<Id>>,
    /// Active end of the selection: the row last clicked or moved to with the
    /// keyboard. Shift+Arrow grows or shrinks the range from the anchor to here.
    pub cursor: RwSignal<Option<Id>>,
}

/// How a cursor move updates the selection.
#[derive(Clone, Copy)]
pub enum CursorMode {
    /// Collapse the selection to the cursor row (plain Arrow).
    Collapse,
    /// Extend the contiguous range from the anchor to the cursor (Shift+Arrow).
    Extend,
    /// Move the cursor only, leaving the selection unchanged (Ctrl+Arrow).
    Focus,
}

impl<Id: Send + Sync + 'static> Clone for Selection<Id> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Id: Send + Sync + 'static> Copy for Selection<Id> {}

impl<Id> Default for Selection<Id>
where
    Id: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Id> Selection<Id>
where
    Id: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// A fresh, empty selection.
    pub fn new() -> Self {
        Self {
            selected: RwSignal::new(HashSet::new()),
            anchor: RwSignal::new(None),
            cursor: RwSignal::new(None),
        }
    }

    /// Apply a row click, given the ids currently visible in display order.
    ///
    /// - `range` (shift): select every row between the anchor and this one.
    /// - `additive` (ctrl/⌘, or a coarse-pointer tap): toggle this row.
    /// - neither: select only this row.
    ///
    /// A shift click with no usable anchor falls back to a plain select.
    pub fn click(&self, id: Id, additive: bool, range: bool, visible: &[Id]) {
        if range
            && let Some(a) = self.anchor.get_untracked()
            && let (Some(i1), Some(i2)) = (
                visible.iter().position(|x| *x == a),
                visible.iter().position(|x| *x == id),
            )
        {
            let (lo, hi) = if i1 <= i2 { (i1, i2) } else { (i2, i1) };
            self.selected
                .set(visible[lo..=hi].iter().cloned().collect());
            self.cursor.set(Some(id));
            return;
        }
        if additive {
            self.selected.update(|s| {
                if !s.insert(id.clone()) {
                    s.remove(&id);
                }
            });
        } else {
            self.selected.set(HashSet::from([id.clone()]));
        }
        self.anchor.set(Some(id.clone()));
        self.cursor.set(Some(id));
    }

    /// Move the keyboard cursor by `delta` rows over `visible`, applying `mode`:
    /// `Collapse` selects only the cursor row (and re-anchors), `Extend` selects
    /// the contiguous range from the anchor to the cursor, `Focus` moves the
    /// cursor and leaves the selection untouched. No-op on an empty list.
    pub fn move_cursor(&self, delta: i32, mode: CursorMode, visible: &[Id]) {
        if visible.is_empty() {
            return;
        }
        let len = visible.len() as i32;
        // Where the cursor sits now (tracked cursor, else the anchor).
        let here = self
            .cursor
            .get_untracked()
            .or_else(|| self.anchor.get_untracked())
            .and_then(|id| visible.iter().position(|x| *x == id).map(|i| i as i32));
        // First press with nothing focused steps in from the near edge.
        let next = match here {
            Some(i) => (i + delta).clamp(0, len - 1),
            None if delta >= 0 => 0,
            None => len - 1,
        } as usize;
        let target = visible[next].clone();
        match mode {
            // Move the cursor only; the selection and anchor stay put.
            CursorMode::Focus => {}
            CursorMode::Extend => {
                // Anchor the range if nothing anchors it yet, then select the
                // span from the anchor to the new cursor.
                let anchor_idx = match self
                    .anchor
                    .get_untracked()
                    .and_then(|a| visible.iter().position(|x| *x == a))
                {
                    Some(i) => i,
                    None => {
                        self.anchor.set(Some(target.clone()));
                        next
                    }
                };
                let (lo, hi) = if anchor_idx <= next {
                    (anchor_idx, next)
                } else {
                    (next, anchor_idx)
                };
                self.selected
                    .set(visible[lo..=hi].iter().cloned().collect());
            }
            CursorMode::Collapse => {
                self.selected.set(HashSet::from([target.clone()]));
                self.anchor.set(Some(target.clone()));
            }
        }
        self.cursor.set(Some(target));
    }

    /// Toggle the current cursor row in the selection (Space). The cursor row
    /// becomes the new anchor so a following Shift+Arrow extends from here.
    /// No-op when there is no cursor or it is not in `visible`.
    pub fn toggle_cursor(&self, visible: &[Id]) {
        let Some(cur) = self.cursor.get_untracked() else {
            return;
        };
        if !visible.contains(&cur) {
            return;
        }
        self.selected.update(|s| {
            if !s.insert(cur.clone()) {
                s.remove(&cur);
            }
        });
        self.anchor.set(Some(cur));
    }

    /// Handle a keydown on a list and scroll the resulting cursor row into view.
    /// ArrowUp/ArrowDown move the cursor — plain collapses, Shift extends the
    /// range, Ctrl/⌘ moves the focus only — and Space toggles the cursor row.
    /// Other keys are left untouched. `list` is the container whose rows carry
    /// `data-row-id="<row_value(id)>"`.
    pub fn handle_key<E>(
        &self,
        ev: &web_sys::KeyboardEvent,
        visible: &[Id],
        list: Option<E>,
        row_value: impl Fn(&Id) -> String,
    ) where
        E: AsRef<web_sys::Element>,
    {
        let key = ev.key();
        match key.as_str() {
            "ArrowDown" | "ArrowUp" => {
                let delta = if key == "ArrowDown" { 1 } else { -1 };
                let mode = if ev.shift_key() {
                    CursorMode::Extend
                } else if ev.ctrl_key() || ev.meta_key() {
                    CursorMode::Focus
                } else {
                    CursorMode::Collapse
                };
                ev.prevent_default();
                self.move_cursor(delta, mode, visible);
            }
            // Space toggles the cursor row (build a non-contiguous selection).
            " " | "Spacebar" => {
                ev.prevent_default();
                self.toggle_cursor(visible);
            }
            _ => return,
        }
        if let (Some(list), Some(cur)) = (list, self.cursor.get_untracked()) {
            // Escape the value for a `[data-row-id="…"]` attribute selector.
            let v = row_value(&cur).replace('\\', "\\\\").replace('"', "\\\"");
            if let Ok(Some(row)) = list
                .as_ref()
                .query_selector(&format!("[data-row-id=\"{v}\"]"))
            {
                let opts = web_sys::ScrollIntoViewOptions::new();
                opts.set_block(web_sys::ScrollLogicalPosition::Nearest);
                row.scroll_into_view_with_scroll_into_view_options(&opts);
            }
        }
    }
}
