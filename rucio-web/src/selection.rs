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
    /// Pivot row for shift+click range selection — the last row clicked without
    /// shift. `None` until a row anchors the range.
    pub anchor: RwSignal<Option<Id>>,
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
        self.anchor.set(Some(id));
    }
}
