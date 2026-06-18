use tabled::Table;
use tabled::settings::object::Column;
use tabled::settings::{Modify, Width};

/// Width (in characters) of the longest label, used to align `label : value`
/// blocks. Computed at runtime so the columns line up whatever the active
/// language makes the labels.
pub fn label_width<'a>(labels: impl IntoIterator<Item = &'a str>) -> usize {
    labels
        .into_iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0)
}

/// Returns the current terminal width, or 120 if not a TTY / undetectable.
pub fn term_width() -> usize {
    terminal_size::terminal_size()
        .map(|(w, _)| w.0 as usize)
        .unwrap_or(120)
}

/// Truncates the given column (`col`, 0-based) of `table` so the rendered
/// table fits within `term_w` columns.  Appends `…` to truncated cells.
///
/// `max_content` is the display width of the longest value in that column
/// (used to compute how many characters to remove).  If the table already
/// fits, this is a no-op.
pub fn fit_column(table: &mut Table, col: usize, max_content: usize, term_w: usize) {
    let rendered_w = table
        .to_string()
        .lines()
        .next()
        .map(|l| l.len())
        .unwrap_or(0);
    if rendered_w <= term_w {
        return;
    }
    let overflow = rendered_w - term_w;
    // Keep at least 10 visible chars so the column stays readable.
    let new_max = max_content.saturating_sub(overflow).max(10);
    table.with(Modify::new(Column::from(col)).with(Width::truncate(new_max).suffix("…")));
}
