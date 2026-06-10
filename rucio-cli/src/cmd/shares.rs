//! `rucio share list`, `rucio share add <path>`, `rucio share remove <hash|path>`,
//! `rucio share magnet <target>`, `rucio share indexing`

use anyhow::{Result, bail};
use futures_util::StreamExt as _;
use rucio_core::api::shares::ShareResponse;
use rucio_core::api::ws::WsEvent;
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::color;
use crate::table_util::{fit_column, term_width};

// ANSI escape sequences for terminal control.
const CLEAR_LINE: &str = "\x1b[2K\r";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";

/// Default page size for `share list` (a terminal-friendly slice; the API caps
/// a single page at 1000).
const DEFAULT_PAGE: i64 = 50;

pub async fn list(
    client: &ApiClient,
    filter: Option<&str>,
    all: bool,
    page: Option<usize>,
    limit: Option<i64>,
) -> Result<()> {
    // `--all` pulls the whole (filtered) list; otherwise one page. Row numbers
    // are the GLOBAL position (start + i), so they stay consistent across pages
    // and match what `share magnet <n>` resolves against the full list.
    let (shares, start, total) = if all {
        let s = client.list_all_shares(filter).await?;
        let total = s.len() as u64;
        (s, 0i64, total)
    } else {
        let limit = limit.unwrap_or(DEFAULT_PAGE).clamp(1, 1000);
        let page = page.unwrap_or(1).max(1);
        let offset = (page as i64 - 1) * limit;
        let resp = client.list_shares_page(filter, limit, offset).await?;
        (resp.shares, offset, resp.total)
    };

    if shares.is_empty() {
        if total == 0 {
            match filter {
                Some(_) => println!("No shares matching that filter."),
                None => println!("No files shared."),
            }
        } else {
            println!("No files on this page ({total} total) — try a lower --page.");
        }
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "#")]
        idx: u64,
        #[tabled(rename = "Hash")]
        hash: String,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Size")]
        size: String,
        #[tabled(rename = "Chunks")]
        chunks: usize,
        #[tabled(rename = "Path")]
        path: String,
    }

    let rows: Vec<Row> = shares
        .iter()
        .enumerate()
        .map(|(i, s)| Row {
            idx: start as u64 + i as u64 + 1,
            hash: color::value(&s.root_hash[..8]),
            name: s.name.clone(),
            size: human_size(s.size),
            chunks: s.chunk_count,
            path: color::value(&s.path),
        })
        .collect();

    let tw = term_width();
    let max_name = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    let max_path = rows
        .iter()
        .map(|r| r.path.chars().count())
        .max()
        .unwrap_or(0);
    let mut table = Table::new(rows);
    fit_column(&mut table, 2, max_name, tw);
    fit_column(&mut table, 5, max_path, tw);
    println!("{table}");

    // Footer: a plain count when the whole set is on screen, otherwise the page
    // range plus a hint on how to see the rest.
    let shown = shares.len() as u64;
    let shown_to = start as u64 + shown;
    if all || shown == total {
        match filter {
            Some(f) => println!("{total} file(s) matching '{f}'"),
            None => println!("{total} file(s) shared"),
        }
    } else {
        let mut footer = format!("Showing {}–{} of {}", start as u64 + 1, shown_to, total);
        if shown_to < total {
            footer.push_str(" · use --page N or --all");
        }
        if let Some(f) = filter {
            footer.push_str(&format!(" · filter '{f}'"));
        }
        println!("{footer}");
    }
    Ok(())
}

pub async fn add(client: &ApiClient, path: &str) -> Result<()> {
    match client.add_share(path).await {
        Ok(resp) => {
            println!(
                "{}",
                color::success(&format!("Queued {} file(s) for indexing.", resp.queued))
            );
            if !resp.errors.is_empty() {
                println!("{} file(s) could not be read:", resp.errors.len());
                for e in &resp.errors {
                    println!("  {e}");
                }
            }
        }
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

/// Remove by hash (single file) or by path (file or directory tree).
pub async fn remove(client: &ApiClient, target: &str) -> Result<()> {
    // Heuristic: if it looks like a 64-char hex string it's a hash,
    // otherwise treat it as a filesystem path.
    if target.len() == 64 && target.chars().all(|c| c.is_ascii_hexdigit()) {
        client.remove_share(target).await?;
        println!("Removed share: {}", color::value(target));
    } else {
        let n = client.remove_shares_by_path(target).await?;
        match n {
            0 => println!("No shares found under: {}", color::value(target)),
            1 => println!("{}", color::success("Removed 1 share.")),
            n => println!("{}", color::success(&format!("Removed {n} shares."))),
        }
    }
    Ok(())
}

/// Print the magnet link for a file.
///
/// With `--file <path>`: hashes the file locally, no daemon required.
///
/// Otherwise `target` is resolved against local shares in order:
///   1. Row number from `rucio share list` (e.g. `3`)
///   2. Exact file name — if unique among all shares
///   3. Hash prefix / full hash
///
/// If a name matches multiple shares, the user is told to use the hash instead.
pub async fn magnet(client: &ApiClient, target: Option<&str>, file: Option<&str>) -> Result<()> {
    // --file mode: hash locally, no daemon needed.
    if let Some(path_str) = file {
        use rucio_core::protocol::chunk::Hash;
        use rucio_core::protocol::hashing::hash_file;
        use rucio_core::protocol::magnet::MagnetLink;
        use std::path::Path;

        let path = Path::new(path_str);
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path_str.to_string());

        let fh = hash_file(path)
            .map_err(|e| anyhow::anyhow!("Failed to hash '{}': {e}", path.display()))?;

        let link = MagnetLink {
            root_hash: Hash(fh.root_hash),
            name: Some(name),
            size: Some(fh.size),
            providers: vec![],
        };
        println!("{}", color::value(&link.to_string()));
        return Ok(());
    }

    let target = target.ok_or_else(|| {
        anyhow::anyhow!("Provide a target (row number, name, or hash) or use --file <path>")
    })?;

    // Full list so row numbers and name/hash lookups match `share list`
    // regardless of library size (the endpoint pages at 1000 rows).
    let shares = client.list_all_shares(None).await?;

    // 1. Numeric row index.
    if let Ok(n) = target.trim().parse::<usize>() {
        match shares.get(n.wrapping_sub(1)) {
            Some(s) => {
                println!("{}", color::value(&s.magnet));
                return Ok(());
            }
            None => bail!("No share at row {n}. Run `rucio share list` to see the list."),
        }
    }

    // 2. Exact name match.
    let by_name: Vec<&ShareResponse> = shares
        .iter()
        .filter(|s| s.name.eq_ignore_ascii_case(target))
        .collect();

    match by_name.len() {
        1 => {
            println!("{}", color::value(&by_name[0].magnet));
            return Ok(());
        }
        n if n > 1 => {
            eprintln!("Ambiguous: {n} shares named '{target}'. Use a hash prefix instead:");
            for s in &by_name {
                eprintln!("  {}  {}", color::value(&s.root_hash[..8]), s.name);
            }
            std::process::exit(1);
        }
        _ => {}
    }

    // 3. Hash prefix / full hash — delegate to the daemon endpoint.
    let link = client.get_share_magnet(target).await?;
    println!("{}", color::value(&link));
    Ok(())
}

pub async fn indexing(client: &ApiClient, watch: bool) -> Result<()> {
    let pending = client.indexing_pending().await?;

    if !watch {
        if pending == 0 {
            println!("No files being indexed.");
        } else {
            println!(
                "{} file(s) being indexed…",
                color::value(&pending.to_string())
            );
        }
        return Ok(());
    }

    // --watch mode ----------------------------------------------------------
    if pending == 0 {
        println!("No files being indexed.");
        return Ok(());
    }

    print!("{HIDE_CURSOR}");
    let result = tokio::select! {
        r = indexing_watch_loop(client, pending) => r,
        _ = tokio::signal::ctrl_c() => {
            println!();
            Ok(())
        }
    };
    print!("{SHOW_CURSOR}");
    result
}

/// WS-first watch loop; falls back to HTTP polling if the WebSocket is
/// unavailable.
async fn indexing_watch_loop(client: &ApiClient, initial: usize) -> Result<()> {
    match client.ws_stream().await {
        Ok(stream) => indexing_watch_ws(stream, initial).await,
        Err(e) => {
            tracing::debug!("WebSocket unavailable ({e}), falling back to HTTP polling");
            indexing_watch_http(client, initial).await
        }
    }
}

async fn indexing_watch_ws(mut stream: crate::client::WsStream, initial: usize) -> Result<()> {
    let mut pending = initial;
    print_indexing_line(pending);

    loop {
        match stream.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                let event: WsEvent = match serde_json::from_str(&text) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if let WsEvent::IndexingCount { pending: p } = event {
                    pending = p;
                    print_indexing_line(pending);
                    if pending == 0 {
                        println!("\n{}", color::success("Indexing complete."));
                        return Ok(());
                    }
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                println!("\nWebSocket error: {e}");
                return Ok(());
            }
            None => {
                println!("\nDaemon disconnected.");
                return Ok(());
            }
        }
    }
}

async fn indexing_watch_http(client: &ApiClient, initial: usize) -> Result<()> {
    use tokio::time::{Duration, interval};

    let mut pending = initial;
    let mut ticker = interval(Duration::from_secs(1));
    print_indexing_line(pending);

    loop {
        ticker.tick().await;

        pending = match client.indexing_pending().await {
            Ok(n) => n,
            Err(e) => {
                println!("\nError contacting daemon: {e}");
                return Ok(());
            }
        };
        print_indexing_line(pending);
        if pending == 0 {
            println!("\n{}", color::success("Indexing complete."));
            return Ok(());
        }
    }
}

/// Overwrite the current terminal line with the latest count.
fn print_indexing_line(pending: usize) {
    if pending == 0 {
        // Caller prints the final message; just clear the spinner line.
        print!("{CLEAR_LINE}");
    } else {
        print!(
            "{CLEAR_LINE}Indexing: {} file(s) pending…",
            color::value(&pending.to_string())
        );
    }
    // Flush stdout so the partial line is visible immediately.
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut val = bytes as f64;
    let mut unit = UNITS[0];
    for u in &UNITS[1..] {
        if val < 1024.0 {
            break;
        }
        val /= 1024.0;
        unit = u;
    }
    if val < 10.0 {
        format!("{val:.1} {unit}")
    } else {
        format!("{val:.0} {unit}")
    }
}
