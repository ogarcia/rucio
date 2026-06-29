//! `rucio share list`, `rucio share dirs`, `rucio share add <path>`,
//! `rucio share remove <#|path>`, `rucio share magnet <target>`,
//! `rucio share ed2k <target>`, `rucio share indexing`

use anyhow::{Result, bail};
use futures_util::StreamExt as _;
use rucio_core::api::shares::ShareResponse;
use rucio_core::api::ws::WsEvent;
use rust_i18n::t;
use tabled::builder::Builder;

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
                Some(_) => println!("{}", t!("share.none_filter")),
                None => println!("{}", t!("share.none")),
            }
        } else {
            println!("{}", t!("share.none_page", total = total));
        }
        return Ok(());
    }

    let rows: Vec<[String; 6]> = shares
        .iter()
        .enumerate()
        .map(|(i, s)| {
            [
                (start as u64 + i as u64 + 1).to_string(),
                color::value(&s.root_hash[..8]),
                s.name.clone(),
                human_size(s.size),
                s.chunk_count.to_string(),
                color::value(&s.path),
            ]
        })
        .collect();

    let tw = term_width();
    let max_name = rows.iter().map(|r| r[2].chars().count()).max().unwrap_or(0);
    let max_path = rows.iter().map(|r| r[5].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("share.col.num").to_string(),
        t!("share.col.hash").to_string(),
        t!("share.col.name").to_string(),
        t!("share.col.size").to_string(),
        t!("share.col.chunks").to_string(),
        t!("share.col.path").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    fit_column(&mut table, 2, max_name, tw);
    fit_column(&mut table, 5, max_path, tw);
    println!("{table}");

    // Footer: a plain count when the whole set is on screen, otherwise the page
    // range plus a hint on how to see the rest.
    let shown = shares.len() as u64;
    let shown_to = start as u64 + shown;
    if all || shown == total {
        match filter {
            Some(f) => println!("{}", t!("share.matching", total = total, filter = f)),
            None => println!("{}", t!("share.total_shared", total = total)),
        }
    } else {
        let mut footer = t!(
            "share.showing",
            from = start as u64 + 1,
            to = shown_to,
            total = total
        )
        .to_string();
        if shown_to < total {
            footer.push_str(&t!("share.showing_more"));
        }
        if let Some(f) = filter {
            footer.push_str(&t!("share.showing_filter", filter = f));
        }
        println!("{footer}");
    }
    Ok(())
}

/// List the directories being shared (the watched set), with how many files
/// each contains and their total size. Protected dirs (the download/pin dirs,
/// any category dir, and config-declared `shared_dirs`) cannot be removed with
/// `share remove`.
pub async fn dirs(client: &ApiClient) -> Result<()> {
    let resp = client.list_shared_dirs().await?;
    if resp.dirs.is_empty() {
        println!("{}", t!("share.no_dirs"));
        return Ok(());
    }

    let rows: Vec<[String; 5]> = resp
        .dirs
        .iter()
        .enumerate()
        .map(|(i, d)| {
            [
                (i + 1).to_string(),
                color::value(&d.path),
                d.file_count.to_string(),
                human_size(d.total_size),
                if d.protected {
                    t!("share.yes").to_string()
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect();

    let tw = term_width();
    let max_path = rows.iter().map(|r| r[1].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("share.col.num").to_string(),
        t!("share.col.directory").to_string(),
        t!("share.col.files").to_string(),
        t!("share.col.size").to_string(),
        t!("share.col.protected").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    fit_column(&mut table, 1, max_path, tw);
    println!("{table}");
    let footer = if resp.dirs.len() == 1 {
        t!("share.dirs_footer_one")
    } else {
        t!("share.dirs_footer_many", n = resp.dirs.len())
    };
    println!("{footer}");
    Ok(())
}

pub async fn add(client: &ApiClient, path: &str) -> Result<()> {
    match client.add_share(path).await {
        Ok(resp) => {
            println!(
                "{}",
                color::success(&t!("share.queued_indexing", n = resp.queued))
            );
            if !resp.errors.is_empty() {
                println!("{}", t!("share.read_errors", n = resp.errors.len()));
                for e in &resp.errors {
                    println!("  {e}");
                }
            }
        }
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}

/// Stop sharing a directory, given its `share dirs` number or its path.
pub async fn remove(client: &ApiClient, target: &str) -> Result<()> {
    let target = target.trim();

    // A bare number is a directory index from `share dirs`: resolve it to the
    // directory's path (and refuse a protected one before bothering the daemon).
    if let Ok(n) = target.parse::<usize>() {
        let dirs = client.list_shared_dirs().await?.dirs;
        let dir = dirs
            .get(n.wrapping_sub(1))
            .ok_or_else(|| anyhow::anyhow!(t!("share.no_dir_n", n = n)))?;
        if dir.protected {
            bail!(t!("share.protected_n", n = n, path = dir.path));
        }
        let removed = client.remove_shares_by_path(&dir.path).await?;
        println!(
            "{}",
            color::success(&t!("share.stopped_dir", path = dir.path, removed = removed))
        );
        return Ok(());
    }

    // Otherwise treat it as a directory path. Removing a single file is not a
    // real operation — its directory stays watched and the file is re-indexed —
    // so only whole directories can be un-shared.
    let n = client.remove_shares_by_path(target).await?;
    match n {
        0 => println!("{}", t!("share.no_dir_at", path = color::value(target))),
        1 => println!("{}", color::success(&t!("share.stopped_one"))),
        n => println!("{}", color::success(&t!("share.stopped_many", n = n))),
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

        let fh = hash_file(path).map_err(|e| {
            anyhow::anyhow!(t!("share.hash_failed", path = path.display(), msg = e))
        })?;

        let link = MagnetLink {
            root_hash: Hash(fh.root_hash),
            name: Some(name),
            size: Some(fh.size),
            providers: vec![],
        };
        println!("{}", color::value(&link.to_string()));
        return Ok(());
    }

    let target = target.ok_or_else(|| anyhow::anyhow!(t!("share.magnet_need_target")))?;

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
            None => bail!(t!("share.no_share_row", n = n)),
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
            eprintln!("{}", t!("share.ambiguous", n = n, target = target));
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

/// Print the eMule (`ed2k://`) link for a shared file.
///
/// `target` is resolved against local shares exactly like `share magnet`
/// (row number, unique name, or hash prefix). The link only exists once the
/// file has been hashed for eMule — we then seed it to Kad — so a file that
/// hasn't been hashed yet (or any file when eMule support is off) reports that
/// it isn't available rather than printing nothing.
pub async fn ed2k(client: &ApiClient, target: Option<&str>) -> Result<()> {
    let target = target.ok_or_else(|| anyhow::anyhow!(t!("share.ed2k_need_target")))?;

    // Full list so row numbers and name/hash lookups match `share list`.
    let shares = client.list_all_shares(None).await?;
    let share = resolve_share(&shares, target)?;

    match &share.ed2k {
        Some(link) => {
            println!("{}", color::value(link));
            Ok(())
        }
        None => bail!(t!("share.ed2k_unavailable", name = share.name.clone())),
    }
}

/// Resolve a target (row number, unique file name, or hash prefix) to a single
/// shared file. On an ambiguous name/hash it prints the candidates and exits.
fn resolve_share<'a>(shares: &'a [ShareResponse], target: &str) -> Result<&'a ShareResponse> {
    // 1. Numeric row index.
    if let Ok(n) = target.trim().parse::<usize>() {
        return shares
            .get(n.wrapping_sub(1))
            .ok_or_else(|| anyhow::anyhow!(t!("share.no_share_row", n = n)));
    }

    // 2. Exact name match.
    let by_name: Vec<&ShareResponse> = shares
        .iter()
        .filter(|s| s.name.eq_ignore_ascii_case(target))
        .collect();
    match by_name.len() {
        1 => return Ok(by_name[0]),
        n if n > 1 => {
            eprintln!("{}", t!("share.ambiguous", n = n, target = target));
            for s in &by_name {
                eprintln!("  {}  {}", color::value(&s.root_hash[..8]), s.name);
            }
            std::process::exit(1);
        }
        _ => {}
    }

    // 3. Hash prefix / full hash (root_hash is lowercase hex).
    let needle = target.to_ascii_lowercase();
    let by_hash: Vec<&ShareResponse> = shares
        .iter()
        .filter(|s| s.root_hash.starts_with(&needle))
        .collect();
    match by_hash.len() {
        1 => Ok(by_hash[0]),
        n if n > 1 => {
            eprintln!("{}", t!("share.ambiguous_hash", n = n, target = target));
            for s in &by_hash {
                eprintln!("  {}  {}", color::value(&s.root_hash[..8]), s.name);
            }
            std::process::exit(1);
        }
        _ => bail!(t!("share.no_share_match", target = target)),
    }
}

pub async fn indexing(client: &ApiClient, watch: bool) -> Result<()> {
    let pending = client.indexing_pending().await?;

    if !watch {
        if pending == 0 {
            println!("{}", t!("share.no_indexing"));
        } else {
            println!(
                "{}",
                t!("share.indexing_n", n = color::value(&pending.to_string()))
            );
        }
        return Ok(());
    }

    // --watch mode ----------------------------------------------------------
    if pending == 0 {
        println!("{}", t!("share.no_indexing"));
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
                        println!("\n{}", color::success(&t!("share.indexing_complete")));
                        return Ok(());
                    }
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                println!("\n{}", t!("common.ws_error", msg = e));
                return Ok(());
            }
            None => {
                println!("\n{}", t!("common.daemon_disconnected"));
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
                println!("\n{}", t!("common.daemon_contact_error", msg = e));
                return Ok(());
            }
        };
        print_indexing_line(pending);
        if pending == 0 {
            println!("\n{}", color::success(&t!("share.indexing_complete")));
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
            "{CLEAR_LINE}{}",
            t!(
                "share.indexing_line",
                n = color::value(&pending.to_string())
            )
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
