//! `rucio download list`, `rucio download add <target>`, `rucio download show <X>`,
//! `rucio download cancel <idx|hash>`, `rucio download clean`

use anyhow::{Result, bail};
use futures_util::StreamExt as _;
use rucio_core::api::downloads::{DownloadResponse, DownloadState};
use rucio_core::api::ws::WsEvent;
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::color;
use crate::state::LastSearch;
use crate::table_util::{fit_column, term_width};

// ANSI escape sequences for terminal control.
const CLEAR_SCREEN: &str = "\x1b[2J\x1b[H";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";

fn is_finished(state: &DownloadState) -> bool {
    matches!(
        state,
        DownloadState::Completed | DownloadState::Failed | DownloadState::Cancelled
    )
}

pub async fn list(client: &ApiClient, watch: bool, active: bool, done: bool) -> Result<()> {
    if !watch {
        let resp = client.list_downloads().await?;
        let downloads = filter_downloads(resp.downloads, active, done);
        print_table(downloads, active, done);
        return Ok(());
    }

    // Watch mode: refresh every second, exit when nothing is in-progress.
    // We restore the cursor even when the user presses Ctrl-C by racing the
    // loop against a ctrl_c future.  Either way we reach the show-cursor line.
    print!("{HIDE_CURSOR}");
    let result = tokio::select! {
        r = watch_loop(client, active, done) => r,
        _ = tokio::signal::ctrl_c() => {
            // Ctrl-C: clear the "Press Ctrl-C" line and exit gracefully.
            println!();
            Ok(())
        }
    };
    print!("{SHOW_CURSOR}");
    result
}

fn filter_downloads(
    downloads: Vec<rucio_core::api::downloads::DownloadResponse>,
    active: bool,
    done: bool,
) -> Vec<rucio_core::api::downloads::DownloadResponse> {
    if active {
        downloads
            .into_iter()
            .filter(|d| !is_finished(&d.state))
            .collect()
    } else if done {
        downloads
            .into_iter()
            .filter(|d| is_finished(&d.state))
            .collect()
    } else {
        downloads
    }
}

async fn watch_loop(client: &ApiClient, active: bool, done: bool) -> Result<()> {
    let mut stream = match client.ws_stream().await {
        Ok(s) => s,
        Err(e) => {
            // Daemon may not support WebSocket yet or connection refused —
            // fall back to HTTP polling so the command still works.
            tracing::debug!("WebSocket unavailable ({e}), falling back to HTTP polling");
            return watch_loop_http(client, active, done).await;
        }
    };

    // Snapshot the current state immediately so the screen is not blank
    // while waiting for the first WS event.
    let initial = client
        .list_downloads()
        .await
        .unwrap_or_else(|_| rucio_core::api::downloads::DownloadsResponse { downloads: vec![] });
    let mut last_downloads: Vec<DownloadResponse> = initial.downloads;
    let mut ever_active = last_downloads.iter().any(|d| !is_finished(&d.state));

    render(&last_downloads, active, done, ever_active);

    loop {
        match stream.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                let event: WsEvent = match serde_json::from_str(&text) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if let WsEvent::DownloadProgress(downloads) = event {
                    // Merge: keep finished entries from last snapshot, replace
                    // active ones with fresh data from the event.
                    for fresh in &downloads {
                        if let Some(pos) = last_downloads
                            .iter()
                            .position(|d| d.root_hash == fresh.root_hash)
                        {
                            last_downloads[pos] = fresh.clone();
                        } else {
                            last_downloads.push(fresh.clone());
                        }
                    }
                    let any_active = last_downloads.iter().any(|d| !is_finished(&d.state));
                    if any_active {
                        ever_active = true;
                    }
                    render(&last_downloads, active, done, ever_active);
                    if ever_active && !any_active {
                        println!("\n{}", color::success("All downloads finished."));
                        return Ok(());
                    }
                }
            }
            Some(Ok(_)) => {} // ping/pong/binary — ignore
            Some(Err(e)) => {
                print!("{CLEAR_SCREEN}");
                println!("WebSocket error: {e}");
                println!("\nPress Ctrl-C to exit.");
            }
            None => {
                // Daemon closed the connection.
                println!("\nDaemon disconnected.");
                return Ok(());
            }
        }
    }
}

/// Fallback HTTP polling watch loop (identical logic to the old implementation).
async fn watch_loop_http(client: &ApiClient, active: bool, done: bool) -> Result<()> {
    use tokio::time::{Duration, interval};

    let mut ticker = interval(Duration::from_secs(1));
    let mut ever_active = false;

    loop {
        ticker.tick().await;

        let resp = match client.list_downloads().await {
            Ok(r) => r,
            Err(e) => {
                print!("{CLEAR_SCREEN}");
                println!("Error contacting daemon: {e}");
                println!("\nPress Ctrl-C to exit.");
                continue;
            }
        };

        let any_active = resp.downloads.iter().any(|d| !is_finished(&d.state));
        if any_active {
            ever_active = true;
        }

        render(&resp.downloads, active, done, ever_active);

        if ever_active && !any_active {
            println!("\n{}", color::success("All downloads finished."));
            return Ok(());
        }
    }
}

fn render(downloads: &[DownloadResponse], active: bool, done: bool, ever_active: bool) {
    print!("{CLEAR_SCREEN}");
    let filtered = filter_downloads(downloads.to_vec(), active, done);
    print_table(filtered, active, done);
    if !ever_active {
        println!("\nWaiting for downloads… Press Ctrl-C to exit.");
    } else {
        println!("\nPress Ctrl-C to exit.");
    }
}

fn print_table(
    downloads: Vec<rucio_core::api::downloads::DownloadResponse>,
    active: bool,
    done: bool,
) {
    if downloads.is_empty() {
        if active {
            println!("No active downloads.");
        } else if done {
            println!("No finished downloads.");
        } else {
            println!("No downloads.");
        }
        return;
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "#")]
        idx: usize,
        #[tabled(rename = "Hash")]
        hash: String,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Size")]
        size: String,
        #[tabled(rename = "Progress")]
        progress: String,
        #[tabled(rename = "State")]
        state: String,
    }

    let rows: Vec<Row> = downloads
        .into_iter()
        .enumerate()
        .map(|(i, d)| {
            let total = d.size.unwrap_or(0);
            Row {
                idx: i + 1,
                hash: truncate(&d.root_hash, 16),
                name: d.name.unwrap_or_else(|| "-".to_string()),
                size: d.size.map(human_size).unwrap_or_else(|| "-".to_string()),
                progress: color::progress_bar(d.bytes_done, total),
                state: color::download_state(&d.state),
            }
        })
        .collect();

    let max_name = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    let mut table = Table::new(rows);
    fit_column(&mut table, 2, max_name, term_width());
    println!("{table}");
}

/// Start a download.
///
/// `target` is:
///   - a 1-based integer index into the last search results, or
///   - a `rucio:<hash>` magnet link (optionally with name/size/provider params), or
///   - an `ed2k://|file|…|…|…|/` link to download from the eMule network, or
///   - a quoted string containing multiple `magnet:` / `ed2k://` links separated by
///     whitespace (each link is started individually).
///
/// `--provider` is optional — the DHT will find providers automatically.
pub async fn start(
    client: &ApiClient,
    target: &str,
    provider: Option<&str>,
    category_id: Option<i64>,
) -> Result<()> {
    let links = split_links(target);

    if links.len() == 1 {
        return start_single(client, links[0], provider, category_id).await;
    }

    // Multiple links: start each one, collecting errors.
    let mut ok = 0usize;
    let mut errors = 0usize;
    for link in &links {
        match start_single(client, link, provider, category_id).await {
            Ok(()) => ok += 1,
            Err(e) => {
                eprintln!("{}: {e}", color::error("Error"));
                errors += 1;
            }
        }
    }
    if errors == 0 {
        println!("{}", color::success(&format!("Queued {ok} download(s).")));
    } else {
        println!("Queued {ok} download(s), {errors} error(s).");
    }
    Ok(())
}

async fn start_single(
    client: &ApiClient,
    target: &str,
    provider: Option<&str>,
    category_id: Option<i64>,
) -> Result<()> {
    // Detect ed2k scheme first.
    if target.trim_start().starts_with("ed2k://") {
        client
            .start_ed2k_download(target.trim(), category_id)
            .await?;
        println!("{}", color::success("eMule download queued."));
        return Ok(());
    }

    let (magnet, mut providers) = if let Ok(idx) = target.trim().parse::<usize>() {
        let state = LastSearch::load();
        let entry = state.get(idx).ok_or_else(|| {
            anyhow::anyhow!("No result #{idx} in last search. Run `rucio search` first.")
        })?;
        (entry.download_link.clone(), entry.providers.clone())
    } else {
        (target.to_string(), vec![])
    };

    if let Some(p) = provider
        && !providers.contains(&p.to_string())
    {
        providers.push(p.to_string());
    }

    client
        .start_download(&magnet, providers, category_id)
        .await?;
    println!("{}", color::success("Download queued."));
    Ok(())
}

/// Split a string into individual `magnet:` / `ed2k://` links.
///
/// Finds every occurrence of a known link prefix, sorts them by position, and
/// returns the substring from each prefix to the start of the next one
/// (trailing whitespace trimmed).  If no known prefix is found the entire
/// trimmed input is returned as a single element (handles numeric indices and
/// bare hashes unchanged).
fn split_links(input: &str) -> Vec<&str> {
    const PREFIXES: &[&str] = &["magnet:", "ed2k://"];

    let mut positions: Vec<usize> = PREFIXES
        .iter()
        .flat_map(|prefix| {
            let mut found = vec![];
            let mut start = 0;
            while let Some(idx) = input[start..].find(prefix) {
                found.push(start + idx);
                start += idx + prefix.len();
            }
            found
        })
        .collect();

    if positions.is_empty() {
        return vec![input.trim()];
    }

    positions.sort_unstable();
    positions.dedup();

    let last = *positions.last().unwrap();
    positions
        .windows(2)
        .map(|w| input[w[0]..w[1]].trim())
        .chain(std::iter::once(input[last..].trim()))
        .collect()
}

/// Show full details for a single download identified by row number or hash.
/// Move a download to a category, or clear it when `category_id` is `None`.
pub async fn set_category(
    client: &ApiClient,
    target: &str,
    category_id: Option<i64>,
) -> Result<()> {
    let Some(dl) = client.find_download_by_idx_or_hash(target).await? else {
        bail!("No download found for '{target}'");
    };
    client.set_download_category(dl.id, category_id).await?;
    match category_id {
        Some(c) => println!(
            "{}",
            color::success(&format!("Moved download to category {c}."))
        ),
        None => println!("{}", color::success("Cleared the download's category.")),
    }
    Ok(())
}

pub async fn show(client: &ApiClient, target: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(target).await?;
    let Some(dl) = dl else {
        bail!("No download found for '{target}'");
    };
    let d = client.get_download(dl.id).await?;

    let total = d.size.unwrap_or(0);
    let pct = if total > 0 {
        (d.bytes_done as f64 / total as f64 * 100.0).round() as u64
    } else {
        0
    };

    println!(
        "{}",
        color::section(d.name.as_deref().unwrap_or("(unknown)"))
    );
    println!("  ID:         {} ({})", d.id, d.kind);
    println!("  Hash:       {}", color::value(&d.root_hash));
    println!("  State:      {}", color::download_state(&d.state));
    // Resolve the category name (falls back to "#id" if it was since deleted).
    if let Some(cid) = d.category_id {
        let name = client
            .list_categories()
            .await
            .ok()
            .and_then(|r| r.categories.into_iter().find(|c| c.id == cid))
            .map(|c| c.name)
            .unwrap_or_else(|| format!("#{cid}"));
        println!("  Category:   {}", color::value(&name));
    }
    println!(
        "  Size:       {}",
        d.size.map(human_size).unwrap_or_else(|| "-".to_string())
    );
    println!("  Downloaded: {} ({pct}%)", human_size(d.bytes_done));
    println!("  Progress:   {}", color::progress_bar(d.bytes_done, total));
    if let (Some(done), Some(total)) = (d.pieces_done, d.pieces_total) {
        let label = if d.kind == "emule" {
            "Slices"
        } else {
            "Chunks"
        };
        println!("  {label}:     {done} / {total}");
    }
    // Live stats — present only while the download is active.
    if let Some(total) = d.sources_total {
        let active = d.sources_active.unwrap_or(0);
        println!("  Sources:    {active} active / {total} known");
    }
    if let Some(n) = d.pieces_in_flight {
        println!("  In flight:  {n}");
    }
    if let Some(n) = d.queued_sources {
        match d.best_queue_rank {
            Some(r) => println!("  Queued:     {n} source(s), best rank {r}"),
            None => println!("  Queued:     {n} source(s)"),
        }
    }
    if let Some(bps) = d.speed_bps.filter(|&b| b > 0) {
        println!("  Speed:      {}/s", human_size(bps));
    }
    if let Some(eta) = d.eta_secs {
        println!("  ETA:        {}", human_duration(eta));
    }
    // Per-peer sources (libp2p; empty for eMule for now).
    if !d.peers.is_empty() {
        println!("  Downloading from:");
        for p in &d.peers {
            let who = p
                .address
                .clone()
                .unwrap_or_else(|| truncate(&p.peer_id, 20));
            let rate = if p.rate_bps > 0 {
                format!("{}/s", human_size(p.rate_bps))
            } else {
                "idle".to_string()
            };
            println!(
                "    {:>11}  {}  ({}, {} in flight)",
                rate,
                who,
                human_size(p.bytes_downloaded),
                p.chunks_in_flight,
            );
        }
    }
    if let Some(path) = &d.dest_path {
        println!("  Saved to:   {}", color::value(path));
    }
    if let Some(link) = &d.link {
        if d.kind == "emule" {
            println!("  ed2k link:  {}", color::value(link));
        } else {
            println!("  Magnet:     {}", color::value(link));
        }
    }
    println!("  Added:      {}", human_time_ago(d.added_at));
    println!("  Updated:    {}", human_time_ago(d.updated_at));
    if let Some(err) = &d.error {
        println!("  Error:      {}", color::error(err));
    }

    Ok(())
}

pub async fn cancel(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!("No download found for '{hash}'"),
        Some(d) => {
            client.cancel_download(d.id).await?;
            println!(
                "Cancelled: {} ({})",
                d.name.unwrap_or_else(|| "-".to_string()),
                color::value(&d.root_hash)
            );
            Ok(())
        }
    }
}

pub async fn pause(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!("No download found for '{hash}'"),
        Some(d) => {
            client.pause_download(d.id).await?;
            println!(
                "Paused: {} ({})",
                d.name.unwrap_or_else(|| "-".to_string()),
                color::value(&d.root_hash)
            );
            Ok(())
        }
    }
}

pub async fn resume(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!("No download found for '{hash}'"),
        Some(d) => {
            client.resume_download(d.id).await?;
            println!(
                "Resumed: {} ({})",
                d.name.unwrap_or_else(|| "-".to_string()),
                color::value(&d.root_hash)
            );
            Ok(())
        }
    }
}

/// Remove finished downloads from the history.
///
/// If `hash` is given, removes only the matching entry (completed, failed, or
/// cancelled).  Otherwise removes all finished downloads.
pub async fn clean(client: &ApiClient, hash: Option<&str>) -> Result<()> {
    if let Some(h) = hash {
        // Single entry — must be finished (not active).
        let dl = client.find_download_by_idx_or_hash(h).await?;
        match dl {
            None => bail!("No download found for '{h}'"),
            Some(d) if !is_finished(&d.state) => {
                bail!(
                    "Download '{}' is still active. Use `rucio download cancel` to stop it first.",
                    d.name.unwrap_or_else(|| d.root_hash.clone())
                )
            }
            Some(d) => {
                client.delete_download(d.id).await?;
                println!(
                    "Removed: {} ({})",
                    d.name.unwrap_or_else(|| "-".to_string()),
                    color::value(&d.root_hash[..16.min(d.root_hash.len())])
                );
            }
        }
    } else {
        // Bulk — remove all finished downloads.
        let resp = client.list_downloads().await?;
        let finished: Vec<_> = resp
            .downloads
            .into_iter()
            .filter(|d| is_finished(&d.state))
            .collect();

        if finished.is_empty() {
            println!("Nothing to clean.");
            return Ok(());
        }

        let n = finished.len();
        for d in finished {
            if let Err(e) = client.delete_download(d.id).await {
                eprintln!("Warning: could not remove {}: {e}", d.root_hash);
            }
        }
        println!(
            "{}",
            color::success(&format!("Removed {n} finished download(s)."))
        );
    }
    Ok(())
}

pub(crate) fn human_size(bytes: u64) -> String {
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

/// Format a duration in seconds as a coarse human string (e.g. "3m 20s").
fn human_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}

/// Format a Unix timestamp (seconds) as a coarse "… ago" string.
fn human_time_ago(unix_secs: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let secs = (now - unix_secs).max(0);
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m {}s ago", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m ago", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h ago", secs / 86400, (secs % 86400) / 3600)
    }
}

pub(crate) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
