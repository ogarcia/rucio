//! `rucio download list`, `rucio download add <target>`, `rucio download show <X>`,
//! `rucio download cancel <idx|hash>`, `rucio download clean`

use anyhow::{Result, bail};
use futures_util::StreamExt as _;
use rucio_core::api::downloads::{DownloadResponse, DownloadState};
use rucio_core::api::ws::WsEvent;
use rust_i18n::t;
use tabled::builder::Builder;

use crate::client::ApiClient;
use crate::color;
use crate::state::LastSearch;
use crate::table_util::{fit_column, label_width, term_width};

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
                        println!("\n{}", color::success(&t!("download.all_finished")));
                        return Ok(());
                    }
                }
            }
            Some(Ok(_)) => {} // ping/pong/binary — ignore
            Some(Err(e)) => {
                print!("{CLEAR_SCREEN}");
                println!("{}", t!("common.ws_error", msg = e));
                println!("\n{}", t!("common.press_ctrl_c"));
            }
            None => {
                // Daemon closed the connection.
                println!("\n{}", t!("common.daemon_disconnected"));
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
                println!("{}", t!("common.daemon_contact_error", msg = e));
                println!("\n{}", t!("common.press_ctrl_c"));
                continue;
            }
        };

        let any_active = resp.downloads.iter().any(|d| !is_finished(&d.state));
        if any_active {
            ever_active = true;
        }

        render(&resp.downloads, active, done, ever_active);

        if ever_active && !any_active {
            println!("\n{}", color::success(&t!("download.all_finished")));
            return Ok(());
        }
    }
}

fn render(downloads: &[DownloadResponse], active: bool, done: bool, ever_active: bool) {
    print!("{CLEAR_SCREEN}");
    let filtered = filter_downloads(downloads.to_vec(), active, done);
    print_table(filtered, active, done);
    if !ever_active {
        println!("\n{}", t!("download.waiting"));
    } else {
        println!("\n{}", t!("common.press_ctrl_c"));
    }
}

fn print_table(
    downloads: Vec<rucio_core::api::downloads::DownloadResponse>,
    active: bool,
    done: bool,
) {
    if downloads.is_empty() {
        if active {
            println!("{}", t!("download.none_active"));
        } else if done {
            println!("{}", t!("download.none_done"));
        } else {
            println!("{}", t!("download.none"));
        }
        return;
    }

    let rows: Vec<[String; 6]> = downloads
        .into_iter()
        .enumerate()
        .map(|(i, d)| {
            let total = d.size.unwrap_or(0);
            [
                (i + 1).to_string(),
                truncate(&d.root_hash, 16),
                d.name.unwrap_or_else(|| "-".to_string()),
                d.size.map(human_size).unwrap_or_else(|| "-".to_string()),
                color::progress_bar(d.bytes_done, total),
                color::download_state(&d.state),
            ]
        })
        .collect();

    let max_name = rows.iter().map(|r| r[2].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("download.col.num").to_string(),
        t!("download.col.hash").to_string(),
        t!("download.col.name").to_string(),
        t!("download.col.size").to_string(),
        t!("download.col.progress").to_string(),
        t!("download.col.state").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
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
                eprintln!("{}", color::error(&t!("common.error", msg = e)));
                errors += 1;
            }
        }
    }
    if errors == 0 {
        println!("{}", color::success(&t!("download.queued_n", n = ok)));
    } else {
        println!(
            "{}",
            t!("download.queued_n_errors", ok = ok, errors = errors)
        );
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
        println!("{}", color::success(&t!("download.emule_queued")));
        return Ok(());
    }

    let (magnet, mut providers) = if let Ok(idx) = target.trim().parse::<usize>() {
        let state = LastSearch::load();
        let entry = state
            .get(idx)
            .ok_or_else(|| anyhow::anyhow!(t!("download.no_result_idx", idx = idx)))?;
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
    println!("{}", color::success(&t!("download.queued")));
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
        bail!(t!("download.no_download_for", target = target));
    };
    client.set_download_category(dl.id, category_id).await?;
    match category_id {
        Some(c) => println!("{}", color::success(&t!("download.moved_category", id = c))),
        None => println!("{}", color::success(&t!("download.cleared_category"))),
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

    let title = d
        .name
        .clone()
        .unwrap_or_else(|| t!("common.unknown").to_string());
    println!("{}", color::section(&title));

    // Detail labels carry their own colon; align values to the widest one.
    let l_id = t!("download.show.id");
    let l_hash = t!("download.show.hash");
    let l_state = t!("download.show.state");
    let l_category = t!("download.show.category");
    let l_size = t!("download.show.size");
    let l_downloaded = t!("download.show.downloaded");
    let l_progress = t!("download.show.progress");
    let l_chunks = t!("download.show.chunks");
    let l_slices = t!("download.show.slices");
    let l_sources = t!("download.show.sources");
    let l_in_flight = t!("download.show.in_flight");
    let l_queued = t!("download.show.queued");
    let l_speed = t!("download.show.speed");
    let l_eta = t!("download.show.eta");
    let l_saved_to = t!("download.show.saved_to");
    let l_ed2k = t!("download.show.ed2k_link");
    let l_magnet = t!("download.show.magnet");
    let l_added = t!("download.show.added");
    let l_updated = t!("download.show.updated");
    let l_error = t!("download.show.error");
    let w = label_width([
        l_id.as_ref(),
        l_hash.as_ref(),
        l_state.as_ref(),
        l_category.as_ref(),
        l_size.as_ref(),
        l_downloaded.as_ref(),
        l_progress.as_ref(),
        l_chunks.as_ref(),
        l_slices.as_ref(),
        l_sources.as_ref(),
        l_in_flight.as_ref(),
        l_queued.as_ref(),
        l_speed.as_ref(),
        l_eta.as_ref(),
        l_saved_to.as_ref(),
        l_ed2k.as_ref(),
        l_magnet.as_ref(),
        l_added.as_ref(),
        l_updated.as_ref(),
        l_error.as_ref(),
    ]);

    println!(
        "  {l_id:<w$} {}",
        t!("download.show.id_val", id = d.id, kind = d.kind)
    );
    println!("  {l_hash:<w$} {}", color::value(&d.root_hash));
    println!("  {l_state:<w$} {}", color::download_state(&d.state));
    // Resolve the category name (falls back to "#id" if it was since deleted).
    if let Some(cid) = d.category_id {
        let name = client
            .list_categories()
            .await
            .ok()
            .and_then(|r| r.categories.into_iter().find(|c| c.id == cid))
            .map(|c| c.name)
            .unwrap_or_else(|| format!("#{cid}"));
        println!("  {l_category:<w$} {}", color::value(&name));
    }
    println!(
        "  {l_size:<w$} {}",
        d.size.map(human_size).unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  {l_downloaded:<w$} {}",
        t!(
            "download.show.downloaded_val",
            size = human_size(d.bytes_done),
            pct = pct
        )
    );
    println!(
        "  {l_progress:<w$} {}",
        color::progress_bar(d.bytes_done, total)
    );
    if let (Some(done), Some(total)) = (d.pieces_done, d.pieces_total) {
        let label = if d.kind == "emule" {
            &l_slices
        } else {
            &l_chunks
        };
        println!(
            "  {label:<w$} {}",
            t!("download.show.pieces_val", done = done, total = total)
        );
    }
    // Live stats — present only while the download is active.
    if let Some(total) = d.sources_total {
        let active = d.sources_active.unwrap_or(0);
        println!(
            "  {l_sources:<w$} {}",
            t!("download.show.sources_val", active = active, total = total)
        );
    }
    if let Some(n) = d.pieces_in_flight {
        println!("  {l_in_flight:<w$} {n}");
    }
    if let Some(n) = d.queued_sources {
        let val = match d.best_queue_rank {
            Some(r) => t!("download.show.queued_val_rank", n = n, rank = r),
            None => t!("download.show.queued_val", n = n),
        };
        println!("  {l_queued:<w$} {val}");
    }
    if let Some(bps) = d.speed_bps.filter(|&b| b > 0) {
        println!("  {l_speed:<w$} {}/s", human_size(bps));
    }
    if let Some(eta) = d.eta_secs {
        println!("  {l_eta:<w$} {}", human_duration(eta));
    }
    // Per-peer sources (libp2p; empty for eMule for now).
    if !d.peers.is_empty() {
        println!("  {}", t!("download.show.downloading_from"));
        for p in &d.peers {
            let who = p
                .address
                .clone()
                .unwrap_or_else(|| truncate(&p.peer_id, 20));
            let rate = if p.rate_bps > 0 {
                format!("{}/s", human_size(p.rate_bps))
            } else {
                t!("download.show.idle").to_string()
            };
            println!(
                "    {}",
                t!(
                    "download.show.peer_line",
                    rate = format!("{rate:>11}"),
                    who = who,
                    downloaded = human_size(p.bytes_downloaded),
                    in_flight = p.chunks_in_flight
                )
            );
        }
    }
    if let Some(path) = &d.dest_path {
        println!("  {l_saved_to:<w$} {}", color::value(path));
    }
    if let Some(link) = &d.link {
        if d.kind == "emule" {
            println!("  {l_ed2k:<w$} {}", color::value(link));
        } else {
            println!("  {l_magnet:<w$} {}", color::value(link));
        }
    }
    println!("  {l_added:<w$} {}", human_time_ago(d.added_at));
    println!("  {l_updated:<w$} {}", human_time_ago(d.updated_at));
    if let Some(err) = &d.error {
        println!("  {l_error:<w$} {}", color::error(err));
    }

    Ok(())
}

pub async fn cancel(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!(t!("download.no_download_for", target = hash)),
        Some(d) => {
            client.cancel_download(d.id).await?;
            println!(
                "{}",
                t!(
                    "download.cancelled_msg",
                    name = d.name.unwrap_or_else(|| "-".to_string()),
                    hash = color::value(&d.root_hash)
                )
            );
            Ok(())
        }
    }
}

pub async fn pause(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!(t!("download.no_download_for", target = hash)),
        Some(d) => {
            client.pause_download(d.id).await?;
            println!(
                "{}",
                t!(
                    "download.paused_msg",
                    name = d.name.unwrap_or_else(|| "-".to_string()),
                    hash = color::value(&d.root_hash)
                )
            );
            Ok(())
        }
    }
}

pub async fn resume(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_idx_or_hash(hash).await?;
    match dl {
        None => bail!(t!("download.no_download_for", target = hash)),
        Some(d) => {
            client.resume_download(d.id).await?;
            println!(
                "{}",
                t!(
                    "download.resumed_msg",
                    name = d.name.unwrap_or_else(|| "-".to_string()),
                    hash = color::value(&d.root_hash)
                )
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
            None => bail!(t!("download.no_download_for", target = h)),
            Some(d) if !is_finished(&d.state) => {
                bail!(t!(
                    "download.still_active_clean",
                    name = d.name.unwrap_or_else(|| d.root_hash.clone())
                ))
            }
            Some(d) => {
                client.delete_download(d.id).await?;
                println!(
                    "{}",
                    t!(
                        "download.removed_msg",
                        name = d.name.unwrap_or_else(|| "-".to_string()),
                        hash = color::value(&d.root_hash[..16.min(d.root_hash.len())])
                    )
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
            println!("{}", t!("download.nothing_to_clean"));
            return Ok(());
        }

        let n = finished.len();
        for d in finished {
            if let Err(e) = client.delete_download(d.id).await {
                eprintln!(
                    "{}",
                    t!("download.remove_warning", hash = d.root_hash, msg = e)
                );
            }
        }
        println!("{}", color::success(&t!("download.removed_n", n = n)));
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
