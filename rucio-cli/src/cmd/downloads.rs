//! `rucio downloads`, `rucio get <target>`, `rucio cancel <hash>`, `rucio clean`

use anyhow::{Result, bail};
use futures_util::StreamExt as _;
use rucio_core::api::downloads::{DownloadResponse, DownloadState};
use rucio_core::api::ws::WsEvent;
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::state::LastSearch;

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
    print!("{HIDE_CURSOR}");
    let result = watch_loop(client, active, done).await;
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
                        println!("\nAll downloads finished.");
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
            println!("\nAll downloads finished.");
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
        .map(|d| {
            let total = d.size.unwrap_or(0);
            let bar = if total > 0 {
                let ratio = d.bytes_done as f64 / total as f64;
                let filled = (ratio * 20.0).round() as usize;
                format!(
                    "[{}{}] {:.0}%",
                    "#".repeat(filled),
                    ".".repeat(20 - filled),
                    ratio * 100.0
                )
            } else {
                "[-                  ] -".to_string()
            };
            Row {
                hash: truncate(&d.root_hash, 16),
                name: truncate(&d.name.unwrap_or_else(|| "-".to_string()), 32),
                size: d.size.map(human_size).unwrap_or_else(|| "-".to_string()),
                progress: bar,
                state: state_label(&d.state),
            }
        })
        .collect();

    println!("{}", Table::new(rows));
}

fn state_label(state: &DownloadState) -> String {
    match state {
        DownloadState::FindingProviders => "finding providers…".to_string(),
        DownloadState::Queued => "queued".to_string(),
        DownloadState::Downloading => "downloading".to_string(),
        DownloadState::Completed => "completed".to_string(),
        DownloadState::Failed => "failed".to_string(),
        DownloadState::Cancelled => "cancelled".to_string(),
    }
}

/// Start a download.
///
/// `target` is either:
///   - a 1-based integer index into the last search results, or
///   - a `rucio:<hash>` magnet link (optionally with name/size/provider params)
///
/// `--provider` is optional — the DHT will find providers automatically.
pub async fn start(client: &ApiClient, target: &str, provider: Option<&str>) -> Result<()> {
    let (magnet, mut providers) = if let Ok(idx) = target.trim().parse::<usize>() {
        let state = LastSearch::load();
        let entry = state.get(idx).ok_or_else(|| {
            anyhow::anyhow!("No result #{idx} in last search. Run `rucio search` first.")
        })?;
        (entry.magnet.clone(), entry.providers.clone())
    } else {
        (target.to_string(), vec![])
    };

    if let Some(p) = provider
        && !providers.contains(&p.to_string())
    {
        providers.push(p.to_string());
    }

    client.start_download(&magnet, providers).await?;
    println!("Download queued.");
    Ok(())
}

pub async fn cancel(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_hash(hash).await?;
    match dl {
        None => bail!("No download found with hash prefix '{hash}'"),
        Some(d) => {
            client.cancel_download(d.id).await?;
            println!(
                "Cancelled: {} ({})",
                d.name.unwrap_or_else(|| "-".to_string()),
                d.root_hash
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
        let dl = client.find_download_by_hash(h).await?;
        match dl {
            None => bail!("No download found with hash prefix '{h}'"),
            Some(d) if !is_finished(&d.state) => {
                bail!(
                    "Download '{}' is still active. Use `rucio cancel` to stop it first.",
                    d.name.unwrap_or_else(|| d.root_hash.clone())
                )
            }
            Some(d) => {
                client.delete_download(d.id).await?;
                println!(
                    "Removed: {} ({})",
                    d.name.unwrap_or_else(|| "-".to_string()),
                    &d.root_hash[..16.min(d.root_hash.len())]
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
        println!("Removed {n} finished download(s).");
    }
    Ok(())
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

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
