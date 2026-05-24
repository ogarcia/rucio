//! `rucio downloads`, `rucio get <target>`, `rucio cancel <hash>`, `rucio clean`

use anyhow::{Result, bail};
use rucio_core::api::downloads::DownloadState;
use tabled::{Table, Tabled};
use tokio::time::{Duration, interval};

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
    let mut ticker = interval(Duration::from_secs(1));

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

        print!("{CLEAR_SCREEN}");

        // Exit when there are no more in-progress downloads.
        let any_active = resp.downloads.iter().any(|d| !is_finished(&d.state));

        let filtered = filter_downloads(resp.downloads, active, done);
        print_table(filtered, active, done);

        if !any_active {
            println!("\nAll downloads finished.");
            return Ok(());
        }

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
