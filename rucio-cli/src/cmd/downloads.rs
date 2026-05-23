//! `rucio downloads`, `rucio get <target>`, `rucio cancel <hash>`

use anyhow::{Result, bail};
use tabled::{Table, Tabled};
use tokio::time::{Duration, interval};

use crate::client::ApiClient;
use crate::state::LastSearch;

// ANSI escape sequences for terminal control.
const CLEAR_SCREEN: &str = "\x1b[2J\x1b[H";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";

pub async fn list(client: &ApiClient, watch: bool) -> Result<()> {
    if !watch {
        let resp = client.list_downloads().await?;
        print_table(resp.downloads);
        return Ok(());
    }

    // Watch mode: refresh every second, exit when nothing is in-progress.
    print!("{HIDE_CURSOR}");
    let result = watch_loop(client).await;
    print!("{SHOW_CURSOR}");
    result
}

async fn watch_loop(client: &ApiClient) -> Result<()> {
    let mut ticker = interval(Duration::from_secs(1));

    loop {
        ticker.tick().await;

        let resp = match client.list_downloads().await {
            Ok(r) => r,
            Err(e) => {
                // Don't exit on a transient error — show it and retry.
                print!("{CLEAR_SCREEN}");
                println!("Error contacting daemon: {e}");
                println!("\nPress Ctrl-C to exit.");
                continue;
            }
        };

        print!("{CLEAR_SCREEN}");

        let all_done = resp.downloads.iter().all(|d| {
            matches!(
                d.state,
                rucio_core::api::downloads::DownloadState::Completed
                    | rucio_core::api::downloads::DownloadState::Failed
                    | rucio_core::api::downloads::DownloadState::Cancelled
            )
        });

        print_table(resp.downloads);

        if all_done {
            println!("\nAll downloads finished.");
            return Ok(());
        }

        println!("\nPress Ctrl-C to exit.");
    }
}

fn print_table(downloads: Vec<rucio_core::api::downloads::DownloadResponse>) {
    if downloads.is_empty() {
        println!("No downloads.");
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
            let (pct, bar) = if total > 0 {
                let ratio = d.bytes_done as f64 / total as f64;
                let filled = (ratio * 20.0).round() as usize;
                let bar = format!(
                    "[{}{}] {:.0}%",
                    "#".repeat(filled),
                    ".".repeat(20 - filled),
                    ratio * 100.0
                );
                (format!("{:.0}%", ratio * 100.0), bar)
            } else {
                ("-".to_string(), "[-                  ] -".to_string())
            };
            let _ = pct; // bar already contains the percentage
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

fn state_label(state: &rucio_core::api::downloads::DownloadState) -> String {
    use rucio_core::api::downloads::DownloadState;
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
///   - a full `rucio:<hash>...` magnet link (requires `--provider`)
pub async fn start(client: &ApiClient, target: &str, provider: Option<&str>) -> Result<()> {
    let (magnet, providers) = if let Ok(idx) = target.trim().parse::<usize>() {
        // Numeric index — look up in last search state.
        let state = LastSearch::load();
        let entry = state.get(idx).ok_or_else(|| {
            anyhow::anyhow!("No result #{idx} in last search. Run `rucio search` first.")
        })?;
        (entry.magnet.clone(), entry.providers.clone())
    } else {
        // Treat as a raw magnet link.
        let p = provider.ok_or_else(|| {
            anyhow::anyhow!("--provider <PeerId> is required when passing a magnet link directly")
        })?;
        (target.to_string(), vec![p.to_string()])
    };

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
