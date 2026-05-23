//! `rucio downloads`, `rucio get <target>`, `rucio cancel <hash>`

use anyhow::{Result, bail};
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::state::LastSearch;

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_downloads().await?;

    if resp.downloads.is_empty() {
        println!("No downloads.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Hash")]
        hash: String,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Size")]
        size: String,
        #[tabled(rename = "Done")]
        done: String,
        #[tabled(rename = "State")]
        state: String,
    }

    let rows: Vec<Row> = resp
        .downloads
        .into_iter()
        .map(|d| {
            let total = d.size.unwrap_or(0);
            let pct = if total > 0 {
                format!("{:.0}%", d.bytes_done as f64 / total as f64 * 100.0)
            } else {
                "-".to_string()
            };
            Row {
                hash: truncate(&d.root_hash, 16),
                name: d.name.unwrap_or_else(|| "-".to_string()),
                size: d.size.map(human_size).unwrap_or_else(|| "-".to_string()),
                done: pct,
                state: format!("{:?}", d.state),
            }
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

/// Start a download.
///
/// `target` is either:
///   - a 1-based integer index into the last search results, or
///   - a full `rucio:<hash>...` magnet link (requires `--provider`)
pub async fn start(client: &ApiClient, target: &str, provider: Option<&str>) -> Result<()> {
    let (magnet, resolved_provider) = if let Ok(idx) = target.trim().parse::<usize>() {
        // Numeric index — look up in last search state.
        let state = LastSearch::load();
        let entry = state.get(idx).ok_or_else(|| {
            anyhow::anyhow!("No result #{idx} in last search. Run `rucio search` first.")
        })?;
        (entry.magnet.clone(), entry.provider.clone())
    } else {
        // Treat as a raw magnet link.
        let p = provider.ok_or_else(|| {
            anyhow::anyhow!("--provider <PeerId> is required when passing a magnet link directly")
        })?;
        (target.to_string(), p.to_string())
    };

    client
        .start_download(&magnet, Some(&resolved_provider))
        .await?;
    println!("Download queued.");
    Ok(())
}

pub async fn cancel(client: &ApiClient, hash: &str) -> Result<()> {
    let dl = client.find_download_by_hash(hash).await?;
    match dl {
        None => bail!("No download found with hash {hash}"),
        Some(d) => {
            // The cancel endpoint uses numeric ID; we store the hash as root_hash.
            // Since the API only exposes cancel-by-id, we need the id — but our
            // current DownloadResponse doesn't carry it yet.
            // For now use a best-effort approach: pass the hash to the daemon
            // which will look it up. TODO: add id field to DownloadResponse.
            println!("Cancelling download: {}", d.root_hash);
            // Temporary: show a not-yet-implemented note
            println!("(cancel by hash not yet wired — use the numeric id)");
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
