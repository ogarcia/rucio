//! `rucio shares`, `rucio add <path>`, `rucio remove <hash|path>`

use anyhow::Result;
use tabled::{Table, Tabled};

use crate::client::ApiClient;

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_shares().await?;

    if resp.shares.is_empty() {
        println!("No files shared.");
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
        #[tabled(rename = "Chunks")]
        chunks: usize,
        #[tabled(rename = "Path")]
        path: String,
    }

    let rows: Vec<Row> = resp
        .shares
        .into_iter()
        .map(|s| Row {
            hash: truncate(&s.root_hash, 16),
            name: s.name,
            size: human_size(s.size),
            chunks: s.chunk_count,
            path: s.path,
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

pub async fn add(client: &ApiClient, path: &str) -> Result<()> {
    match client.add_share(path).await {
        Ok(resp) => {
            println!("Queued {} file(s) for indexing.", resp.queued);
            if !resp.errors.is_empty() {
                println!("{} file(s) could not be read:", resp.errors.len());
                for e in &resp.errors {
                    println!("  {e}");
                }
            }
        }
        Err(e) => {
            // Surface the daemon's error message (e.g. "only directories can be shared")
            eprintln!("Error: {e}");
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
        println!("Removed share: {target}");
    } else {
        let n = client.remove_shares_by_path(target).await?;
        match n {
            0 => println!("No shares found under: {target}"),
            1 => println!("Removed 1 share."),
            n => println!("Removed {n} shares."),
        }
    }
    Ok(())
}

/// Print the magnet link for a locally shared file.
///
/// `hash` can be a full 64-char hex or an unambiguous prefix.
pub async fn magnet(client: &ApiClient, hash: &str) -> Result<()> {
    let link = client.get_share_magnet(hash).await?;
    println!("{link}");
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
