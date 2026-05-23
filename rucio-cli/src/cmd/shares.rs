//! `rucio shares`, `rucio add <path>`, `rucio remove <hash>`

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
        #[tabled(rename = "MIME")]
        mime: String,
    }

    let rows: Vec<Row> = resp
        .shares
        .into_iter()
        .map(|s| Row {
            hash: truncate(&s.root_hash, 16),
            name: s.name,
            size: human_size(s.size),
            chunks: s.chunk_count,
            mime: s.mime_type.unwrap_or_else(|| "-".to_string()),
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

pub async fn add(client: &ApiClient, path: &str) -> Result<()> {
    client.add_share(path).await?;
    println!("Share queued: {path}");
    Ok(())
}

pub async fn remove(client: &ApiClient, hash: &str) -> Result<()> {
    client.remove_share(hash).await?;
    println!("Removed share: {hash}");
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
