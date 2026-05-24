//! `rucio shares`, `rucio add <path>`, `rucio remove <hash|path>`, `rucio magnet <target>`

use anyhow::{Result, bail};
use tabled::{Table, Tabled};

use rucio_core::api::shares::ShareResponse;

use crate::client::ApiClient;

pub async fn list(client: &ApiClient, filter: Option<&str>) -> Result<()> {
    let resp = client.list_shares().await?;

    let shares: Vec<ShareResponse> = match filter {
        Some(f) => {
            let f = f.to_lowercase();
            resp.shares
                .into_iter()
                .filter(|s| s.name.to_lowercase().contains(&f))
                .collect()
        }
        None => resp.shares,
    };

    if shares.is_empty() {
        if filter.is_some() {
            println!("No shares matching that filter.");
        } else {
            println!("No files shared.");
        }
        return Ok(());
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
        #[tabled(rename = "Chunks")]
        chunks: usize,
        #[tabled(rename = "Path")]
        path: String,
    }

    let rows: Vec<Row> = shares
        .iter()
        .enumerate()
        .map(|(i, s)| Row {
            idx: i + 1,
            hash: s.root_hash[..8].to_string(),
            name: s.name.clone(),
            size: human_size(s.size),
            chunks: s.chunk_count,
            path: s.path.clone(),
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

/// Print the magnet link for a file.
///
/// With `--file <path>`: hashes the file locally, no daemon required.
///
/// Otherwise `target` is resolved against local shares in order:
///   1. Row number from `rucio shares` (e.g. `3`)
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
        println!("{link}");
        return Ok(());
    }

    let target = target.ok_or_else(|| {
        anyhow::anyhow!("Provide a target (row number, name, or hash) or use --file <path>")
    })?;

    let shares = client.list_shares().await?.shares;

    // 1. Numeric row index.
    if let Ok(n) = target.trim().parse::<usize>() {
        match shares.get(n.wrapping_sub(1)) {
            Some(s) => {
                println!("{}", s.magnet);
                return Ok(());
            }
            None => bail!("No share at row {n}. Run `rucio shares` to see the list."),
        }
    }

    // 2. Exact name match.
    let by_name: Vec<&ShareResponse> = shares
        .iter()
        .filter(|s| s.name.eq_ignore_ascii_case(target))
        .collect();

    match by_name.len() {
        1 => {
            println!("{}", by_name[0].magnet);
            return Ok(());
        }
        n if n > 1 => {
            eprintln!("Ambiguous: {n} shares named '{target}'. Use a hash prefix instead:");
            for s in &by_name {
                eprintln!("  {}  {}", &s.root_hash[..8], s.name);
            }
            std::process::exit(1);
        }
        _ => {}
    }

    // 3. Hash prefix / full hash — delegate to the daemon endpoint.
    let link = client.get_share_magnet(target).await?;
    println!("{link}");
    Ok(())
}

pub async fn indexing(client: &ApiClient) -> Result<()> {
    let pending = client.indexing_pending().await?;
    if pending == 0 {
        println!("No files being indexed.");
    } else {
        println!("{pending} file(s) being indexed…");
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
