//! `rucio search <keywords...>`
//!
//! Starts an async search, then polls every second until the daemon reports
//! `pending = false` or a timeout is reached.
//!
//! Results are deduplicated by root_hash so that the same file offered by
//! multiple peers is shown as a single row with a "Sources" count.  All
//! provider PeerIds are saved in `~/.local/share/rucio/last_search.json`
//! so that `rucio get <N>` can start a multi-source download automatically.

use std::collections::HashMap;

use anyhow::Result;
use tabled::{Table, Tabled};
use tokio::time::{Duration, sleep};

use crate::client::ApiClient;
use crate::state::{CachedResult, LastSearch};

const POLL_INTERVAL_MS: u64 = 1_000;
const MAX_POLLS: u32 = 30; // 30 seconds max

pub async fn search(client: &ApiClient, keywords: Vec<String>) -> Result<()> {
    if keywords.is_empty() {
        anyhow::bail!("Provide at least one keyword.");
    }

    println!("Searching for: {}", keywords.join(" "));

    let started = client.start_search(keywords).await?;
    let query_id = started.query_id;
    println!("Query ID: {query_id}");

    for attempt in 0..MAX_POLLS {
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let resp = client.poll_search(&query_id).await?;

        if !resp.results.is_empty() || !resp.pending {
            // Deduplicate by root_hash: group all providers for the same file.
            let mut grouped: Vec<CachedResult> = Vec::new();
            let mut hash_to_idx: HashMap<String, usize> = HashMap::new();

            for r in &resp.results {
                if let Some(&idx) = hash_to_idx.get(&r.root_hash) {
                    grouped[idx].providers.push(r.provider.clone());
                } else {
                    let idx = grouped.len();
                    hash_to_idx.insert(r.root_hash.clone(), idx);
                    grouped.push(CachedResult {
                        name: r.name.clone(),
                        size: r.size,
                        magnet: r.magnet.clone(),
                        providers: vec![r.provider.clone()],
                    });
                }
            }

            let state = LastSearch { results: grouped };
            if let Err(e) = state.save() {
                eprintln!("Warning: could not save search state: {e}");
            }

            print_results(&state.results);
            if resp.pending {
                println!("(search still in progress — showing results so far)");
            }
            return Ok(());
        }

        if attempt % 5 == 4 {
            println!("Still searching… ({}/{}s)", attempt + 1, MAX_POLLS);
        }
    }

    println!("Search timed out with no results.");
    Ok(())
}

fn print_results(results: &[CachedResult]) {
    if results.is_empty() {
        println!("No results found.");
        return;
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "#")]
        idx: usize,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Size")]
        size: String,
        #[tabled(rename = "Sources")]
        sources: usize,
    }

    let rows: Vec<Row> = results
        .iter()
        .enumerate()
        .map(|(i, r)| Row {
            idx: i + 1,
            name: r.name.clone(),
            size: human_size(r.size),
            sources: r.providers.len(),
        })
        .collect();

    println!("{}", Table::new(rows));
    println!("Use `rucio get <#>` to download.");
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
