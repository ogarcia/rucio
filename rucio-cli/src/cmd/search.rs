//! `rucio search <keywords...>`
//!
//! Starts an async search, then polls every second until:
//!   - the daemon closes the query (`pending = false`), or
//!   - no new results arrive for IDLE_POLLS consecutive polls (early exit), or
//!   - the global timeout is reached.
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
const MAX_POLLS: u32 = 30; // 30-second hard timeout
/// Exit after this many consecutive polls with no new results (early idle exit).
const IDLE_POLLS: u32 = 3;

pub async fn search(client: &ApiClient, keywords: Vec<String>) -> Result<()> {
    if keywords.is_empty() {
        anyhow::bail!("Provide at least one keyword.");
    }

    println!("Searching for: {}", keywords.join(" "));

    let started = client.start_search(keywords).await?;
    let query_id = started.query_id;
    println!("Query ID: {query_id}");

    // Accumulated deduplicated results across all polls.
    let mut grouped: Vec<CachedResult> = Vec::new();
    let mut hash_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idle_count: u32 = 0;

    for attempt in 0..MAX_POLLS {
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let resp = match client.poll_search(&query_id).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Poll error: {e}");
                continue;
            }
        };

        // Merge new results into grouped, tracking whether anything is new.
        let mut new_results = 0usize;
        for r in &resp.results {
            if let Some(&idx) = hash_to_idx.get(&r.root_hash) {
                // Already have this file; add provider if not already listed.
                if !grouped[idx].providers.contains(&r.provider) {
                    grouped[idx].providers.push(r.provider.clone());
                    new_results += 1;
                }
            } else {
                let idx = grouped.len();
                hash_to_idx.insert(r.root_hash.clone(), idx);
                grouped.push(CachedResult {
                    name: r.name.clone(),
                    size: r.size,
                    magnet: r.magnet.clone(),
                    providers: vec![r.provider.clone()],
                });
                new_results += 1;
            }
        }

        if new_results > 0 {
            idle_count = 0;
        } else if !grouped.is_empty() {
            // Results exist but nothing new arrived this tick.
            idle_count += 1;
        }

        if attempt % 5 == 4 && resp.pending && idle_count < IDLE_POLLS {
            println!(
                "Still searching… ({}/{}s, {} result(s) so far)",
                attempt + 1,
                MAX_POLLS,
                grouped.len()
            );
        }

        // Exit if the daemon closed the query or we've been idle long enough.
        if !resp.pending || idle_count >= IDLE_POLLS {
            let reason = if !resp.pending {
                "query closed"
            } else {
                "no new results"
            };
            save_and_print(&grouped, reason);
            return Ok(());
        }
    }

    if grouped.is_empty() {
        println!("Search timed out with no results.");
    } else {
        save_and_print(&grouped, "timeout");
    }
    Ok(())
}

fn save_and_print(results: &[CachedResult], _reason: &str) {
    let state = LastSearch {
        results: results.to_vec(),
    };
    if let Err(e) = state.save() {
        eprintln!("Warning: could not save search state: {e}");
    }
    print_results(results);
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
