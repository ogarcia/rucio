//! `rucio search <keywords...>`
//!
//! Starts a unified search (Gossipsub + Kad2), then polls every second until:
//!   - the daemon reports the search as Done or Cancelled, or
//!   - the global timeout is reached (65 seconds — a bit beyond KAD2_TIMEOUT).
//!
//! Results are deduplicated by download link.  All entries are saved in
//! `~/.local/share/rucio/last_search.json` so that `rucio download get <N>` can
//! start a download automatically.

use std::collections::HashMap;

use anyhow::Result;
use rucio_core::api::searches::{ResultSource, SearchState};
use tabled::{Table, Tabled};
use tokio::time::{Duration, sleep};

use crate::client::ApiClient;
use crate::color;
use crate::state::{CachedResult, LastSearch};

const POLL_INTERVAL_MS: u64 = 1_000;
/// Hard timeout in seconds — slightly beyond Kad2's 60 s window.
const MAX_POLLS: u32 = 65;

pub async fn search(client: &ApiClient, keywords: Vec<String>) -> Result<()> {
    if keywords.is_empty() {
        anyhow::bail!("Provide at least one keyword.");
    }

    println!("Searching for: {}", color::value(&keywords.join(" ")));

    let started = client.start_search(keywords).await?;
    let search_id = started.id;
    println!("Search ID: {}", color::value(&search_id.to_string()));

    // Track accumulated results by download_link to deduplicate.
    let mut cached: Vec<CachedResult> = Vec::new();
    let mut link_to_idx: HashMap<String, usize> = HashMap::new();

    for attempt in 0..MAX_POLLS {
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let resp = match client.get_search(search_id).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Poll error: {e}");
                continue;
            }
        };

        // Merge new results.
        for r in &resp.results {
            let link = r.download_link.clone().unwrap_or_default();
            if let Some(&idx) = link_to_idx.get(&link) {
                // Already have this entry — add provider if present and new.
                if let Some(p) = &r.provider
                    && !cached[idx].providers.contains(p)
                {
                    cached[idx].providers.push(p.clone());
                }
            } else {
                let source_str = match r.source {
                    ResultSource::Rucio => "rucio",
                    ResultSource::Emule => "emule",
                };
                let idx = cached.len();
                link_to_idx.insert(link.clone(), idx);
                cached.push(CachedResult {
                    name: r.name.clone(),
                    size: r.size,
                    download_link: link,
                    providers: r.provider.clone().into_iter().collect(),
                    source: source_str.to_string(),
                });
            }
        }

        if attempt % 5 == 4 && matches!(resp.state, SearchState::Running) {
            println!(
                "Still searching… ({}/{}s, {} result(s) so far)",
                attempt + 1,
                MAX_POLLS,
                cached.len()
            );
        }

        // Exit when the daemon reports the search is finished.
        if !matches!(resp.state, SearchState::Running) {
            let reason = match resp.state {
                SearchState::Done => "done",
                SearchState::Cancelled => "cancelled",
                SearchState::Running => unreachable!(),
            };
            save_and_print(&cached, reason);
            return Ok(());
        }
    }

    if cached.is_empty() {
        println!("Search timed out with no results.");
    } else {
        save_and_print(&cached, "timeout");
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
        #[tabled(rename = "Source")]
        source: String,
        #[tabled(rename = "Providers")]
        providers: String,
    }

    let rows: Vec<Row> = results
        .iter()
        .enumerate()
        .map(|(i, r)| Row {
            idx: i + 1,
            name: r.name.clone(),
            size: human_size(r.size),
            source: r.source.clone(),
            providers: if r.providers.is_empty() {
                "-".to_string()
            } else {
                color::sources(r.providers.len())
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    println!("Use `rucio download get <#>` to download.");
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
