//! `rucio search <keywords...>`
//!
//! Starts an async search, then polls every second until the daemon reports
//! `pending = false` or a timeout is reached.

use anyhow::Result;
use tabled::{Table, Tabled};
use tokio::time::{Duration, sleep};

use crate::client::ApiClient;

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
            print_results(&resp.results);
            if resp.pending {
                println!("(search still in progress — showing results so far)");
            }
            return Ok(());
        }

        // Print a simple progress indicator every 5 polls
        if attempt % 5 == 4 {
            println!("Still searching… ({}/{}s)", attempt + 1, MAX_POLLS);
        }
    }

    println!("Search timed out with no results.");
    Ok(())
}

fn print_results(results: &[rucio_core::api::search::SearchResultResponse]) {
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
        #[tabled(rename = "Provider")]
        provider: String,
    }

    let rows: Vec<Row> = results
        .iter()
        .enumerate()
        .map(|(i, r)| Row {
            idx: i + 1,
            name: r.name.clone(),
            size: human_size(r.size),
            provider: truncate(&r.provider, 24),
        })
        .collect();

    println!("{}", Table::new(rows));
    println!();

    // Print magnet + full provider for easy copy-paste
    for (i, r) in results.iter().enumerate() {
        println!(
            "[{}] rucio get '{}' --provider '{}'",
            i + 1,
            r.magnet,
            r.provider
        );
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
