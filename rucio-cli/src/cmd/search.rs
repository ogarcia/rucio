//! `rucio search` subcommands.
//!
//! add <keywords...>    — start a search (prints ID, returns immediately; --wait to block)
//! list                 — list all searches in memory
//! show <id>            — show results (waits if still running)
//! cancel <id>          — cancel a running search
//! clean [<id>]         — remove done/cancelled searches from daemon memory
//! relaunch <id>        — relaunch a search (same ID, preserves results)

use std::collections::HashMap;

use anyhow::Result;
use rucio_core::api::searches::{ResultSource, SearchResult, SearchState};
use tabled::{Table, Tabled};
use tokio::time::{Duration, sleep};

use crate::client::ApiClient;
use crate::color;
use crate::state::{CachedResult, LastSearch};

const POLL_INTERVAL_MS: u64 = 1_000;
const MAX_POLLS: u32 = 65;

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

pub async fn add(client: &ApiClient, keywords: Vec<String>, wait: bool) -> Result<()> {
    if keywords.is_empty() {
        anyhow::bail!("Provide at least one keyword.");
    }

    let started = client.start_search(keywords.clone()).await?;
    let id = started.id;
    println!("{id}");

    if wait {
        println!("Searching for: {}", color::value(&keywords.join(" ")));
        poll_until_done(client, id).await
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_searches().await?;

    if resp.searches.is_empty() {
        println!("No searches in memory.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "ID")]
        id: u64,
        #[tabled(rename = "Keywords")]
        keywords: String,
        #[tabled(rename = "State")]
        state: String,
        #[tabled(rename = "Results")]
        results: usize,
    }

    let rows: Vec<Row> = resp
        .searches
        .iter()
        .map(|s| Row {
            id: s.id,
            keywords: s.keywords.join(" "),
            state: state_label(s.state.clone()),
            results: s.result_count,
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

pub async fn show(client: &ApiClient, id: u64) -> Result<()> {
    let resp = client.get_search(id).await.map_err(|e| {
        if e.to_string().contains("404") {
            anyhow::anyhow!("Search #{id} not found.")
        } else {
            e
        }
    })?;

    if matches!(resp.state, SearchState::Running) {
        println!(
            "Search #{id} is still running ({} result(s) so far)…",
            resp.results.len()
        );
        poll_until_done(client, id).await
    } else {
        let cached = build_cached(&resp.results);
        save_and_print(&cached);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// cancel
// ---------------------------------------------------------------------------

pub async fn cancel(client: &ApiClient, id: u64) -> Result<()> {
    client.delete_search(id).await.map_err(|e| {
        if e.to_string().contains("404") {
            anyhow::anyhow!("Search #{id} not found.")
        } else {
            e
        }
    })?;
    println!("Search #{id} cancelled.");
    Ok(())
}

// ---------------------------------------------------------------------------
// clean
// ---------------------------------------------------------------------------

pub async fn clean(client: &ApiClient, id: Option<u64>) -> Result<()> {
    if let Some(id) = id {
        let resp = client.get_search(id).await.map_err(|e| {
            if e.to_string().contains("404") {
                anyhow::anyhow!("Search #{id} not found.")
            } else {
                e
            }
        })?;
        if matches!(resp.state, SearchState::Running) {
            anyhow::bail!(
                "Search #{id} is still running. \
                 Use `rucio search cancel {id}` to stop it first."
            );
        }
        client.delete_search(id).await?;
        println!("Search #{id} removed.");
    } else {
        let resp = client.list_searches().await?;
        let removable: Vec<u64> = resp
            .searches
            .iter()
            .filter(|s| !matches!(s.state, SearchState::Running))
            .map(|s| s.id)
            .collect();

        if removable.is_empty() {
            println!("Nothing to clean.");
            return Ok(());
        }

        for id in &removable {
            client.delete_search(*id).await?;
        }
        println!("Removed {} search(es).", removable.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// relaunch
// ---------------------------------------------------------------------------

pub async fn relaunch(client: &ApiClient, id: u64) -> Result<()> {
    client.relaunch_search(id).await.map_err(|e| {
        if e.to_string().contains("404") {
            anyhow::anyhow!("Search #{id} not found.")
        } else {
            e
        }
    })?;
    println!(
        "Search #{id} relaunched. \
         Use `rucio search show {id}` to follow results."
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn poll_until_done(client: &ApiClient, id: u64) -> Result<()> {
    let mut cached: Vec<CachedResult> = Vec::new();
    let mut link_to_idx: HashMap<String, usize> = HashMap::new();

    for attempt in 0..MAX_POLLS {
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let resp = match client.get_search(id).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Poll error: {e}");
                continue;
            }
        };

        merge_results(&resp.results, &mut cached, &mut link_to_idx);

        if attempt % 5 == 4 && matches!(resp.state, SearchState::Running) {
            println!(
                "Still searching… ({}/{}s, {} result(s) so far)",
                attempt + 1,
                MAX_POLLS,
                cached.len()
            );
        }

        if !matches!(resp.state, SearchState::Running) {
            save_and_print(&cached);
            return Ok(());
        }
    }

    if cached.is_empty() {
        println!("Search timed out with no results.");
    } else {
        save_and_print(&cached);
    }
    Ok(())
}

fn build_cached(results: &[SearchResult]) -> Vec<CachedResult> {
    let mut cached: Vec<CachedResult> = Vec::new();
    let mut link_to_idx: HashMap<String, usize> = HashMap::new();
    merge_results(results, &mut cached, &mut link_to_idx);
    cached
}

fn merge_results(
    results: &[SearchResult],
    cached: &mut Vec<CachedResult>,
    link_to_idx: &mut HashMap<String, usize>,
) {
    for r in results {
        let link = r.download_link.clone().unwrap_or_default();
        if let Some(&idx) = link_to_idx.get(&link) {
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
}

fn state_label(state: SearchState) -> String {
    match state {
        SearchState::Running => "running".to_string(),
        SearchState::Done => "done".to_string(),
        SearchState::Cancelled => "cancelled".to_string(),
    }
}

fn save_and_print(results: &[CachedResult]) {
    if let Err(e) = (LastSearch {
        results: results.to_vec(),
    })
    .save()
    {
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
    println!("Use `rucio download add <#>` to download.");
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
