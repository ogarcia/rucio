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
use rucio_core::api::searches::{ResultSource, SearchNetwork, SearchResult, SearchState};
use rust_i18n::t;
use tabled::builder::Builder;
use tokio::time::{Duration, sleep};

use crate::client::ApiClient;
use crate::color;
use crate::state::{CachedResult, LastSearch};
use crate::table_util::{fit_column, term_width};

const POLL_INTERVAL_MS: u64 = 1_000;
const MAX_POLLS: u32 = 65;

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

pub async fn add(
    client: &ApiClient,
    keywords: Vec<String>,
    wait: bool,
    network: SearchNetwork,
) -> Result<()> {
    if keywords.is_empty() {
        anyhow::bail!(t!("search.no_keyword"));
    }

    let started = client.start_search(keywords.clone(), network).await?;
    let id = started.id;
    println!("{id}");

    if wait {
        println!(
            "{}",
            t!(
                "search.searching_for",
                keywords = color::value(&keywords.join(" "))
            )
        );
        poll_until_done(client, id, Vec::new()).await
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
        println!("{}", t!("search.none_memory"));
        return Ok(());
    }

    let rows: Vec<[String; 4]> = resp
        .searches
        .iter()
        .map(|s| {
            [
                s.id.to_string(),
                s.keywords.join(" "),
                state_label(s.state.clone()),
                s.result_count.to_string(),
            ]
        })
        .collect();

    let max_kw = rows.iter().map(|r| r[1].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("search.col.id").to_string(),
        t!("search.col.keywords").to_string(),
        t!("search.col.state").to_string(),
        t!("search.col.results").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    fit_column(&mut table, 1, max_kw, term_width());
    println!("{table}");
    Ok(())
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

pub async fn show(client: &ApiClient, id: u64) -> Result<()> {
    let resp = client.get_search(id).await.map_err(|e| {
        if e.to_string().contains("404") {
            anyhow::anyhow!(t!("search.not_found", id = id))
        } else {
            e
        }
    })?;

    if matches!(resp.state, SearchState::Running) {
        let initial = build_cached(&resp.results);
        if initial.is_empty() {
            poll_until_done(client, id, Vec::new()).await
        } else {
            save_and_print(&initial);
            println!("{}", color::limited(&t!("search.still_running", id = id)));
            Ok(())
        }
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
            anyhow::anyhow!(t!("search.not_found", id = id))
        } else {
            e
        }
    })?;
    println!("{}", t!("search.cancelled", id = id));
    Ok(())
}

// ---------------------------------------------------------------------------
// clean
// ---------------------------------------------------------------------------

pub async fn clean(client: &ApiClient, id: Option<u64>) -> Result<()> {
    if let Some(id) = id {
        let resp = client.get_search(id).await.map_err(|e| {
            if e.to_string().contains("404") {
                anyhow::anyhow!(t!("search.not_found", id = id))
            } else {
                e
            }
        })?;
        if matches!(resp.state, SearchState::Running) {
            anyhow::bail!(t!("search.still_running_clean", id = id));
        }
        client.delete_search(id).await?;
        println!("{}", t!("search.removed", id = id));
    } else {
        let resp = client.list_searches().await?;
        let removable: Vec<u64> = resp
            .searches
            .iter()
            .filter(|s| !matches!(s.state, SearchState::Running))
            .map(|s| s.id)
            .collect();

        if removable.is_empty() {
            println!("{}", t!("search.nothing_to_clean"));
            return Ok(());
        }

        for id in &removable {
            client.delete_search(*id).await?;
        }
        println!("{}", t!("search.removed_n", n = removable.len()));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// relaunch
// ---------------------------------------------------------------------------

pub async fn relaunch(client: &ApiClient, id: u64) -> Result<()> {
    client.relaunch_search(id).await.map_err(|e| {
        if e.to_string().contains("404") {
            anyhow::anyhow!(t!("search.not_found", id = id))
        } else {
            e
        }
    })?;
    println!("{}", t!("search.relaunched", id = id));
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn poll_until_done(client: &ApiClient, id: u64, initial: Vec<CachedResult>) -> Result<()> {
    let mut cached = initial;
    let mut link_to_idx: HashMap<String, usize> = cached
        .iter()
        .enumerate()
        .map(|(i, r)| (r.download_link.clone(), i))
        .collect();

    for attempt in 0..MAX_POLLS {
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let resp = match client.get_search(id).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", t!("search.poll_error", msg = e));
                continue;
            }
        };

        merge_results(&resp.results, &mut cached, &mut link_to_idx);

        if attempt % 5 == 4 && matches!(resp.state, SearchState::Running) {
            println!(
                "{}",
                t!(
                    "search.still_searching",
                    attempt = attempt + 1,
                    max = MAX_POLLS,
                    count = cached.len()
                )
            );
        }

        if !matches!(resp.state, SearchState::Running) {
            if cached.is_empty() {
                println!("{}", t!("search.no_results"));
            } else {
                save_and_print(&cached);
            }
            return Ok(());
        }
    }

    if cached.is_empty() {
        println!("{}", t!("search.timed_out"));
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
        // The full link now grows as the daemon merges providers into the
        // magnet, so it can't be the dedup key. Key by the stable part before
        // '?' (the hash for rucio:, the whole ed2k:// link for eMule).
        let key = link.split('?').next().unwrap_or(&link).to_string();
        if let Some(&idx) = link_to_idx.get(&key) {
            // Refresh with the authoritative merged link and provider set.
            cached[idx].download_link = link;
            for p in r.providers.iter().flatten() {
                if !cached[idx].providers.contains(p) {
                    cached[idx].providers.push(p.clone());
                }
            }
        } else {
            let source_str = match r.source {
                ResultSource::Rucio => "rucio",
                ResultSource::Emule => "emule",
            };
            let idx = cached.len();
            link_to_idx.insert(key, idx);
            cached.push(CachedResult {
                name: r.name.clone(),
                size: r.size,
                download_link: link,
                providers: r.providers.clone().unwrap_or_default(),
                source: source_str.to_string(),
            });
        }
    }
}

fn state_label(state: SearchState) -> String {
    match state {
        SearchState::Running => t!("search.state.running"),
        SearchState::Done => t!("search.state.done"),
        SearchState::Cancelled => t!("search.state.cancelled"),
    }
    .to_string()
}

fn save_and_print(results: &[CachedResult]) {
    if let Err(e) = (LastSearch {
        results: results.to_vec(),
    })
    .save()
    {
        eprintln!("{}", t!("search.save_warning", msg = e));
    }
    print_results(results);
}

fn print_results(results: &[CachedResult]) {
    if results.is_empty() {
        println!("{}", t!("search.no_results"));
        return;
    }

    let rows: Vec<[String; 5]> = results
        .iter()
        .enumerate()
        .map(|(i, r)| {
            [
                (i + 1).to_string(),
                r.name.clone(),
                human_size(r.size),
                r.source.clone(),
                if r.providers.is_empty() {
                    "-".to_string()
                } else {
                    color::sources(r.providers.len())
                },
            ]
        })
        .collect();

    let max_name = rows.iter().map(|r| r[1].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("search.col.num").to_string(),
        t!("search.col.name").to_string(),
        t!("search.col.size").to_string(),
        t!("search.col.source").to_string(),
        t!("search.col.providers").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    fit_column(&mut table, 1, max_name, term_width());
    println!("{table}");
    println!("{}", t!("search.use_download_add"));
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
