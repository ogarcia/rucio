//! `rucio pin list`, `rucio pin add <magnet>`, `rucio pin remove <hash>`.

use anyhow::{Context, Result, bail};
use rust_i18n::t;
use tabled::builder::Builder;

use rucio_core::api::pins::PinState;

use crate::client::ApiClient;
use crate::cmd::downloads::human_size;
use crate::color;

fn state_label(state: PinState) -> String {
    match state {
        PinState::Available => t!("pin.state.available"),
        PinState::Fetching => t!("pin.state.fetching"),
        PinState::Missing => t!("pin.state.missing"),
    }
    .to_string()
}

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_pins().await?;
    if resp.pins.is_empty() {
        println!("{}", t!("pin.none"));
        return Ok(());
    }

    let mut table = Builder::new();
    table.push_record([
        t!("pin.col.hash").to_string(),
        t!("pin.col.name").to_string(),
        t!("pin.col.size").to_string(),
        t!("pin.col.state").to_string(),
        t!("pin.col.collection").to_string(),
    ]);
    for p in &resp.pins {
        table.push_record([
            // Short hash prefix is enough to identify a pin (and to `pin remove`).
            p.root_hash.chars().take(16).collect(),
            p.name.clone().unwrap_or_else(|| "-".to_string()),
            p.size.map(human_size).unwrap_or_else(|| "-".to_string()),
            state_label(p.state),
            p.collection.clone().unwrap_or_else(|| "-".to_string()),
        ]);
    }

    println!("{}", table.build());
    Ok(())
}

/// Turn a `pin add` target into a `rucio:` magnet:
/// - a `rucio:` magnet is used as-is (fetched if missing);
/// - a 64-char hex string is treated as a root hash to pin directly;
/// - a positive integer is a download id resolved to its root hash (pinning
///   something you already have — no re-fetch).
async fn resolve_to_magnet(client: &ApiClient, target: &str) -> Result<String> {
    let t = target.trim();
    if t.starts_with("rucio:") {
        return Ok(t.to_string());
    }
    if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(format!("rucio:{}", t.to_lowercase()));
    }
    if let Ok(id) = t.parse::<i64>() {
        if id <= 0 {
            bail!(t!("pin.err_positive_id"));
        }
        let dl = client
            .get_download(id)
            .await
            .with_context(|| t!("pin.err_no_download", id = id).to_string())?;
        return Ok(format!("rucio:{}", dl.root_hash));
    }
    bail!(t!("pin.err_bad_target", target = target));
}

pub async fn add(
    client: &ApiClient,
    target: &str,
    providers: Vec<String>,
    collection: Option<String>,
) -> Result<()> {
    let magnet = match resolve_to_magnet(client, target).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    };
    let collection = collection
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty());
    match client.create_pin(&magnet, providers, collection).await {
        Ok(p) => {
            let name = p
                .name
                .clone()
                .unwrap_or_else(|| t!("common.unknown").to_string());
            println!(
                "{}",
                color::success(&t!("pin.added", name = name, state = state_label(p.state)))
            );
        }
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, hash: &str) -> Result<()> {
    match client.delete_pin(hash).await {
        Ok(()) => println!("{}", color::success(&t!("pin.removed", hash = hash))),
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}
