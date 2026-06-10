//! `rucio pin list`, `rucio pin add <magnet>`, `rucio pin remove <hash>`.

use anyhow::{Context, Result, bail};
use tabled::{Table, Tabled};

use rucio_core::api::pins::PinState;

use crate::client::ApiClient;
use crate::cmd::downloads::human_size;
use crate::color;

fn state_label(state: PinState) -> &'static str {
    match state {
        PinState::Available => "available",
        PinState::Fetching => "fetching",
        PinState::Missing => "missing",
    }
}

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_pins().await?;
    if resp.pins.is_empty() {
        println!("No pins.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Root hash")]
        hash: String,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Size")]
        size: String,
        #[tabled(rename = "State")]
        state: String,
    }

    let rows: Vec<Row> = resp
        .pins
        .iter()
        .map(|p| Row {
            // Short hash prefix is enough to identify a pin (and to `pin remove`).
            hash: p.root_hash.chars().take(16).collect(),
            name: p.name.clone().unwrap_or_else(|| "-".to_string()),
            size: p.size.map(human_size).unwrap_or_else(|| "-".to_string()),
            state: state_label(p.state).to_string(),
        })
        .collect();

    println!("{}", Table::new(rows));
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
            bail!("pin works on Rucio downloads only (use a positive download id)");
        }
        let dl = client
            .get_download(id)
            .await
            .with_context(|| format!("no Rucio download with id {id}"))?;
        return Ok(format!("rucio:{}", dl.root_hash));
    }
    bail!("'{target}' is not a rucio: magnet, a download id, or a 64-char root hash");
}

pub async fn add(client: &ApiClient, target: &str, providers: Vec<String>) -> Result<()> {
    let magnet = match resolve_to_magnet(client, target).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    };
    match client.create_pin(&magnet, providers).await {
        Ok(p) => {
            let name = p.name.as_deref().unwrap_or("(unknown)");
            println!(
                "{}",
                color::success(&format!("Pinned '{name}' ({}).", state_label(p.state)))
            );
        }
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, hash: &str) -> Result<()> {
    match client.delete_pin(hash).await {
        Ok(()) => println!("{}", color::success(&format!("Unpinned {hash}."))),
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}
