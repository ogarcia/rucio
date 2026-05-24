//! `rucio config show / set / unset`

use anyhow::{Result, bail};

use crate::client::ApiClient;
use crate::color;

pub async fn show(client: &ApiClient) -> Result<()> {
    let cfg = client.get_config().await?;

    println!("{}", color::section("[node]"));
    println!(
        "  identity_path = {}",
        color::value(&cfg.node.identity_path)
    );
    for addr in &cfg.node.listen_addrs {
        println!("  listen        = {}", color::value(addr));
    }

    println!("\n{}", color::section("[api]"));
    println!("  listen = {}", color::value(&cfg.api.listen));

    println!("\n{}", color::section("[network]"));
    if cfg.network.bootstrap_peers.is_empty() {
        println!("  bootstrap_peers = (none)");
    } else {
        for peer in &cfg.network.bootstrap_peers {
            println!("  bootstrap_peers = {}", color::value(peer));
        }
    }

    println!("\n{}", color::section("[storage]"));
    println!(
        "  download_dir  = {}",
        color::value(&cfg.storage.download_dir)
    );
    println!("  temp_dir      = {}", color::value(&cfg.storage.temp_dir));
    println!(
        "  database_path = {}",
        color::value(&cfg.storage.database_path)
    );

    Ok(())
}

/// `rucio config set <key> <value>`
///
/// Scalar keys replace the current value; list keys append one entry.
pub async fn set(client: &ApiClient, key: &str, value: &str) -> Result<()> {
    let mut cfg = client.get_config().await?;

    match key {
        "storage.download_dir" => cfg.storage.download_dir = value.to_string(),
        "storage.temp_dir" => cfg.storage.temp_dir = value.to_string(),
        "network.bootstrap_peers" => {
            if !cfg.network.bootstrap_peers.contains(&value.to_string()) {
                cfg.network.bootstrap_peers.push(value.to_string());
            }
        }
        "node.listen_addrs" => {
            if !cfg.node.listen_addrs.contains(&value.to_string()) {
                cfg.node.listen_addrs.push(value.to_string());
            }
        }
        other => bail!(
            "Unknown or read-only key '{other}'.\n\
             Settable keys:\n\
               storage.download_dir\n\
               storage.temp_dir\n\
               network.bootstrap_peers  (appends)\n\
               node.listen_addrs        (appends)"
        ),
    }

    client.put_config(&cfg).await?;
    println!(
        "{}",
        color::success("ok — restart the daemon for changes to take effect")
    );
    Ok(())
}

/// `rucio config unset <key> <value>`
///
/// Removes one entry from a list key.
pub async fn unset(client: &ApiClient, key: &str, value: &str) -> Result<()> {
    let mut cfg = client.get_config().await?;

    match key {
        "network.bootstrap_peers" => {
            let before = cfg.network.bootstrap_peers.len();
            cfg.network.bootstrap_peers.retain(|v| v != value);
            if cfg.network.bootstrap_peers.len() == before {
                bail!("Value '{value}' not found in network.bootstrap_peers");
            }
        }
        "node.listen_addrs" => {
            let before = cfg.node.listen_addrs.len();
            cfg.node.listen_addrs.retain(|v| v != value);
            if cfg.node.listen_addrs.len() == before {
                bail!("Value '{value}' not found in node.listen_addrs");
            }
        }
        other => bail!(
            "'{other}' is not a list key or does not support unset.\n\
             List keys:\n\
               network.bootstrap_peers\n\
               node.listen_addrs"
        ),
    }

    client.put_config(&cfg).await?;
    println!(
        "{}",
        color::success("ok — restart the daemon for changes to take effect")
    );
    Ok(())
}
