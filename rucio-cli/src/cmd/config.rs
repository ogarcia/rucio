//! `rucio config show / set / unset`

use anyhow::{Result, bail};

use crate::client::ApiClient;
use crate::color;

/// Parse a boolean from common textual forms.
fn parse_bool(value: &str) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => bail!("'{value}' is not a valid boolean (use true/false)"),
    }
}

/// Parse a TCP/UDP port (1–65535).
fn parse_port(value: &str) -> Result<u16> {
    match value.parse::<u16>() {
        Ok(n) if n > 0 => Ok(n),
        _ => bail!("'{value}' is not a valid port (1-65535)"),
    }
}

/// Parse a slot count constrained to 1–50.
fn parse_slots(value: &str) -> Result<usize> {
    match value.parse::<usize>() {
        Ok(n) if (1..=50).contains(&n) => Ok(n),
        Ok(_) => bail!("'{value}' is out of range (1-50)"),
        Err(_) => bail!("'{value}' is not a valid integer"),
    }
}

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
        println!("  bootstrap_peers      = (none)");
    } else {
        for peer in &cfg.network.bootstrap_peers {
            println!("  bootstrap_peers      = {}", color::value(peer));
        }
    }
    let ul = cfg.network.upload_limit_kbps;
    let dl = cfg.network.download_limit_kbps;
    println!(
        "  upload_limit_kbps    = {}",
        color::value(&if ul == 0 {
            "unlimited".to_string()
        } else {
            format!("{ul}")
        })
    );
    println!(
        "  download_limit_kbps  = {}",
        color::value(&if dl == 0 {
            "unlimited".to_string()
        } else {
            format!("{dl}")
        })
    );

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

    let e = &cfg.emule;
    println!("\n{}", color::section("[emule]"));
    println!(
        "  enabled                  = {}",
        color::value(&e.enabled.to_string())
    );
    println!("  temp_dir                 = {}", color::value(&e.temp_dir));
    println!(
        "  udp_port                 = {}",
        color::value(&e.udp_port.to_string())
    );
    println!(
        "  tcp_port                 = {}",
        color::value(&e.tcp_port.to_string())
    );
    println!(
        "  external_ip              = {}",
        color::value(e.external_ip.as_deref().unwrap_or("(auto)"))
    );
    println!(
        "  download_slots_per_file  = {}",
        color::value(&e.download_slots_per_file.to_string())
    );
    println!(
        "  max_upload_slots         = {}",
        color::value(&e.max_upload_slots.to_string())
    );
    println!(
        "  max_concurrent_downloads = {}",
        color::value(&e.max_concurrent_downloads.to_string())
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
        "network.upload_limit_kbps" => {
            cfg.network.upload_limit_kbps = value
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("'{value}' is not a valid integer"))?;
        }
        "network.download_limit_kbps" => {
            cfg.network.download_limit_kbps = value
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("'{value}' is not a valid integer"))?;
        }
        "emule.enabled" => cfg.emule.enabled = parse_bool(value)?,
        "emule.temp_dir" => cfg.emule.temp_dir = value.to_string(),
        "emule.udp_port" => cfg.emule.udp_port = parse_port(value)?,
        "emule.tcp_port" => cfg.emule.tcp_port = parse_port(value)?,
        "emule.external_ip" => {
            value
                .parse::<std::net::Ipv4Addr>()
                .map_err(|_| anyhow::anyhow!("'{value}' is not a valid IPv4 address"))?;
            cfg.emule.external_ip = Some(value.to_string());
        }
        "emule.download_slots_per_file" => {
            cfg.emule.download_slots_per_file = parse_slots(value)?;
        }
        "emule.max_upload_slots" => cfg.emule.max_upload_slots = parse_slots(value)?,
        "emule.max_concurrent_downloads" => {
            cfg.emule.max_concurrent_downloads = parse_slots(value)?;
        }
        other => bail!(
            "Unknown or read-only key '{other}'.\n\
             Settable keys:\n\
               storage.download_dir\n\
               storage.temp_dir\n\
               network.bootstrap_peers         (appends)\n\
               node.listen_addrs               (appends)\n\
               network.upload_limit_kbps       (KB/s, 0 = unlimited, applied immediately)\n\
               network.download_limit_kbps     (KB/s, 0 = unlimited, applied immediately)\n\
               emule.enabled                   (true/false)\n\
               emule.temp_dir\n\
               emule.udp_port                  (1-65535)\n\
               emule.tcp_port                  (1-65535)\n\
               emule.external_ip               (IPv4; unset to auto-detect)\n\
               emule.download_slots_per_file   (1-50)\n\
               emule.max_upload_slots          (1-50)\n\
               emule.max_concurrent_downloads  (1-50)"
        ),
    }

    client.put_config(&cfg).await?;
    let msg = match key {
        "network.upload_limit_kbps" | "network.download_limit_kbps" => {
            "ok — bandwidth limit applied immediately"
        }
        _ => "ok — restart the daemon for changes to take effect",
    };
    println!("{}", color::success(msg));
    Ok(())
}

/// `rucio config unset <key> [value]`
///
/// List keys remove the given entry; scalar keys revert to their default.
pub async fn unset(client: &ApiClient, key: &str, value: Option<&str>) -> Result<()> {
    let mut cfg = client.get_config().await?;

    // List keys require a value to identify the entry to remove.
    let require_value = || -> Result<&str> {
        value.ok_or_else(|| anyhow::anyhow!("key '{key}' requires a value to remove"))
    };

    match key {
        "network.bootstrap_peers" => {
            let value = require_value()?;
            let before = cfg.network.bootstrap_peers.len();
            cfg.network.bootstrap_peers.retain(|v| v != value);
            if cfg.network.bootstrap_peers.len() == before {
                bail!("Value '{value}' not found in network.bootstrap_peers");
            }
        }
        "node.listen_addrs" => {
            let value = require_value()?;
            let before = cfg.node.listen_addrs.len();
            cfg.node.listen_addrs.retain(|v| v != value);
            if cfg.node.listen_addrs.len() == before {
                bail!("Value '{value}' not found in node.listen_addrs");
            }
        }
        "emule.external_ip" => {
            // Clears the manual override so the daemon auto-detects again.
            cfg.emule.external_ip = None;
        }
        other => bail!(
            "'{other}' is not a list key or does not support unset.\n\
             Keys that support unset:\n\
               network.bootstrap_peers   (removes one entry)\n\
               node.listen_addrs         (removes one entry)\n\
               emule.external_ip         (reverts to auto-detect)"
        ),
    }

    client.put_config(&cfg).await?;
    println!(
        "{}",
        color::success("ok — restart the daemon for changes to take effect")
    );
    Ok(())
}
