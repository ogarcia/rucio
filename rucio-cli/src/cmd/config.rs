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

/// Format a scalar field, appending `→ new  (restart required)` when the
/// on-disk value differs from the running value.
fn pending_scalar(current: &str, pending: Option<&str>) -> String {
    match pending {
        Some(p) if p != current => format!(
            "{}  →  {}  {}",
            color::value(current),
            color::limited(p),
            color::limited("(restart required)"),
        ),
        _ => color::value(current),
    }
}

/// Format a list field.  Shows each current entry on its own line; if the
/// pending list differs, appends an annotation line below the last entry.
fn print_list_field(label: &str, pad: usize, current: &[String], pending: Option<&[String]>) {
    if current.is_empty() {
        let base = format!("  {label:pad$} = (none)");
        if let Some(pl) = pending
            && !pl.is_empty()
        {
            println!(
                "{}  →  {}  {}",
                base,
                color::limited(&pl.join(", ")),
                color::limited("(restart required)"),
            );
            return;
        }
        println!("{base}");
    } else {
        for (i, item) in current.iter().enumerate() {
            if i == 0 {
                println!("  {label:pad$} = {}", color::value(item));
            } else {
                println!("  {:pad$}   {}", "", color::value(item));
            }
        }
        if let Some(pl) = pending
            && pl != current
        {
            println!(
                "  {:pad$}   →  {}  {}",
                "",
                color::limited(&pl.join(", ")),
                color::limited("(restart required)"),
            );
        }
    }
}

pub async fn show(client: &ApiClient) -> Result<()> {
    let cfg = client.get_config().await?;
    let p = cfg.pending.as_deref();

    println!("{}", color::section("[node]"));
    println!(
        "  identity_path = {}",
        color::value(&cfg.node.identity_path)
    );
    print_list_field(
        "listen",
        13,
        &cfg.node.listen_addrs,
        p.map(|p| p.node.listen_addrs.as_slice()),
    );

    println!("\n{}", color::section("[api]"));
    println!("  listen = {}", color::value(&cfg.api.listen));

    println!("\n{}", color::section("[network]"));
    print_list_field(
        "bootstrap_peers",
        20,
        &cfg.network.bootstrap_peers,
        p.map(|p| p.network.bootstrap_peers.as_slice()),
    );
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
    println!(
        "  max_upload_tasks     = {}",
        pending_scalar(
            &cfg.network.max_upload_tasks.to_string(),
            p.map(|p| p.network.max_upload_tasks.to_string()).as_deref(),
        )
    );

    println!("\n{}", color::section("[storage]"));
    println!(
        "  download_dir  = {}",
        pending_scalar(
            &cfg.storage.download_dir,
            p.map(|p| p.storage.download_dir.as_str()),
        )
    );
    println!(
        "  temp_dir      = {}",
        pending_scalar(
            &cfg.storage.temp_dir,
            p.map(|p| p.storage.temp_dir.as_str()),
        )
    );
    println!(
        "  database_path = {}",
        pending_scalar(
            &cfg.storage.database_path,
            p.map(|p| p.storage.database_path.as_str()),
        )
    );

    let e = &cfg.emule;
    let pe = p.map(|p| &p.emule);
    println!("\n{}", color::section("[emule]"));
    println!(
        "  enabled                  = {}",
        pending_scalar(
            &e.enabled.to_string(),
            pe.map(|pe| pe.enabled.to_string()).as_deref(),
        )
    );
    println!(
        "  temp_dir                 = {}",
        pending_scalar(&e.temp_dir, pe.map(|pe| pe.temp_dir.as_str()))
    );
    println!(
        "  udp_port                 = {}",
        pending_scalar(
            &e.udp_port.to_string(),
            pe.map(|pe| pe.udp_port.to_string()).as_deref(),
        )
    );
    println!(
        "  tcp_port                 = {}",
        pending_scalar(
            &e.tcp_port.to_string(),
            pe.map(|pe| pe.tcp_port.to_string()).as_deref(),
        )
    );
    println!(
        "  external_ip              = {}",
        pending_scalar(
            e.external_ip.as_deref().unwrap_or("(auto)"),
            pe.map(|pe| {
                pe.external_ip
                    .as_deref()
                    .unwrap_or("(auto)")
                    .to_string()
            })
            .as_deref(),
        )
    );
    println!(
        "  download_slots_per_file  = {}",
        pending_scalar(
            &e.download_slots_per_file.to_string(),
            pe.map(|pe| pe.download_slots_per_file.to_string()).as_deref(),
        )
    );
    println!(
        "  max_upload_slots         = {}",
        pending_scalar(
            &e.max_upload_slots.to_string(),
            pe.map(|pe| pe.max_upload_slots.to_string()).as_deref(),
        )
    );
    println!(
        "  max_concurrent_downloads = {}",
        pending_scalar(
            &e.max_concurrent_downloads.to_string(),
            pe.map(|pe| pe.max_concurrent_downloads.to_string()).as_deref(),
        )
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
        "network.max_upload_tasks" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| anyhow::anyhow!("'{value}' is not a valid integer"))?;
            if n < 1 {
                anyhow::bail!("network.max_upload_tasks must be at least 1");
            }
            cfg.network.max_upload_tasks = n;
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
               network.max_upload_tasks        (integer ≥1, requires restart)\n\
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
