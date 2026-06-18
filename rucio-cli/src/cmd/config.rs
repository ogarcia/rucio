//! `rucio config show / set / unset`

use anyhow::{Result, bail};
use rust_i18n::t;

use crate::client::ApiClient;
use crate::color;

/// Parse a boolean from common textual forms.
fn parse_bool(value: &str) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => bail!(t!("config.bool_invalid", value = value)),
    }
}

/// Parse a TCP/UDP port (1–65535).
fn parse_port(value: &str) -> Result<u16> {
    match value.parse::<u16>() {
        Ok(n) if n > 0 => Ok(n),
        _ => bail!(t!("config.port_invalid", value = value)),
    }
}

/// Parse a slot count constrained to 1–50.
fn parse_slots(value: &str) -> Result<usize> {
    match value.parse::<usize>() {
        Ok(n) if (1..=50).contains(&n) => Ok(n),
        Ok(_) => bail!(t!("config.slots_range", value = value)),
        Err(_) => bail!(t!("config.not_integer", value = value)),
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
            color::limited(&t!("config.restart_required")),
        ),
        _ => color::value(current),
    }
}

/// Format a list field.  Shows each current entry on its own line; if the
/// pending list differs, appends an annotation line below the last entry.
fn print_list_field(label: &str, pad: usize, current: &[String], pending: Option<&[String]>) {
    if current.is_empty() {
        let none = t!("common.none");
        let base = format!("  {label:pad$} = {none}");
        if let Some(pl) = pending
            && !pl.is_empty()
        {
            println!(
                "{}  →  {}  {}",
                base,
                color::limited(&pl.join(", ")),
                color::limited(&t!("config.restart_required")),
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
                color::limited(&t!("config.restart_required")),
            );
        }
    }
}

pub async fn show(client: &ApiClient) -> Result<()> {
    let cfg = client.get_config().await?;
    let cur = &cfg.current;
    let p = cfg.pending.as_deref();

    println!("{}", color::section("[node]"));
    println!(
        "  identity_path = {}",
        color::value(&cur.node.identity_path)
    );
    print_list_field(
        "listen",
        13,
        &cur.node.listen_addrs,
        p.map(|p| p.node.listen_addrs.as_slice()),
    );

    println!("\n{}", color::section("[api]"));
    println!("  listen = {}", color::value(&cur.api.listen));

    println!("\n{}", color::section("[network]"));
    print_list_field(
        "bootstrap_peers",
        20,
        &cur.network.bootstrap_peers,
        p.map(|p| p.network.bootstrap_peers.as_slice()),
    );
    let ul = cur.network.upload_limit_kbps;
    let dl = cur.network.download_limit_kbps;
    println!(
        "  upload_limit_kbps    = {}",
        color::value(&if ul == 0 {
            t!("config.unlimited").to_string()
        } else {
            format!("{ul}")
        })
    );
    println!(
        "  download_limit_kbps  = {}",
        color::value(&if dl == 0 {
            t!("config.unlimited").to_string()
        } else {
            format!("{dl}")
        })
    );
    println!(
        "  max_upload_tasks     = {}",
        pending_scalar(
            &cur.network.max_upload_tasks.to_string(),
            p.map(|p| p.network.max_upload_tasks.to_string()).as_deref(),
        )
    );

    println!("\n{}", color::section("[storage]"));
    println!(
        "  download_dir  = {}",
        pending_scalar(
            &cur.storage.download_dir,
            p.map(|p| p.storage.download_dir.as_str()),
        )
    );
    println!(
        "  temp_dir      = {}",
        pending_scalar(
            &cur.storage.temp_dir,
            p.map(|p| p.storage.temp_dir.as_str()),
        )
    );
    println!(
        "  outboard_dir  = {}",
        pending_scalar(
            &cur.storage.outboard_dir,
            p.map(|p| p.storage.outboard_dir.as_str()),
        )
    );
    println!(
        "  database_path = {}",
        pending_scalar(
            &cur.storage.database_path,
            p.map(|p| p.storage.database_path.as_str()),
        )
    );

    let e = &cur.emule;
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
        "  identity_path            = {}",
        color::value(&e.identity_path)
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
    let auto = t!("config.auto");
    println!(
        "  external_ip              = {}",
        pending_scalar(
            e.external_ip.as_deref().unwrap_or(&auto),
            pe.map(|pe| { pe.external_ip.as_deref().unwrap_or(&auto).to_string() })
                .as_deref(),
        )
    );
    println!(
        "  download_slots_per_file  = {}",
        pending_scalar(
            &e.download_slots_per_file.to_string(),
            pe.map(|pe| pe.download_slots_per_file.to_string())
                .as_deref(),
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
            pe.map(|pe| pe.max_concurrent_downloads.to_string())
                .as_deref(),
        )
    );

    Ok(())
}

/// `rucio config set <key> <value>`
///
/// Scalar keys replace the current value; list keys append one entry.
pub async fn set(client: &ApiClient, key: &str, value: &str) -> Result<()> {
    let mut cfg = client.get_config().await?;
    let c = &mut cfg.current;

    match key {
        "storage.download_dir" => c.storage.download_dir = value.to_string(),
        "storage.temp_dir" => c.storage.temp_dir = value.to_string(),
        "storage.outboard_dir" => c.storage.outboard_dir = value.to_string(),
        "network.bootstrap_peers" => {
            if !c.network.bootstrap_peers.contains(&value.to_string()) {
                c.network.bootstrap_peers.push(value.to_string());
            }
        }
        "node.listen_addrs" => {
            if !c.node.listen_addrs.contains(&value.to_string()) {
                c.node.listen_addrs.push(value.to_string());
            }
        }
        "network.upload_limit_kbps" => {
            c.network.upload_limit_kbps = value
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!(t!("config.not_integer", value = value)))?;
        }
        "network.download_limit_kbps" => {
            c.network.download_limit_kbps = value
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!(t!("config.not_integer", value = value)))?;
        }
        "network.max_upload_tasks" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| anyhow::anyhow!(t!("config.not_integer", value = value)))?;
            if n < 1 {
                anyhow::bail!(t!("config.max_upload_min"));
            }
            c.network.max_upload_tasks = n;
        }
        "emule.enabled" => c.emule.enabled = parse_bool(value)?,
        "emule.temp_dir" => c.emule.temp_dir = value.to_string(),
        "emule.udp_port" => c.emule.udp_port = parse_port(value)?,
        "emule.tcp_port" => c.emule.tcp_port = parse_port(value)?,
        "emule.external_ip" => {
            value
                .parse::<std::net::Ipv4Addr>()
                .map_err(|_| anyhow::anyhow!(t!("config.ipv4_invalid", value = value)))?;
            c.emule.external_ip = Some(value.to_string());
        }
        "emule.download_slots_per_file" => {
            c.emule.download_slots_per_file = parse_slots(value)?;
        }
        "emule.max_upload_slots" => c.emule.max_upload_slots = parse_slots(value)?,
        "emule.max_concurrent_downloads" => {
            c.emule.max_concurrent_downloads = parse_slots(value)?;
        }
        other => bail!(t!("config.unknown_key", key = other)),
    }

    client.put_config(&cfg).await?;
    let msg = match key {
        "network.upload_limit_kbps" | "network.download_limit_kbps" => t!("config.ok_bandwidth"),
        _ => t!("config.ok_restart"),
    };
    println!("{}", color::success(&msg));
    Ok(())
}

/// `rucio config unset <key> [value]`
///
/// List keys remove the given entry; scalar keys revert to their default.
pub async fn unset(client: &ApiClient, key: &str, value: Option<&str>) -> Result<()> {
    let mut cfg = client.get_config().await?;
    let c = &mut cfg.current;

    // List keys require a value to identify the entry to remove.
    let require_value = || -> Result<&str> {
        value.ok_or_else(|| anyhow::anyhow!(t!("config.requires_value", key = key)))
    };

    match key {
        "network.bootstrap_peers" => {
            let value = require_value()?;
            let before = c.network.bootstrap_peers.len();
            c.network.bootstrap_peers.retain(|v| v != value);
            if c.network.bootstrap_peers.len() == before {
                bail!(t!(
                    "config.not_found_in",
                    value = value,
                    key = "network.bootstrap_peers"
                ));
            }
        }
        "node.listen_addrs" => {
            let value = require_value()?;
            let before = c.node.listen_addrs.len();
            c.node.listen_addrs.retain(|v| v != value);
            if c.node.listen_addrs.len() == before {
                bail!(t!(
                    "config.not_found_in",
                    value = value,
                    key = "node.listen_addrs"
                ));
            }
        }
        "emule.external_ip" => {
            // Clears the manual override so the daemon auto-detects again.
            c.emule.external_ip = None;
        }
        other => bail!(t!("config.unset_unknown", key = other)),
    }

    client.put_config(&cfg).await?;
    println!("{}", color::success(&t!("config.ok_restart")));
    Ok(())
}
