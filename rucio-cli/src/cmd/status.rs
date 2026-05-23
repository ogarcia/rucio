//! `rucio status` and `rucio peers`

use anyhow::Result;
use tabled::{Table, Tabled};

use crate::client::ApiClient;

pub async fn status(client: &ApiClient) -> Result<()> {
    let s = client.status().await?;

    println!("Peer ID  : {}", s.peer_id);
    println!("Class    : {:?}", s.class);
    println!("Peers    : {}", s.connected_peers);
    println!("Uptime   : {}", format_uptime(s.uptime_secs));
    println!("Version  : {}", s.version);

    if s.listen_addrs.is_empty() {
        println!("Listening: (none)");
    } else {
        println!("Listening:");
        for addr in &s.listen_addrs {
            println!("  {addr}");
        }
    }

    if !s.observed_addrs.is_empty() {
        println!("External (observed by peers):");
        for addr in &s.observed_addrs {
            println!("  {addr}");
        }
    }

    // Bootstrap multiaddrs: prefer observed (public) addresses; fall back to
    // listen addresses filtering out loopback/unspecified.
    let bootstrap_base: Vec<&str> = if !s.observed_addrs.is_empty() {
        s.observed_addrs.iter().map(String::as_str).collect()
    } else {
        s.listen_addrs
            .iter()
            .map(String::as_str)
            .filter(|a| {
                !a.contains("/127.0.0.1")
                    && !a.contains("/::1")
                    && !a.contains("/0.0.0.0")
                    && !a.contains("/::")
            })
            .collect()
    };

    if !bootstrap_base.is_empty() {
        println!();
        println!("Bootstrap multiaddrs (paste into another node's config.toml):");
        for addr in &bootstrap_base {
            println!("  {addr}/p2p/{}", s.peer_id);
        }
    }

    Ok(())
}

pub async fn peers(client: &ApiClient) -> Result<()> {
    let resp = client.peers().await?;

    if resp.peers.is_empty() {
        println!("No peers known.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Peer ID")]
        peer_id: String,
        #[tabled(rename = "Class")]
        class: String,
        #[tabled(rename = "Addresses")]
        addresses: String,
    }

    let rows: Vec<Row> = resp
        .peers
        .into_iter()
        .map(|p| Row {
            peer_id: truncate(&p.peer_id, 24),
            class: format!("{:?}", p.class),
            addresses: if p.addresses.is_empty() {
                "-".to_string()
            } else {
                p.addresses.join(", ")
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}
