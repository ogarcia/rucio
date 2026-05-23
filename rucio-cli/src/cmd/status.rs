//! `rucio status` and `rucio peers`

use anyhow::Result;
use tabled::{Table, Tabled};

use crate::client::ApiClient;

pub async fn status(client: &ApiClient) -> Result<()> {
    let s = client.status().await?;

    println!("Peer ID  : {}", s.peer_id);
    println!("Class    : {:?}", s.class);
    println!("Peers    : {}", s.connected_peers);
    println!("Uptime   : {}s", s.uptime_secs);
    println!("Version  : {}", s.version);
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
            addresses: p.addresses.first().cloned().unwrap_or_default(),
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
