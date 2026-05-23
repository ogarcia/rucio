//! `rucio config show`

use anyhow::Result;

use crate::client::ApiClient;

pub async fn show(client: &ApiClient) -> Result<()> {
    let cfg = client.get_config().await?;

    println!("[node]");
    println!("  identity_path = {}", cfg.node.identity_path);
    for addr in &cfg.node.listen_addrs {
        println!("  listen        = {addr}");
    }

    println!("\n[api]");
    println!("  listen = {}", cfg.api.listen);

    println!("\n[network]");
    if cfg.network.bootstrap_peers.is_empty() {
        println!("  bootstrap_peers = (none)");
    } else {
        for peer in &cfg.network.bootstrap_peers {
            println!("  bootstrap_peers = {peer}");
        }
    }

    println!("\n[storage]");
    println!("  download_dir  = {}", cfg.storage.download_dir);
    println!("  database_path = {}", cfg.storage.database_path);

    Ok(())
}
