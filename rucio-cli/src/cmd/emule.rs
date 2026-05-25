//! `rucio emule` subcommand — eMule Kad compatibility.

use anyhow::Result;
use clap::Subcommand;
use owo_colors::OwoColorize;

use crate::client::ApiClient;

#[derive(Debug, Subcommand)]
pub enum EmuleCmd {
    /// Show eMule compatibility status and nodes.dat info.
    Status,

    /// Download and install a fresh nodes.dat from the eMule Kad network.
    ///
    /// The file is saved to the path configured in the daemon
    /// (`storage.nodes_dat_path`).  Run this once before using `rucio get
    /// ed2k://…` for the first time.
    Bootstrap {
        /// URL to download nodes.dat from.
        ///
        /// Defaults to `http://upd.emule-security.net/nodes.dat`.
        #[arg(long)]
        url: Option<String>,
    },
}

pub async fn run(client: &ApiClient, cmd: EmuleCmd) -> Result<()> {
    match cmd {
        EmuleCmd::Status => status(client).await,
        EmuleCmd::Bootstrap { url } => bootstrap(client, url).await,
    }
}

async fn status(client: &ApiClient) -> Result<()> {
    let s = client.emule_status().await?;

    let feature = if s.feature_enabled {
        "enabled".green().to_string()
    } else {
        "disabled".red().to_string()
    };
    println!("eMule compatibility: {feature}");

    if !s.feature_enabled {
        println!(
            "\n{} Rebuild the daemon with `--features emule-compat` to enable eMule support.",
            "hint:".yellow()
        );
        return Ok(());
    }

    // nodes.dat
    match &s.nodes_dat_path {
        None => println!("nodes.dat path:      {}", "(unknown)".yellow()),
        Some(p) => {
            println!("nodes.dat path:      {p}");
            if s.nodes_dat_present {
                println!(
                    "nodes.dat status:    {} ({} contacts)",
                    "present".green(),
                    s.contacts
                );
            } else {
                println!(
                    "nodes.dat status:    {} — run `rucio emule bootstrap` to download it",
                    "missing".yellow()
                );
            }
        }
    }

    // connectivity
    let peers_str = if s.connected_peers == 1 {
        "1 peer".to_string()
    } else {
        format!("{} peers", s.connected_peers)
    };
    if s.is_connected {
        println!(
            "Network:             {} ({})",
            "connected".green(),
            peers_str
        );
    } else if s.connected_peers > 0 {
        println!(
            "Network:             {} ({}) — connecting…",
            "degraded".yellow(),
            peers_str
        );
    } else {
        println!(
            "Network:             {} — no peers yet, wait a moment or check your bootstrap peers",
            "offline".red()
        );
    }

    Ok(())
}

async fn bootstrap(client: &ApiClient, url: Option<String>) -> Result<()> {
    let display_url = url
        .as_deref()
        .unwrap_or(rucio_core::api::emule::DEFAULT_NODES_DAT_URL);
    println!("Downloading nodes.dat from {}…", display_url.cyan());

    let resp = client.emule_bootstrap(url).await?;

    println!(
        "{} {} Kad2 contacts saved to {}",
        "Done.".green(),
        resp.contacts.bold(),
        resp.path.cyan()
    );
    println!("You can now use `rucio get ed2k://…` to download eMule files.");

    Ok(())
}
