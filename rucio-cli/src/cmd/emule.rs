//! `rucio node emule` subcommand — eMule Kad compatibility.

use anyhow::Result;
use clap::Subcommand;
use owo_colors::OwoColorize;
use rucio_core::api::emule::EmuleConnectivity;

use crate::client::ApiClient;

#[derive(Debug, Subcommand)]
pub enum EmuleCmd {
    /// Show eMule compatibility status and nodes.dat info.
    Status,

    /// Download and install a fresh nodes.dat from the eMule Kad network.
    ///
    /// The file is saved to the path configured in the daemon
    /// (`storage.nodes_dat_path`).  Run this once before using `rucio download
    /// get ed2k://…` for the first time.
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

    let active = s.feature_enabled && s.runtime_enabled;
    let label = if active {
        "enabled".green().to_string()
    } else {
        "disabled".red().to_string()
    };
    println!("eMule compatibility: {label}");

    if !s.feature_enabled {
        println!(
            "\n{} Rebuild the daemon with `--features emule-compat` to enable eMule support.",
            "hint:".yellow()
        );
        return Ok(());
    }
    if !s.runtime_enabled {
        println!(
            "\n{} Set `emule.enabled = true` in config.toml or `RUCIOD_EMULE_ENABLED=true`.",
            "hint:".yellow()
        );
        return Ok(());
    }

    // ── Identity: external IP and ports ──────────────────────────────────────
    match (&s.external_ip, s.external_ip_source.as_deref()) {
        (Some(ip), Some("upnp")) => {
            println!("External IP:         {ip} ({})", "via UPnP".dimmed())
        }
        (Some(ip), Some("config")) => {
            println!("External IP:         {ip} ({})", "configured".dimmed())
        }
        (Some(ip), Some("peers")) => {
            println!("External IP:         {ip} ({})", "via peers".dimmed())
        }
        (Some(ip), _) => println!("External IP:         {ip}"),
        (None, _) => println!("External IP:         {}", "(unknown)".yellow()),
    }
    if let Some(p) = s.tcp_port {
        println!("eMule TCP port:      {p}");
    }
    if let Some(p) = s.udp_port {
        println!("Kad UDP port:        {p}");
    }

    // ── Connectivity ─────────────────────────────────────────────────────────
    let conn_label = match s.connectivity {
        EmuleConnectivity::Open => "open".green().to_string(),
        EmuleConnectivity::Firewalled => "firewalled".red().to_string(),
        EmuleConnectivity::Unknown => "unknown".yellow().to_string(),
    };
    match s.connectivity_reason.as_deref() {
        Some(reason) => println!("Connectivity:        {conn_label} ({})", reason.dimmed()),
        None => println!("Connectivity:        {conn_label}"),
    }

    // ── nodes.dat ────────────────────────────────────────────────────────────
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
                    "nodes.dat status:    {} — run `rucio node emule bootstrap` to download it",
                    "missing".yellow()
                );
            }
        }
    }

    // ── Kad routing table state ──────────────────────────────────────────────
    let contacts_str = if s.connected_peers == 1 {
        "1 contact".to_string()
    } else {
        format!("{} contacts", s.connected_peers)
    };
    if s.is_connected {
        println!(
            "Kad routing table:   {contacts_str} ({})",
            "connected".green()
        );
    } else if s.connected_peers > 0 {
        println!(
            "Kad routing table:   {contacts_str} ({})",
            "connecting…".yellow()
        );
    } else {
        println!(
            "Kad routing table:   {} — no contacts yet, wait a moment or check your bootstrap peers",
            "empty".red()
        );
    }

    // ── Activity ─────────────────────────────────────────────────────────────
    println!("Active downloads:    {}", s.active_downloads);
    println!(
        "Upload slots:        {} / {} in use",
        s.upload_slots_in_use, s.upload_slots_total
    );
    println!("Inbound connections: {}", s.inbound_connections);

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
    println!("You can now use `rucio download add ed2k://…` to download eMule files.");

    Ok(())
}
