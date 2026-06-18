//! `rucio node emule` subcommand — eMule Kad compatibility.

use anyhow::Result;
use clap::Subcommand;
use owo_colors::OwoColorize;
use rucio_core::api::emule::EmuleConnectivity;
use rust_i18n::t;

use crate::client::ApiClient;
use crate::table_util::label_width;

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
    let state = if active {
        t!("emule.state_enabled").green().to_string()
    } else {
        t!("emule.state_disabled").red().to_string()
    };
    println!("{}", t!("emule.compat", state = state));

    if !s.feature_enabled {
        println!(
            "\n{} {}",
            t!("emule.hint_word").yellow(),
            t!("emule.hint_rebuild")
        );
        return Ok(());
    }
    if !s.runtime_enabled {
        println!(
            "\n{} {}",
            t!("emule.hint_word").yellow(),
            t!("emule.hint_enable")
        );
        return Ok(());
    }

    // Labels carry their own colon; align values to the widest translated label.
    let l_ip = t!("emule.lbl_external_ip");
    let l_tcp = t!("emule.lbl_tcp_port");
    let l_udp = t!("emule.lbl_udp_port");
    let l_conn = t!("emule.lbl_connectivity");
    let l_npath = t!("emule.lbl_nodes_path");
    let l_nstatus = t!("emule.lbl_nodes_status");
    let l_kad = t!("emule.lbl_kad_table");
    let l_active = t!("emule.lbl_active_downloads");
    let l_slots = t!("emule.lbl_upload_slots");
    let l_inbound = t!("emule.lbl_inbound");
    let w = label_width([
        l_ip.as_ref(),
        l_tcp.as_ref(),
        l_udp.as_ref(),
        l_conn.as_ref(),
        l_npath.as_ref(),
        l_nstatus.as_ref(),
        l_kad.as_ref(),
        l_active.as_ref(),
        l_slots.as_ref(),
        l_inbound.as_ref(),
    ]);

    // ── Identity: external IP and ports ──────────────────────────────────────
    match (&s.external_ip, s.external_ip_source.as_deref()) {
        (Some(ip), Some("upnp")) => {
            println!("{l_ip:<w$} {ip} ({})", t!("emule.src_upnp").dimmed())
        }
        (Some(ip), Some("config")) => {
            println!("{l_ip:<w$} {ip} ({})", t!("emule.src_config").dimmed())
        }
        (Some(ip), Some("peers")) => {
            println!("{l_ip:<w$} {ip} ({})", t!("emule.src_peers").dimmed())
        }
        (Some(ip), _) => println!("{l_ip:<w$} {ip}"),
        (None, _) => println!("{l_ip:<w$} {}", t!("common.unknown").yellow()),
    }
    if let Some(p) = s.tcp_port {
        println!("{l_tcp:<w$} {p}");
    }
    if let Some(p) = s.udp_port {
        println!("{l_udp:<w$} {p}");
    }

    // ── Connectivity ─────────────────────────────────────────────────────────
    let conn_label = match s.connectivity {
        EmuleConnectivity::Open => t!("emule.conn_open").green().to_string(),
        EmuleConnectivity::Firewalled => t!("emule.conn_firewalled").red().to_string(),
        EmuleConnectivity::Unknown => t!("emule.conn_unknown").yellow().to_string(),
    };
    let conn_val = match s.connectivity_reason.as_deref() {
        Some(reason) => t!(
            "emule.conn_val",
            state = conn_label,
            reason = reason.dimmed().to_string()
        )
        .to_string(),
        None => conn_label,
    };
    println!("{l_conn:<w$} {conn_val}");

    // ── nodes.dat ────────────────────────────────────────────────────────────
    match &s.nodes_dat_path {
        None => println!("{l_npath:<w$} {}", t!("common.unknown").yellow()),
        Some(p) => {
            println!("{l_npath:<w$} {p}");
            let nstatus_val = if s.nodes_dat_present {
                t!(
                    "emule.nodes_present_val",
                    state = t!("emule.nodes_present").green().to_string(),
                    contacts = s.contacts
                )
                .to_string()
            } else {
                t!(
                    "emule.nodes_missing_val",
                    state = t!("emule.nodes_missing").yellow().to_string()
                )
                .to_string()
            };
            println!("{l_nstatus:<w$} {nstatus_val}");
        }
    }

    // ── Kad routing table state ──────────────────────────────────────────────
    let contacts_str = if s.connected_peers == 1 {
        t!("emule.contacts_one").to_string()
    } else {
        t!("emule.contacts_many", n = s.connected_peers).to_string()
    };
    let kad_val = if s.is_connected {
        t!(
            "emule.kad_val",
            contacts = contacts_str,
            state = t!("emule.kad_connected").green().to_string()
        )
        .to_string()
    } else if s.connected_peers > 0 {
        t!(
            "emule.kad_val",
            contacts = contacts_str,
            state = t!("emule.kad_connecting").yellow().to_string()
        )
        .to_string()
    } else {
        t!(
            "emule.kad_empty_val",
            state = t!("emule.kad_empty").red().to_string()
        )
        .to_string()
    };
    println!("{l_kad:<w$} {kad_val}");

    // ── Activity ─────────────────────────────────────────────────────────────
    println!("{l_active:<w$} {}", s.active_downloads);
    println!(
        "{l_slots:<w$} {}",
        t!(
            "emule.upload_slots_val",
            in_use = s.upload_slots_in_use,
            total = s.upload_slots_total
        )
    );
    println!("{l_inbound:<w$} {}", s.inbound_connections);

    Ok(())
}

async fn bootstrap(client: &ApiClient, url: Option<String>) -> Result<()> {
    let display_url = url
        .as_deref()
        .unwrap_or(rucio_core::api::emule::DEFAULT_NODES_DAT_URL);
    println!(
        "{}",
        t!("emule.downloading", url = display_url.cyan().to_string())
    );

    let resp = client.emule_bootstrap(url).await?;

    println!(
        "{} {}",
        t!("emule.done_word").green(),
        t!(
            "emule.saved",
            contacts = resp.contacts.bold().to_string(),
            path = resp.path.cyan().to_string()
        )
    );
    println!("{}", t!("emule.ready"));

    Ok(())
}
