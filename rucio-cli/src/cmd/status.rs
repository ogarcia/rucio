//! `rucio node status`, `rucio node peers`, `rucio node metrics`

use anyhow::Result;
use rucio_core::protocol::node::NodeClass;
use rust_i18n::t;
use tabled::builder::Builder;

use crate::client::ApiClient;
use crate::color;
use crate::table_util::{fit_column, label_width, term_width};

pub async fn status(client: &ApiClient) -> Result<()> {
    let s = client.status().await?;

    let l_peer = t!("node.status.peer_id");
    let l_class = t!("node.status.class");
    let l_peers = t!("node.status.peers");
    let l_uptime = t!("node.status.uptime");
    let l_version = t!("node.status.version");
    let l_listening = t!("node.status.listening");
    let w = label_width([
        l_peer.as_ref(),
        l_class.as_ref(),
        l_peers.as_ref(),
        l_uptime.as_ref(),
        l_version.as_ref(),
        l_listening.as_ref(),
    ]);

    println!("{l_peer:<w$} : {}", color::value(&s.peer_id));
    println!("{l_class:<w$} : {}", color::node_class(&s.class));
    println!("{l_peers:<w$} : {}", s.connected_peers);
    println!("{l_uptime:<w$} : {}", format_uptime(s.uptime_secs));
    println!("{l_version:<w$} : {}", s.version);

    if s.listen_addrs.is_empty() {
        println!("{l_listening:<w$} : {}", t!("node.status.none_paren"));
    } else {
        println!("{l_listening}:");
        for addr in &s.listen_addrs {
            println!("  {}", color::value(addr));
        }
    }

    if !s.observed_addrs.is_empty() {
        println!("{}", t!("node.status.observed"));
        for addr in &s.observed_addrs {
            println!("  {}", color::value(addr));
        }
    }

    // Connectivity summary line
    println!();
    println!(
        "{} {}",
        t!("node.status.connectivity"),
        connectivity_summary(&s.class, s.connected_peers, &s.observed_addrs)
    );

    // Bootstrap multiaddrs: prefer observed (public) addresses; fall back to
    // listen addresses.  Either way, classify each address and annotate
    // local-only ones so the user knows they won't work across the internet.
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
        let public: Vec<&str> = bootstrap_base
            .iter()
            .copied()
            .filter(|a| addr_scope_hint(a).is_empty())
            .collect();
        let mut local: Vec<&str> = bootstrap_base
            .iter()
            .copied()
            .filter(|a| !addr_scope_hint(a).is_empty())
            .collect();
        // Stable sort within each group (link-local last).
        local.sort_by_key(|a| addr_scope_hint(a));

        if !public.is_empty() {
            println!();
            println!("{}", t!("node.status.bootstrap_public"));
            for addr in &public {
                println!("  {}/p2p/{}", color::value(addr), color::value(&s.peer_id));
            }
        }

        if !local.is_empty() {
            println!();
            println!("{}", t!("node.status.bootstrap_local"));
            for addr in &local {
                println!(
                    "  {}/p2p/{}  [{}]",
                    color::value(addr),
                    color::value(&s.peer_id),
                    scope_label(addr_scope_hint(addr))
                );
            }
        }

        if public.is_empty() && !local.is_empty() {
            println!();
            println!("{}", t!("node.status.note_no_public"));
        }
    }

    // Session metrics summary (best-effort — daemon may not support it yet)
    if let Ok(m) = client.metrics().await {
        let sess = &m.session;
        println!();
        println!("{}", t!("node.status.session_transfer"));
        println!(
            "  {}",
            t!(
                "node.status.up_line",
                speed = format_bytes(sess.upload_speed),
                total = format_bytes(sess.uploaded_bytes),
                chunks = sess.chunks_served
            )
        );
        println!(
            "  {}",
            t!(
                "node.status.down_line",
                speed = format_bytes(sess.download_speed),
                total = format_bytes(sess.downloaded_bytes),
                chunks = sess.chunks_received,
                rejected = sess.chunks_rejected
            )
        );
    }

    Ok(())
}

/// Human-readable connectivity class label (plain, used only if color module
/// is bypassed — actual coloured output is produced by `color::node_class`).
fn _format_class(class: &NodeClass) -> &'static str {
    match class {
        NodeClass::HighId => "HighID (publicly reachable, can serve files)",
        NodeClass::LowId => "LowID  (behind NAT, download-only mode)",
        NodeClass::Unknown => "Unknown (still determining…)",
    }
}

/// One-line connectivity summary combining class, peers and observed addrs.
fn connectivity_summary(class: &NodeClass, peers: usize, observed: &[String]) -> String {
    match class {
        NodeClass::Unknown if peers == 0 => color::offline(&t!("node.conn.offline_no_peers")),
        NodeClass::Unknown => color::limited(&t!("node.conn.limited", peers = peers)),
        NodeClass::LowId if peers == 0 => color::offline(&t!("node.conn.offline_nat")),
        NodeClass::LowId => color::limited(&t!("node.conn.online_lowid", peers = peers)),
        NodeClass::HighId => {
            let addr_hint = if observed.is_empty() {
                String::new()
            } else {
                t!("node.conn.external_hint", addr = color::value(&observed[0])).to_string()
            };
            color::online(&t!(
                "node.conn.online_highid",
                peers = peers,
                addr_hint = addr_hint
            ))
        }
    }
}

/// Translate an `addr_scope_hint` sentinel for display. The sentinel stays in
/// English internally so the filter/sort logic is locale-independent.
fn scope_label(hint: &str) -> String {
    match hint {
        "local network only" => t!("node.scope.local").to_string(),
        "link-local only" => t!("node.scope.link_local").to_string(),
        other => other.to_string(),
    }
}

pub async fn peers(client: &ApiClient) -> Result<()> {
    let resp = client.peers().await?;

    if resp.peers.is_empty() {
        println!("{}", t!("node.peers.none"));
        return Ok(());
    }

    let rows: Vec<[String; 4]> = resp
        .peers
        .into_iter()
        .map(|p| {
            let public_addrs: Vec<&str> = p
                .addresses
                .iter()
                .map(String::as_str)
                .filter(|a| !is_loopback_or_unspecified(a))
                .collect();
            [
                p.peer_id,
                format!("{:?}", p.class),
                p.agent_version.unwrap_or_else(|| "-".to_string()),
                if public_addrs.is_empty() {
                    "-".to_string()
                } else {
                    public_addrs.join(", ")
                },
            ]
        })
        .collect();

    let max_addr = rows.iter().map(|r| r[3].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("node.peers.col_peer_id").to_string(),
        t!("node.peers.col_class").to_string(),
        t!("node.peers.col_agent").to_string(),
        t!("node.peers.col_addresses").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    fit_column(&mut table, 3, max_addr, term_width());
    println!("{table}");
    Ok(())
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

/// Format a byte count as a human-readable string (B, KiB, MiB, GiB).
fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

// ---------------------------------------------------------------------------
// `rucio node metrics`
// ---------------------------------------------------------------------------

/// Print full metrics (session + lifetime totals).
pub async fn metrics_cmd(client: &ApiClient) -> Result<()> {
    let m = client.metrics().await?;
    let sess = &m.session;
    let total = &m.total;

    let l_up = t!("node.metrics.upload");
    let l_down = t!("node.metrics.download");
    let l_sup = t!("node.metrics.speed_up");
    let l_sdown = t!("node.metrics.speed_down");
    let w = label_width([
        l_up.as_ref(),
        l_down.as_ref(),
        l_sup.as_ref(),
        l_sdown.as_ref(),
    ]);

    println!("{}", t!("node.metrics.session_header"));
    println!(
        "  {l_up:<w$} : {}",
        t!(
            "node.metrics.upload_val",
            bytes = color::value(&format_bytes(sess.uploaded_bytes)),
            chunks = sess.chunks_served
        )
    );
    println!(
        "  {l_down:<w$} : {}",
        t!(
            "node.metrics.download_val",
            bytes = color::value(&format_bytes(sess.downloaded_bytes)),
            chunks = sess.chunks_received,
            rejected = sess.chunks_rejected
        )
    );
    println!(
        "  {l_sup:<w$} : {}",
        t!(
            "node.metrics.speed_val",
            speed = color::value(&format_bytes(sess.upload_speed))
        )
    );
    println!(
        "  {l_sdown:<w$} : {}",
        t!(
            "node.metrics.speed_val",
            speed = color::value(&format_bytes(sess.download_speed))
        )
    );

    println!();
    println!("{}", t!("node.metrics.lifetime_header"));
    println!(
        "  {l_up:<w$} : {}",
        t!(
            "node.metrics.upload_val",
            bytes = color::value(&format_bytes(total.uploaded_bytes)),
            chunks = total.chunks_served
        )
    );
    println!(
        "  {l_down:<w$} : {}",
        t!(
            "node.metrics.download_val",
            bytes = color::value(&format_bytes(total.downloaded_bytes)),
            chunks = total.chunks_received,
            rejected = total.chunks_rejected
        )
    );

    Ok(())
}

/// Return a short scope label for a multiaddr string, or empty string if the
/// address is publicly routable (no annotation needed).
///
/// Covers the most common non-routable ranges:
///   IPv4 private  : 10.x, 172.16-31.x, 192.168.x
///   IPv4 link-local: 169.254.x
///   IPv6 link-local: fe80::
///   IPv6 ULA       : fc00::/7  (fd... and fc...)
fn addr_scope_hint(multiaddr: &str) -> &'static str {
    // Extract the IP portion from multiaddr segments like /ip4/1.2.3.4/... or
    // /ip6/fe80::1/...  We just do prefix matching on the string — no need to
    // parse the full multiaddr type here.
    if let Some(ip) = extract_ip(multiaddr) {
        if ip.starts_with("10.") || ip.starts_with("192.168.") || ip.starts_with("169.254.") {
            return "local network only";
        }
        // 172.16.0.0/12 → 172.16.x through 172.31.x
        if ip.starts_with("172.")
            && ip
                .split('.')
                .nth(1)
                .and_then(|s| s.parse::<u8>().ok())
                .is_some_and(|n| (16..=31).contains(&n))
        {
            return "local network only";
        }
        // IPv6 link-local
        if ip.starts_with("fe80:") || ip.starts_with("fe80::") {
            return "link-local only";
        }
        // IPv6 ULA (fc00::/7 covers fc** and fd**)
        if ip.starts_with("fd") || ip.starts_with("fc") {
            return "local network only";
        }
    }
    ""
}

/// Returns `true` for loopback, unspecified, and link-local addresses that are
/// never useful in a peer table shown to the user.
fn is_loopback_or_unspecified(multiaddr: &str) -> bool {
    match extract_ip(multiaddr) {
        Some("127.0.0.1") | Some("0.0.0.0") | Some("::1") | Some("::") => true,
        Some(ip) => ip.starts_with("fe80:") || ip.starts_with("fe80::"),
        None => false,
    }
}

/// Extract the raw IP string from a multiaddr like `/ip4/1.2.3.4/tcp/...`
/// or `/ip6/fe80::1/tcp/...`.
fn extract_ip(multiaddr: &str) -> Option<&str> {
    // multiaddr segments: ["", "ip4" or "ip6", "<ip>", ...]
    let mut parts = multiaddr.splitn(4, '/');
    parts.next(); // leading empty string before first '/'
    let proto = parts.next()?;
    if proto == "ip4" || proto == "ip6" {
        parts.next()
    } else {
        None
    }
}
