//! `rucio subscription list/add/remove/link` — mirror other nodes' pin-sets.

use anyhow::{Result, bail};
use tabled::{Table, Tabled};

use rucio_core::api::subscriptions::peer_link;

use crate::client::ApiClient;
use crate::cmd::downloads::human_size;
use crate::color;

/// Parse a human disk-size into bytes: a plain number, or a number with a
/// `K`/`M`/`G`/`T` suffix (base 1024, optional trailing `B`, case-insensitive).
pub fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        bail!("empty size");
    }
    let s = s.strip_suffix(['b', 'B']).unwrap_or(s);
    let (num, mult): (&str, u64) = match s.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => {
            let mult = match c.to_ascii_uppercase() {
                'K' => 1024,
                'M' => 1024 * 1024,
                'G' => 1024 * 1024 * 1024,
                'T' => 1024u64 * 1024 * 1024 * 1024,
                other => bail!("unknown size suffix '{other}' (use K, M, G or T)"),
            };
            (&s[..s.len() - 1], mult)
        }
        _ => (s, 1),
    };
    let value: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid size '{input}'"))?;
    if value <= 0.0 {
        bail!("size must be greater than zero");
    }
    Ok((value * mult as f64) as u64)
}

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_subscriptions().await?;
    if resp.subscriptions.is_empty() {
        println!("No subscriptions.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Peer")]
        peer: String,
        #[tabled(rename = "Mirrored")]
        mirrored: String,
        #[tabled(rename = "Files")]
        files: String,
        #[tabled(rename = "Synced")]
        synced: String,
    }

    let rows: Vec<Row> = resp
        .subscriptions
        .iter()
        .map(|s| {
            let files = if s.skipped_count > 0 {
                format!("{} (+{} over quota)", s.wanted_count, s.skipped_count)
            } else {
                s.wanted_count.to_string()
            };
            Row {
                // A short prefix is enough to identify it (and to `remove`).
                peer: s.peer_id.chars().take(16).collect::<String>() + "…",
                mirrored: format!(
                    "{} / {}",
                    human_size(s.used_bytes),
                    human_size(s.quota_bytes)
                ),
                files,
                synced: if s.last_synced_at == 0 {
                    "never".to_string()
                } else {
                    "yes".to_string()
                },
            }
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

pub async fn add(client: &ApiClient, peer: &str, quota: &str) -> Result<()> {
    let quota_bytes = match parse_size(quota) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    };
    match client.create_subscription(peer, quota_bytes).await {
        Ok(s) => {
            println!(
                "{}",
                color::success(&format!(
                    "Subscribed to {} (quota {}).",
                    s.peer_id,
                    human_size(s.quota_bytes)
                ))
            );
        }
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, peer_id: &str, keep: bool) -> Result<()> {
    match client.delete_subscription(peer_id, keep).await {
        Ok(()) => {
            let tail = if keep {
                " Mirrored content kept as your own shares."
            } else {
                " Mirror-only content freed."
            };
            println!(
                "{}",
                color::success(&format!("Unsubscribed from {peer_id}.{tail}"))
            )
        }
        Err(e) => {
            eprintln!("{}", color::error(&format!("Error: {e}")));
            std::process::exit(1);
        }
    }
    Ok(())
}

/// Print this node's shareable subscription link, so others can mirror us.
pub async fn link(client: &ApiClient) -> Result<()> {
    let status = client.status().await?;
    println!("{}", peer_link(&status.peer_id));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn parses_sizes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("10G").unwrap(), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1.5M").unwrap(), (1.5 * 1024.0 * 1024.0) as u64);
        assert_eq!(parse_size("2 T").unwrap(), 2u64 * 1024 * 1024 * 1024 * 1024);
        assert!(parse_size("0").is_err());
        assert!(parse_size("-5G").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("5X").is_err());
    }
}
