//! `rucio subscription list/add/remove/link` — mirror other nodes' pin-sets.

use anyhow::{Result, bail};
use rust_i18n::t;
use tabled::builder::Builder;

use rucio_core::api::subscriptions::peer_link;

use crate::client::ApiClient;
use crate::cmd::downloads::human_size;
use crate::color;

/// Parse a human disk-size into bytes: a plain number, or a number with a
/// `K`/`M`/`G`/`T` suffix (base 1024, optional trailing `B`, case-insensitive).
pub fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        bail!(t!("size.empty"));
    }
    let s = s.strip_suffix(['b', 'B']).unwrap_or(s);
    let (num, mult): (&str, u64) = match s.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => {
            let mult = match c.to_ascii_uppercase() {
                'K' => 1024,
                'M' => 1024 * 1024,
                'G' => 1024 * 1024 * 1024,
                'T' => 1024u64 * 1024 * 1024 * 1024,
                other => bail!(t!("size.unknown_suffix", suffix = other)),
            };
            (&s[..s.len() - 1], mult)
        }
        _ => (s, 1),
    };
    let value: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!(t!("size.invalid", input = input)))?;
    if value <= 0.0 {
        bail!(t!("size.not_positive"));
    }
    Ok((value * mult as f64) as u64)
}

pub async fn list(client: &ApiClient) -> Result<()> {
    let resp = client.list_subscriptions().await?;
    if resp.subscriptions.is_empty() {
        println!("{}", t!("subscription.none"));
        return Ok(());
    }

    let mut table = Builder::new();
    table.push_record([
        t!("subscription.col.peer").to_string(),
        t!("subscription.col.mirrored").to_string(),
        t!("subscription.col.files").to_string(),
        t!("subscription.col.synced").to_string(),
    ]);
    for s in &resp.subscriptions {
        let files = if s.skipped_count > 0 {
            t!(
                "subscription.files_over_quota",
                wanted = s.wanted_count,
                skipped = s.skipped_count
            )
            .to_string()
        } else {
            s.wanted_count.to_string()
        };
        table.push_record([
            // A short prefix is enough to identify it (and to `remove`).
            s.peer_id.chars().take(16).collect::<String>() + "…",
            format!(
                "{} / {}",
                human_size(s.used_bytes),
                human_size(s.quota_bytes)
            ),
            files,
            if s.last_synced_at == 0 {
                t!("subscription.synced_never").to_string()
            } else {
                t!("subscription.synced_yes").to_string()
            },
        ]);
    }

    println!("{}", table.build());
    Ok(())
}

pub async fn add(client: &ApiClient, peer: &str, quota: &str) -> Result<()> {
    let quota_bytes = match parse_size(quota) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    };
    match client.create_subscription(peer, quota_bytes).await {
        Ok(s) => {
            println!(
                "{}",
                color::success(&t!(
                    "subscription.added",
                    peer = s.peer_id,
                    quota = human_size(s.quota_bytes)
                ))
            );
        }
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn remove(client: &ApiClient, peer_id: &str, keep: bool) -> Result<()> {
    match client.delete_subscription(peer_id, keep).await {
        Ok(()) => {
            let msg = if keep {
                t!("subscription.removed_kept", peer = peer_id)
            } else {
                t!("subscription.removed_freed", peer = peer_id)
            };
            println!("{}", color::success(&msg))
        }
        Err(e) => {
            eprintln!("{}", color::error(&t!("common.error", msg = e)));
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
