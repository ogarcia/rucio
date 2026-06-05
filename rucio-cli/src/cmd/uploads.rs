//! `rucio upload list [--watch]` — peers currently downloading from this node.
//!
//! The upload-side counterpart to `rucio download list`. Uploads are inherently
//! live and ephemeral, so `--watch` consumes the daemon's `UploadProgress`
//! WebSocket stream (full snapshot each tick, one empty snapshot when the last
//! upload ends) and re-renders until Ctrl-C.

use anyhow::Result;
use futures_util::StreamExt as _;
use rucio_core::api::uploads::{ActiveUpload, UploadNetwork};
use rucio_core::api::ws::WsEvent;
use tabled::{Table, Tabled};

use crate::client::ApiClient;
use crate::cmd::downloads::{human_size, truncate};
use crate::table_util::{fit_column, term_width};

// ANSI escape sequences for terminal control (match `download list --watch`).
const CLEAR_SCREEN: &str = "\x1b[2J\x1b[H";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";

pub async fn list(client: &ApiClient, watch: bool) -> Result<()> {
    if !watch {
        let resp = client.list_uploads().await?;
        print_table(resp.uploads);
        return Ok(());
    }

    print!("{HIDE_CURSOR}");
    let result = tokio::select! {
        r = watch_loop(client) => r,
        _ = tokio::signal::ctrl_c() => {
            println!();
            Ok(())
        }
    };
    print!("{SHOW_CURSOR}");
    result
}

async fn watch_loop(client: &ApiClient) -> Result<()> {
    let mut stream = match client.ws_stream().await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("WebSocket unavailable ({e}), falling back to HTTP polling");
            return watch_loop_http(client).await;
        }
    };

    // Snapshot immediately so the screen isn't blank before the first event.
    let initial = client
        .list_uploads()
        .await
        .map(|r| r.uploads)
        .unwrap_or_default();
    render(&initial);

    loop {
        match stream.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                if let Ok(WsEvent::UploadProgress(uploads)) = serde_json::from_str(&text) {
                    render(&uploads);
                }
            }
            Some(Ok(_)) => {} // ping/pong/binary — ignore
            Some(Err(e)) => {
                print!("{CLEAR_SCREEN}");
                println!("WebSocket error: {e}");
                println!("\nPress Ctrl-C to exit.");
            }
            None => {
                println!("\nDaemon disconnected.");
                return Ok(());
            }
        }
    }
}

/// HTTP polling fallback when the WebSocket is unavailable.
async fn watch_loop_http(client: &ApiClient) -> Result<()> {
    use tokio::time::{Duration, interval};

    let mut ticker = interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        match client.list_uploads().await {
            Ok(r) => render(&r.uploads),
            Err(e) => {
                print!("{CLEAR_SCREEN}");
                println!("Error contacting daemon: {e}");
                println!("\nPress Ctrl-C to exit.");
            }
        }
    }
}

fn render(uploads: &[ActiveUpload]) {
    print!("{CLEAR_SCREEN}");
    print_table(uploads.to_vec());
    println!("\nPress Ctrl-C to exit.");
}

fn print_table(uploads: Vec<ActiveUpload>) {
    if uploads.is_empty() {
        println!("No active uploads.");
        return;
    }

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "#")]
        idx: usize,
        #[tabled(rename = "Net")]
        net: String,
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Peer")]
        peer: String,
        #[tabled(rename = "Sent")]
        sent: String,
        #[tabled(rename = "Rate")]
        rate: String,
    }

    let rows: Vec<Row> = uploads
        .into_iter()
        .enumerate()
        .map(|(i, u)| Row {
            idx: i + 1,
            net: match u.network {
                UploadNetwork::Rucio => "rucio".to_string(),
                UploadNetwork::Emule => "eMule".to_string(),
            },
            name: u.file_name.unwrap_or_else(|| truncate(&u.file_hash, 16)),
            peer: truncate(&u.peer, 20),
            sent: human_size(u.bytes_sent),
            rate: if u.rate_bps == 0 {
                "-".to_string()
            } else {
                format!("{}/s", human_size(u.rate_bps))
            },
        })
        .collect();

    let max_name = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    let mut table = Table::new(rows);
    // Name is the 3rd column (index 2), same as in `download list`.
    fit_column(&mut table, 2, max_name, term_width());
    println!("{table}");
}
