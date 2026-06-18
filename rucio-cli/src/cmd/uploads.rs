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
use rust_i18n::t;
use tabled::builder::Builder;

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
                println!("{}", t!("common.ws_error", msg = e));
                println!("\n{}", t!("common.press_ctrl_c"));
            }
            None => {
                println!("\n{}", t!("common.daemon_disconnected"));
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
                println!("{}", t!("common.daemon_contact_error", msg = e));
                println!("\n{}", t!("common.press_ctrl_c"));
            }
        }
    }
}

fn render(uploads: &[ActiveUpload]) {
    print!("{CLEAR_SCREEN}");
    print_table(uploads.to_vec());
    println!("\n{}", t!("common.press_ctrl_c"));
}

fn print_table(uploads: Vec<ActiveUpload>) {
    if uploads.is_empty() {
        println!("{}", t!("upload.none"));
        return;
    }

    let rows: Vec<[String; 6]> = uploads
        .into_iter()
        .enumerate()
        .map(|(i, u)| {
            [
                (i + 1).to_string(),
                match u.network {
                    UploadNetwork::Rucio => "rucio".to_string(),
                    UploadNetwork::Emule => "eMule".to_string(),
                },
                u.file_name.unwrap_or_else(|| truncate(&u.file_hash, 16)),
                truncate(&u.peer, 20),
                human_size(u.bytes_sent),
                if u.rate_bps == 0 {
                    "-".to_string()
                } else {
                    format!("{}/s", human_size(u.rate_bps))
                },
            ]
        })
        .collect();

    let max_name = rows.iter().map(|r| r[2].chars().count()).max().unwrap_or(0);

    let mut builder = Builder::new();
    builder.push_record([
        t!("upload.col.num").to_string(),
        t!("upload.col.net").to_string(),
        t!("upload.col.name").to_string(),
        t!("upload.col.peer").to_string(),
        t!("upload.col.sent").to_string(),
        t!("upload.col.rate").to_string(),
    ]);
    for r in rows {
        builder.push_record(r);
    }
    let mut table = builder.build();
    // Name is the 3rd column (index 2), same as in `download list`.
    fit_column(&mut table, 2, max_name, term_width());
    println!("{table}");
}
