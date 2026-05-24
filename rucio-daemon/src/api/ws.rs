//! WebSocket event stream handler.
//!
//! `GET /api/ws` upgrades the connection to a WebSocket and streams
//! [`WsEvent`] messages as JSON text frames.  The connection is server-push
//! only; messages from the client are silently discarded.

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use rucio_core::api::ws::WsEvent;
use tokio::sync::broadcast;

use super::AppState;

/// Upgrade handler for `GET /api/ws`.
///
/// Clients connect once and receive a stream of [`WsEvent`] JSON messages.
/// The connection stays open until the client disconnects or the daemon shuts
/// down.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state.ws_tx.subscribe()))
}

async fn handle_socket(mut socket: WebSocket, mut rx: broadcast::Receiver<WsEvent>) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                let text = match serde_json::to_string(&event) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!("Failed to serialize WsEvent: {e}");
                        continue;
                    }
                };
                if socket.send(Message::Text(text.into())).await.is_err() {
                    // Client disconnected.
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // The client is too slow; some events were dropped.
                // Continue without disconnecting — next events will be fresh.
                tracing::debug!("WebSocket client lagged, dropped {n} event(s)");
            }
            Err(broadcast::error::RecvError::Closed) => {
                // Daemon is shutting down.
                break;
            }
        }
    }
}
