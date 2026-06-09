# Architecture

## Overview

Rucio is structured as a Cargo workspace with four crates:

```
rucio/
  rucio-core/      # Shared types: API request/response structs, protocol types
  rucio-emule/     # eMule/Kad2 protocol implementation (feature-gated)
  rucio-daemon/    # The node: libp2p networking, download engine, REST API
  rucio-cli/       # The command-line client: speaks to the daemon over HTTP
```

The build produces a **single binary**. Whether it runs as a daemon or as a
CLI client is determined at startup by inspecting `argv[0]`:

```rust
if argv[0].contains("ruciod") {
    run_daemon();
} else {
    run_cli();
}
```

This means the user installs one file and creates one symlink:

```sh
install -m755 rucio /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

### Why a fat binary?

- Single artifact to distribute and update.
- Shared code (types, hashing) is in `rucio-core` and compiled once.
- The CLI needs to know the same API types as the daemon; sharing a crate
  guarantees they stay in sync without code generation.

## rucio-core

Contains only pure types and functions — no I/O, no async runtime. This crate
is also publishable to crates.io for third-party integrations.

Key modules:

| Module | Contents |
|---|---|
| `api::*` | Request/response structs for every REST endpoint |
| `protocol::magnet` | `MagnetLink` type, `Display` (URL-encodes name), `parse_magnet` |
| `protocol::hashing` | `hash_file`, `collect_files`, `detect_mime`, `FileHash` |
| `protocol::node` | `NodeClass` enum: `HighId`, `LowId`, `Unknown` |
| `protocol::transfer` | Chunk layout constants, `DEFAULT_CHUNK_SIZE` (256 KiB) |
| `logging` | `init(prefix)` — centralised `tracing` subscriber setup |

## rucio-emule

Optional crate compiled only with the `emule-compat` feature flag. Contains
the full eMule/Kad2 protocol stack:

| Module | Contents |
|---|---|
| `kad::packet` | Packet encoder/decoder; handles `0xe4` (Kad2) and `0xe5` (packed/zlib) opcodes |
| `kad::routing` | Routing table; `parse_nodes_dat` |
| `kad::task` | `KadTask` — owns the UDP socket, runs iterative bootstrap, keepalive, re-bootstrap |

`rucio-emule` has no dependency on `rucio-daemon`. It communicates via
`KadHandle` (a channel-based API) so it can be tested in isolation.

## rucio-daemon

Runs as a long-lived async process (Tokio runtime). Its responsibilities:

1. **libp2p node** — manages swarm, handles peer discovery, serves and
   requests file chunks.
2. **Download engine** — tracks pending/active/completed downloads, manages
   `.part` files, resumes on restart.
3. **Watcher service** — monitors shared directories for changes and triggers
   re-indexing.
4. **REST API** — Axum HTTP server on `127.0.0.1:3003` (default).
5. **SQLite database** — persists shares, downloads, peers and configuration.
6. **UPnP task** — optional background task that maps TCP and UDP ports via
   IGD/UPnP on the LAN router (see `network.upnp`).
7. **KadTask** *(emule-compat)* — persistent Tokio task owning the Kad2 UDP
   socket exclusively; runs iterative bootstrap and keepalive.

### Startup sequence

```
1. Load / create config.toml
2. Open SQLite database (create schema if missing)
3. Start libp2p swarm
4. Start UPnP task (if network.upnp = true)
5. Resume interrupted downloads (DownloadEngine::resume_interrupted)
6. Re-announce shared files to Kademlia (reannounce_shares)
7. Start watcher service for each shared directory
8. Start KadTask and bootstrap Kad2 (emule-compat only)
9. Start REST API server
10. Run main event loop
```

### AppState

All daemon subsystems share an `Arc<AppState>`:

```
AppState {
    db:              Arc<Mutex<Connection>>
    node_tx:         mpsc::Sender<NodeCommand>
    node_rx:         broadcast::Receiver<NodeEvent>
    download_engine: Arc<DownloadEngine>
    config:          Arc<RwLock<DaemonConfig>>
    indexing_count:  Arc<AtomicUsize>
    external_ip:     Arc<RwLock<Option<String>>>   // set by UPnP task
    kad_handle:      Option<KadHandle>              // emule-compat only
}
```

## rucio-cli

A thin HTTP client. It serializes command-line arguments into API requests,
sends them to the daemon, and formats the responses for the terminal.

Key design choices:

- **No local path validation.** The daemon may be running on a different
  machine. Paths are passed as strings and validated by the daemon.
- **No local state.** The CLI holds no persistent state; everything lives in
  the daemon.
- **Human-readable output by default.** No JSON flag yet; designed for
  interactive use.

## REST API

The daemon exposes a JSON REST API on `http://127.0.0.1:3003/api/v1/`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/status` | Node status, connectivity, peer count, external IP |
| `GET` | `/peers` | List of known peers |
| `GET` | `/shares` | List of shared files |
| `POST` | `/shares` | Add a shared directory |
| `DELETE` | `/shares` | Remove a shared directory by path |
| `DELETE` | `/shares/:hash` | Remove a specific shared file by hash |
| `GET` | `/shares/:hash/magnet` | Get magnet link for a shared file |
| `GET` | `/shares/indexing` | Number of files pending indexing |
| `GET` | `/downloads` | List downloads (with optional state filter) |
| `POST` | `/downloads` | Start a Rucio download (magnet or hash) |
| `POST` | `/downloads/ed2k` | Start an eMule download (ed2k:// link) — emule-compat |
| `POST` | `/downloads/:id/cancel` | Cancel an active download |
| `POST` | `/downloads/:id/pause` | Pause an active download (keeps progress) |
| `POST` | `/downloads/:id/resume` | Resume a paused download |
| `DELETE` | `/downloads/:id` | Remove a completed/cancelled entry from history |
| `POST` | `/search` | Start a keyword search, returns a query ID |
| `GET` | `/search/:query_id` | Poll accumulated search results |
| `GET` | `/config` | Current configuration |
| `PUT` | `/config` | Update configuration |
| `GET` | `/config/notifications` | Notification toggles |
| `PUT` | `/config/notifications` | Update notification toggles (applied live) |
| `GET` | `/config/notifications/webhooks` | List outbound webhooks |
| `PUT` | `/config/notifications/webhooks` | Replace the webhook list (applied live) |
| `POST` | `/config/notifications/webhooks/test` | Send a test delivery to a webhook |
| `GET` | `/notifications` | Recent notifications + unread count |
| `POST` | `/notifications/read` | Mark all notifications read |
| `DELETE` | `/notifications` | Clear all notifications |
| `DELETE` | `/notifications/:id` | Delete one notification |
| `GET` | `/metrics` | Transfer metrics (bytes up/down, active chunks) |
| `GET` | `/emule/status` | Kad2 contact count and nodes.dat status — emule-compat |
| `GET` | `/health` | Liveness probe (always `200 OK`) |

All endpoints return JSON. Error responses follow the shape
`{ "error": "message" }`.

## Notification centre

The daemon keeps a small, persisted log of user-facing events — a download
finishing, indexing completing — surfaced in the web UI's bell drawer.

- Records live in the `notifications` table (`kind` + `title` + `body` +
  optional `ref_key` resource reference + `created_at` + `read`). The model is
  deliberately generic so the same rows also feed outbound webhooks (below).
  Each insert prunes the table to the most recent 200 rows.
- All recording goes through one `Notifier` (DB pool + WS sender + live toggles
  + webhook list + HTTP client). Call sites — the download engine, the eMule
  task, the indexing tick — just call `notify(kind, title, body, ref)`; gating,
  persistence, the `WsEvent::Notification` push and the webhook fan-out happen
  there.
- Two kinds today: **download** (a Rucio or eMule download completed) and
  **system** (e.g. indexing finished — fired on the pending-count → 0 edge via a
  latch, so a sub-second batch still notifies). Per-kind toggles plus a master
  switch (`[notifications]` in `config.toml`) are honoured at the source: a
  disabled kind is never persisted or pushed. Toggles apply live (no restart).
- New notifications are pushed over the WebSocket bus so the bell badge updates
  without polling; the client seeds its history from `GET /notifications` on
  load.

### Outbound webhooks

Each notification that passes the gates is also POSTed to every configured
webhook whose `kinds` accept it. Delivery is **best-effort and never queued**: a
short timeout with two retries, then give up — the row still lives in the centre
(the source of truth). The fan-out is spawned per webhook so a slow endpoint
never blocks the notifier.

Configured under `[[notifications.webhooks]]` in `config.toml`:

```toml
[[notifications.webhooks]]
url     = "https://discord.com/api/webhooks/…"
format  = "discord"        # generic | discord | slack | telegram | ntfy | custom
kinds   = ["download"]     # empty = all kinds
secret  = "shared-secret"  # optional: body signed as X-Rucio-Signature: sha256=<hex>

[[notifications.webhooks]]
format = "telegram"
# chat id rides in the query; it's moved into the JSON body automatically.
url    = "https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<CHAT_ID>"

[[notifications.webhooks]]
format = "ntfy"
url    = "https://ntfy.sh/my-rucio-topic"   # topic is the URL

[[notifications.webhooks]]
url          = "https://example.com/hook"
format       = "custom"
template     = '{"msg":"{title} — {body}"}'   # {title} {body} {kind} {ref} {id} {created_at}
content_type = "application/json"
```

Formats:

- **generic** — the `NotificationDto` as JSON (the receiver parses it).
- **discord** / **slack** — their native `{"content":…}` / `{"text":…}` shapes.
- **telegram** — Telegram Bot API `sendMessage`. Put the bot token in the path
  and the chat id in the query; the chat id is moved into the JSON body
  (`{"chat_id":…,"text":…}`) since Telegram doesn't combine query params with a
  JSON body.
- **ntfy** — the topic is the URL, the body is the plain-text message, and the
  title is sent as the `Title` header.
- **custom** — a user-supplied body template. It substitutes only the exact
  known tokens in one pass, so a structural JSON `{` (or a value that happens to
  contain `{body}`) is never mis-expanded. For a JSON `content_type`, values are
  JSON-string-escaped (without the surrounding quotes the template provides), so
  a title with quotes can't produce invalid JSON.

The **generic** payload (Content-Type `application/json`, plus `User-Agent:
rucio` and the optional `X-Rucio-Signature` header):

```json
{
  "id": 42,
  "kind": "download",
  "title": "Download complete",
  "body": "ubuntu-24.04.iso",
  "ref_key": "9f86d0818…",
  "created_at": 1781000861,
  "read": false
}
```

`kind` is `download` or `system`; `ref_key` is the resource reference (a blake3
hex for downloads) and is omitted when absent.

Webhooks can be edited from the web UI (Settings → Notifications) or directly in
`config.toml`. The UI loads them from `GET /config/notifications/webhooks` and
saves the whole list with `PUT /config/notifications/webhooks`, which applies
them live (no restart) and persists them. The runtime list lives behind a lock
in the
notifier, so a save takes effect on the next notification.
