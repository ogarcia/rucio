# Architecture

## Overview

rucio is structured as a Cargo workspace with three crates:

```
rucio/
  rucio-core/      # Shared types: API request/response structs, protocol types
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

## rucio-daemon

Runs as a long-lived async process (Tokio runtime). Its responsibilities:

1. **libp2p node** — manages swarm, handles peer discovery, serves and
   requests file chunks.
2. **Download engine** — tracks pending/active/completed downloads, manages
   `.part` files, resumes on restart.
3. **Watcher service** — monitors shared directories for changes and triggers
   re-indexing.
4. **REST API** — Axum HTTP server on `127.0.0.1:8742` (default).
5. **SQLite database** — persists shares, downloads, peers and configuration.

### Startup sequence

```
1. Load / create config.toml
2. Open SQLite database (create schema if missing)
3. Start libp2p swarm
4. Resume interrupted downloads (DownloadEngine::resume_interrupted)
5. Re-announce shared files to Kademlia (reannounce_shares)
6. Start watcher service for each shared directory
7. Start REST API server
8. Run main event loop
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

The daemon exposes a JSON REST API on `http://127.0.0.1:8742/api/v1/`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/status` | Node status, connectivity, peer count |
| `GET` | `/peers` | List of known peers |
| `GET` | `/shares` | List of shared files |
| `POST` | `/shares` | Add a shared directory |
| `DELETE` | `/shares/:hash` | Remove a shared directory |
| `GET` | `/shares/:hash/magnet` | Get magnet link for a shared file |
| `GET` | `/shares/indexing` | Number of files pending indexing |
| `GET` | `/downloads` | List downloads (with optional state filter) |
| `POST` | `/downloads` | Start a download |
| `DELETE` | `/downloads/:hash` | Cancel or remove a download from history |
| `GET` | `/config` | Current configuration |
| `PUT` | `/config` | Update configuration |

All endpoints return JSON. Error responses follow the shape
`{ "error": "message" }`.
