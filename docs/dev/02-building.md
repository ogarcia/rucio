# Building

## Workspace layout

```
rucio/
  rucio-core/      # Shared types — no I/O, no async runtime
  rucio-net/       # libp2p swarm and event loop
  rucio-emule/     # eMule / Kad2 protocol stack (emule-compat feature)
  rucio-daemon/    # Daemon binary: networking + REST API
  rucio-cli/       # CLI binary: thin HTTP client for the daemon
  rucio-web/       # Web control panel (Leptos CSR, compiled by trunk)
  rucio/           # Fat binary: dispatches to daemon or CLI based on argv[0]
  rucio-bootstrap/ # Dedicated DHT bootstrap node binary
```

`rucio-web` compiles to `wasm32-unknown-unknown` and is excluded from the
default workspace build (see `default-members` in the root `Cargo.toml`).
It is compiled separately by trunk — see [03-web-ui.md](03-web-ui.md).

---

## Feature flags

| Feature | Crate | What it adds |
|---|---|---|
| *(none)* | `rucio-daemon`, `rucio` | Core daemon: libp2p, download engine, REST API |
| `emule-compat` | `rucio-daemon`, `rucio` | eMule / Kad2 network, `ed2k://` downloads |
| `web-ui` | `rucio-daemon`, `rucio` | Embeds the `rucio-web/dist/` assets and serves them at `/` |

Features are independent and can be combined:

```sh
cargo build --release --features emule-compat,web-ui
```

---

## Build commands

All commands are run from the workspace root (`rucio/`).

### Standard (no optional features)

```sh
cargo build --release
```

Produces `target/release/rucio` and `target/release/ruciod` (the daemon is
also built as a separate binary; both are produced from the same workspace).

### With eMule / Kad2 support

```sh
cargo build --release --features emule-compat
```

Adds `rucio node emule` subcommands and support for `ed2k://` downloads.

### With embedded web UI

The frontend must be built by trunk before compiling the daemon.  See
[03-web-ui.md](03-web-ui.md) for the full frontend workflow.

```sh
# Step 1 — compile the Leptos frontend to WASM
cd rucio-web
trunk build --release
cd ..

# Step 2 — compile the daemon with the embedded assets
cargo build --release --features web-ui
```

The daemon will serve the control panel at `http://<listen-addr>/`.

### Full build (all features)

```sh
cd rucio-web && trunk build --release && cd ..
cargo build --release --features emule-compat,web-ui
```

---

## Running tests

```sh
# Unit and integration tests for all default-member crates
cargo test

# A specific crate
cargo test -p rucio-core
cargo test -p rucio-daemon

# Include emule-compat tests
cargo test --features emule-compat
```

Some daemon integration tests spin up a real SQLite database and use a
temporary directory; they are single-threaded by default via `serial_test`.

---

## Installing the binaries

```sh
cargo build --release            # or with --features as above

install -m755 target/release/rucio    /usr/local/bin/rucio
install -m755 target/release/ruciod   /usr/local/bin/ruciod
# or via symlink:
ln -sf /usr/local/bin/rucio /usr/local/bin/ruciod
```

The `ruciod` name is what triggers daemon mode.  See
[Architecture](../design/01-architecture.md) for details.
