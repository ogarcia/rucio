# Configuration

## Viewing current configuration

```sh
rucio config show
```

This prints all settings and their current values, including the resolved
paths for all directories.

## Setting a value

```sh
rucio config set <key> <value>
```

## Unsetting a value

```sh
rucio config unset <key>
```

Unsetting a key reverts it to its default value. Not all keys can be unset —
see the table below.

---

## Available keys

### `storage.download_dir`

Directory where completed downloads are placed.

```sh
rucio config set storage.download_dir /mnt/data/downloads
rucio config unset storage.download_dir     # revert to platform default
```

**Default:**

| Platform | Default path |
|---|---|
| Linux (desktop) | `$XDG_DOWNLOAD_DIR/rucio` or `~/Downloads/rucio` |
| macOS | `~/Downloads/rucio` |
| Linux (server / no XDG) | `~/rucio` |

---

### `storage.temp_dir`

Directory where incomplete downloads are stored as `.part` files while
transferring. Must be on the same filesystem as `download_dir` for an
efficient rename on completion; if they are on different filesystems rucio
falls back to a copy-then-delete.

```sh
rucio config set storage.temp_dir /mnt/data/.rucio-tmp
rucio config unset storage.temp_dir
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.cache/rucio/tmp` |
| macOS | `~/Library/Caches/rucio/tmp` |

---

### `network.bootstrap_peers`

List of multiaddrs used to bootstrap into the DHT when no local peers are
found via mDNS. Each address must include the peer ID. This key appends to
the list; use `unset` with the exact value to remove one entry.

```sh
rucio config set network.bootstrap_peers \
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooW..."
rucio config unset network.bootstrap_peers \
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooW..."
```

**Default:** built-in list of public bootstrap nodes (empty until infrastructure
is available — LAN discovery via mDNS still works without this).

---

### `node.listen_addrs`

List of multiaddrs the daemon listens on for P2P connections. This key
appends to the list; use `unset` with the exact value to remove one entry.

```sh
rucio config set node.listen_addrs "/ip4/0.0.0.0/tcp/4321"
rucio config unset node.listen_addrs "/ip6/::/tcp/4321"
```

**Default:** `/ip4/0.0.0.0/tcp/4321` and `/ip6/::/tcp/4321` (all interfaces,
port 4321).

---

## Configuration file

The configuration is stored as TOML and is loaded at daemon startup.
Changes made with `rucio config set` are written back to this file and take
effect after a daemon restart (unless stated otherwise below).

| Platform | Path |
|---|---|
| Linux | `~/.config/rucio/config.toml` |
| macOS | `~/Library/Application Support/rucio/config.toml` |

You can edit the file directly with a text editor. A custom path can be
passed at startup:

```sh
ruciod --config /etc/rucio/config.toml
# or via environment variable
RUCIOD_CONFIG=/etc/rucio/config.toml ruciod
```

### Example `config.toml`

```toml
[node]
listen_addrs = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

[api]
listen = "127.0.0.1:7070"

[network]
bootstrap_peers = [
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooWXXX...",
]

[storage]
download_dir = "/mnt/data/downloads"
temp_dir     = "/mnt/data/.rucio-tmp"
```

---

## Environment variable overrides

All daemon settings can be overridden with environment variables **without
modifying the config file**. This is the recommended approach for containers
and automated deployments.

Environment variables are applied on top of the config file (or built-in
defaults if no file exists). An empty string is treated as unset and leaves
the file value untouched.

| Variable | Config key | Format |
|---|---|---|
| `RUCIOD_API_LISTEN` | `api.listen` | `host:port` |
| `RUCIOD_P2P_LISTEN` | `node.listen_addrs` | comma-separated multiaddrs |
| `RUCIOD_DOWNLOAD_DIR` | `storage.download_dir` | path |
| `RUCIOD_TEMP_DIR` | `storage.temp_dir` | path |
| `RUCIOD_DB_PATH` | `storage.database_path` | path |
| `RUCIOD_BOOTSTRAP_PEERS` | `network.bootstrap_peers` | comma-separated multiaddrs |

### Docker / container example

```dockerfile
FROM debian:bookworm-slim
COPY ruciod /usr/local/bin/ruciod

ENV RUCIOD_API_LISTEN=0.0.0.0:7070
ENV RUCIOD_P2P_LISTEN=/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321
ENV RUCIOD_DOWNLOAD_DIR=/data/downloads
ENV RUCIOD_TEMP_DIR=/data/tmp
ENV RUCIOD_DB_PATH=/data/rucio.db

VOLUME ["/data"]
EXPOSE 7070 4321

ENTRYPOINT ["ruciod"]
```

Or with `docker run`:

```sh
docker run \
  -e RUCIOD_API_LISTEN=0.0.0.0:7070 \
  -e RUCIOD_DOWNLOAD_DIR=/data/downloads \
  -e RUCIOD_DB_PATH=/data/rucio.db \
  -v rucio-data:/data \
  -p 7070:7070 -p 4321:4321 \
  ghcr.io/yourorg/rucio
```

### Comma-separated list variables

`RUCIOD_P2P_LISTEN` and `RUCIOD_BOOTSTRAP_PEERS` accept multiple values
separated by commas. Surrounding whitespace around each entry is trimmed:

```sh
export RUCIOD_P2P_LISTEN="/ip4/0.0.0.0/tcp/4321, /ip6/::/tcp/4321"
export RUCIOD_BOOTSTRAP_PEERS="\
  /ip4/203.0.113.1/tcp/4321/p2p/12D3KooWAAA,\
  /ip4/203.0.113.2/tcp/4321/p2p/12D3KooWBBB"
```

---

## Precedence

Settings are resolved in this order (highest wins):

1. **Environment variables** (`RUCIOD_*`)
2. **Config file** (`config.toml`)
3. **Built-in defaults**
