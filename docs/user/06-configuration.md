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

### `network.listen_port`

The TCP port the daemon listens on for incoming libp2p connections. This port
must be reachable from the internet for other peers to connect to you (HighID
operation). When running in a container, map this port with `-p 4321:4321`.

UPnP will attempt to open this port automatically when `network.upnp = true`.

```sh
rucio config set network.listen_port 4321
```

**Default:** `4321`

---

### `network.upnp`

Enable or disable automatic UPnP/IGD port mapping. When enabled, the daemon
asks the LAN router to forward:

- TCP `network.listen_port` (libp2p)
- UDP `emule.udp_port` (Kad2, only with the `emule-compat` feature)

Set to `false` if:
- You have already configured port forwarding manually on your router.
- You are running on a VPS / cloud server with a direct public IP (no NAT).
- You are running inside a container and the host handles forwarding.
- UPnP is disabled or unavailable on your network.

When `false`, the `external_ip` field in `rucio status` will always be empty,
and `rucio emule status` will report `Connectivity: unknown` unless
`emule.external_ip` is configured manually (or a peer has already connected
to us, in which case it reports `open`).

```sh
rucio config set network.upnp true
rucio config set network.upnp false
```

**Default:** `true`

---

### `network.upload_limit_kbps`

Maximum upload bandwidth used for serving file chunks to other peers,
in kilobytes per second. Set to `0` for unlimited.

```sh
rucio config set network.upload_limit_kbps 500    # 500 KB/s cap
rucio config set network.upload_limit_kbps 0      # unlimited
```

**Default:** `0` (unlimited)

---

### `network.download_limit_kbps`

Maximum download bandwidth used when fetching file chunks, in kilobytes per
second. Set to `0` for unlimited.

```sh
rucio config set network.download_limit_kbps 2000   # 2 MB/s cap
rucio config set network.download_limit_kbps 0      # unlimited
```

**Default:** `0` (unlimited)

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

### `emule.enabled`

Enable or disable the eMule / Kad2 subsystem at runtime.

Set to `false` to disable all eMule functionality without recompiling.
This is useful when running a fat binary that includes eMule support but
you do not want to use it — the eMule-related ports are not bound and no
eMule downloads can be started.

```sh
rucio config set emule.enabled false
rucio config set emule.enabled true
```

**Default:** `true`

---

### `emule.udp_port`

UDP port for the Kad2 socket used to communicate with the eMule network.
Only meaningful when the daemon is built with the `emule-compat` feature.

This port must be reachable from the internet for Kad2 bootstrap and source
search to work.  When running in a container, map it with `-p 4672:4672/udp`.

```sh
rucio config set emule.udp_port 4672
```

**Default:** `4672` (eMule standard)

---

### `emule.tcp_port`

TCP port on which ruciod listens for incoming eMule peer connections.
Only meaningful when the daemon is built with the `emule-compat` feature.

Having this port reachable from the internet makes the node **High-ID** on the
eMule network, which gives it higher priority in upload queues and results in
significantly faster downloads.  Without it the node runs as Low-ID.
When running in a container, map it with `-p 4662:4662/tcp`.

```sh
rucio config set emule.tcp_port 4662
```

**Default:** `4662` (eMule standard)

---

### `emule.max_parallel_peers`

Maximum number of simultaneous peer connections per eMule download.

Each active eMule download opens up to this many concurrent TCP connections
to different sources and fetches different file slices in parallel.  Higher
values can improve speed when many sources are available, but increase the
number of open sockets.  The effective concurrency is also bounded by the
number of discovered sources and the number of remaining slices.

```sh
rucio config set emule.max_parallel_peers 5
```

**Default:** `5`  **Range:** `1–50`

---

### `emule.max_upload_slots`

Maximum number of simultaneous eMule upload connections.

Rucio serves partially-downloaded eMule files back to other peers on the
network to build upload credit, which improves its queue priority and results
in faster download speeds.  This setting caps how many peers can download from
us at the same time.  When all slots are busy, incoming peers receive a queue
position message and retry automatically.

```sh
rucio config set emule.max_upload_slots 4
```

**Default:** `4`  **Range:** `1–50`

---

### `emule.max_concurrent_downloads`

Maximum number of eMule downloads that run at the same time.

When you queue more eMule downloads than this, the surplus wait in the
`queued` state until a running download finishes.  Because each active
download opens up to `emule.max_parallel_peers` TCP connections, this cap
bounds the total number of open sockets so a large queue cannot exhaust them.

```sh
rucio config set emule.max_concurrent_downloads 3
```

**Default:** `3`  **Range:** `1–50`

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
# token = "secret"               # enable API auth (disabled by default)

[network]
listen_port          = 4321
upnp                 = true
upload_limit_kbps    = 0         # 0 = unlimited
download_limit_kbps  = 0         # 0 = unlimited
bootstrap_peers = [
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooWXXX...",
]

[storage]
# Paths below show Linux defaults; see the sections above for macOS paths.
# download_dir   = "~/Downloads/rucio"
# temp_dir       = "~/.cache/rucio/tmp"
# nodes_dat_path = "~/.local/share/rucio/nodes.dat"  # omit to disable Kad bootstrap

[emule]
enabled            = true
udp_port           = 4672
tcp_port           = 4662
max_parallel_peers = 5
max_upload_slots   = 4
max_concurrent_downloads = 3
# temp_dir     = "~/.cache/rucio/emule-tmp"  # platform default
# external_ip  = "1.2.3.4"                   # auto-detected via UPnP or peer responses
```

---

## Environment variable overrides

All daemon settings can be overridden with environment variables **without
modifying the config file**. This is the recommended approach for containers
and automated deployments.

Environment variables are applied on top of the config file (or built-in
defaults if no file exists). An empty string is treated as unset and leaves
the file value untouched.

| Variable | Config key | Default | Format |
|---|---|---|---|
| `RUCIOD_API_LISTEN` | `api.listen` | `127.0.0.1:7070` | `host:port` |
| `RUCIOD_P2P_LISTEN` | `node.listen_addrs` | `0.0.0.0:4321, :::4321` | comma-separated multiaddrs |
| `RUCIOD_DOWNLOAD_DIR` | `storage.download_dir` | platform default | path |
| `RUCIOD_TEMP_DIR` | `storage.temp_dir` | platform default | path |
| `RUCIOD_DB_PATH` | `storage.database_path` | platform default | path |
| `RUCIOD_BOOTSTRAP_PEERS` | `network.bootstrap_peers` | *(empty)* | comma-separated multiaddrs |
| `RUCIOD_UPLOAD_LIMIT_KBPS` | `network.upload_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_DOWNLOAD_LIMIT_KBPS` | `network.download_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_UPNP` | `network.upnp` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_NODES_DAT` | `storage.nodes_dat_path` | *(unset)* | path |
| `RUCIOD_EMULE_ENABLED` | `emule.enabled` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_EMULE_TEMP_DIR` | `emule.temp_dir` | platform default | path |
| `RUCIOD_EMULE_UDP_PORT` | `emule.udp_port` | `4672` | integer 1–65535 |
| `RUCIOD_EMULE_TCP_PORT` | `emule.tcp_port` | `4662` | integer 1–65535 |
| `RUCIOD_EXTERNAL_IP` | `emule.external_ip` | *(auto)* | IPv4 address |
| `RUCIOD_EMULE_MAX_PARALLEL` | `emule.max_parallel_peers` | `5` | integer 1–50 |
| `RUCIOD_EMULE_MAX_UPLOAD_SLOTS` | `emule.max_upload_slots` | `4` | integer 1–50 |
| `RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS` | `emule.max_concurrent_downloads` | `3` | integer 1–50 |

### Docker / container example

```dockerfile
FROM debian:bookworm-slim
COPY ruciod /usr/local/bin/ruciod

ENV RUCIOD_API_LISTEN=0.0.0.0:7070
ENV RUCIOD_P2P_LISTEN=/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321
ENV RUCIOD_DOWNLOAD_DIR=/data/downloads
ENV RUCIOD_TEMP_DIR=/data/tmp
ENV RUCIOD_DB_PATH=/data/rucio.db
ENV RUCIOD_UPNP=false

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
  -e RUCIOD_UPNP=false \
  -v rucio-data:/data \
  -p 7070:7070 -p 4321:4321 \
  ghcr.io/yourorg/rucio
```

With eMule/Kad2 support:

```sh
docker run \
  -e RUCIOD_API_LISTEN=0.0.0.0:7070 \
  -e RUCIOD_DOWNLOAD_DIR=/data/downloads \
  -e RUCIOD_DB_PATH=/data/rucio.db \
  -e RUCIOD_NODES_DAT=/data/nodes.dat \
  -e RUCIOD_EMULE_UDP_PORT=40066 \
  -e RUCIOD_EMULE_TCP_PORT=40067 \
  -e RUCIOD_UPNP=false \
  -v rucio-data:/data \
  -p 7070:7070 -p 4321:4321 -p 40066:40066/udp -p 40067:40067/tcp \
  ghcr.io/yourorg/rucio
```

> **Note:** `-p 40066:40066` without a `/udp` suffix maps **both TCP and UDP**
> in Podman and Docker. This is correct for the Kad2 socket.

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
