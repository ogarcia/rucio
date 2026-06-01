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

### `network.upnp`

Enable or disable automatic UPnP/IGD port mapping. When enabled, the daemon
asks the LAN router to forward:

- TCP port from `node.listen_addrs` (libp2p)
- UDP `emule.udp_port` (Kad2, only with the `emule-compat` feature)

Set to `false` if:
- You have already configured port forwarding manually on your router.
- You are running on a VPS / cloud server with a direct public IP (no NAT).
- You are running inside a container and the host handles forwarding.
- UPnP is disabled or unavailable on your network.

When `false`, the `external_ip` field in `rucio node status` will always be empty,
and `rucio node emule status` will report `Connectivity: unknown` unless
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

### Bandwidth recommendations

Setting limits is recommended for anyone who does not want rucio to saturate
their connection while running in the background. The table below shows the
**80 % rule**: leave 20 % free for other traffic (web browsing, gaming, video
calls). Values are in KB/s.

> **Rule of thumb:** multiply your line speed in Mbps by 100 to get the 80 %
> upload and download limits in KB/s.  Example: 300 Mbps → `30000`.

| Line speed | `upload_limit_kbps` | `download_limit_kbps` |
|---:|---:|---:|
| 100 Mbps | `10000` | `10000` |
| 200 Mbps | `20000` | `20000` |
| 300 Mbps | `30000` | `30000` |
| 400 Mbps | `40000` | `40000` |
| 500 Mbps | `50000` | `50000` |
| 600 Mbps | `60000` | `60000` |
| 700 Mbps | `70000` | `70000` |
| 800 Mbps | `80000` | `80000` |
| 900 Mbps | `90000` | `90000` |
| 1000 Mbps | `100000` | `100000` |

**Asymmetric connections (ADSL, VDSL, cable):** apply the upload percentage to
your *actual upload speed*, not the download speed.  Example: a 600/30 Mbps
connection should use `upload_limit_kbps 24000` (80 % of 30 Mbps) and
`download_limit_kbps 60000` (80 % of 600 Mbps).

**Servers and VPS:** these have dedicated bandwidth and no other users sharing
the line, so you can safely use 90–95 % or leave the limit at `0` (unlimited).

---

### `network.max_upload_tasks`

Maximum number of concurrent chunk-upload tasks.  Each inbound chunk request
spawns an async task that reads the chunk from disk and waits for the bandwidth
throttle before sending.  This cap prevents excessive disk I/O contention when
many peers request chunks simultaneously.

Reducing this value on spinning-disk systems or low-RAM servers can smooth out
disk latency under heavy upload load.

```sh
rucio config set network.max_upload_tasks 32   # quieter disk, lower peak I/O
rucio config set network.max_upload_tasks 128  # more parallelism for fast NVMe
```

**Default:** `64`  **Minimum:** `1`

> Requires a daemon restart to take effect.

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

**Default:** empty. The peers you configure here are **added** to the built-in
public bootstrap node (`208.85.21.46:4321`, IPv4 + IPv6) — they do not replace
it — so a fresh node still reaches the public network out of the box. LAN
discovery via mDNS also works independently.

---

### `network.exclusive_bootstrap`

When `true`, the daemon bootstraps **only** from `network.bootstrap_peers` and
ignores the built-in list entirely. **Default:** `false` (additive).

Use it to run a **separate network** that doesn't touch the public one: point
every node at your own bootstrap peer(s) and set this flag on each. In the web
panel it's the **"Use only my bootstrap peers"** toggle in
**Settings → Network**. Like the other network settings, it takes effect after
a daemon restart.

```toml
[network]
bootstrap_peers = ["/ip4/203.0.113.1/tcp/4321/p2p/12D3KooW..."]
exclusive_bootstrap = true
```

> **Scope and privacy — read this.** `exclusive_bootstrap` is **not** a privacy
> or security boundary. The rucio network has no membership authentication and
> does not encrypt *who* may join; it only controls **which** bootstrap peers
> *this* node dials. Anyone who learns one of your peers' multiaddrs — by
> capturing a packet, reading a config file, or scanning — can bootstrap into
> your "separate" network just by adding it to their own `bootstrap_peers`. It
> does not hide, authenticate, or wall off anything. For a genuinely private
> deployment, isolate it at the network layer (VPN, firewall rules, or a
> private address range that outsiders cannot route to).

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

### `emule.nick`

The nickname advertised to eMule peers — the name other clients show for you in
their transfer lists ("downloading from <nick>"). Purely cosmetic; your credit
identity is a separate internal user hash, not the nick.

```sh
rucio config set emule.nick "rucio"
```

**Default:** `rucio`  (override at runtime with `RUCIOD_EMULE_NICK`)

---

### `emule.download_slots_per_file`

Number of simultaneous peer connections opened per eMule download.

Each active eMule download opens up to this many concurrent TCP connections
to different sources and fetches different file slices in parallel.  Higher
values can improve speed when many sources are available, but increase the
number of open sockets.  The effective concurrency is also bounded by the
number of discovered sources and the number of remaining slices.

```sh
rucio config set emule.download_slots_per_file 5
```

**Default:** `5`  **Range:** `1–50`

---

### `emule.max_upload_slots`

Maximum number of simultaneous eMule upload connections.

Rucio serves eMule files back to other peers — both while downloading and, as
a good Kad citizen, after a download completes — to build upload credit, which
improves its queue priority and results in faster download speeds.  Completed
files keep being shared until they are modified or removed on disk.  This
setting caps how many peers can download from us at the same time.  When all
slots are busy, incoming peers receive a queue position message and retry
automatically.

```sh
rucio config set emule.max_upload_slots 4
```

**Default:** `4`  **Range:** `1–50`

---

### `emule.max_concurrent_downloads`

Maximum number of eMule downloads that run at the same time.

When you queue more eMule downloads than this, the surplus wait in the
`queued` state until a running download finishes.  Because each active
download opens up to `emule.download_slots_per_file` TCP connections, this cap
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
listen = "127.0.0.1:3003"
# token = "secret"               # enable API auth (disabled by default)

[network]
upnp                 = true
upload_limit_kbps    = 0         # 0 = unlimited
download_limit_kbps  = 0         # 0 = unlimited
max_upload_tasks     = 64        # concurrent chunk-upload tasks
bootstrap_peers = [
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooWXXX...",
]
exclusive_bootstrap  = false       # true = use only the peers above (separate network)

[storage]
# Paths below show Linux defaults; see the sections above for macOS paths.
# download_dir   = "~/Downloads/rucio"
# temp_dir       = "~/.cache/rucio/tmp"
# nodes_dat_path = "~/.local/share/rucio/nodes.dat"  # omit to disable Kad bootstrap

[emule]
enabled            = true
tcp_port           = 4662
udp_port           = 4672
download_slots_per_file  = 5
max_upload_slots         = 4
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
| `RUCIOD_API_LISTEN` | `api.listen` | `127.0.0.1:3003` | `host:port` |
| `RUCIOD_P2P_LISTEN` | `node.listen_addrs` | `/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321` | comma-separated multiaddrs |
| `RUCIOD_DOWNLOAD_DIR` | `storage.download_dir` | platform default | path |
| `RUCIOD_TEMP_DIR` | `storage.temp_dir` | platform default | path |
| `RUCIOD_DB_PATH` | `storage.database_path` | platform default | path |
| `RUCIOD_BOOTSTRAP_PEERS` | `network.bootstrap_peers` | *(empty)* | comma-separated multiaddrs |
| `RUCIOD_UPLOAD_LIMIT_KBPS` | `network.upload_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_DOWNLOAD_LIMIT_KBPS` | `network.download_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_MAX_UPLOAD_TASKS` | `network.max_upload_tasks` | `64` | integer ≥1 |
| `RUCIOD_UPNP` | `network.upnp` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_NODES_DAT` | `storage.nodes_dat_path` | *(unset)* | path |
| `RUCIOD_EMULE_ENABLED` | `emule.enabled` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_EMULE_TEMP_DIR` | `emule.temp_dir` | platform default | path |
| `RUCIOD_EMULE_TCP_PORT` | `emule.tcp_port` | `4662` | integer 1–65535 |
| `RUCIOD_EMULE_UDP_PORT` | `emule.udp_port` | `4672` | integer 1–65535 |
| `RUCIOD_EXTERNAL_IP` | `emule.external_ip` | *(auto)* | IPv4 address |
| `RUCIOD_EMULE_DOWNLOAD_SLOTS_PER_FILE` | `emule.download_slots_per_file` | `5` | integer 1–50 |
| `RUCIOD_EMULE_MAX_UPLOAD_SLOTS` | `emule.max_upload_slots` | `4` | integer 1–50 |
| `RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS` | `emule.max_concurrent_downloads` | `3` | integer 1–50 |

### Docker / container example

```dockerfile
FROM debian:bookworm-slim
COPY ruciod /usr/local/bin/ruciod

ENV RUCIOD_API_LISTEN=0.0.0.0:3003
ENV RUCIOD_P2P_LISTEN=/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321
ENV RUCIOD_DOWNLOAD_DIR=/data/downloads
ENV RUCIOD_TEMP_DIR=/data/tmp
ENV RUCIOD_DB_PATH=/data/rucio.db
ENV RUCIOD_UPNP=false

VOLUME ["/data"]
EXPOSE 3003 4321

ENTRYPOINT ["ruciod"]
```

Or with `docker run`:

```sh
docker run \
  -e RUCIOD_API_LISTEN=0.0.0.0:3003 \
  -e RUCIOD_DOWNLOAD_DIR=/data/downloads \
  -e RUCIOD_DB_PATH=/data/rucio.db \
  -e RUCIOD_UPNP=false \
  -v rucio-data:/data \
  -p 3003:3003 -p 4321:4321 \
  ghcr.io/ogarcia/rucio
```

With eMule/Kad2 support:

```sh
docker run \
  -e RUCIOD_API_LISTEN=0.0.0.0:3003 \
  -e RUCIOD_DOWNLOAD_DIR=/data/downloads \
  -e RUCIOD_DB_PATH=/data/rucio.db \
  -e RUCIOD_NODES_DAT=/data/nodes.dat \
  -e RUCIOD_EMULE_UDP_PORT=40066 \
  -e RUCIOD_EMULE_TCP_PORT=40067 \
  -e RUCIOD_UPNP=false \
  -v rucio-data:/data \
  -p 3003:3003 -p 4321:4321 -p 40067:40067/tcp -p 40066:40066/udp \
  ghcr.io/ogarcia/rucio
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
