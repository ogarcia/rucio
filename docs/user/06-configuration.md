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

Completed downloads and pinned content both live under one visible Rucio folder
(`downloads/` and `pins/` as siblings), so the files you host are easy to find
and browse — unlike the database and identity key, which stay in the hidden
app-state directories.

**Default:**

| Platform | Default path |
|---|---|
| Linux (desktop) | `$XDG_DOWNLOAD_DIR/rucio/downloads` or `~/Downloads/rucio/downloads` |
| macOS | `~/Downloads/rucio/downloads` |
| Linux (server / no XDG) | `~/rucio/downloads` |

---

### `storage.pin_dir`

Directory where pinned content that had to be fetched is stored and shared. Kept
separate from `download_dir` so it's clear which files the node hosts on purpose.
Pinned files are content you deliberately keep available (sometimes the only live
copy on the network), so they sit next to your downloads in a persistent,
visible place — not in a cache. See [Pinning](10-pinning.md).

```sh
rucio config set storage.pin_dir /mnt/data/rucio-pins
rucio config unset storage.pin_dir
```

A `pins` directory beside `downloads/` in the same Rucio content folder.

**Default:**

| Platform | Default path |
|---|---|
| Linux (desktop) | `$XDG_DOWNLOAD_DIR/rucio/pins` or `~/Downloads/rucio/pins` |
| macOS | `~/Downloads/rucio/pins` |
| Linux (server / no XDG) | `~/rucio/pins` |

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

### `storage.outboard_dir`

Directory for the **bao outboard cache** of completed shares — one small
`<root_hash>.obao` sidecar per served file (sharded into subdirectories by the
first hash byte). These are the BLAKE3 verified-streaming Merkle trees that let
the node serve any chunk with a self-verifying proof; they are **regenerable**
from the file at any time, so the directory is safe to wipe.

It defaults to a directory **beside** `temp_dir` (not inside it), so the
short-lived transfer scratch and this longer-lived cache stay independent. It is
configurable on its own because a large library's outboards add up (about
1/16384 of the total shared bytes — roughly 3 MB per 50 GB shared) and you may
want them on a different volume. In-progress downloads keep their partial
outboard next to the `.part` file in `temp_dir`, not here.

Most entries are written **lazily** — the first time a peer requests a chunk of
a file — so a freshly-started node with a small library may have an empty (or
absent until first served) outboard directory. Only files at or above 1 GiB get
their outboard persisted eagerly at index time. The directory itself is created
on startup.

```sh
rucio config set storage.outboard_dir /mnt/big/rucio-outboards
rucio config unset storage.outboard_dir     # back to <cache>/rucio/outboards
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.cache/rucio/outboards` |
| macOS | `~/Library/Caches/rucio/outboards` |

---

### `storage.shared_dirs`

A list of directories to share **declaratively**, in addition to any you add
through the app. Unlike directories added with `rucio share add` (or the web
UI) — which live only in the database — these are written in the config file,
so they:

- can be declared **while the daemon is stopped** (edit the file, then start);
- are **protected** (always re-shared on startup and not removable through the
  API — like the download directory); and
- **survive a database reset**.

They are reconciled on every startup: each is created on disk if missing and
indexed by the file watcher. This is the recommended way to pin a fixed share
layout for containers or reproducible/headless deployments.

```toml
[storage]
shared_dirs = ["/srv/media/music", "/srv/media/video"]
```

Or via the environment (comma-separated):

```sh
RUCIOD_SHARED_DIRS="/srv/media/music,/srv/media/video"
```

To stop sharing one of these, remove it from the config and restart — the API
won't delete a config-declared share. Directories added through the app are
unaffected and stay removable as usual.

**Default:** empty (no extra directories).

---

### `storage.database_path`

Path to the SQLite database holding all persistent state (shares, downloads,
peers, pins, …). This is app state, not content, so it lives in the data
directory rather than the content folder. Pre-1.0 the schema is volatile: if it
changes between versions the file must be deleted and the daemon restarted (see
[Storage](../design/04-storage.md)).

```sh
rucio config set storage.database_path /mnt/data/rucio.db
rucio config unset storage.database_path
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.local/share/rucio/rucio.db` |
| macOS | `~/Library/Application Support/rucio/rucio.db` |

---

### `storage.nodes_dat_path`

Path to an eMule `nodes.dat` file used to bootstrap the Kad2 network. **Optional
— when unset, eMule Kad search is disabled.** Point it at a `nodes.dat` you have
(or one fetched by the daemon) to enable Kad bootstrap and source search.

```sh
rucio config set storage.nodes_dat_path ~/.local/share/rucio/nodes.dat
rucio config unset storage.nodes_dat_path     # disable Kad bootstrap
```

**Default:** unset (no Kad bootstrap file).

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

### `network.download_limit_kbps`

Maximum download bandwidth used when fetching file chunks, in kilobytes per
second. Set to `0` for unlimited.

```sh
rucio config set network.download_limit_kbps 2000   # 2 MB/s cap
rucio config set network.download_limit_kbps 0      # unlimited
```

**Default:** `0` (unlimited)

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

### `network.temp_download_limit_kbps` / `network.temp_upload_limit_kbps`

A second, temporary set of bandwidth caps that apply only while the temporary
limit is **engaged** (toggled from the web panel or API — handy to throttle
quickly during a call or a game without editing your normal limits). When the
toggle is off, the regular `download_limit_kbps` / `upload_limit_kbps` apply.

```sh
rucio config set network.temp_download_limit_kbps 5120   # 5 MB/s while engaged
rucio config set network.temp_upload_limit_kbps   5120
```

**Default:** `5120` (5 MB/s) each.

---

### Bandwidth recommendations

Setting limits is recommended for anyone who does not want Rucio to saturate
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

### `node.identity_path`

Path to the Ed25519 keypair that is this node's permanent libp2p identity (its
PeerId derives from it). Generated on first start if absent; back it up to keep
the same PeerId across reinstalls. App state, so it lives in the config dir.

```sh
rucio config set node.identity_path /mnt/data/identity.key
rucio config unset node.identity_path
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.config/rucio/identity.key` |
| macOS | `~/Library/Application Support/rucio/identity.key` |

---

### `api.listen`

Address the HTTP API and web panel listen on (`host:port`). Keep it on
`127.0.0.1` unless you front it with a reverse proxy; bind `0.0.0.0` only behind
one. In a container, set it to `0.0.0.0:3003` and publish the port.

```sh
rucio config set api.listen 0.0.0.0:3003
```

**Default:** `127.0.0.1:3003`

---

### `api.token`

Optional bearer token for the API. When set, requests must send
`Authorization: Bearer <token>`. When unset (the default), the API has **no
authentication** — Rucio expects access control to be handled by a reverse proxy
(e.g. nginx `auth_basic`) when exposed beyond localhost.

```sh
rucio config set api.token "a-long-random-secret"
rucio config unset api.token            # disable token auth
```

**Default:** unset (no token).

---

### `api.base_path`

Path prefix the web panel is served under. Leave as `/` (the default) when Rucio
owns its own (sub)domain. Set it when reverse-proxying the panel into a
subdirectory, e.g. `example.com/rucio`, so the daemon injects a matching
`<base href>` and the panel resolves its assets and API/WebSocket URLs under the
prefix. The reverse proxy is expected to strip the prefix before forwarding (see
[Option D](01-installation.md#under-a-subpath-examplecomrucio)). `/rucio`,
`/rucio/` and `rucio` all normalise to `/rucio/`. Override via `RUCIOD_BASE_PATH`.

```sh
rucio config set api.base_path /rucio/
rucio config unset api.base_path        # back to the origin root
```

**Default:** `/`.

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

### `emule.identity_path`

Path to the persistent **eMule user-hash identity** — the 16-byte hash advertised
to eMule peers that keys your upload credit (and thus your queue priority). It is
generated on first start if absent; back it up to keep the credit you earn by
seeding across reinstalls. The libp2p mirror of this is `node.identity_path`.

Kept separate from `node.identity_path` so each can be relocated independently,
but defaults next to it (`emule_identity.key` in the same config dir) so both
node identities sit together out of the box.

```sh
rucio config set emule.identity_path /mnt/data/emule_identity.key
rucio config unset emule.identity_path
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.config/rucio/emule_identity.key` |
| macOS | `~/Library/Application Support/rucio/emule_identity.key` |

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

### `emule.temp_dir`

Directory for in-progress eMule (`.part`) downloads. Separate from the libp2p
`storage.temp_dir` so the eMule subsystem stays self-contained. Completed eMule
files are moved into `storage.download_dir` like any other download.

```sh
rucio config set emule.temp_dir /mnt/data/emule-tmp
rucio config unset emule.temp_dir
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.cache/rucio/emule-tmp` |
| macOS | `~/Library/Caches/rucio/emule-tmp` |

---

### `emule.external_ip`

The node's public IPv4 address, used in eMule/Kad messages and to determine
High-ID. Normally **auto-detected** (via UPnP or from peer responses); set it
manually only when auto-detection can't work — e.g. on a VPS with a known static
IP and UPnP disabled.

```sh
rucio config set emule.external_ip 203.0.113.7
rucio config unset emule.external_ip       # back to auto-detection
```

**Default:** unset (auto-detected).

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

### `emule.min_source_speed_kib_s`

Minimum sustained per-source download speed, in KiB/s. A peer that grants you
an upload slot but then transfers below this rate for a full window is dropped
mid-transfer in favour of another source, so a single slow peer cannot tie up
one of your `emule.download_slots_per_file` workers.

The drop only happens when **other sources are available**: if a file has a
single source, it is never dropped (a slow transfer still beats none). A short
grace period at the start of each piece avoids penalising TCP slow-start.

```sh
rucio config set emule.min_source_speed_kib_s 2
```

**Default:** `2`  (set to `0` to disable the check; override at runtime with
`RUCIOD_EMULE_MIN_SOURCE_SPEED_KIB_S`)

---

### `[notifications]`

The in-app notification centre and outbound webhooks are configured under the
`[notifications]` table (`enabled`, `downloads`, `system`) and
`[[notifications.webhooks]]` entries. These have their own guide —
see [Notifications](08-notifications.md), which covers the keys, the webhook
formats (generic / Discord / Slack / custom) and HMAC signing.

**Defaults:** `enabled = true`, `downloads = true`, `system = true`, no webhooks.

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
upnp                     = true
download_limit_kbps      = 0      # 0 = unlimited
upload_limit_kbps        = 0      # 0 = unlimited
temp_download_limit_kbps = 5120   # cap while the temporary limit is engaged (5 MB/s)
temp_upload_limit_kbps   = 5120   # cap while the temporary limit is engaged (5 MB/s)
max_upload_tasks         = 64     # concurrent chunk-upload tasks
bootstrap_peers = [
  "/ip4/203.0.113.1/tcp/4321/p2p/12D3KooWXXX...",
]
exclusive_bootstrap  = false       # true = use only the peers above (separate network)

[storage]
# Paths below show Linux defaults; see the sections above for macOS paths.
# download_dir   = "~/Downloads/rucio/downloads"
# pin_dir        = "~/Downloads/rucio/pins"
# temp_dir       = "~/.cache/rucio/tmp"
# outboard_dir   = "~/.cache/rucio/outboards"  # regenerable bao cache; relocate for large libraries
# nodes_dat_path = "~/.local/share/rucio/nodes.dat"  # omit to disable Kad bootstrap
# shared_dirs    = ["/srv/media"]  # protected shares declared here, survive a DB reset

[emule]
enabled            = true
tcp_port           = 4662
udp_port           = 4672
download_slots_per_file  = 5
max_upload_slots         = 4
max_concurrent_downloads = 3
min_source_speed_kib_s   = 2
# identity_path = "~/.config/rucio/emule_identity.key"  # user-hash credit identity
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
| `RUCIOD_BASE_PATH` | `api.base_path` | `/` | subpath, e.g. `/rucio/` |
| `RUCIOD_P2P_LISTEN` | `node.listen_addrs` | `/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321` | comma-separated multiaddrs |
| `RUCIOD_IDENTITY_PATH` | `node.identity_path` | `<config dir>/identity.key` | path |
| `RUCIOD_BASE_DIR` | *(portable mode)* | *(unset)* | absolute path — roots identity, DB, downloads, temp, outboards, pins and nodes.dat under one dir |
| `RUCIOD_DOWNLOAD_DIR` | `storage.download_dir` | platform default | path |
| `RUCIOD_TEMP_DIR` | `storage.temp_dir` | platform default | path |
| `RUCIOD_OUTBOARD_DIR` | `storage.outboard_dir` | `<cache>/rucio/outboards` | path |
| `RUCIOD_PIN_DIR` | `storage.pin_dir` | platform default | path |
| `RUCIOD_DB_PATH` | `storage.database_path` | platform default | path |
| `RUCIOD_SHARED_DIRS` | `storage.shared_dirs` | *(empty)* | comma-separated paths |
| `RUCIOD_BOOTSTRAP_PEERS` | `network.bootstrap_peers` | *(empty)* | comma-separated multiaddrs |
| `RUCIOD_DOWNLOAD_LIMIT_KBPS` | `network.download_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_UPLOAD_LIMIT_KBPS` | `network.upload_limit_kbps` | `0` (unlimited) | integer KB/s |
| `RUCIOD_TEMP_DOWNLOAD_LIMIT_KBPS` | `network.temp_download_limit_kbps` | `5120` (5 MB/s) | integer KB/s |
| `RUCIOD_TEMP_UPLOAD_LIMIT_KBPS` | `network.temp_upload_limit_kbps` | `5120` (5 MB/s) | integer KB/s |
| `RUCIOD_MAX_UPLOAD_TASKS` | `network.max_upload_tasks` | `64` | integer ≥1 |
| `RUCIOD_UPNP` | `network.upnp` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_NODES_DAT` | `storage.nodes_dat_path` | *(unset)* | path |
| `RUCIOD_EMULE_ENABLED` | `emule.enabled` | `true` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `RUCIOD_EMULE_IDENTITY_PATH` | `emule.identity_path` | `<config dir>/emule_identity.key` | path |
| `RUCIOD_EMULE_TEMP_DIR` | `emule.temp_dir` | platform default | path |
| `RUCIOD_EMULE_TCP_PORT` | `emule.tcp_port` | `4662` | integer 1–65535 |
| `RUCIOD_EMULE_UDP_PORT` | `emule.udp_port` | `4672` | integer 1–65535 |
| `RUCIOD_EMULE_NICK` | `emule.nick` | `rucio` | string |
| `RUCIOD_EXTERNAL_IP` | `emule.external_ip` | *(auto)* | IPv4 address |
| `RUCIOD_EMULE_DOWNLOAD_SLOTS_PER_FILE` | `emule.download_slots_per_file` | `5` | integer 1–50 |
| `RUCIOD_EMULE_MAX_UPLOAD_SLOTS` | `emule.max_upload_slots` | `4` | integer 1–50 |
| `RUCIOD_EMULE_MAX_CONCURRENT_DOWNLOADS` | `emule.max_concurrent_downloads` | `3` | integer 1–50 |
| `RUCIOD_EMULE_MIN_SOURCE_SPEED_KIB_S` | `emule.min_source_speed_kib_s` | `2` | integer (`0` = off) |

### Docker / container example

> The official `ghcr.io/ogarcia/rucio` image already sets `RUCIOD_UPNP=false`
> (UPnP rarely works from inside a container), so the `docker run` examples
> below don't repeat it. Set `RUCIOD_UPNP=true` to re-enable it. When you build
> your own image, set it yourself as shown.

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
