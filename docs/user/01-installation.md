# Installation

## Requirements

| Platform | Support |
|---|---|
| Linux (x86\_64, aarch64) | Full support |
| macOS (Apple Silicon, Intel) | Full support |
| Windows (WSL2) | Supported as Linux |
| Windows native | Not supported |

Rust 1.85 or later is required if building from source (2024 edition features
are used).

## Option A — Release binary

Download the pre-compiled binary for your platform from the
[Releases](../../../releases) page.

```sh
# Linux x86_64 example — adjust the filename for your platform
curl -Lo rucio https://github.com/anomalyco/rucio/releases/latest/download/rucio-linux-x86_64
install -m755 rucio /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

Verify the installation:

```sh
rucio --version
```

## Option B — Build from source

### Standard build

```sh
git clone https://github.com/anomalyco/rucio
cd rucio
cargo build --release
install -m755 target/release/rucio /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

### Build with eMule / Kad2 compatibility

The `emule-compat` feature adds support for downloading files via `ed2k://`
links from the eMule Kademlia network.  It is opt-in because it pulls in an
HTTP client (`reqwest`) and the `md4` crate, which increases binary size and
compile time.

```sh
cargo build --release --features emule-compat
install -m755 target/release/rucio /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

To check at runtime whether the running daemon was compiled with this feature:

```sh
rucio node emule status
```

The field `eMule compatibility` will show `enabled` or `disabled`.

The build produces a single binary. The `ruciod` symlink is what triggers
daemon mode — see [Architecture](../design/01-architecture.md) for details.

## Option C — Container image

Pre-built images are published to `ghcr.io/anomalyco/rucio`.

| Tag | Contents | Typical use |
|---|---|---|
| `latest` / `0.1.x` | `ruciod` daemon only | Production nodes, VPS, minimal footprint |
| `latest-web` / `0.1.x-web` | `ruciod` + embedded web panel | Single-host deployments with browser UI |
| `latest-full` / `0.1.x-full` | `ruciod` + `rucio` CLI + web panel | Development / debugging |
| `latest-bootstrap` / `0.1.x-bootstrap` | `rucio-bootstrap` with indexer | Dedicated DHT bootstrap node |

### Quick start

```sh
docker run -d --name ruciod \
  -v rucio-data:/var/lib/rucio \
  -p 4321:4321/tcp \
  ghcr.io/anomalyco/rucio:latest
```

### With web control panel

```sh
docker run -d --name ruciod \
  -e RUCIOD_API_LISTEN=0.0.0.0:7070 \
  -e RUCIOD_UPNP=false \
  -v rucio-data:/var/lib/rucio \
  -p 4321:4321/tcp \
  -p 7070:7070/tcp \
  -p 4672:4672/udp \
  ghcr.io/anomalyco/rucio:latest-web
```

Open `http://<host>:7070/` in a browser to access the panel.  The REST API
remains available at the same address under `/api/v1/`.

### With eMule / Kad2 support and web panel

```sh
docker run -d --name ruciod \
  -e RUCIOD_API_LISTEN=0.0.0.0:7070 \
  -e RUCIOD_UPNP=false \
  -v rucio-data:/var/lib/rucio \
  -p 4321:4321/tcp \
  -p 7070:7070/tcp \
  -p 4662:4662/tcp \
  -p 4672:4672/udp \
  ghcr.io/anomalyco/rucio:latest-full
```

### Volume ownership (UID / GID)

The container process runs as **UID 10001 / GID 10001** (user `rucio`).

**Named volumes** (created with `docker volume create`) are managed by the
container engine and work without any extra steps.

**Bind mounts** require the host directory to be owned by that UID before the
container starts, otherwise the daemon cannot write its config or database:

```sh
mkdir -p /srv/rucio
chown 10001:10001 /srv/rucio
docker run ... -v /srv/rucio:/var/lib/rucio ...
```

Alternatively pass `--user` with the numeric UID of a local user that owns
the directory — just make sure it matches the mount:

```sh
docker run --user "$(id -u):$(id -g)" -v /srv/rucio:/var/lib/rucio ...
```

See [Configuration](06-configuration.md) for the full list of environment
variables accepted by the container.

---

## Running as a system service (Linux / systemd)

Create `/etc/systemd/system/ruciod.service`:

```ini
[Unit]
Description=Rucio P2P daemon
After=network.target

[Service]
ExecStart=/usr/local/bin/ruciod
Restart=on-failure
User=YOUR_USER

[Install]
WantedBy=multi-user.target
```

```sh
systemctl daemon-reload
systemctl enable --now ruciod
```

## Default paths

rucio stores its files under standard platform directories.  
Run `rucio config show` at any time to see the actual paths in use.

| Path | Linux | macOS |
|---|---|---|
| Config | `~/.config/rucio/config.toml` | `~/Library/Application Support/rucio/config.toml` |
| Identity key | `~/.config/rucio/identity.key` | same parent dir |
| Database | `~/.local/share/rucio/rucio.db` | `~/Library/Application Support/rucio/rucio.db` |
| Downloads | `$XDG_DOWNLOAD_DIR/rucio` or `~/Downloads/rucio` | `~/Downloads/rucio` |
| Temp (parts) | `~/.cache/rucio/tmp` | `~/Library/Caches/rucio/tmp` |
| eMule nodes.dat | `~/.local/share/rucio/nodes.dat` | `~/Library/Application Support/rucio/nodes.dat` |

> **Note:** the database schema is volatile before a stable release.
> If rucio refuses to start after an upgrade, delete the database file and
> restart — downloads in progress will be lost but shares are re-indexed
> automatically.
