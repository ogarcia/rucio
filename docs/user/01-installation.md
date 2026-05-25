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
rucio emule status
```

The field `eMule compatibility` will show `enabled` or `disabled`.

The build produces a single binary. The `ruciod` symlink is what triggers
daemon mode — see [Architecture](../design/01-architecture.md) for details.

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
