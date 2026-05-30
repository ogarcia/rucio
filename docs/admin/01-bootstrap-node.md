# Bootstrap node

A **bootstrap node** is a long-lived server with a fixed public IP and a
stable Peer ID. Its only job is to be a known entry point: when a new
`ruciod` starts up it dials one or more bootstrap nodes to discover the rest
of the DHT and connect to the network.

Any `ruciod` instance already acts as a passive DHT participant, so you do
not _need_ a dedicated bootstrap node for a small network. A dedicated one
becomes valuable when you want:

- A reliable, always-on entry point independent of end-user nodes going
  offline.
- A server that does not consume bandwidth serving files or running the API.
- A foundation for the optional [DHT indexer](02-indexer.md).

`rucio-bootstrap` is the dedicated binary for this role.

---

## Prerequisites

- A server with a **static public IP** or a DNS name that resolves to it.
- **Port 4321/TCP** open and reachable from the internet (the default rucio
  DHT port). No other ports are required for the seed role alone.
- At least 64 MB of RAM and a small amount of storage (identity key only,
  ~100 bytes).

---

## Installation

### From a release binary

```sh
# Download the latest release binary
curl -L https://github.com/ogarcia/rucio/releases/latest/download/rucio-bootstrap \
     -o /usr/local/bin/rucio-bootstrap
chmod +x /usr/local/bin/rucio-bootstrap
```

### From source

```sh
cargo install --path rucio-bootstrap --locked
# or, to include the optional indexer role:
cargo install --path rucio-bootstrap --features indexer --locked
```

### Container image

```sh
podman pull ghcr.io/ogarcia/rucio:latest-bootstrap
```

The `latest-bootstrap` image is compiled with `--features indexer` so the
indexer role is available at runtime — it is just disabled until you enable
it in the config. See [below](#container-deployment) for a full example.

---

## First run

`rucio-bootstrap` needs no config file — every setting has a built-in default,
so you can run it straight away and drive it with env vars or flags. The only
file it persists is the identity key, so the Peer ID stays stable across
restarts:

| File | Default path | Purpose |
|---|---|---|
| `identity.key` | `~/.local/share/rucio-bootstrap/identity.key` | Ed25519 keypair (stable Peer ID), generated on first run |
| `config.toml` | `~/.config/rucio-bootstrap/config.toml` | Optional. Written only when you run `--init-config` (see below) |

```
$ rucio-bootstrap
INFO rucio_bootstrap: No config file — using defaults (override with env vars / flags, or run --init-config to create one)
  path=/home/rucio/.config/rucio-bootstrap/config.toml
WARN rucio_net::identity: Identity file not found — generating new keypair
  path=/home/rucio/.local/share/rucio-bootstrap/identity.key
INFO rucio_net::identity: Generated new identity  peer_id=12D3KooW...
INFO rucio_bootstrap: Starting rucio-bootstrap  peer_id=12D3KooW...
INFO rucio_bootstrap: No bootstrap peers configured — running as a seed node (listen only)
INFO rucio_bootstrap: Ready — add one of these to a node's bootstrap_peers:
INFO rucio_bootstrap:   /ip4/0.0.0.0/tcp/4321/p2p/12D3KooW...   (replace 0.0.0.0 with the server's public IP)
```

> **Tip for servers:** the single most useful setting is the identity path —
> point `RUCIO_BOOTSTRAP_IDENTITY` at a persistent location and you rarely need
> anything else. A config file is entirely optional.

Copy the full `multiaddr` printed at startup (with your server's real IP
substituted for `0.0.0.0`) and add it to each client's `network.bootstrap_peers`:

```sh
rucio config set network.bootstrap_peers "/ip4/203.0.113.5/tcp/4321/p2p/12D3KooW..."
```

The Peer ID printed at startup will be the same on every subsequent restart
as long as `identity.key` is not deleted.

---

## Configuration

A config file is optional — the node runs on defaults without one. When you do
want one, write a documented template and edit it:

```sh
rucio-bootstrap --init-config
# writes ~/.config/rucio-bootstrap/config.toml (or $RUCIO_BOOTSTRAP_CONFIG),
# refusing to overwrite an existing file
```

Every value in the template is also the built-in default, so you only need to
keep the lines you actually change. CLI flags and env vars override the file.

### `[node]` section

| Key | Default | Description |
|---|---|---|
| `identity` | `~/.local/share/rucio-bootstrap/identity.key` | Path to the Ed25519 identity file. Keep this file backed up — losing it changes the node's Peer ID and breaks any bootstrap address pointing at it. |
| `listen` | `["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]` | Multiaddrs to listen on. |
| `bootstrap_peers` | `[]` | Existing nodes to join the DHT through. Leave empty to run as a **seed node** (other nodes will find you, not the other way around). |

### Minimal example

```toml
[node]
identity = "/srv/rucio-bootstrap/identity.key"
listen   = ["/ip4/0.0.0.0/tcp/4321", "/ip6/::/tcp/4321"]

# Optional: join an existing DHT before accepting connections.
# bootstrap_peers = ["/ip4/203.0.113.1/tcp/4321/p2p/12D3KooWXXX..."]
```

### CLI flags

Every config key can be overridden for a single invocation via a flag.
Flags always win over the config file.

```
rucio-bootstrap --help

Options:
  --config <PATH>          Config file path [env: RUCIO_BOOTSTRAP_CONFIG]
  --identity <PATH>        Identity key path [env: RUCIO_BOOTSTRAP_IDENTITY]
  --listen <ADDR,...>      Listen multiaddr(s) [env: RUCIO_BOOTSTRAP_LISTEN]
  --bootstrap-peer <ADDR>  Bootstrap peer multiaddr(s) [env: RUCIO_BOOTSTRAP_PEERS]
```

### Environment variables

| Variable | Config key | Default |
|---|---|---|
| `RUCIO_BOOTSTRAP_CONFIG` | *(config file path)* | `~/.config/rucio-bootstrap/config.toml` |
| `RUCIO_BOOTSTRAP_IDENTITY` | `node.identity` | `~/.local/share/rucio-bootstrap/identity.key` |
| `RUCIO_BOOTSTRAP_LISTEN` | `node.listen` | `/ip4/0.0.0.0/tcp/4321,/ip6/::/tcp/4321` |
| `RUCIO_BOOTSTRAP_PEERS` | `node.bootstrap_peers` | *(empty)* |
| `RUCIO_BOOTSTRAP_LOG` | *(log filter)* | `info` |

---

## Systemd service

Create `/etc/systemd/system/rucio-bootstrap.service`:

```ini
[Unit]
Description=Rucio DHT bootstrap node
Documentation=https://github.com/ogarcia/rucio/blob/master/docs/admin/01-bootstrap-node.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=rucio-bootstrap
Group=rucio-bootstrap
ExecStart=/usr/local/bin/rucio-bootstrap
Restart=on-failure
RestartSec=10
# Keep logs in the journal
StandardOutput=journal
StandardError=journal
# Give the process a stable working directory
WorkingDirectory=/var/lib/rucio-bootstrap
# Put data in a predictable location regardless of XDG defaults
Environment=RUCIO_BOOTSTRAP_CONFIG=/etc/rucio-bootstrap/config.toml
Environment=RUCIO_BOOTSTRAP_IDENTITY=/var/lib/rucio-bootstrap/identity.key
Environment=RUCIO_BOOTSTRAP_LOG=info

[Install]
WantedBy=multi-user.target
```

Set up the user and directories:

```sh
useradd -r -s /sbin/nologin -d /var/lib/rucio-bootstrap rucio-bootstrap
mkdir -p /var/lib/rucio-bootstrap /etc/rucio-bootstrap
chown rucio-bootstrap:rucio-bootstrap /var/lib/rucio-bootstrap

systemctl daemon-reload
systemctl enable --now rucio-bootstrap
journalctl -u rucio-bootstrap -f
```

---

## Container deployment

The container image runs as a non-root user (`rucio`, uid 10001) with
`WORKDIR /var/lib/rucio` as the home directory.  On first run the identity key
is created inside this directory, so **mount a named volume there** to keep a
stable Peer ID across container restarts (no config file is written — the node
runs on defaults plus the env vars below):

```sh
podman run -d \
  --name rucio-bootstrap \
  --restart unless-stopped \
  -p 4321:4321 \
  -v rucio-bootstrap-data:/var/lib/rucio \
  ghcr.io/ogarcia/rucio:latest-bootstrap
```

The Peer ID printed on first run is stable as long as the volume is
preserved.  To find it in the logs:

```sh
podman logs rucio-bootstrap 2>&1 | grep "peer_id"
```

### Customising via environment variables

```sh
podman run -d \
  --name rucio-bootstrap \
  --restart unless-stopped \
  -p 4321:4321 \
  -e RUCIO_BOOTSTRAP_PEERS="/ip4/203.0.113.1/tcp/4321/p2p/12D3KooW..." \
  -e RUCIO_BOOTSTRAP_LOG=debug \
  -v rucio-bootstrap-data:/var/lib/rucio \
  ghcr.io/ogarcia/rucio:latest-bootstrap
```

### Using a config file from the host

Generate a template on the host (`rucio-bootstrap --init-config`, or write it by
hand), then mount it read-only — the node never writes to it:

```sh
podman run -d \
  --name rucio-bootstrap \
  --restart unless-stopped \
  -p 4321:4321 \
  -e RUCIO_BOOTSTRAP_CONFIG=/config/config.toml \
  -v /etc/rucio-bootstrap:/config:ro \
  -v rucio-bootstrap-data:/var/lib/rucio \
  ghcr.io/ogarcia/rucio:latest-bootstrap
```

> The container image already includes the `indexer` feature compiled in.
> To activate the indexer see [DHT indexer](02-indexer.md).

---

## Backing up the identity key

The identity file is the only file that **must** be backed up.  Losing it
means the node gets a new Peer ID and any multiaddrs published to clients
stop working.

```sh
# Backup
cp ~/.local/share/rucio-bootstrap/identity.key identity.key.bak

# Restore (place it at the path configured in config.toml)
cp identity.key.bak ~/.local/share/rucio-bootstrap/identity.key
```
