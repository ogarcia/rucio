# Installation

## Requirements

Rucio runs natively on Linux (x86\_64, aarch64), macOS (Apple Silicon, Intel)
and Windows (x86\_64). On Linux and macOS it ships as a single daemon + CLI
binary (Options A–D below); on Windows it ships as a portable desktop app — see
[Windows](#windows--portable-desktop-app).

Rust 1.85 or later is required if building from source (2024 edition features
are used).

## Option A — Release binary

Download the archive for your platform from the [Releases](../../../releases)
page. Each release ships a `rucio-<version>-<target>.tar.gz` per target:

| Platform | Archive |
|---|---|
| Linux x86\_64 | `rucio-<version>-x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64 | `rucio-<version>-aarch64-unknown-linux-musl.tar.gz` |
| macOS Intel | `rucio-<version>-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `rucio-<version>-aarch64-apple-darwin.tar.gz` |

Each archive contains the `rucio` binary plus a `ruciod` symlink to it (the
name that triggers daemon mode). Unpack it and place both on your PATH:

```sh
# Linux x86_64 example — adjust the filename for your platform
tar -xzf rucio-*-x86_64-unknown-linux-musl.tar.gz
install -m755 rucio /usr/local/bin/rucio
cp -P ruciod /usr/local/bin/ruciod   # relative symlink -> rucio
```

The release binary is the complete client — daemon, CLI, embedded web panel and
eMule support all built in.

Verify the installation:

```sh
rucio --version
```

## Windows — portable desktop app

Windows ships as a self-contained desktop app rather than a daemon + CLI pair.
Download `rucio-<version>-windows-x86_64-portable.zip` from the
[Releases](../../../releases) page and extract it into a folder you can write to
— your Desktop or Documents, **not** Program Files. Then run `Rucio.exe`.

It is the complete client in one window: the daemon, the eMule support and the
web panel all run embedded inside a WebView2 window. There is nothing to install
and no `ruciod` symlink to set up.

The build is fully **portable** — all of its state (settings, database,
downloads and node identity) lives in the same folder as `Rucio.exe`, not under
`%APPDATA%`. Moving the folder moves your whole node with it; deleting the folder
removes everything.

On the first run the Windows Firewall will ask whether to allow Rucio to
communicate on your networks:

- **Allow** — full connectivity, including incoming connections (High-ID).
- Decline — downloads still work, but the node runs as Low-ID (no incoming
  connections).

The app needs the Microsoft Edge **WebView2** runtime, which is preinstalled on
Windows 10 and 11.

## Option B — Build from source

### Standard build

```sh
git clone https://github.com/ogarcia/rucio
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

Pre-built images are published to `ghcr.io/ogarcia/rucio`.

| Tag | Contents | Typical use |
|---|---|---|
| `latest` / `0.1.x` | Complete: `rucio` (daemon + CLI) + embedded web panel + eMule | **Default.** Desktop and single-host servers |
| `latest-headless` / `0.1.x-headless` | `ruciod` daemon only — no web, no CLI | Servers/VPS controlled via the API, smallest footprint |
| `latest-cli` / `0.1.x-cli` | Standalone `rucio-cli` only — no daemon | Driving a remote daemon from another host/CI |
| `latest-bootstrap` / `0.1.x-bootstrap` | `rucio-bootstrap` with indexer | Dedicated DHT bootstrap node |

> `latest` is the full client — if you came from an earlier tag where `latest`
> was the bare daemon, that's now `latest-headless`.

### Quick start (complete)

The default image runs the daemon, serves the web panel, and lets you `exec`
in to use the `rucio` CLI:

```sh
docker run -d --name rucio \
  -e RUCIOD_API_LISTEN=0.0.0.0:3003 \
  -v rucio-data:/var/lib/rucio \
  -p 3003:3003/tcp \
  -p 4321:4321/tcp \
  -p 4662:4662/tcp \
  -p 4672:4672/udp \
  ghcr.io/ogarcia/rucio:latest
```

Open `http://<host>:3003/` in a browser for the panel; the REST API is at the
same address under `/api/v1/`. Run CLI commands with
`docker exec rucio rucio <command>`.

### Headless daemon (no web panel)

For servers where the panel isn't needed:

```sh
docker run -d --name ruciod \
  -v rucio-data:/var/lib/rucio \
  -p 4321:4321/tcp \
  ghcr.io/ogarcia/rucio:latest-headless
```

### Standalone CLI (drive a remote daemon)

A tiny image with just the `rucio-cli` client — no daemon, no libp2p. Point it
at a daemon's REST API with `RUCIO_API` and append any CLI command:

```sh
docker run --rm \
  -e RUCIO_API=http://daemon-host:3003 \
  ghcr.io/ogarcia/rucio:latest-cli \
  download list
```

The entrypoint is the CLI itself, so everything after the image name is passed
straight through (`download list`, `search add "query"`, `node status`, …).

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

## Option D — Behind your own web server (nginx)

There are two ways to put Rucio behind nginx — typically for TLS termination
on a public hostname. Pick one depending on **which image serves the panel**:

- **Reverse-proxy a complete daemon** — the daemon serves both panel and API;
  nginx just forwards everything. Simplest, nothing to deploy on the web host.
- **Serve the panel assets yourself** — nginx serves the static files and only
  proxies the API to a **headless** daemon. Use this when the web tier and the
  daemon are separate hosts, or you don't want to ship the panel from the
  daemon at all.

Either way the panel is fully same-origin (`/api/v1/...` for REST, `/api/ws`
for the live WebSocket), so the WebSocket upgrade headers below are required in
both setups.

### Reverse-proxying a complete daemon (panel served by the daemon)

The `latest`/complete daemon already serves the panel and the API on the same
port, so nginx hosts nothing of its own — it forwards every request to the
daemon:

```nginx
server {
    listen 443 ssl;
    server_name rucio.example.com;

    # TLS config (certificates, etc.) omitted.

    # Everything — panel, REST API and the /api/ws WebSocket — is the daemon.
    location / {
        proxy_pass http://daemon-host:3003;

        # Required for the /api/ws live-events WebSocket.
        proxy_http_version 1.1;
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Run the daemon with `latest` (complete) and keep `3003` on a private network
between nginx and the daemon rather than on the public internet.

### Serving the panel assets yourself (against a headless daemon)

Download the pre-built assets from the [Releases](../../../releases) page
(`rucio-web-<version>.tar.gz`) and unpack them where nginx can read them:

```sh
mkdir -p /srv/rucio-web
tar -xzf rucio-web-*.tar.gz -C /srv/rucio-web
```

nginx serves the static files and reverse-proxies only `/api/` (which covers
both `/api/v1/` and `/api/ws`) to the daemon's API port:

```nginx
server {
    listen 443 ssl;
    server_name rucio.example.com;

    # TLS config (certificates, etc.) omitted.

    root /srv/rucio-web;

    # Static panel — SPA fallback to index.html.
    location / {
        try_files $uri $uri/ /index.html;
    }

    # REST API and WebSocket — proxy to the headless daemon. This single
    # block covers both /api/v1/ and /api/ws.
    location /api/ {
        proxy_pass http://daemon-host:3003;

        # Required for the /api/ws live-events WebSocket.
        proxy_http_version 1.1;
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Run the daemon with `latest-headless` (the panel is served by nginx, not the
daemon) and expose its API to nginx — keep `3003` on a private network rather
than the public internet.

### Add authentication (strongly recommended)

> Rucio has **no built-in authentication** — anything that can reach the daemon
> can drive it. When you expose it on a public hostname, put a basic auth gate
> in front of it at the nginx layer.

Create a password file (the `htpasswd` tool ships with `apache2-utils` /
`httpd-tools`):

```sh
htpasswd -c /etc/nginx/rucio.htpasswd youruser
```

Then add two lines to the `server` block of either variant above:

```nginx
server {
    # ... listen / server_name / TLS as above ...

    auth_basic           "rucio";
    auth_basic_user_file /etc/nginx/rucio.htpasswd;

    # ... location blocks as above ...
}
```

The credentials cover the panel, the REST API and the `/api/ws` WebSocket in
one go. Because the panel uses same-origin requests, the browser replays the
basic-auth header to `/api/...` automatically once you've logged in. Only serve
this over HTTPS — basic auth sends the password on every request.

---

## Install the panel as an app (PWA)

The web panel is a Progressive Web App: it can be installed to a phone home
screen (or desktop) and launches standalone — its own icon, full screen, no
browser chrome.

> **Requires HTTPS.** Service workers only run in a secure context, so install
> works on `localhost` or behind the HTTPS reverse proxy from
> [Option D](#option-d--behind-your-own-web-server-nginx). A plain
> `http://<lan-ip>:3003` will load the panel but won't offer to install it.

It works through the Option D **basic-auth** gate: the manifest link uses
`crossorigin="use-credentials"`, so the browser sends the credentials when
fetching it. (Without that the manifest request returns `401` and the browser
silently treats the app as non-installable — the usual reason an HTTPS PWA
behind basic auth won't install.)

- **Android (Chrome):** open the panel, then menu → **Install app** /
  **Add to Home screen**.
- **iOS (Safari):** **Share** → **Add to Home Screen**.
- **Desktop (Chrome/Edge):** an install icon appears in the address bar.

Once installed it behaves like an app, but it is **not** offline-capable: every
download, search and status update comes from the daemon, so the daemon (and the
proxy) must be reachable. Offline you get the shell, not the data.

### ed2k links open the app

The installed PWA registers as a handler for `ed2k://` links. After installing,
clicking an `ed2k://…` link (e.g. on a website) can open Rucio and queue the
download directly. The first time, the browser asks for permission to let Rucio
handle `ed2k` links. Native `rucio:` magnets are a custom scheme browsers don't
allow PWAs to claim, so those still go through the **Add downloads** box.

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

Rucio stores its files under standard platform directories.  
Run `rucio config show` at any time to see the actual paths in use.

| Path | Linux | macOS |
|---|---|---|
| Config | `~/.config/rucio/config.toml` | `~/Library/Application Support/rucio/config.toml` |
| Identity key | `~/.config/rucio/identity.key` | same parent dir |
| Database | `~/.local/share/rucio/rucio.db` | `~/Library/Application Support/rucio/rucio.db` |
| Downloads | `$XDG_DOWNLOAD_DIR/rucio/downloads` or `~/Downloads/rucio/downloads` | `~/Downloads/rucio/downloads` |
| Pinned content | `~/Downloads/rucio/pins` (sibling of downloads) | `~/Downloads/rucio/pins` |
| Temp (parts) | `~/.cache/rucio/tmp` | `~/Library/Caches/rucio/tmp` |
| eMule nodes.dat | `~/.local/share/rucio/nodes.dat` | `~/Library/Application Support/rucio/nodes.dat` |

> **Windows (portable app):** the paths above do not apply — every file
> (config, identity, database, downloads, parts, `nodes.dat`) lives next to
> `Rucio.exe` in the folder you extracted, not under `%APPDATA%`.

> **Note:** the database schema is volatile before a stable release.
> If Rucio refuses to start after an upgrade, delete the database file and
> restart — downloads in progress will be lost but shares are re-indexed
> automatically.
