# Downloading

## Starting a download

You can start a download from a search result or from a magnet link.

**From a search result** — use the row number printed by `rucio search`:

```sh
rucio search "moby dick"
rucio download add 1
```

**From a magnet link:**

```sh
rucio download add "rucio:7b4a...?name=moby-dick.epub&size=980123"
```

Rucio immediately registers the download and begins locating peers that have
the file. The download appears in `rucio download list` right away, even before any
data has been transferred.

## Listing downloads

```sh
rucio download list
```

```
 Hash     Name                     Size     Status        Progress
 7b4a...  moby-dick.epub           980 KB   downloading    47%
 d931...  great-expectations.epub  1.2 MB   completed     100%
```

Filter by state:

```sh
rucio download list --active     # only in-progress downloads
rucio download list --done       # only completed/failed/cancelled
```

## Watching progress in real time

```sh
rucio download list --watch
```

The command refreshes every second and exits automatically once all active
downloads reach a terminal state (completed, failed or cancelled).

## Inspecting a single download

```sh
rucio download show 1            # by row number from `rucio download list`
rucio download show 7b4a         # hash prefix also works
```

This prints the full detail for one download: its hash, state, size and
progress (both in bytes and in completed pieces — libp2p chunks for Rucio
downloads, 9.28 MB slices for eMule), the destination path, when it was added
and last updated, and the link used to re-add it (a `rucio:` magnet for Rucio
downloads, the original `ed2k://` link for eMule downloads).

While a download is active it also shows live stats: how many sources are
known and how many are actively transferring, how many pieces are in flight,
the current download speed and an ETA.

## Download states

| State | Meaning |
|---|---|
| `finding providers` | Querying the DHT for peers that have the file |
| `queued` | Providers found; waiting for a download slot |
| `downloading` | Actively transferring chunks |
| `stalled` | Stuck after several search rounds with no (or unresponsive) sources; still retrying in the background |
| `completed` | All chunks received and file moved to download directory |
| `failed` | Could not complete, or could not be saved to the download folder; any data received is kept — **resume** to retry or **cancel** to discard |
| `cancelled` | Cancelled by the user |

The `finding providers` state is normal for files that are not yet cached
locally in the DHT. It can last up to a minute on a cold start.

A `failed` download is not a dead end. If every chunk was received but the file
could not be moved into place — the download folder was missing, read-only, or
the disk was full — the fully-verified data is kept, and the download waits for
you to decide:

- **Resume** retries the operation: it re-attempts the move (once you have fixed
  the folder), or, for a partially-received download, continues fetching.
- **Cancel** discards it: the partial data is removed and the entry becomes
  `cancelled`.

A `failed` download therefore cannot be removed from the history directly (that
would strand its data on disk) — resume or cancel it first.

## Resuming interrupted downloads

If the daemon is stopped while a download is in progress, Rucio resumes
automatically on the next startup. Chunks that were already received are not
re-downloaded.

No action is required — resumption is automatic.

## Moving a partial download to another machine

A half-finished Rucio download can be continued on a different machine. Copy the
in-progress file across, re-add the same `rucio:` magnet there, and Rucio picks
up where the transfer left off instead of starting over.

1. On the source machine, locate the temp directory (`storage.temp_dir`, shown
   by `rucio config show`). It holds the in-progress `.part` file, whose name
   encodes the content hash.
2. Copy that `.part` file — unchanged, same name — into the temp directory of
   the destination machine. If a matching `.part.obao` sidecar sits next to it,
   copy that too.
3. On the destination machine, re-add the same magnet:

   ```sh
   rucio download add "rucio:7b4a...?name=moby-dick.epub&size=980123"
   ```

Rucio verifies the copied bytes against the magnet's root hash: the parts that
check out are kept, and only the missing chunks are fetched. Nothing is trusted
without verification, so a wrong or corrupted file never counts as complete.

The `.part.obao` sidecar makes recovery instant and fully offline. Without it,
Rucio re-fetches the verification tree from a peer that has the complete file,
so recovery still works as long as such a peer is online; if none is, the file
simply downloads from scratch.

Keep the file's name and `.part` extension exactly as they were and place it in
the temp directory: the name encodes the content hash and Rucio looks for it
there by that name. A renamed or misplaced file is not found and the download
starts from zero — harmless, just no bandwidth saved.

## Cancelling a download

```sh
rucio download cancel 7b4a           # hash prefix is enough
```

This removes the in-progress `.part` file from the temp directory and marks
the download as cancelled. The entry remains visible in `rucio download list`
until you clean it.

## Cleaning up the history

Completed, failed and cancelled entries stay in the list until explicitly
removed. Active downloads cannot be deleted — cancel them first.

```sh
rucio download clean                 # removes all non-active entries
rucio download clean 7b4a            # removes a specific entry by hash prefix
```

## Where files land

Finished downloads are moved to `storage.download_dir` (default:
`~/Downloads/rucio/downloads` on most systems). Check the current value with:

```sh
rucio config show
```

To change it:

```sh
rucio config set storage.download_dir /mnt/data/downloads
```

See [Configuration](06-configuration.md) for all available settings.

To route certain downloads into their own folders, see
[Categories](09-categories.md).

---

## Downloading from the eMule network (ed2k://)

> **Requires** the daemon to be built with the `emule-compat` feature.
> See [Installation](01-installation.md#build-with-emule--kad2-compatibility).

### First-time setup — nodes.dat

Rucio locates eMule sources through the Kad2 distributed hash table.  To
bootstrap into the Kad2 network you need a `nodes.dat` file containing a list
of known Kad2 nodes.  Download one automatically with:

```sh
rucio node emule bootstrap
```

This downloads a fresh `nodes.dat` from `http://upd.emule-security.org/nodes.dat`,
validates it, and saves it to `~/.cache/rucio/kad/nodes.dat` (or to
`storage.nodes_dat_path` if you have set it in the configuration).

You only need to run this once.  Repeat it if the Kad2 bootstrap stops working
after a long period of inactivity (node lists go stale over time).

To use a different source:

```sh
rucio node emule bootstrap --url http://kademlia.ru/download/nodes.dat
```

To make a different mirror the default for both this command and the automatic
first-start download, set `emule.nodes_dat_url` (or `RUCIOD_EMULE_NODES_DAT_URL`):

```sh
rucio config set emule.nodes_dat_url http://kademlia.ru/download/nodes.dat
```

### Check status

```sh
rucio node emule status
```

```
eMule compatibility: enabled
nodes.dat path:      /home/user/.cache/rucio/kad/nodes.dat
nodes.dat status:    present (150 contacts)
```

### Starting an eMule download

Pass any `ed2k://` link to `rucio download add`:

```sh
rucio download add "ed2k://|file|ubuntu-24.04.2-desktop-amd64.iso|6114656256|a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4|/"
```

The daemon will:

1. Parse the ed2k link and extract the file hash (MD4) and size.
2. Bootstrap into the Kad2 network using your `nodes.dat`.
3. Search for peers that have the file.
4. Download chunks over the eMule TCP protocol, verifying each chunk with MD4.
5. Compute the BLAKE3 hash of the completed file and register it in the Rucio
   DHT so other Rucio peers can find it.

The download appears in `rucio download list` and supports `--watch` like any
other download.

### Capturing `ed2k://` links from the browser

On Linux you can make a click on an `ed2k://` link on any website go straight
into Rucio, through a desktop URL-scheme handler. This is **not Firefox-only**:
any browser that hands external schemes to the system (Firefox, Chromium, …)
works, because the handler is registered desktop-wide, not in the browser. It is
the native alternative to the [PWA handler](01-installation.md#ed2k-links-open-the-app),
which needs the panel installed over HTTPS. The handler is a small Python script
(Python 3 ships on virtually every Linux), and it talks to a daemon that may be
running locally or on **another machine**.

Only `ed2k://` is worth wiring up: websites render those as clickable links,
whereas `rucio:` magnets are not linkified by browsers, so they keep going
through the panel's **Add downloads** box.

1. Create the handler script at `~/.local/bin/rucio-ed2k-handler`. Set `MODE`
   near the top to `cli` (shell out to the `rucio` CLI) or `api` (POST to the
   REST API — set `RUCIO_API` to the daemon's address when it runs on another
   machine). `api` mode uses only the Python standard library, so it needs no
   `curl`:

   ```python
   #!/usr/bin/env python3
   """Hand an ed2k:// link clicked in the browser to Rucio."""
   import base64
   import json
   import os
   import shutil
   import subprocess
   import sys
   import urllib.error
   import urllib.parse
   import urllib.request

   # cli = run the `rucio` CLI; api = POST to RUCIO_API (set it to a remote
   # host when Rucio runs on another machine). Either can come from the env.
   MODE = os.environ.get("RUCIO_ED2K_MODE", "cli")
   RUCIO_API = os.environ.get("RUCIO_API", "http://127.0.0.1:3003")


   def notify(summary, body, urgent=False):
       # Notify only when notify-send exists, so the handler never fails on a
       # minimal or headless desktop.
       if not shutil.which("notify-send"):
           return
       args = ["notify-send"] + (["-u", "critical"] if urgent else [])
       subprocess.run(args + [summary, body], check=False)


   def add_via_cli(link):
       try:
           p = subprocess.run(["rucio", "download", "add", link],
                              capture_output=True, text=True)
       except FileNotFoundError:
           return "rucio not found on PATH"
       if p.returncode == 0:
           return None
       return (p.stderr or p.stdout).strip() or f"exit code {p.returncode}"


   def add_via_api(link):
       headers = {"Content-Type": "application/json"}
       # Pull HTTP Basic credentials out of RUCIO_API if it carries any
       # (e.g. https://user:pass@host), so a URL with auth just works.
       parts = urllib.parse.urlsplit(RUCIO_API)
       if parts.username:
           creds = "%s:%s" % (urllib.parse.unquote(parts.username),
                              urllib.parse.unquote(parts.password or ""))
           headers["Authorization"] = "Basic " + base64.b64encode(
               creds.encode()).decode()
           netloc = parts.hostname + (":%d" % parts.port if parts.port else "")
           base = urllib.parse.urlunsplit((parts.scheme, netloc, "", "", ""))
       else:
           base = RUCIO_API.rstrip("/")
       req = urllib.request.Request(
           f"{base}/api/v1/downloads/ed2k",
           data=json.dumps({"link": link}).encode(),
           headers=headers,
           method="POST",
       )
       try:
           urllib.request.urlopen(req, timeout=10)
           return None
       except urllib.error.HTTPError as e:
           return e.read().decode(errors="replace").strip() or f"HTTP {e.code}"
       except urllib.error.URLError as e:
           return f"cannot reach {base}: {e.reason}"


   def main():
       if len(sys.argv) < 2 or not sys.argv[1]:
           notify("Rucio", "No link given", urgent=True)
           return 1
       link = sys.argv[1]
       if MODE == "cli":
           error = add_via_cli(link)
       elif MODE == "api":
           error = add_via_api(link)
       else:
           notify("Rucio", f"Invalid MODE '{MODE}' (use cli or api)", urgent=True)
           return 1
       if error is None:
           notify("Rucio", "Download queued")
           return 0
       notify("Rucio", f"Could not queue download: {error}", urgent=True)
       return 1


   if __name__ == "__main__":
       sys.exit(main())
   ```

   ```sh
   chmod +x ~/.local/bin/rucio-ed2k-handler
   ```

2. Create the desktop entry at `~/.local/share/applications/rucio-ed2k.desktop`:

   ```ini
   [Desktop Entry]
   Type=Application
   Name=Rucio ed2k handler
   Exec=rucio-ed2k-handler %u
   NoDisplay=true
   MimeType=x-scheme-handler/ed2k;
   ```

   If clicking a link later does nothing, your desktop session's `PATH` may not
   include `~/.local/bin`; put the script's absolute path in `Exec` instead.

3. Register it as the default handler for the scheme:

   ```sh
   xdg-mime default rucio-ed2k.desktop x-scheme-handler/ed2k
   update-desktop-database ~/.local/share/applications 2>/dev/null || true
   ```

4. Test it without a browser — this should land in `rucio download list` (it
   won't find sources, the hash is a dummy, but the capture worked):

   ```sh
   xdg-open "ed2k://|file|test|1|00000000000000000000000000000000|/"
   ```

> **Notes.** The daemon must be running; the handler only forwards the link and,
> on failure, puts the daemon's own reason in the notification. The first time you
> click an `ed2k://` link the browser asks whether to open it with Rucio — allow
> it (and tick *always* if offered). The desktop entry runs with a minimal
> environment, so in `cli` mode give `rucio` an absolute path if it is not found.

For `api` mode behind an nginx basic-auth gate (the
[Option D](01-installation.md#option-d--behind-your-own-web-server-nginx) setup),
put the credentials straight in `RUCIO_API` — the script turns them into an
`Authorization: Basic` header:

```python
RUCIO_API = os.environ.get("RUCIO_API", "https://user:password@rucio.example.com")
```

Prefer `https` so the credentials are encrypted in transit; over plain `http`
they travel only base64-encoded.

### Configuration

| Key | Default | Description |
|---|---|---|
| `storage.nodes_dat_path` | `<cache-dir>/rucio/kad/nodes.dat` | Path to the Kad2 bootstrap file |
| `emule.nodes_dat_url` | *(built-in mirror)* | URL to download `nodes.dat` from |
| `emule.temp_dir` | `<cache-dir>/rucio/emule-tmp` | Temporary directory for eMule `.part` files |
| `emule.tcp_port` | `4662` | TCP port for incoming peer connections (High-ID) |
| `emule.udp_port` | `4672` | UDP port for Kad2 routing and source search |

Environment variable overrides:

```sh
RUCIOD_NODES_DAT=/path/to/nodes.dat ruciod
RUCIOD_EMULE_NODES_DAT_URL=http://kademlia.ru/download/nodes.dat ruciod
RUCIOD_EMULE_TEMP_DIR=/mnt/fast/emule-tmp ruciod
RUCIOD_EMULE_TCP_PORT=4662 ruciod
RUCIOD_EMULE_UDP_PORT=4672 ruciod
```

### Network requirements — port mapping

Two ports must be **reachable from the internet** for full eMule functionality:

| Port | Protocol | Purpose |
|---|---|---|
| `4662` (`emule.tcp_port`) | TCP | Incoming peer connections (High-ID) |
| `4672` (`emule.udp_port`) | UDP | Kad2 routing, bootstrap, source search |

Without the UDP port, bootstrap packets can be sent but responses never arrive.
Without the TCP port, the node runs as **Low-ID** — still functional, but with
lower priority in upload queues and noticeably slower downloads.

| Environment | What to do |
|---|---|
| Container (Docker/Podman) | `-p 4662:4662/tcp -p 4672:4672/udp` |
| VPS / bare metal | `ufw allow 4672/udp && ufw allow 4662/tcp` |
| Home router | Port-forward both ports → local IP of the server |
| Windows (portable app) | Allow at the Firewall prompt + port-forward both ports |

Both ports can be changed via their respective env vars or `config.toml` keys.
When changed, update the firewall / port mapping accordingly.

