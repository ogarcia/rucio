# Downloading

## Starting a download

You can start a download from a search result or from a magnet link.

**From a search result** — use the row number printed by `rucio search`:

```sh
rucio search "moby dick"
rucio get 1
```

**From a magnet link:**

```sh
rucio get "rucio:7b4a...?name=moby-dick.epub&size=980123"
```

rucio immediately registers the download and begins locating peers that have
the file. The download appears in `rucio downloads` right away, even before any
data has been transferred.

## Listing downloads

```sh
rucio downloads
```

```
 Hash     Name                     Size     Status        Progress
 7b4a...  moby-dick.epub           980 KB   downloading    47%
 d931...  great-expectations.epub  1.2 MB   completed     100%
```

Filter by state:

```sh
rucio downloads --active     # only in-progress downloads
rucio downloads --done       # only completed/failed/cancelled
```

## Watching progress in real time

```sh
rucio downloads --watch
```

The command refreshes every second and exits automatically once all active
downloads reach a terminal state (completed, failed or cancelled).

## Download states

| State | Meaning |
|---|---|
| `finding providers` | Querying the DHT for peers that have the file |
| `queued` | Providers found; waiting for a download slot |
| `downloading` | Actively transferring chunks |
| `completed` | All chunks received and file moved to download directory |
| `failed` | Could not complete the download |
| `cancelled` | Cancelled by the user |

The `finding providers` state is normal for files that are not yet cached
locally in the DHT. It can last up to a minute on a cold start.

## Resuming interrupted downloads

If the daemon is stopped while a download is in progress, rucio resumes
automatically on the next startup. Chunks that were already received are not
re-downloaded.

No action is required — resumption is automatic.

## Cancelling a download

```sh
rucio cancel 7b4a           # hash prefix is enough
```

This removes the in-progress `.part` file from the temp directory and marks
the download as cancelled. The entry remains visible in `rucio downloads`
until you clean it.

## Cleaning up the history

Completed, failed and cancelled entries stay in the list until explicitly
removed. Active downloads cannot be deleted — cancel them first.

```sh
rucio clean                 # removes all non-active entries
rucio clean 7b4a            # removes a specific entry by hash prefix
```

## Where files land

Finished downloads are moved to `storage.download_dir` (default:
`~/Downloads/rucio` on most systems). Check the current value with:

```sh
rucio config show
```

To change it:

```sh
rucio config set storage.download_dir /mnt/data/downloads
```

See [Configuration](06-configuration.md) for all available settings.

---

## Downloading from the eMule network (ed2k://)

> **Requires** the daemon to be built with the `emule-compat` feature.
> See [Installation](01-installation.md#build-with-emule--kad2-compatibility).

### First-time setup — nodes.dat

Rucio locates eMule sources through the Kad2 distributed hash table.  To
bootstrap into the Kad2 network you need a `nodes.dat` file containing a list
of known Kad2 nodes.  Download one automatically with:

```sh
rucio emule bootstrap
```

This downloads a fresh `nodes.dat` from `http://upd.emule-security.org/nodes.dat`,
validates it, and saves it to `~/.local/share/rucio/nodes.dat` (or to
`storage.nodes_dat_path` if you have set it in the configuration).

You only need to run this once.  Repeat it if the Kad2 bootstrap stops working
after a long period of inactivity (node lists go stale over time).

To use a different source:

```sh
rucio emule bootstrap --url http://kademlia.ru/download/nodes.dat
```

### Check status

```sh
rucio emule status
```

```
eMule compatibility: enabled
nodes.dat path:      /home/user/.local/share/rucio/nodes.dat
nodes.dat status:    present (150 contacts)
```

### Starting an eMule download

Pass any `ed2k://` link to `rucio get`:

```sh
rucio get "ed2k://|file|ubuntu-24.04.2-desktop-amd64.iso|6114656256|a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4|/"
```

The daemon will:

1. Parse the ed2k link and extract the file hash (MD4) and size.
2. Bootstrap into the Kad2 network using your `nodes.dat`.
3. Search for peers that have the file.
4. Download chunks over the eMule TCP protocol, verifying each chunk with MD4.
5. Compute the BLAKE3 hash of the completed file and register it in the Rucio
   DHT so other Rucio peers can find it.

The download appears in `rucio downloads` and supports `--watch` like any
other download.

### Configuration

| Key | Default | Description |
|---|---|---|
| `storage.nodes_dat_path` | `<data-dir>/rucio/nodes.dat` | Path to the Kad2 bootstrap file |
| `storage.emule_temp_dir` | `<cache-dir>/rucio/emule-tmp` | Temporary directory for eMule `.part` files |
| `emule.kad_port` | `4672` | UDP port for the Kad2 socket |

Environment variable overrides:

```sh
RUCIOD_NODES_DAT=/path/to/nodes.dat ruciod
RUCIOD_EMULE_TEMP_DIR=/mnt/fast/emule-tmp ruciod
RUCIOD_KAD_PORT=4672 ruciod
```

### Network requirements — port mapping

The Kad2 protocol requires that the UDP port `4672` (or the value of
`emule.kad_port`) is **reachable from the internet**.  Without this,
bootstrap packets can be sent outbound but responses never arrive.

| Environment | What to do |
|---|---|
| Container (Docker/Podman) | `-p 4672:4672/udp` in `docker run` / `podman run` |
| VPS / bare metal | Open `4672/udp` in the firewall (`ufw allow 4672/udp`) |
| Home router | Port-forward `4672/udp` → local IP of the server |
| WSL2 | Port-forward from Windows + allow in Windows Firewall |

The port can be changed via `RUCIOD_KAD_PORT` or `emule.kad_port` in
`config.toml`.  When changed, update the firewall / port mapping accordingly.

