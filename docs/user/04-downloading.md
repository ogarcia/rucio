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
