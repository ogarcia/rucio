# Storage

## SQLite database

Rucio uses a single SQLite database for all persistent state. The database
file location is `storage.database_path` (see
[Configuration](../user/06-configuration.md) for defaults).

### Schema volatility

The schema is considered **volatile** until Rucio reaches a stable release.
There are no migrations. If the schema changes between versions, the database
must be deleted manually and the daemon restarted. Shares are re-indexed
automatically on restart; downloads in progress are lost.

This is a deliberate trade-off: migration infrastructure is non-trivial to
implement correctly, and the schema is still evolving. A proper migration
system will be introduced before the first stable release.

### Tables

#### `shares`

Stores every indexed file.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment row ID |
| `root_hash` | TEXT UNIQUE | BLAKE3 hash hex, primary key for lookups |
| `name` | TEXT | File name |
| `size` | INTEGER | File size in bytes |
| `mime_type` | TEXT | Detected MIME type |
| `path` | TEXT | Absolute path on disk |
| `dir_path` | TEXT | Parent shared directory path |
| `indexed_at` | INTEGER | Unix timestamp of last indexing |

The `dir_path` column enables bulk operations on a shared directory: when a
directory is removed, all rows with a matching `dir_path` prefix are deleted
in a single query (`delete_by_path_prefix`).

The `download_dir` is protected at the application layer — attempting to add
it as a shared directory returns a 409 error.

#### `downloads`

Tracks every download, past and present.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment row ID |
| `root_hash` | TEXT UNIQUE | BLAKE3 hash of the target file |
| `name` | TEXT | File name (may be null until manifest is fetched) |
| `total_size` | INTEGER | File size in bytes (null until manifest) |
| `dest_path` | TEXT | Final destination path (null until manifest) |
| `status` | TEXT | See states below |
| `progress` | INTEGER | Bytes received so far |
| `created_at` | INTEGER | Unix timestamp |
| `completed_at` | INTEGER | Unix timestamp (null if not done) |

**Status values:** `finding_providers`, `queued`, `downloading`, `completed`,
`failed`, `cancelled`.

#### `chunks`

Tracks the state of each individual chunk for active downloads.

| Column | Type | Description |
|---|---|---|
| `download_id` | INTEGER FK → downloads.id | |
| `chunk_index` | INTEGER | Zero-based chunk index |
| `status` | TEXT | `pending`, `in_flight`, `done` |

`in_flight` chunks are reset to `pending` on startup (via
`reset_in_flight_chunks`) so they are re-requested after a crash or restart.

#### `peers`

Stores discovered peers for display in `rucio node peers` and for bootstrap hints.

| Column | Type | Description |
|---|---|---|
| `peer_id` | TEXT UNIQUE | libp2p PeerID (base58) |
| `addrs` | TEXT | JSON array of known multiaddrs |
| `last_seen` | INTEGER | Unix timestamp |

## Directory sharing model

Rucio shares **directories**, not individual files. This simplifies the
inotify/FSEvents watcher model — there is a single watcher per shared
directory rather than one per file.

When a directory is added:

1. `collect_files` walks the tree recursively.
2. Each file is hashed (`hash_file`) and inserted or updated in `shares`.
3. The daemon calls `kad.start_providing` for each hash.

When a directory is removed:

1. All rows with `dir_path` matching the removed directory are deleted.
2. The daemon stops announcing those hashes.

## WatcherService

The `WatcherService` monitors shared directories for filesystem changes using
[notify](https://github.com/notify-rs/notify) (cross-platform wrapper over
inotify / FSEvents / kqueue).

**Debounce:** Create and Modify events for the same path are coalesced with a
500 ms debounce window. This prevents a flood of events during large file
copies. Remove events bypass the debounce and are processed immediately.

**Ticker:** The watcher loop polls a 250 ms ticker. At each tick, any paths
whose debounce window has elapsed are processed (re-hashed and re-announced).

## Temp and download directories

Downloads land in two stages:

1. **In progress:** `<temp_dir>/<hash>.part` — written chunk by chunk.
2. **Complete:** moved to `<download_dir>/<name>` via `move_file()`.

`move_file()` attempts an atomic rename first. If the rename fails because
`temp_dir` and `download_dir` are on different filesystems (EXDEV), it falls
back to a full copy followed by deletion of the source.

Path resolution precedence (example for `download_dir` on Linux):

```
1. Explicit value in config.toml
2. $XDG_DOWNLOAD_DIR/rucio      (Linux desktop environments)
3. ~/Downloads/rucio             (common default)
4. ~/rucio                       (server / no XDG)
5. /tmp/rucio                    (last resort)
```

`home_dir()` filters out any result that is not an absolute path, whether it
comes from the `$HOME` environment variable or from the `dirs` crate. This
guards against misconfigured environments.
