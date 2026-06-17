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

Anything declared in the config rather than the DB survives a reset — notably
`storage.shared_dirs`, which is re-shared on every startup (see
[Configuration](../user/06-configuration.md#storageshared_dirs)).

### Tables

The authoritative schema lives in [`rucio-daemon/src/db/schema.sql`](../../rucio-daemon/src/db/schema.sql);
this section describes the central tables and the model, not every column. All
hashes are stored as raw `BLOB` (32-byte BLAKE3, or 16-byte MD4 for eMule), not
hex. Surrogate `id` columns are plain `INTEGER PRIMARY KEY` (auto-assigned
rowids; no `AUTOINCREMENT`, since the durable cross-node identity is the content
hash).

#### `shared_files`

Stores every indexed file that this node shares.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `root_hash` | BLOB UNIQUE | 32-byte BLAKE3 — the canonical file id (= `blake3` of the content; see [Hashing](06-hashing.md)) |
| `name` | TEXT | File name |
| `size` | INTEGER | File size in bytes |
| `mime_type` | TEXT | Detected MIME type |
| `path` | TEXT | Absolute path on disk (indexed) |
| `chunk_size` | INTEGER | Transfer chunk size (default 4 MiB) |
| `added_at` | INTEGER | Unix timestamp first indexed |
| `mtime` | INTEGER | File mtime — the change signal for re-indexing |

Removing a shared directory deletes all rows whose `path` is under that prefix
in one query (`delete_by_path_prefix`). There is no per-chunk hash table: with
BLAKE3 verified streaming each chunk is checked as a self-verifying slice
against `root_hash`, and the Merkle tree (outboard) lives as a regenerable
sidecar on disk, not in the DB (see [Transfer protocol](03-transfer-protocol.md)).

#### `shared_dirs`

The set of registered share directories the watcher monitors.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `path` | TEXT UNIQUE | Absolute directory path |
| `protected` | INTEGER | `1` = cannot be removed via the API |
| `added_at` | INTEGER | Unix timestamp |

The `download_dir`, `pin_dir`, category dirs and `storage.shared_dirs` entries
are reconciled in as `protected` on startup; the DELETE share endpoint refuses
to remove a protected directory (403). Dirs added with `rucio share add` are
unprotected and removable.

#### `downloads`

Tracks every libp2p download, past and present. (eMule downloads live in a
separate `emule_downloads` table so the eMule subsystem stays detachable.)

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `root_hash` | BLOB UNIQUE | BLAKE3 of the target file |
| `name` | TEXT | File name |
| `total_size` | INTEGER | File size in bytes |
| `dest_path` | TEXT | `.part` path while downloading, final path once complete |
| `status` | TEXT | See states below |
| `bytes_done` | INTEGER | Bytes received so far |
| `error_msg` | TEXT | Failure reason (null unless failed) |
| `category_id` | INTEGER FK → categories.id | NULL = global download dir (`ON DELETE SET NULL`) |
| `added_at` / `updated_at` | INTEGER | Unix timestamps |

**Status values:** `finding_providers`, `queued`, `downloading`, `stalled`,
`paused`, `completed`, `failed`, `cancelled`.

#### `download_chunks`

Per-chunk state for an in-progress download.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `download_id` | INTEGER FK → downloads.id | `ON DELETE CASCADE` |
| `idx` | INTEGER | Zero-based chunk index |
| `size` | INTEGER | Chunk size in bytes |
| `status` | TEXT | `pending`, `downloading`, `done` |

There is no per-chunk hash column — a chunk is verified as a bao slice against
the file's `root_hash` on arrival. `downloading` chunks are reset to `pending`
on startup so they are re-requested after a crash or restart.

#### `known_peers`

Discovered peers, shown in `rucio node peers` and reused as bootstrap hints.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `peer_id` | TEXT UNIQUE | libp2p PeerId (base58) |
| `addrs` | TEXT | JSON array of known multiaddrs |
| `first_seen` / `last_seen` | INTEGER | Unix timestamps |
| `high_id` | INTEGER | `1` = HighID, `0` = LowID |

#### Other tables

- `pins` and the cooperative-pinning cluster (`pin_subscriptions`,
  `pin_subscription_collections`, `subscription_seen_collections`,
  `mirror_pins`, `mirror_owned`, `mirror_optouts`) — see [Pinning](../user/10-pinning.md).
- `categories` — optional download categories (name, dir, color, keyword rules).
- `emule_downloads` / `emule_shared_files` — eMule transfers and the files
  seeded back to Kad after they finish.
- `notifications` — in-app notification centre records.
- `metrics` — a single row of cumulative lifetime counters.

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

### Declarative shares

Beyond directories added at runtime (stored in the DB), `storage.shared_dirs`
in the config declares a fixed set. On every startup `reconcile_protected_dirs`
folds them — together with `download_dir`, `pin_dir` and category dirs — into
the protected set: created on disk if missing, indexed by the watcher, and
flagged undeletable through the API. Because they live in the config rather than
the DB, they survive a database reset and can be declared while the daemon is
stopped (useful for containers). The daemon only reads them; it never writes the
config back, so there is no second mutable source of truth.

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

1. **In progress:** `<temp_dir>/<name>.<frag>.part`, written chunk by chunk
   (each verified slice at its offset), alongside a `<part>.obao` sidecar that
   accumulates the bao outboard as chunks verify. The sidecar is what lets the
   node serve verified chunks while still downloading (partial sharing) and
   resume after a restart.
2. **Complete:** moved to `<download_dir>/<name>` (or the pin/category dir) via
   `move_file()`, and the now-complete outboard is promoted to the share
   outboard cache at `<temp_dir>/outboards/<aa>/<root_hex>.obao` (sharded by the
   first hash byte). That cache is regenerable and pruned of orphans on startup.

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
