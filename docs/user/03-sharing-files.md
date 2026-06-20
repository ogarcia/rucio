# Sharing files

Rucio shares **directories**, not individual files. Adding a directory causes
every file inside it (recursively) to be hashed and announced to the network.
New files dropped into a shared directory are picked up automatically.

## Adding a directory

```sh
rucio share add /path/to/directory
```

The path must be absolute and must exist on the machine where the daemon is
running. If you are connecting to a remote daemon, use the path as seen by that
machine.

The daemon begins indexing immediately. Indexing walks the directory tree,
computes a BLAKE3 hash for every file, stores the results in the database and
announces them to the Kademlia DHT.

### Declaring shares in the config

Directories added with `rucio share add` live in the database. You can also
declare a fixed set in the config under [`storage.shared_dirs`](06-configuration.md#storageshared_dirs)
(or the `RUCIOD_SHARED_DIRS` environment variable). Those are re-shared on every
startup as **protected** directories — they can be declared while the daemon is
stopped, survive a database reset, and aren't removable through the API. This is
the recommended approach for containers and reproducible/headless deployments.

## Checking indexing progress

Large directories can take a while to index. Check how many files are still
pending:

```sh
rucio share indexing
```

```
Indexing: 142 file(s) pending
```

When the output shows `0 file(s) pending`, every file in the directory is live
on the network.

## Listing shared files

```sh
rucio share list
```

```
 #  Name                     Size     Hash
 1  lecture-01.mp4           412 MB   a3f9...
 2  lecture-02.mp4           398 MB   cc01...
 3  notes.pdf                  1 MB   7e22...
```

## Getting a magnet link for a shared file

```sh
rucio share magnet 2          # by row number from `rucio share list`
```

```
rucio:cc01...?name=lecture-02.mp4&size=417333248
```

You can send this link to anyone. They can paste it directly into `rucio download add`.
See [Magnet links](07-magnet-links.md) for more detail.

## Removing a shared directory

```sh
rucio share remove /path/to/directory
```

This removes the directory from the database and stops announcing its files.
It does **not** delete the files on disk.

## Automatic re-indexing

Rucio watches shared directories using an inotify-based watcher (Linux) or
FSEvents (macOS). When a file is added or modified, it is re-hashed and
re-announced within about 500 ms. Deleted files are removed from the index
immediately.

Provider records in the DHT are kept fresh automatically (libp2p republishes
them roughly every 12 hours, well before they expire). Separately, once a day
Rucio reconciles the shared library against disk: any file that no longer exists
is silently removed from the database.

## The download directory is protected

The directory configured as `storage.download_dir` cannot be added as a shared
directory. This prevents accidentally sharing incomplete `.part` files or
creating feedback loops where downloaded files are immediately re-shared before
they are complete.
