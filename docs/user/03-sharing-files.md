# Sharing files

rucio shares **directories**, not individual files. Adding a directory causes
every file inside it (recursively) to be hashed and announced to the network.
New files dropped into a shared directory are picked up automatically.

## Adding a directory

```sh
rucio add /path/to/directory
```

The path must be absolute and must exist on the machine where the daemon is
running. If you are connecting to a remote daemon, use the path as seen by that
machine.

The daemon begins indexing immediately. Indexing walks the directory tree,
computes a BLAKE3 hash for every file, stores the results in the database and
announces them to the Kademlia DHT.

## Checking indexing progress

Large directories can take a while to index. Check how many files are still
pending:

```sh
rucio indexing
```

```
Indexing: 142 file(s) pending
```

When the output shows `0 file(s) pending`, every file in the directory is live
on the network.

## Listing shared files

```sh
rucio shares
```

```
 #  Name                     Size     Hash
 1  lecture-01.mp4           412 MB   a3f9...
 2  lecture-02.mp4           398 MB   cc01...
 3  notes.pdf                  1 MB   7e22...
```

## Getting a magnet link for a shared file

```sh
rucio magnet 2          # by row number from `rucio shares`
```

```
rucio:cc01...?name=lecture-02.mp4&size=417333248
```

You can send this link to anyone. They can paste it directly into `rucio get`.
See [Magnet links](07-magnet-links.md) for more detail.

## Removing a shared directory

```sh
rucio remove /path/to/directory
```

This removes the directory from the database and stops announcing its files.
It does **not** delete the files on disk.

## Automatic re-indexing

rucio watches shared directories using an inotify-based watcher (Linux) or
FSEvents (macOS). When a file is added or modified, it is re-hashed and
re-announced within about 500 ms. Deleted files are removed from the index
immediately.

In addition, rucio re-announces all shared files to the DHT every 22 minutes
to keep provider records from expiring. Any file that no longer exists on disk
at that point is silently removed from the database.

## The download directory is protected

The directory configured as `storage.download_dir` cannot be added as a shared
directory. This prevents accidentally sharing incomplete `.part` files or
creating feedback loops where downloaded files are immediately re-shared before
they are complete.
