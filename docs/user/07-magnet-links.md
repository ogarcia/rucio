# Magnet links

A magnet link in rucio is a self-contained reference to a file. It encodes
the file's content hash, its name and size, and optionally a list of known
peers. Anyone with the link can start downloading the file without going
through search.

## Format

```
rucio:<hash>?name=<url-encoded-name>&size=<bytes>[&peer=<multiaddr>]...
```

Example:

```
rucio:7b4a3f9c...?name=great-expectations.epub&size=1258291
```

- `<hash>` is the BLAKE3 root hash of the file in lowercase hex.
- `name` is URL-encoded (spaces become `%20`, etc.).
- `size` is the file size in bytes.
- `peer` parameters are optional; they hint at known providers.

## Getting a magnet link for a file you share

**From the shares list** — using the row number printed by `rucio shares`:

```sh
rucio magnet 2
```

**Offline, without a running daemon** — from any local file:

```sh
rucio magnet --file /path/to/any/file.mkv
```

This hashes the file locally and prints the magnet link. No daemon or network
connection is required. Useful for generating links on a machine that is not
running rucio.

## Using a magnet link to download

Paste the full link as the argument to `rucio get`:

```sh
rucio get "rucio:7b4a3f9c...?name=great-expectations.epub&size=1258291"
```

rucio parses the link, registers the download, and starts locating peers that
have the file via the Kademlia DHT.

## Sharing links out of band

Magnet links are plain text — you can share them anywhere:

- Paste them in a chat message or email
- Post them on a web page or forum
- Store them in a text file

The recipient needs a running rucio daemon to download the file.

## Privacy note

A magnet link reveals the file's hash, name and size to anyone who sees it.
It does not reveal who is sharing the file or where the data is hosted, but
anyone with the link can query the DHT to find peers that have the file.
