# rucio

A decentralized peer-to-peer file sharing application built in Rust, inspired by
eMule and MLDonkey and adapted to modern infrastructure.

No trackers. No central servers. No relay nodes for data transfer.
Files are discovered via a distributed hash table (Kademlia DHT) and keyword
search (Gossipsub), and transferred directly between peers.

## Features

- **Fully decentralized** — peers discover each other via mDNS (local network)
  and Kademlia DHT (internet)
- **Magnet links** — share any file with a single `rucio:<hash>` link, entirely
  offline if desired
- **Resumable downloads** — interrupted transfers pick up where they left off
  after a restart
- **Directory sharing** — add a directory and every file inside is indexed,
  hashed, and announced automatically
- **HighID / LowID** — nodes behind NAT can still download; publicly reachable
  nodes serve chunks to everyone
- **Single binary** — `rucio` acts as both daemon (`ruciod`) and CLI depending
  on how it is invoked

## Quick install

### From a release binary

Download the latest binary for your platform from the
[Releases](../../releases) page, make it executable and place it on your PATH:

```sh
install -m755 rucio-linux-x86_64 /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

### From source

Requires Rust 1.85 or later (uses the 2024 edition).

```sh
git clone https://github.com/ogarcia/rucio
cd rucio
cargo build --release
install -m755 target/release/rucio /usr/local/bin/rucio
ln -s /usr/local/bin/rucio /usr/local/bin/ruciod
```

## Five-minute walkthrough

**Start the daemon** (keeps running in the foreground; use a service manager or
`tmux`/`screen` for persistent operation):

```sh
ruciod
```

**Share a directory:**

```sh
rucio share add ~/Movies
```

**Check what you are sharing:**

```sh
rucio share list
```

**Search the network:**

```sh
rucio search "big buck bunny"
```

**Download a result** (by index from the last search, or by magnet link):

```sh
rucio download add 1
rucio download add "rucio:abc123...?name=big_buck_bunny.mkv&size=734003200"
```

**Watch progress:**

```sh
rucio download list --watch
```

**Get a magnet link to share with someone:**

```sh
rucio share magnet 1          # by row number from `rucio share list`
rucio share magnet --file /path/to/file.mkv   # offline, no daemon needed
```

## Documentation

| Guide | Description |
|---|---|
| [User guide](docs/user/README.md) | Installation, configuration and everyday usage |
| [Admin guide](docs/admin/README.md) | Running bootstrap nodes and the DHT indexer |
| [Design docs](docs/design/README.md) | Architecture, protocols and implementation decisions |

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).
