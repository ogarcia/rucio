# Configuration

## Viewing current configuration

```sh
rucio config show
```

This prints all settings and their current values, including the resolved
paths for all directories.

## Setting a value

```sh
rucio config set <key> <value>
```

## Unsetting a value

```sh
rucio config unset <key>
```

Unsetting a key reverts it to its default value. Not all keys can be unset —
see the table below.

---

## Available keys

### `storage.download_dir`

Directory where completed downloads are placed.

```sh
rucio config set storage.download_dir /mnt/data/downloads
rucio config unset storage.download_dir     # revert to platform default
```

**Default:**

| Platform | Default path |
|---|---|
| Linux (desktop) | `$XDG_DOWNLOAD_DIR/rucio` or `~/Downloads/rucio` |
| macOS | `~/Downloads/rucio` |
| Linux (server / no XDG) | `~/rucio` |

---

### `storage.temp_dir`

Directory where incomplete downloads are stored as `.part` files while
transferring. Must be on the same filesystem as `download_dir` for an
efficient rename on completion; if they are on different filesystems rucio
falls back to a copy-then-delete.

```sh
rucio config set storage.temp_dir /mnt/data/.rucio-tmp
rucio config unset storage.temp_dir
```

**Default:**

| Platform | Default path |
|---|---|
| Linux | `~/.cache/rucio/tmp` |
| macOS | `~/Library/Caches/rucio/tmp` |

---

### `network.bootstrap_peers`

Comma-separated list of multiaddrs used to bootstrap into the DHT when no
local peers are found via mDNS. Each address must include the peer ID.

```sh
rucio config set network.bootstrap_peers \
  "/ip4/203.0.113.1/tcp/4001/p2p/12D3KooW...,/ip4/203.0.113.2/tcp/4001/p2p/12D3KooW..."
rucio config unset network.bootstrap_peers
```

**Default:** built-in list of public bootstrap nodes (empty until infrastructure
is available — LAN discovery via mDNS still works without this).

---

### `node.listen_addrs`

Comma-separated list of multiaddrs the daemon listens on.

```sh
rucio config set node.listen_addrs "/ip4/0.0.0.0/tcp/4001,/ip6/::/tcp/4001"
rucio config unset node.listen_addrs
```

**Default:** `/ip4/0.0.0.0/tcp/4001` (all IPv4 interfaces, port 4001).

---

## Configuration file location

The configuration is stored as TOML:

| Platform | Path |
|---|---|
| Linux | `~/.config/rucio/config.toml` |
| macOS | `~/Library/Application Support/rucio/config.toml` |

You can edit this file directly with a text editor. Changes take effect on the
next daemon restart.

## Applying changes at runtime

`rucio config set` and `rucio config unset` communicate with the running daemon
via the API. Most settings (including `download_dir`, `temp_dir` and
`listen_addrs`) are applied immediately without a restart.

> **Note:** changes to `node.listen_addrs` cause the daemon to rebind its
> listening sockets. Existing connections are not dropped, but new connections
> will use the updated address.
