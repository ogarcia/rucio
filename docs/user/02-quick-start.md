# Quick start

This guide gets you from zero to a running node that is sharing files and can
download from the network in about five minutes.

## 1. Start the daemon

The daemon must be running for any other command to work.

```sh
ruciod
```

You should see log output similar to:

```
INFO rucio_daemon: listening on /ip4/0.0.0.0/tcp/4321
INFO rucio_daemon: peer id: 12D3KooW...
```

Leave this terminal open. For persistent operation use a service manager or a
terminal multiplexer (`tmux`, `screen`).

## 2. Check node status

In a second terminal:

```sh
rucio node status
```

Example output:

```
Peer ID:        12D3KooWAbcDef...
Listen addrs:   /ip4/192.168.1.10/tcp/4001
Connectivity:   LowID  ·  1 peer(s)  ·  no observed public address yet
Shared files:   0
```

`LowID` is normal on first start or behind NAT. The node will still be able to
download. See [Node classes](../design/05-node-classes.md) for more detail.

## 3. Share a directory

```sh
rucio share add ~/Documents/ebooks
```

Rucio scans the directory, hashes every file with BLAKE3, and announces them
to the network. Large directories take a moment — check progress with:

```sh
rucio share indexing
```

Once indexing is complete, list what you are sharing:

```sh
rucio share list
```

```
 #  Name                     Size     Hash
 1  my-book.epub             2.1 MB   a3f9...
 2  another-book.pdf        14.7 MB   cc01...
```

## 4. Search the network

```sh
rucio search "epub"
```

Rucio queries the network using keyword search and accumulates results for a
few seconds:

```
 #  Name                     Size     Peers  Hash
 1  great-expectations.epub  1.2 MB   3      7b4a...
 2  moby-dick.epub           980 KB   1      d931...
```

## 5. Download a file

Use the row number from the last search:

```sh
rucio download add 1
```

Or paste a magnet link directly:

```sh
rucio download add "rucio:7b4a...?name=great-expectations.epub&size=1258291"
```

## 6. Watch download progress

```sh
rucio download list --watch
```

The command exits automatically once all active downloads finish. Completed
downloads stay in the list until you clean them:

```sh
rucio download clean          # removes all completed entries
rucio download clean 7b4a     # removes a specific entry by hash prefix
```

## Next steps

| Next step | Description |
|---|---|
| [Sharing files](03-sharing-files.md) | Managing shared directories in detail |
| [Downloading](04-downloading.md) | Resuming, cancelling and monitoring downloads |
| [Configuration](06-configuration.md) | Change download directory, listen address, and more |
