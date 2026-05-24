# Searching

## Basic search

```sh
rucio search "keyword"
rucio search "multiple words"
```

rucio broadcasts the query to connected peers via Gossipsub and simultaneously
looks up providers in the Kademlia DHT. Results are accumulated for a few
seconds and printed as they arrive.

```
 #  Name                          Size     Peers  Hash
 1  great-expectations.epub       1.2 MB   3      7b4a...
 2  great-expectations-annotated  2.8 MB   1      f112...
 3  great-expectations.mobi       900 KB   2      9c33...
```

The search exits automatically after seeing three consecutive idle polling
cycles with no new results — typically 5–10 seconds.

## Columns explained

| Column | Meaning |
|---|---|
| `#` | Row number, used with `rucio get` |
| `Name` | File name as announced by the sharing peer |
| `Size` | File size in human-readable form |
| `Peers` | Number of peers known to have this file at query time |
| `Hash` | BLAKE3 root hash (truncated) — uniquely identifies the file content |

## Downloading a result

Pass the row number directly to `rucio get`:

```sh
rucio get 3
```

The row numbers are only valid for the most recent search. If you run another
search, previous row numbers refer to the new results.

## Tips

**Search is keyword-based, not fuzzy.** The query is split into words and each
word is matched against file names announced by peers. A search for
`"great expectations"` will match files whose name contains both words.

**Results depend on connected peers.** If the network is small or your node has
few connections, results may be sparse. Check `rucio status` to see how many
peers you are connected to.

**The same file from multiple peers is deduplicated by hash.** If three peers
share an identical file, it appears as one row with `Peers: 3`. rucio will
download chunks from all of them in parallel.

**You can download without searching** if you already have a magnet link:

```sh
rucio get "rucio:7b4a...?name=great-expectations.epub&size=1258291"
```

See [Magnet links](07-magnet-links.md) for more detail.
