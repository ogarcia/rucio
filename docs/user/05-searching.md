# Searching

## Basic search

```sh
rucio search "keyword"
rucio search "multiple words"
```

Rucio broadcasts the query to connected peers via Gossipsub and simultaneously
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
| `#` | Row number, used with `rucio download add` |
| `Name` | File name as announced by the sharing peer |
| `Size` | File size in human-readable form |
| `Peers` | Number of peers known to have this file at query time |
| `Hash` | BLAKE3 root hash (truncated) — uniquely identifies the file content |

## Downloading a result

Pass the row number directly to `rucio download add`:

```sh
rucio download add 3
```

The row numbers are only valid for the most recent search. If you run another
search, previous row numbers refer to the new results.

## Searching a single network

By default a search queries both the Rucio P2P network and the eMule/Kad2
network in parallel. To restrict it to one protocol, pass `--network` to
`rucio search add`:

```sh
rucio search add --network rucio "great expectations"   # Rucio peers only
rucio search add --network emule "great expectations"   # eMule/Kad2 only
rucio search add --network both  "great expectations"   # both (the default)
```

This is mostly useful for scripting or when you only care about one network.
Omitting `--network` (or passing `both`) keeps the default unified search.

Asking for `--network emule` on a daemon built without eMule support is an
error (the daemon has no Kad2 leg to run). At the API level this is the
optional `network` field of `POST /api/v1/searches`; omitting it keeps the
default unified search.

## Tips

**Search is keyword-based, not fuzzy.** The query is split into words and each
word is matched against file names announced by peers. A search for
`"great expectations"` will match files whose name contains both words.

**Very short keywords only search the Rucio network.** Searches also query the
eMule/Kad2 network in parallel, but eMule indexes only whole words of **3 or
more characters** — so a 1–2 character keyword such as `1x` returns results
from Rucio peers but never from eMule (its index has no entry for it, and there
is no partial-word search in Kad). To get eMule results too, search a longer
word that actually appears in the file name (e.g. `1x01` instead of `1x`).
Rucio skips the (guaranteed-empty) eMule lookup for such short keywords, so
these searches also finish a little faster.

**Accents matter on the eMule network, but not on Rucio.** Rucio normalizes
diacritics, so `camion` and `camión` return the same Rucio results. The
eMule/Kad network only lowercases keywords — it does **not** fold accents — so
`camión` and `camion` are distinct entries in its index and return different
results. To get eMule matches, type the keyword with the same accents it has in
the file name. This — and other quirks that come from the eMule network rather
than from Rucio — is explained in
[eMule/Kad network limitations](../design/08-emule-kad-limitations.md).

**Results depend on connected peers.** If the network is small or your node has
few connections, results may be sparse. Check `rucio node status` to see how many
peers you are connected to.

**The same file from multiple peers is deduplicated by hash.** If three peers
share an identical file, it appears as one row with `Peers: 3`. Rucio will
download chunks from all of them in parallel.

**On the eMule network, the peer/source count is only a hint.** A Kad result
may advertise many sources yet have no real provider when you try to download
it — the number is a published, unverified value cached by the network, not a
live check. Rucio confirms real availability when it looks for providers, so a
result that "had sources" but finds none is the network being unreliable, not a
Rucio fault. See
[eMule/Kad network limitations](../design/08-emule-kad-limitations.md).

**You can download without searching** if you already have a magnet link:

```sh
rucio download add "rucio:7b4a...?name=great-expectations.epub&size=1258291"
```

See [Magnet links](07-magnet-links.md) for more detail.
