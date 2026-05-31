# Networking

rucio uses [libp2p](https://libp2p.io/) as its networking layer. The swarm
combines several behaviours that serve distinct purposes.

## Behaviours

### Identify

The Identify protocol exchanges metadata (protocol version, listen addresses,
observed address) with every peer upon connection.

When rucio receives an Identify response from a peer it:

1. Emits a `PeerDiscovered` event with the peer's announced `listen_addrs`.
2. Adds those addresses to the Kademlia routing table, so the peer is
   reachable even if it was found via mDNS.
3. Records the peer in the SQLite database for display in `rucio node peers`.

The peer's **observed address** (what the remote end sees as our source
address) is also stored and used to classify the node as HighID or LowID.

### mDNS

mDNS (Multicast DNS) enables zero-configuration peer discovery on the local
network and on the same machine. No bootstrap address is required — nodes on
the same LAN find each other automatically within a few seconds.

mDNS discovery triggers a `PeerDiscovered` event, which causes the peer to be
added to the Kademlia routing table.

### Kademlia DHT

The Kademlia DHT is the backbone of internet-wide discovery.

**Provider records** — when a file is indexed, rucio calls
`kad.start_providing(key)` where `key` is the BLAKE3 hash of the file. Other
nodes can find providers for a hash by calling `kad.get_providers(key)`.

**Bootstrap** — on startup, rucio dials a set of bootstrap peers. By default the
configured `network.bootstrap_peers` are **added** to `BUILTIN_BOOTSTRAP_PEERS`
(deduplicated); setting `network.exclusive_bootstrap = true` uses only the
configured peers and ignores the built-ins (for a separate network — not a
privacy boundary, since anyone with a peer multiaddr can still join). Addresses
may be literal IPs or `/dns4` / `/dns6` names — the transport is built
`.with_dns()` so domains resolve, which lets the infrastructure survive IP
changes. Once connected, it runs a random walk to populate its routing table.

**Re-announcement & republication** — libp2p republishes our provided keys on
its own (~12 h, before the ~24 h provider-record TTL) and re-replicates them to
the *current* closest peers, which keeps records fresh and heals DHT churn.
There is therefore **no periodic re-announce timer**; the daemon only has to
populate libp2p's in-RAM *provided set*, which it does:

1. from the database **on startup** (registers the keys locally — these queries
   reach nobody yet because no peer is connected),
2. once more **~5 s after the first peer connects** (Kad bootstrap has populated
   the routing table by then, so this is the publication that actually lands),
3. **per file** as the watcher indexes a new share.

After that, libp2p owns freshness. (Earlier versions ran a 22-minute re-announce
tick; it duplicated libp2p's republication and spiked CPU proportionally to the
share count, so it was removed.)

**De-publishing** — to stop providing a removed file it must be `StopProviding`'d
so libp2p drops it from the in-RAM provided set; **deleting it from the database
alone is not enough**, because republication reads the provided set, not the DB.
Every removal path pairs the DB delete with `StopProviding`: the live watcher
(inotify), the periodic share rescan, and the `share remove` API.

**Share rescan** — inotify only sees changes while the daemon runs, so a rescan
reconciles the index with disk at **startup and every 24 h**: it hashes new
files, de-indexes (and `StopProviding`s) vanished ones, and re-hashes those whose
`size` or `mtime` changed. On a stable library it only walks + `stat`s (no
hashing). This is what catches changes made while the daemon — or inotify — was
not running.

**Re-bootstrap** — the main event loop runs a 10-minute tick that re-adds
bootstrap peers and triggers a new random walk if the node has no connected
peers. This handles transient network outages and natural DHT churn.

**Provider store limits** — the Kademlia `MemoryStore` caps how many keys it
holds, tuned per role in `BehaviourConfig`:

- `kad_max_provided_keys` — our *own* shared files we announce. The libp2p
  default (1024) is far too low for a real library, so a full node sets it to
  1M. (A node sharing more than the cap fails to announce the excess with
  "store cannot contain any more provider records".)
- `kad_max_records` — provider records from *other* peers held in RAM to serve
  `GET_PROVIDERS` as a DHT server. A client keeps this modest (100k) so it
  doesn't become a large in-memory store; a bootstrap/indexer node sets it high
  (1M) since it sees the whole network.

**Bootstrap/indexer storage** — a `rucio-bootstrap` node running the passive
indexer keeps two independent stores: a persistent **SQLite** index (the source
of truth for REST search, unbounded by the caps above) and the in-RAM
`MemoryStore` (bounded, used only to re-serve records over the DHT). A full
MemoryStore therefore degrades DHT re-serving but never search, and is not a
single point of failure since a file's real providers answer `GET_PROVIDERS`
themselves. Moving the DHT store to disk (a SQLite-backed `RecordStore`) was
deliberately deferred — see the `indexer` module docs for the cost/options
hierarchy.

**Memory model of shares** — RAM scales ~linearly with the **number** of shared
files, not with their size, and the per-file cost is tiny:

- Each shared file adds one entry to Kademlia's in-RAM provided set (the key +
  bookkeeping, tens of bytes), capped at `kad_max_provided_keys` — order of tens
  of MB even at ~1M files.
- The authoritative index (`shared_files` + `chunks`) lives **on disk** in
  SQLite and is read on demand; there is **no resident in-memory map of all
  shares** in the daemon. The `chunks` table grows on disk with the total bytes
  shared, but not in RAM (SQLite's page cache is bounded).
- File **content** is never held in RAM — chunks are read from disk when serving.
- Allocations that are proportional to the file count (the hash list built for a
  re-announce, the rescan's disk-vs-index maps) are transient and freed after use.

### Gossipsub

Gossipsub is a publish/subscribe protocol used for keyword search.

When the user runs `rucio search "query"`, the daemon publishes a
`SearchQuery` message to the `rucio-search` topic. All subscribed peers
receive it, look up matching files in their local database, and reply with
`SearchResult` messages published to the same topic.

The CLI polls the daemon for accumulated results and exits after three
consecutive idle cycles (no new results in the last polling window).

### request_response

File transfer uses libp2p's `request_response` behaviour with a custom
protocol (`/rucio/transfer/2.0.0`). See
[Transfer protocol](03-transfer-protocol.md) for details.

### Circuit relay server

Every full node mounts `relay::Behaviour` (circuit relay v2 hop server).
This lets LowID peers make a reservation and advertise the node's address
as a reachable endpoint via `/p2p-circuit`. Resource limits are applied by
the libp2p relay implementation to prevent the server from being abused as
a bandwidth relay.

The relay server is **disabled** on bootstrap-only and indexer nodes to
keep their protocol surface minimal.

### Circuit relay client

`relay::client::Behaviour` is always mounted (it is wired into the
transport layer by the SwarmBuilder). When the node is classified as LowID
and an Identify response reveals that a connected peer supports the relay
hop protocol, the daemon calls `swarm.listen_on(<relay-circuit-addr>)` to
initiate a reservation. On success the swarm starts advertising the
`/p2p-circuit` address so other peers can reach the LowID node through the
relay.

The relay is only used for the **connection** — see DCUtR below for how
that connection is typically upgraded to a direct one.

### DCUtR — Direct Connection Upgrade through Relay

When a LowID node is connected to a remote peer via a relay circuit, both
ends run the DCUtR protocol (`dcutr::Behaviour`). DCUtR coordinates
simultaneous TCP/QUIC dials (NAT hole punch) through the relay as a
signaling channel:

1. The two peers exchange their observed addresses through the relay
   connection.
2. Both dial each other at the same instant, hoping to punch through their
   respective NATs.
3. If the hole punch succeeds, the relay connection is replaced by a direct
   one and the relay is no longer needed for that peer.
4. If it fails (e.g. symmetric NAT), the relay connection is kept as a
   fallback.

For most consumer routers (cone NAT), hole punching succeeds and data flows
directly. The relay is used only during the hole-punch window and as a
persistent fallback for the minority of symmetric-NAT cases.

## Peer lifecycle

```
ConnectionEstablished  →  PeerConnected  (increments peer counter)
Identify::Received     →  PeerDiscovered (upsert in DB, add to Kademlia)
ConnectionClosed       →  PeerDisconnected (decrements peer counter)
```

`PeerConnected` and `PeerDisconnected` are distinct from `PeerDiscovered`.
The peer counter reflects currently connected peers, not the total number of
known peers.

## Design principles

- **No central tracker.** File discovery goes through Kademlia provider
  records only.
- **No TURN-style data relay.** Circuit relay is used only to establish
  connections and as a short-term fallback when NAT hole punching fails.
  Once a direct connection is established via DCUtR, data flows between
  peers without any intermediary. Relay servers are ordinary full nodes
  with built-in resource limits — there is no dedicated relay
  infrastructure.
- **No BitTorrent compatibility.** The protocol is incompatible with
  BitTorrent by design. This allows us to use BLAKE3 instead of SHA1/SHA256
  and to define a simpler, more efficient chunk protocol.

## PEX — Peer Exchange

Transfer responses include a `peers` field listing other providers of the
same file (`PeerExchange`). The downloader adds these addresses to its known
providers list and can dial them for additional chunk sources. This is the
only gossip mechanism for provider addresses outside of the DHT.

---

## UPnP / IGD port mapping

When `network.upnp = true` (the default), the daemon spawns a background
`UPnP task` that:

1. Discovers the LAN router via the IGD (Internet Gateway Device) protocol.
2. Requests port mappings for:
   - TCP port from `node.listen_addrs` (libp2p)
   - UDP `emule.udp_port` (Kad2, only with the `emule-compat` feature)
   - TCP `emule.tcp_port` (eMule peer connections, only with the `emule-compat` feature)
3. Renews the leases periodically before they expire.
4. Writes the discovered external IP address to `AppState.external_ip`, which
   is returned in `GET /api/v1/status` and displayed in `rucio node status`.

UPnP is silently skipped if the router does not support IGD or if the daemon
is running on a host with a direct public IP (no NAT). Set `network.upnp =
false` to disable it explicitly (recommended in containers and on VPS).

---

## eMule / Kad2 network (emule-compat)

> This section applies only when the daemon is compiled with the
> `emule-compat` feature.

### Overview

The `rucio-emule` crate implements a compatible Kad2 client that can
bootstrap into the eMule/aMule Kademlia network and search for ed2k sources.
It runs as a separate Tokio task (`KadTask`) and communicates with the rest
of the daemon via a channel-based `KadHandle` API.

### KadTask

`KadTask` owns the Kad2 UDP socket exclusively for the lifetime of the
daemon. No other task shares the socket; this avoids race conditions on
receive.

The task:

1. Binds a UDP socket on `emule.udp_port` (default `4672`).
2. Reads a `nodes.dat` file (`storage.nodes_dat_path`) to obtain bootstrap
   seeds.
3. Runs iterative bootstrap (up to 3 rounds, stops early at 50 contacts).
4. Runs a keepalive loop that re-bootstraps from saved seeds when the routing
   table drops below `min_contacts` (4).

### Packet encoding

All Kad2 packets use little-endian integers. The protocol header byte
determines the packet type:

| Byte | Meaning |
|---|---|
| `0xe4` | `KAD2_PROTO` — standard Kad2 packet |
| `0xe5` | `OP_KADEMLIAPACKEDPROT` — zlib-compressed Kad2 packet |
| `0xe3` | Kad1 (older eMule) — ignored |

Modern eMule/aMule nodes respond with `0xe5` packed packets. The decoder
decompresses the payload with zlib (`flate2`, zlib-rs backend) before
decoding. Packets with an unrecognised protocol byte are logged at `trace`
level and discarded.

### IP byte order

Node addresses in Kad2 packets are stored as 32-bit little-endian unsigned
integers. The decoder reads them with `read_u32_le()` and reconstructs the
`Ipv4Addr` using `to_be_bytes()` on the resulting value. This is correct:
`read_u32_le` returns the integer with LE byte order semantics, and
`to_be_bytes()` lays it out MSB-first as `Ipv4Addr` expects.

### nodes.dat

The bootstrap file (`nodes.dat`) is the standard eMule format (version 2).
It is fetched from `http://upd.emule-security.org/nodes.dat` via
`rucio node emule bootstrap`. The file contains up to ~200 bootstrap contacts;
some entries may have private or multicast IPs — those are invalid entries
in the file itself and are silently skipped.

### Re-bootstrap

If the Kad2 routing table drops below `min_contacts = 4`, the keepalive
loop re-reads the saved bootstrap seeds and sends a new round of bootstrap
requests. This handles router restarts, ISP IP changes, and long idle periods.
