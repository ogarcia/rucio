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
3. Records the peer in the SQLite database for display in `rucio peers`.

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

**Bootstrap** — on startup, rucio dials a set of bootstrap peers
(`network.bootstrap_peers` in config). Once connected, it runs a random walk
to populate its routing table. Re-announcement happens every 22 minutes to
keep provider records alive (DHT records expire after roughly 24–48 hours
depending on the implementation).

**Stale share pruning** — during re-announcement, any file path that no longer
exists on disk is removed from the database and not re-announced.

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

## Peer lifecycle

```
ConnectionEstablished  →  PeerConnected  (increments peer counter)
Identify::Received     →  PeerDiscovered (upsert in DB, add to Kademlia)
ConnectionClosed       →  PeerDisconnected (decrements peer counter)
```

`PeerConnected` and `PeerDisconnected` are distinct from `PeerDiscovered`.
The peer counter reflects currently connected peers, not the total number of
known peers.

## No trackers, no relays

rucio deliberately has no tracker infrastructure:

- **No central tracker.** File discovery goes through Kademlia provider
  records only.
- **No relay nodes for data.** File data is always transferred directly
  between the downloading and uploading peer. There is no TURN-style relay.
  LowID nodes can download but cannot serve chunks to peers they cannot
  reach directly (see [Node classes](05-node-classes.md)).
- **No BitTorrent compatibility.** The protocol is incompatible with
  BitTorrent by design. This allows us to use BLAKE3 instead of SHA1/SHA256
  and to define a simpler, more efficient chunk protocol.

## PEX — Peer Exchange

Transfer responses include a `peers` field listing other providers of the
same file (`PeerExchange`). The downloader adds these addresses to its known
providers list and can dial them for additional chunk sources. This is the
only gossip mechanism for provider addresses outside of the DHT.
