# Node classes

rucio classifies every node — including itself — into one of three classes
based on network reachability. The concept is borrowed from eMule's HighID /
LowID distinction.

## Classes

### HighID

The node is reachable directly on a public IP address (a cold dial works, as
confirmed by AutoNAT). Other peers can dial it without an intermediary. A
HighID node:

- Can serve file chunks to any peer that requests them.
- Is a full participant in the DHT as both a client and a server.
- **Announces its shared files as Kademlia provider records** — only HighID
  nodes do (see [DHT provider announcements](#dht-provider-announcements)).
- Appears in `rucio node status` as `HighID`.

### LowID

The node is behind NAT or a firewall and has no observed public address.
Other peers on the internet generally cannot dial it directly. A LowID node:

- **Can still download** — it dials out to providers and pulls chunks.
- **Becomes reachable via relay reservation.** When the node is classified
  as LowID and a relay-capable peer has been discovered, it calls
  `swarm.listen_on(<relay-circuit-addr>)` to make a reservation and starts
  advertising that address. Other peers can then reach it through the relay.
- **DCUtR upgrades relay connections to direct ones** — once a peer
  connects through the relay, both sides attempt a simultaneous NAT hole
  punch. On success the relay is no longer needed for that peer pair.
- **Does NOT announce its shared files as DHT providers** — see
  [DHT provider announcements](#dht-provider-announcements). It participates
  as a downloader and stays reachable for control traffic, but it never
  advertises content that a peer might have to pull through a relay.
- May still serve chunks to peers on the same LAN via mDNS-discovered
  local addresses.
- Appears in `rucio node status` as `LowID`.

### Unknown

The node has not yet received an observed address from a remote peer (i.e.,
Identify has not completed with any connected peer). This is the initial
state on startup. It resolves to HighID or LowID within a few seconds of
connecting to the first peer.

## Classification logic

`ClassificationState` (in `rucio-net/src/classify.rs`) combines two inputs —
addresses **observed** by remote peers via Identify, and addresses **confirmed**
reachable by AutoNAT — and applies a single predicate, `is_direct_public_listen`:

```
HighID  ⇔  we have an address that is, all at once:
             · a public IP (not RFC1918 / link-local / ULA / loopback), and
             · on one of our listen ports (not an ephemeral source port), and
             · direct — NOT a relayed /p2p-circuit address
```

An address satisfies this either because **AutoNAT confirmed it**
(`ExternalAddrConfirmed` — authoritative) or because a peer **observed us on
our listen port** via Identify (a fast hint, which happens when a peer dials
*in*). Then:

```
any address passes is_direct_public_listen   →  HighID
observations exist but none pass              →  LowID
no observations and nothing confirmed yet     →  Unknown
```

Two subtleties this captures that a naive "public IP → HighID" check misses:

- **Ephemeral ports don't count.** When the node only dials *out*, Identify
  reports its public IP on the connection's random source port, not the listen
  port. That is not proof of inbound reachability → LowID until AutoNAT
  confirms the translated listen-port address (see
  [networking → AutoNAT v2](02-networking.md)).
- **Relayed addresses don't count.** AutoNAT will happily confirm a
  `/p2p-circuit` address (the relay path works), but that means the node is
  reachable *only through a relay* — i.e. it is really LowID. Circuit
  addresses are excluded from the HighID decision.

## Display in `rucio node status`

```
Connectivity:   HighID  ·  4 peer(s)  ·  observed 203.0.113.5:4001
Connectivity:   LowID   ·  2 peer(s)  ·  observed 192.168.1.10:4001
Connectivity:   Unknown ·  0 peer(s)  ·  no observed public address yet
```

Bootstrap multiaddrs are split into two sections:

- **Public bootstrap multiaddrs** — internet-routable addresses.
- **Local bootstrap multiaddrs (LAN / same-machine only)** — RFC1918,
  link-local, loopback. These are useful for testing and same-machine setups
  but will not work across the internet.

The separation is cosmetic (both lists are used equally for dialing) but
helps the user understand the network topology at a glance.

## Upload prioritisation

When a remote peer requests a chunk, the daemon classifies the requester as
HighID or LowID using the same address check applied to `PeerDiscovered`
events: if any of the peer's advertised addresses is a publicly-routable IP,
it is HighID; otherwise LowID.

This classification drives the **work-conserving upload priority scheduler**
(`upload_scheduler::UploadScheduler`):

- **HighID requests** acquire the bandwidth throttle immediately, bracketing
  the acquisition with `enter_highid()` / `leave_highid()`.
- **LowID requests** call `wait_for_lowid_turn()` first, which parks the
  task until `highid_active` reaches zero.
- When the node is idle (no HighID uploads competing), LowID requests proceed
  without delay and receive full available bandwidth.

The intent is to make leeching (LowID → download-only) less attractive by
ensuring that peers which contribute to the network get faster uploads in
return.  It is not a hard block: LowID nodes can still download from Rucio
peers; they just get lower priority when HighID peers are also requesting
chunks.

Peers whose class is not yet known (no `PeerDiscovered` event before the
first chunk request) are treated as HighID to avoid accidentally starving
early requestors.

## NAT traversal

LowID nodes use a two-stage approach to become reachable:

### Stage 1 — Relay reservation

When a LowID node discovers a peer that advertises the circuit relay hop
protocol (`/libp2p/circuit/relay/0.2.0/hop`), it issues a reservation on
that peer. After the reservation is accepted the node starts advertising a
`/p2p-circuit` address, and remote peers can connect to it through the relay.

Any full node can act as a relay server — there is no dedicated relay
infrastructure. The relay server enforces built-in resource limits
(maximum reservations, maximum simultaneous circuits) to prevent abuse.

### Stage 2 — DCUtR hole punching

When a remote peer connects to the LowID node through a relay circuit, the
DCUtR protocol kicks in. Both peers exchange their observed addresses
through the relay as a signaling channel and then dial each other
simultaneously, attempting to punch through their respective NATs.

- **Cone NAT (most home routers)** — hole punch succeeds, the relay
  connection is replaced by a direct one.
- **Symmetric NAT / strict firewall** — hole punch fails, the relay
  connection is kept as a fallback. The relay continues to carry traffic
  for that peer pair.

## DHT provider announcements

A node advertises its shared files as Kademlia provider records **only while it
is HighID**. The task tracks the set of keys it wants to provide; it announces
them when the node reaches HighID and calls `stop_providing` when it drops back
out (the announce/stop is driven from `reconcile_provider_announcements`).

The rule is deliberately strict — **HighID, not merely "reachable"** — because
HighID is the one state that guarantees a *direct* data path. The alternatives
do not:

- A **relay-reachable** node would force every downloader to pull file data
  through a relay, turning ordinary full nodes into bandwidth relays. That
  violates the "no TURN-style data relay" principle (see
  [networking](02-networking.md#design-principles)).
- A **DCUtR-reachable** node *usually* ends up direct, but hole punching is
  per-connection and opportunistic: when it fails (symmetric NAT) the
  connection falls back to carrying data over the relay. So DCUtR-capability
  cannot guarantee a direct path either, and is not a safe basis for
  advertising content.

The trade-off: a LowID node — even one reachable via relay/DCUtR — does **not**
make its files discoverable through the DHT. It remains a full downloader and is
reachable for control/coordination, but content it shares is only served once it
becomes HighID (e.g. by opening its listen port, or via UPnP). This keeps relays
free of file-transfer load and keeps the DHT free of provider records that a
peer could not reach directly.

In practice, DCUtR succeeds for the majority of consumer-grade NAT devices,
so the relay is only a short-term bridge during hole punching and a
persistent fallback for the minority of strict-NAT cases.
