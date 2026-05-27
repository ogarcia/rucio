# Node classes

rucio classifies every node — including itself — into one of three classes
based on network reachability. The concept is borrowed from eMule's HighID /
LowID distinction.

## Classes

### HighID

The node is reachable on a public IP address. Other peers can dial it
directly. A HighID node:

- Can serve file chunks to any peer that requests them.
- Is a full participant in the DHT as both a client and a server.
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
- May also serve chunks to peers on the same LAN via mDNS-discovered
  local addresses.
- Appears in `rucio node status` as `LowID`.

### Unknown

The node has not yet received an observed address from a remote peer (i.e.,
Identify has not completed with any connected peer). This is the initial
state on startup. It resolves to HighID or LowID within a few seconds of
connecting to the first peer.

## Classification logic

The class is determined by `addr_scope_hint()` applied to the node's observed
address (as reported by Identify):

```
observed address is RFC1918 (10/8, 172.16/12, 192.168/16)  →  LowID
observed address is link-local (169.254/16)                 →  LowID
observed address is IPv6 ULA (fc00::/7)                     →  LowID
observed address is IPv6 link-local (fe80::/10)             →  LowID
observed address is a public IP                             →  HighID
no observed address yet                                     →  Unknown
```

`addr_scope_hint` extracts the IP component from a multiaddr string and
applies the above rules. It returns a short label (`"LAN"`, `"link-local"`,
etc.) for local addresses, or an empty string for public addresses.

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

In practice, DCUtR succeeds for the majority of consumer-grade NAT devices,
so the relay is only a short-term bridge during hole punching and a
persistent fallback for the minority of strict-NAT cases.
