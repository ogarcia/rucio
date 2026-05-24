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
- Appears in `rucio status` as `HighID`.

### LowID

The node is behind NAT or a firewall and has no observed public address.
Other peers on the internet generally cannot dial it directly. A LowID node:

- **Can still download** — it dials out to providers and pulls chunks.
- **Cannot reliably serve chunks** to arbitrary internet peers (they cannot
  reach it).
- May still serve chunks to peers on the same LAN, since mDNS-discovered
  peers can use the local address.
- Appears in `rucio status` as `LowID`.

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

## Display in `rucio status`

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

## Why not TURN / hole-punching?

Implementing NAT traversal (hole-punching via STUN/TURN or libp2p's Circuit
Relay) would allow LowID nodes to serve chunks to arbitrary peers. This is
on the roadmap but is not implemented yet, because:

1. It requires infrastructure (relay nodes).
2. Circuit Relay in libp2p has significant complexity and bandwidth cost.
3. For the initial release, LowID nodes being download-only is an acceptable
   limitation — the same constraint existed in early eMule.
