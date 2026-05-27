# eMule / Kad2 Compatibility

> This document covers the `rucio-emule` crate and all eMule/Kad2-related
> behaviour. It applies only when the daemon is compiled with the
> `emule-compat` feature flag.

## Overview

rucio includes an opt-in compatible Kad2 client that can:

1. Bootstrap into the eMule/aMule Kademlia network.
2. Search for ed2k sources for a given MD4 hash.
3. Download files via ed2k links, verify chunks with MD4, and register
   completed files in the rucio DHT using their BLAKE3 hash.

The implementation lives in the `rucio-emule` crate. It has no dependency on
`rucio-daemon` — the two communicate exclusively via `KadHandle`, a
channel-based API.

---

## Feature flag

```toml
# Cargo.toml workspace features
emule-compat = ["rucio-emule", "dep:md4", "dep:reqwest"]
```

The feature pulls in:

- `rucio-emule` — the Kad2 protocol stack
- `md4` — chunk verification (eMule uses MD4, not BLAKE3)
- `reqwest` — HTTP client for fetching `nodes.dat`
- `flate2` with the `zlib-rs` backend — decompression of packed Kad2 packets

To check at runtime whether the feature is compiled in:

```sh
rucio emule status
# eMule compatibility: enabled
```

---

## Crate structure (`rucio-emule`)

| Module | Contents |
|---|---|
| `kad::packet` | Encoder/decoder for all Kad2 opcodes; handles packed (`0xe5`) zlib packets |
| `kad::routing` | In-memory routing table; `parse_nodes_dat` for bootstrap seed loading |
| `kad::task` | `KadTask`, `KadHandle`, `KadTaskConfig`, `spawn()` |

---

## KadTask

`KadTask` is a permanent Tokio task that owns the Kad2 UDP socket for the
entire lifetime of the daemon. Exclusive ownership avoids the race conditions
that arise from sharing a socket with `Arc<UdpSocket>`.

### Configuration (`KadTaskConfig`)

| Field | Type | Default | Description |
|---|---|---|---|
| `udp_port` | `u16` | `4672` | UDP port to bind |
| `nodes_dat_path` | `PathBuf` | platform data dir | Path to bootstrap seed file |
| `request_timeout` | `Duration` | `5s` | Per-request timeout |
| `min_contacts` | `usize` | `4` | Re-bootstrap threshold |
| `max_contacts` | `usize` | `50` | Bootstrap stops early when reached |
| `bootstrap_rounds` | `usize` | `3` | Maximum iterative bootstrap rounds |

### Lifecycle

```
spawn()
  └── bind UDP socket on udp_port
  └── load nodes.dat → seed list
  └── iterative_bootstrap() → up to 3 rounds, stops at 50 contacts
  └── keepalive loop (periodic):
        if routing_table.len() < min_contacts:
          re-bootstrap from last_seeds
        else:
          send HELLO to a random contact
```

`last_seeds` is stored in the task state so that re-bootstrap works even
if `nodes.dat` is not re-read from disk.

---

## Packet protocol

### Header bytes

| Byte | Constant | Meaning |
|---|---|---|
| `0xe4` | `KAD2_PROTO` | Standard Kad2 packet |
| `0xe5` | `OP_KADEMLIAPACKEDPROT` | zlib-compressed Kad2 packet |
| `0xe3` | — | Kad1 (legacy) — discarded |

### Packed packets (`0xe5`)

Modern eMule and aMule nodes respond exclusively with `0xe5` packed packets.
The decoder:

1. Reads the `0xe5` header byte.
2. Reads the next byte as the inner opcode.
3. Decompresses the remaining payload with zlib (`flate2`, zlib-rs backend).
4. Decodes the decompressed bytes as a standard Kad2 message.

`PacketError::Decompress` is returned if decompression fails.

Before this was implemented, all responses from modern nodes were discarded
as `WrongProto(229)`, resulting in `contacts=0` after bootstrap.

### Integer encoding

All multi-byte integers in Kad2 packets are **little-endian**. IP addresses
are stored as 32-bit LE unsigned integers. The correct reconstruction is:

```rust
let raw = cursor.read_u32_le()?;      // LE-interpreted u32
let ip = Ipv4Addr::from(raw.to_be_bytes()); // MSB-first for Ipv4Addr
```

`to_be_bytes()` on the LE-read value is correct. This is confirmed by
regression tests `test_bootstrap_res_ip_roundtrip` and `test_res_ip_roundtrip`
in `kad/packet.rs`.

### KAD_VERSION

rucio advertises `KAD_VERSION = 11`, which is compatible with current
eMule/aMule deployments.

---

## Bootstrap

### nodes.dat format

rucio uses the standard eMule `nodes.dat` format (version 2). The file is a
binary sequence of contact records. Each record contains:

- Kad node ID (16 bytes)
- IP address (4 bytes, LE u32)
- UDP port (2 bytes, LE u16)
- TCP port (2 bytes, LE u16)
- Node type / version (1 byte)

`parse_nodes_dat` skips entries with private, multicast or unspecified IP
addresses (RFC1918 `10/8`, `172.16/12`, `192.168/16`; multicast `224/4`;
loopback `127/8`). These are invalid entries in the upstream file and not a
parsing bug.

The official source is `http://upd.emule-security.org/nodes.dat`
(~200 contacts, refreshed regularly). Fetch it with:

```sh
rucio emule bootstrap
```

### Iterative bootstrap

```
Round 1: send BootstrapReq to all seeds
         collect BootstrapRes responses (timeout = request_timeout)
         add new contacts to routing table
         if contacts >= max_contacts → stop

Round 2: send BootstrapReq to newly discovered contacts
         ...

Round 3: (same)
         if contacts >= max_contacts → stop
```

Round timing: deadline = `request_timeout × 2` per round (5 s default →
10 s per round). Total maximum bootstrap time: ~30 s.

With a healthy `nodes.dat`, bootstrap typically reaches 50+ contacts in 2
rounds.

### Re-bootstrap

If the routing table drops below `min_contacts = 4`:

1. The keepalive loop triggers a new bootstrap round.
2. Seeds are taken from `last_seeds` (saved at startup from `nodes.dat`).
3. No file I/O is required for re-bootstrap.

This handles:
- Router restarts
- ISP IP changes
- Long idle periods (nodes drop from the routing table over time)

---

## ed2k download flow

```
rucio download add "ed2k://|file|name.ext|size|md4hash|/"
    │
    ▼
Parse ed2k link → extract MD4 hash + size
    │
    ▼
Register download in DB (state: finding_providers)
    │
    ▼
Kad2: search for sources (FindValue on MD4 hash key)
    │
    ▼
Found sources → state: queued
    │
    ▼
Connect to source via eMule TCP protocol
Fetch chunks, verify each with MD4
    │
    ▼
All chunks done → compute BLAKE3 of complete file
                → announce to rucio DHT
                → state: completed
```

The download appears in `rucio download list` and supports `--watch` throughout.

---

## Port requirements

The Kad2 UDP socket must be **reachable from the internet** for bootstrap
responses to arrive. Without inbound UDP reachability, bootstrap packets can
be sent outbound but responses are blocked by NAT/firewall.

| Environment | Action |
|---|---|
| Container (Docker/Podman) | `-p 4672:4672` (maps both TCP and UDP) |
| VPS / bare metal | `ufw allow 4672/udp && ufw allow 4662/tcp` |
| Home router | Port-forward `4672/udp` and `4662/tcp` → local machine IP |
| WSL2 | Port-forward from Windows + Windows Firewall rules |

Two ports must be open for full functionality:

| Port | Env var | Config key | Protocol | Effect if closed |
|---|---|---|---|---|
| `4672` | `RUCIOD_EMULE_UDP_PORT` | `emule.udp_port` | UDP | Kad2 bootstrap and source search fail |
| `4662` | `RUCIOD_EMULE_TCP_PORT` | `emule.tcp_port` | TCP | Node runs as Low-ID (slower downloads) |

> **Container note:** `-p 4672:4672/udp -p 4662:4662/tcp`
