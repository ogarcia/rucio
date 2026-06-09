# eMule / Kad2 Compatibility

> This document covers the `rucio-emule` crate and all eMule/Kad2-related
> behaviour. It applies only when the daemon is compiled with the
> `emule-compat` feature flag.

## Overview

Rucio includes an opt-in compatible Kad2 client that can:

1. Bootstrap into the eMule/aMule Kademlia network.
2. Search for ed2k sources for a given MD4 hash.
3. Download files via ed2k links, verify chunks with MD4, and register
   completed files in the Rucio DHT using their BLAKE3 hash.
4. Share the chunks it has already verified **while still downloading**
   (partial sharing), contributing to a file's availability from the first
   complete chunk (see
   [Partial sharing](#partial-sharing-uploading-while-downloading)).
5. Keep seeding completed eMule downloads back to the network — including
   serving the ed2k hashset — as a good Kad citizen (see
   [Seeding completed downloads](#seeding-completed-downloads)).
6. Offer **every Rucio-network share** to eMule as a source too (source-only,
   not keyword-published), so anyone holding the ed2k link can fetch it from us
   (see [Seeding Rucio-native shares](#seeding-rucio-native-shares-source-only)).

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
rucio node emule status
# eMule compatibility: enabled
```

---

## Crate structure (`rucio-emule`)

| Module | Contents |
|---|---|
| `kad::packet` | Encoder/decoder for all Kad2 opcodes; handles packed (`0xe5`) zlib packets |
| `kad::routing` | In-memory routing table; `parse_nodes_dat` for bootstrap seed loading |
| `kad::task` | `KadTask`, `KadHandle`, `KadTaskConfig`, `spawn()` |
| `kad::obfuscation` | Kad2 UDP obfuscation (RC4) and per-peer verify keys |
| `transfer` | eMule client-to-client TCP: download sessions and the upload server (serving chunks + the hashset) |
| `ed2k` | ed2k link parsing, the MD4 file hash, and hashset computation (`finalize_hashset`) |
| `progress` | `.part.met` slice-completion bitmap (resume support) |

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

Rucio advertises `KAD_VERSION = 11`, which is compatible with current
eMule/aMule deployments.

---

## Bootstrap

### nodes.dat format

Rucio uses the standard eMule `nodes.dat` format (version 2). The file is a
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
rucio node emule bootstrap
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
All chunks done → compute BLAKE3 + ed2k hashset (single read of the file)
                → announce to rucio DHT
                → record in emule_shared_files (keep seeding to eMule)
                → state: completed
```

The download appears in `rucio download list` and supports `--watch` throughout.

---

## Partial sharing (uploading while downloading)

Rucio is a good Kad citizen *before* a download finishes too: an in-progress
ed2k download is offered to the network, serving the chunks already verified.

- The in-progress download is registered in the upload whitelist
  (`ActiveDownloads`) with `complete = false` and its serving `path` pointing at
  the `.part` file (a completed share points at the final file instead).
- When a peer requests the file, the upload server builds the **`OP_FILESTATUS`
  bitmap** (one bit per 9.28 MB slice, `1` = available) from the `.part.met`
  completion bitmap, so we advertise honestly which slices we hold.
- On `OP_REQUESTPARTS`, a requested byte range is served **only if it falls
  entirely within completed slices**. If the peer asks for a slice we don't have
  yet, the upload session is closed rather than serving partial data — we never
  hand out bytes from a half-written chunk.
- The ed2k **hashset is empty while downloading** (it is computed in a single
  pass on completion), so the peer's chunk-hash verification engages once the
  file is done; the complete slices are still served in the meantime.

The status bitmap is loaded **once per upload session**. Slices that complete
mid-session are offered on the peer's next connection — conservative (it never
over-promises), and sufficient for the good-citizen policy.

---

## Seeding completed downloads

Once a download completes, Rucio keeps serving the file to the eMule network
(good-citizen policy) instead of dropping it the moment the transfer finishes.

- Completed files are recorded in the `emule_shared_files` table (`ed2k_hash`,
  `name`, `size`, `path`, `mtime`, `hashset`), **decoupled from the downloads
  list on purpose**: clearing completed downloads must not stop sharing.
- A file is seeded until it is **modified or removed on disk**, enforced two ways:
  - **At startup**, `load_shared_files` re-validates each entry's size + mtime
    against disk, drops any that changed/vanished, and loads the survivors into
    the upload whitelist.
  - **At runtime**, a dedicated inotify watcher on the downloads directory drops
    a share the moment its file changes/disappears. It re-validates against the
    recorded size + mtime (rather than trusting the event kind), so a
    just-completed file is never self-invalidated.
- The upload whitelist (`ActiveDownloads`) holds both in-progress downloads
  (served from the `.part` file) and completed shares (served from the final
  file in the downloads dir); `UploadInfo` carries the serving `path` and a
  `complete` flag, and the status bitmap is all-complete for finished shares.

### Serving the hashset

Files larger than one 9,728,000-byte chunk have an MD4 **hashset** (one MD4 per
chunk; the ed2k file hash is the MD4 of their concatenation). A downloading peer
requests it via `OP_HASHSETREQUEST` (`0x51`) and we answer with
`OP_HASHSETANSWER` (`0x52`):

```
file_hash(16) | part_count(u16 LE) | part_hash(16) * part_count
```

- The hashset is computed in the **same pass** as the completion BLAKE3 hash
  (one read of the file) and persisted in the `hashset` column — never
  recomputed, survives restarts.
- `ed2k::finalize_hashset` follows eMule's **null-chunk convention** (a trailing
  MD4 of a zero-length chunk when the size is an exact multiple of the chunk
  size) and verifies `MD4(concat(parts))` reproduces the file's ed2k hash before
  serving, so we never hand a peer a hashset that would fail verification.
- Single-chunk files have no hashset (their ed2k hash is `MD4(data)`).

### Seeding Rucio-native shares (source-only)

Good citizenship is not limited to files that *came from* eMule: every file
shared on the Rucio network is also offered to the eMule Kad DHT as a source, so
anyone holding its ed2k link finds us as a provider.

- This is **event-driven, never polled** — no periodic rescan that would cause
  recurring CPU spikes. Two paths feed it:
  - **At startup**, `spawn_ed2k_startup_backfill` does a one-shot catch-up of
    pre-existing shares (`shared_files` rows with no matching
    `emule_shared_files` row, joined on disk `path`) — these generate no
    filesystem event so nothing else would pick them up. It hashes one file at a
    time off the runtime, spaced out, since catching up a large library must not
    contend with normal disk I/O.
  - **While running**, the share watcher hands every freshly-indexed path to
    `spawn_ed2k_indexer` over a bounded channel, so a file is hashed for eMule
    the moment it becomes a Rucio share (live events and offline-reconcile
    alike). The watcher uses a non-blocking `try_send`: eMule seeding can never
    throttle the Rucio share pipeline, and a path dropped on overflow is
    recovered by the next startup backfill.
- Each hashed file is recorded in `emule_shared_files` exactly like a completed
  download, so the **republisher and upload server pick it up unchanged**, and
  the share watcher drops it on modification/removal regardless of origin. The
  feature therefore reuses the whole completed-downloads machinery; the only new
  work is computing the MD4 hash (a second read of the file, since the Rucio
  indexer only computes BLAKE3 — `rucio-core` does not depend on MD4).
- This is **source publishing only**: we announce *"we have the file with hash
  X"* (`KADEMLIA2_PUBLISH_SOURCE_REQ`). We do **not** publish keywords, so a
  Rucio-native file is not discoverable by *name* on eMule — only reachable by
  someone who already has its ed2k link. That is a deliberate scope choice:
  source seeding makes us a good citizen for files the network already knows,
  without injecting Rucio's catalogue into eMule's keyword index.
- Runs only when eMule is enabled (`emule.enabled`); no separate config switch.

---

## Identity: user hash and nickname

Two distinct identifiers, easily confused:

| Identifier | Purpose | Visible to users? | Storage |
|---|---|---|---|
| **User hash** | Credit identity — eMule's credit system keys a peer's standing by this 16-byte hash | No (internal) | `emule_identity` table (generated once) |
| **Nickname** | Cosmetic name shown in peers' transfer lists | Yes | `emule.nick` config (default `rucio`) |

- The **user hash** is generated once per node (random, with the eMule client
  markers `[5] = 14`, `[14] = 111` that real clients check) and persisted, so
  the upload credit we earn accrues to one stable identity across restarts. It
  is advertised in both HELLO directions (serving and downloading).
- The **nickname** is sent as a `CT_NAME` tag in HELLO; purely cosmetic, settable
  via `emule.nick` (or `RUCIOD_EMULE_NICK`), and shown in the web settings.
- Full RSA **secure identification** (anti-spoofing of the user hash) is
  intentionally *not* implemented: peers grant credit to unidentified clients
  keyed by the user hash, so it is not required for credit to accrue — it would
  only prevent another client from impersonating our hash.

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
