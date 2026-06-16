# Design documentation

Internal architecture, protocol decisions and implementation rationale.
Aimed at contributors and anyone who wants to understand how Rucio works
under the hood.

| Document | Description |
|---|---|
| [01 — Architecture](01-architecture.md) | Crate layout, fat binary, daemon/CLI split, REST API |
| [02 — Networking](02-networking.md) | libp2p stack: mDNS, Kademlia DHT, Gossipsub, Identify, PEX, UPnP, eMule Kad2 |
| [03 — Transfer protocol](03-transfer-protocol.md) | Manifest, chunk layout, parallel download, resumption, FindingProviders state |
| [04 — Storage](04-storage.md) | SQLite schema, shared\_dirs, temp/download paths, watcher debounce |
| [05 — Node classes](05-node-classes.md) | HighID / LowID / Unknown classification and connectivity display |
| [06 — Hashing](06-hashing.md) | BLAKE3 content addressing, magnet link format, offline hashing |
| [07 — eMule / Kad2](07-emule-kad.md) | eMule compatibility: KadTask, packed packets, bootstrap, ed2k download flow |
| [08 — eMule / Kad2 limitations](08-emule-kad-limitations.md) | Inherited network quirks (not Rucio bugs): accent-sensitive search, unreliable source counts |
