# Scope and non-goals

Rucio aims to be a small, self-sufficient peer-to-peer file-sharing app: its own
libp2p network, with opt-in eMule/Kad compatibility as a bridge to an existing
user base. To stay focused, a few things are **deliberately out of scope**. This
page lists them so you know what to expect — and what to reach for instead.

These are choices, not missing features: if a request below looks like a gap, it
is working as intended.

## No eMule servers (Kad only)

Rucio's eMule compatibility talks to the **Kad** network — the serverless
Kademlia DHT that modern eMule/aMule clients use. It does **not** connect to the
classic **eD2K servers** (the `server.met` / "connect to a server" world). There
is no server list and nothing to configure: when eMule support is enabled, Rucio
bootstraps straight into Kad.

## Not a BitTorrent client

Rucio is its own network plus eMule/Kad; it does not speak the BitTorrent
protocol. It won't open `.torrent` files or `magnet:?xt=urn:btih:…` links and it
doesn't interoperate with BitTorrent swarms. Rucio uses its own `rucio:` magnet
links instead (see [Magnet links](07-magnet-links.md)).

## Everything is public — it is not an anonymity network

All content on the Rucio network is public: shares, pins and subscriptions are
open offers to host files, and your node's address is visible to peers as in any
P2P network. Rucio is **not** an anonymity tool (it is not Tor or I2P) and makes
no attempt to hide who shares or downloads what. Don't use it for anything you
need to keep private.

## No built-in accounts or passwords

The daemon has **no authentication** of its own — no user accounts, no login.
Access control is delegated to a reverse proxy you put in front of it (e.g.
nginx `auth_basic`), which is both simpler and more flexible. See
[Installation](01-installation.md) for the reverse-proxy setup.

## No chat or social features

Rucio shares files; it is not a messaging or social app. Chat, group rooms,
friends-only private sharing and friend-priority upload slots are out of scope —
other tools (Telegram, Syncthing, …) already do those well, and folding them in
would pull Rucio away from being a lean file-sharing core. Cooperation between
nodes happens through public [subscriptions](10-pinning.md) instead.

---

Everything Rucio *does* do is covered in the rest of this guide.
