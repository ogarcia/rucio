# Pinning

Pinning keeps content available on your node **on purpose**. When you pin
something:

- if you don't have it yet, Rucio **fetches it** (a normal download) and keeps
  it;
- once present, it stays **shared and re-announced** to the network like any
  other shared file;
- it's recorded as a deliberate pin, distinct from a file that just happens to
  be shared.

Think of it as "I want this to stay available here", whether or not you already
have it.

## Where pinned files go

A pinned file you had to fetch lands in the **pin directory** (`storage.pin_dir`,
by default a `pins` folder next to the daemon's data, e.g.
`~/.local/share/rucio/pins`). It's kept separate from your normal downloads so
it's clear which content the node hosts on purpose.

Two exceptions:

- If you **assign the download a category** (it shows up in the downloads list
  while fetching), it goes to that category's folder instead — your choice wins.
- Pinning something you **already have** never moves it; it stays where it is and
  is simply marked as pinned.

Change the location with:

```sh
rucio config set storage.pin_dir /mnt/data/rucio-pins
```

## Pinning from the CLI

```sh
rucio pin add "rucio:7b4a…?name=film.mkv&size=…"   # a magnet → fetch + keep
rucio pin add 3                                     # a download id (something you have/are getting)
rucio pin add 7b4a…<64 hex>                          # a full root hash
```

`pin add` accepts three kinds of target:

| Target | Meaning |
|---|---|
| a `rucio:` magnet | fetch the content if missing, then keep it |
| a numeric download id (from `rucio download list`) | pin content you already have/are downloading — no re-fetch |
| a full 64-character root hash | pin by hash directly |

List and remove:

```sh
rucio pin list
```

```
 Root hash         Name        Size      State
 7b4a…             film.mkv    1.4 GB    available
 d931…             book.epub   980 KB    fetching
```

```sh
rucio pin remove 7b4a…<64 hex>     # unpin (full root hash)
```

### Pin states

| State | Meaning |
|---|---|
| `available` | Present on disk and shared/re-announced |
| `fetching` | Being downloaded |
| `missing` | Pinned but neither present nor in flight (e.g. the fetch was cancelled) |

## Pinning from the web UI

The **Pins** tab mirrors the CLI:

- **Pin content** opens a dialog that accepts a `rucio:` magnet **or** a
  64-character root hash. Content you already have is just marked as kept;
  content you don't is fetched from the network first.
- Each pin shows its name, size and a coloured **state** pill
  (available / fetching / missing).
- **Unpin** removes the pin from the row.

To pin something you already have, the quickest path is the **Shares** tab:
each shared file has a **Pin** button next to its **Magnet** button. One click
keeps that file on purpose and publishes it in your pin-set (for subscribers),
without moving or re-fetching it. The button then turns into **Unpin** (with a
struck-through pin icon), so the Shares list doubles as a view of what you've
pinned — click again to unpin.

## Unpinning

Unpinning removes only the *intent* — it does **not** delete the file. Rucio
never auto-deletes content; to actually stop hosting a pinned file, remove its
directory from sharing (see [Sharing files](03-sharing-files.md)) or delete it
on disk.

# Subscriptions (cooperative pinning)

Pinning keeps *your* chosen content available. A **subscription** keeps
**someone else's** pinned content available: you subscribe to a peer and your
node mirrors that peer's pin-set — within a disk quota you set — fetching it,
sharing it, and re-announcing it. You become an extra provider for that
content, so it survives even if the original node goes offline.

This is what makes Rucio durable: a handful of nodes subscribing to each other
turns one person's pins into many redundant copies. (Everything on the network
is public, so a subscription is simply a public offer to help host content —
there is nothing private about it.)

## Sharing your node so others can mirror you

Others subscribe to you using your **node link** — a `rucio-peer:` string
wrapping your PeerId:

```sh
rucio subscription link
# rucio-peer:12D3KooW…
```

In the web UI, the **Subscriptions** tab has a **Copy my link** button. Share
that link with whoever wants to help keep your pinned content alive.

## Subscribing to a peer

```sh
rucio subscription add rucio-peer:12D3KooW… 10G
```

The first argument is the peer (a `rucio-peer:` link or a bare PeerId); the
second is the **quota** — the most disk you'll devote to mirroring that peer.
Sizes accept `K`/`M`/`G`/`T` suffixes (base 1024), e.g. `500M`, `1.5T`.

How the mirror is built:

- Rucio fetches the peer's pin-set and selects files **smallest-first** until
  the quota is reached. Small files are preferred so one huge pin can't crowd
  out many useful smaller ones.
- Files that don't fit are recorded as **over quota** (skipped) and shown in the
  listing, but not fetched.
- Mirrored files land in the **pin directory** (`storage.pin_dir`), shared and
  re-announced like any pin.
- The node re-syncs each subscription periodically (every few minutes); a fresh
  subscription is synced immediately.

The quota is a **hard ceiling** — Rucio mirrors up to it, never beyond.

## Listing subscriptions

```sh
rucio subscription list
```

```
 Peer               Mirrored          Files                    Synced
 12D3KooWAbc…        3.2 GB / 10 GB    18 (+4 over quota)       yes
```

`Mirrored` is a used / quota meter; `Files` is how many files are mirrored
(with any over-quota count).

In the web **Subscriptions** tab each peer shows a storage meter and a count
that distinguishes files **mirrored** (present on disk) from those still
**fetching** — so you can tell at a glance whether a peer is actively syncing.
The meter is two-tone: the lighter fill is what's committed within the quota,
the solid fill is what's actually on disk. The **info** button (ⓘ) opens a
panel with the peer's full id, usage, an editable **quota** (change it and the
mirror is re-evaluated on the next sync), and the scrollable list of mirror
files with a state pill each (mirrored / fetching / pending / over quota); the
trash button unsubscribes.

Lowering a quota that no longer fits everything makes the now-over-quota files
`skipped` and evicts them on the next sync (respecting manual pins and other
subscriptions); raising it pulls more of the peer's pin-set in.

## Unsubscribing

```sh
rucio subscription remove 12D3KooW…
```

Unsubscribing drops the mirror and then **evicts** the content that was kept
only for that subscription — i.e. files that no other subscription still wants
and that you haven't pinned yourself. Eviction is deliberately conservative:

- it only ever deletes content the node **fetched as a mirror** (your own
  downloads and shares are never touched), and
- only when the file lives under `pin_dir`.

So removing a subscription frees the disk it was using without any risk to your
own files — even if you've configured `pin_dir` and `download_dir` to be the
same folder.

---

See [Downloading](04-downloading.md) for the fetch side and
[Configuration](06-configuration.md) for `storage.pin_dir`.
