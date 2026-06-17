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
by default a `pins` folder beside your downloads in the Rucio content folder,
e.g. `~/Downloads/rucio/pins`). It's kept separate from your normal downloads so
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

## Collections

A pin can be filed under a **collection** — a free-text label like `Manuals`,
`Series` or `Season 1 of Marcianitos SA`. Collections are a *publishing* label
that travels with the pin: subscribers can then follow **just the collections of
yours they care about** instead of everything you pin (see
[Following specific collections](#following-specific-collections)).

Collections are **not** the same as [download categories](09-categories.md):
a category routes a download to a folder and disappears when you clear the
download from the list, whereas a collection sticks to the pin for as long as
it's pinned. A pin belongs to **one** collection (or none).

## Pinning from the CLI

```sh
rucio pin add "rucio:7b4a…?name=film.mkv&size=…"   # a magnet → fetch + keep
rucio pin add 3                                     # a download id (something you have/are getting)
rucio pin add 7b4a…<64 hex>                          # a full root hash
rucio pin add 3 --collection Series                 # file it under a collection
```

`pin add` accepts three kinds of target:

| Target | Meaning |
|---|---|
| a `rucio:` magnet | fetch the content if missing, then keep it |
| a numeric download id (from `rucio download list`) | pin content you already have/are downloading — no re-fetch |
| a full 64-character root hash | pin by hash directly |

Add `--collection <NAME>` to file the pin under a collection (omit it for an
uncollected pin).

List and remove:

```sh
rucio pin list
```

```
 Root hash         Name        Size      State        Collection
 7b4a…             film.mkv    1.4 GB    available    Films
 d931…             book.epub   980 KB    fetching     -
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
  64-character root hash, with an optional **collection** field. Content you
  already have is just marked as kept; content you don't is fetched first.
- Each pin shows its name, size, a coloured **state** pill
  (available / fetching / missing) and its **collection** as a pill — click the
  pill to move the pin to another collection (or clear it).
- **Unpin** removes the pin from the row.

To pin something you already have, the quickest path is the **Shares** tab:
each shared file has a **Pin** button next to its **Magnet** button. It opens a
small dialog asking which collection to file it under (leave blank for none),
then keeps that file on purpose and publishes it in your pin-set (for
subscribers), without moving or re-fetching it. The button then turns into
**Unpin** (with a struck-through pin icon), so the Shares list doubles as a view
of what you've pinned — click again to unpin.

> **Already have it?** Adding a download or pin for content you already hold —
> as a share or another download — doesn't re-fetch it; Rucio tells you where it
> already is. The same applies to mirrored content: a subscription never
> re-downloads files you already have.

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

By default a subscription mirrors **everything** the peer pins. You can narrow
it to just some of their [collections](#collections) — see
[Following specific collections](#following-specific-collections).

## Following specific collections

A peer who organises their pins into collections lets you follow only the ones
you want. In the web **Subscriptions** tab, open a subscription's **info** panel
(ⓘ): the scope editor shows a **"Mirror everything this peer pins"** toggle.

- Leave it **on** to mirror the whole peer (the default).
- Turn it **off** to pick specific collections from the checklist. `(no
  collection)` is the peer's uncollected pins.
- Picking **none** is valid: you stay subscribed to the peer (so you don't lose
  the link) but mirror nothing for now.

The peer's collections appear in the checklist after the **first sync** — if the
list is empty, mirror everything for a moment, or hit the **refresh** button
(↻) next to the toggle to pull their pin-set now. Press **Update collections**
to apply; the change takes effect on the next sync and re-scopes what's
mirrored.

Narrowing the followed set drops the collections you unchecked. If that would
delete content already on disk, Rucio asks the same **Keep / Free** question as
unsubscribing (see [Unsubscribing](#unsubscribing)); if nothing would be freed
it just applies.

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
panel with the peer's full id, usage, an editable **quota**, the collection
scope editor, and the scrollable list of mirror files. The trash button
unsubscribes.

The panel is **live and stays open** as you work:

- **Update quota** and **Update collections** apply in place and show a brief
  confirmation — they don't close the panel (use the **Close** button, the
  **×**, or click outside).
- The file list **refreshes itself**, so states move (pending → fetching →
  mirrored) without reopening, and the **refresh** button (↻) pulls the peer's
  pin-set on demand.
- Each file has a state pill: **mirrored** / **fetching** / **pending** /
  **over quota** / **cancelled**. A file you cancelled stays `cancelled` and is
  not re-fetched; a **re-request** button (↻) on that row brings it back.

Lowering a quota that no longer fits everything makes the now-over-quota files
`over quota` (skipped) and evicts them on the next sync (respecting manual pins
and other subscriptions); raising it pulls more of the peer's pin-set in.

## Unsubscribing

```sh
rucio subscription remove 12D3KooW…           # free the mirrored content (default)
rucio subscription remove 12D3KooW… --keep    # keep it as your own shares
```

Unsubscribing drops the mirror. What happens to the content it pulled is **your
choice**:

- **Free the space** (default) — delete the content that was kept *only* for
  that subscription, and cancel any of its downloads still in flight.
- **Keep it** (`--keep`) — keep those files as permanent shares you own; they're
  no longer managed by any subscription. Files another subscription still wants
  stay mirrored.

In the web UI the trash button asks **Keep / Free** when there's something at
stake; if nothing would actually be freed (the content is your own, pinned,
wanted by another subscription, or sits outside `pin_dir`) it just unsubscribes
without asking.

Eviction is deliberately conservative — it only ever touches content the node
**fetched as a mirror** (your own downloads and shares are never deleted) and
only files under `pin_dir`. So freeing a subscription reclaims its disk without
any risk to your own files, even if `pin_dir` and `download_dir` are the same
folder.

---

See [Downloading](04-downloading.md) for the fetch side and
[Configuration](06-configuration.md) for `storage.pin_dir`.
