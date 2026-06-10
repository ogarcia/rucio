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

- **Pin a magnet** opens a dialog to paste a `rucio:` magnet.
- Each pin shows its name, size and a coloured **state** pill
  (available / fetching / missing).
- **Unpin** removes the pin from the row.

## Unpinning

Unpinning removes only the *intent* — it does **not** delete the file. Rucio
never auto-deletes content; to actually stop hosting a pinned file, remove its
directory from sharing (see [Sharing files](03-sharing-files.md)) or delete it
on disk.

---

See [Downloading](04-downloading.md) for the fetch side and
[Configuration](06-configuration.md) for `storage.pin_dir`.
