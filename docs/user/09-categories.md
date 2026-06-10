# Categories

Categories let you organise downloads. A category does two things:

- **Routes downloads to their own folder.** A download filed under a category
  lands in that category's directory instead of the global download directory.
- **Tags downloads with a coloured badge** so you can tell them apart at a
  glance and filter the list.

A download belongs to at most one category, and categories are optional — a
download with no category simply goes to the global download directory and
carries no badge.

## Creating a category

```sh
rucio category add Movies --dir /mnt/media/movies --color "#3b82f6"
```

| Flag | Meaning |
|---|---|
| `--dir PATH` | Absolute path where this category's downloads are saved. Omit to use the global download directory. |
| `--color HEX` | Badge colour as a hex string, e.g. `#3b82f6`. Omit for no colour. |
| `--match "A\|B\|C"` | Auto-assign keywords, `\|`-separated (see [Auto-assignment](#auto-assignment)). |

The name must be unique. Only the name is required:

```sh
rucio category add Books
```

## Listing categories

```sh
rucio category list
```

```
 ID  Name    Download dir          Color    Match
 1   Movies  /mnt/media/movies     #3b82f6  1080p|bluray
 2   Books   (global)              -        epub|mobi
```

`(global)` means the category has no folder of its own, so its downloads land
in the global download directory — they still get the badge.

## Assigning a download to a category

**When adding a download**, pass the category id (from `rucio category list`):

```sh
rucio download add 1 --category 1
```

The download is saved into that category's directory.

**To move an existing download** to another category, or to clear it:

```sh
rucio download category 7b4a 2      # move to category 2 (hash prefix or row number)
rucio download category 7b4a        # omit the id to clear the category
```

Reassigning a download changes its badge and which folder *future* completion
uses; a file that has already been moved to disk is not moved again.

`rucio download show` displays the current category for a download.

## Auto-assignment

If you add a download **without** `--category`, Rucio tries to file it
automatically using each category's match keywords:

- Keywords are `|`-separated substrings, e.g. `1080p|bluray`.
- A category matches when **any** of its keywords appears in the file name
  (case-insensitive substring match).
- If several categories match, the one with the **lowest id** wins (the oldest
  category takes precedence).
- A download is assigned to at most one category.

For example, with the `Movies` category above (`1080p|bluray`), this download
is filed under *Movies* automatically:

```sh
rucio download add "rucio:...?name=Some.Film.1080p.mkv&size=..."
```

Passing `--category` always overrides auto-assignment.

## Updating a category

```sh
rucio category set 1 Films --dir /mnt/media/films --color "#ef4444" --match "1080p|2160p"
```

`set` takes the id and the new name; the flags work exactly as in `add`. Omit a
flag to **clear** that field (e.g. leaving out `--dir` reverts the category to
the global download directory).

## Removing a category

```sh
rucio category remove 1
```

Deleting a category does not delete its downloads. They lose the badge and fall
back to the global download directory for any future completion.

## Categories in the web UI

The web interface mirrors the CLI:

- **Settings → Categories** — create, edit, recolour and delete categories,
  including their folder and match keywords.
- The **Add download** dialog has a category selector (or *Auto* to let the
  match keywords decide).
- Each download row shows its **coloured badge**, and the filter bar has a
  category dropdown to show only one category (or *Uncategorized*).
- Opening a download shows its category and lets you reassign it on the spot.

---

See [Downloading](04-downloading.md) for where files land and
[Configuration](06-configuration.md) for the global download directory.
