# Hashing

## Algorithm: BLAKE3

Rucio uses [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) for all content
hashing. BLAKE3 was chosen over SHA1 (BitTorrent), SHA256, or MD4 (eMule)
for the following reasons:

| Property | BLAKE3 | SHA256 | SHA1 |
|---|---|---|---|
| Speed (single-core) | ~6 GB/s | ~0.5 GB/s | ~0.7 GB/s |
| Parallelism | SIMD + multi-thread | no | no |
| Security | 256-bit, collision-resistant | 256-bit | broken |
| Pure Rust crate | yes (`blake3`) | yes | yes |

For large files, BLAKE3 hashing time is dominated by I/O, not CPU. On an
NVMe drive the entire hashing pipeline is effectively I/O-bound.

## Hash granularity

Rucio computes a single **root hash** per file. BLAKE3 is internally a Merkle
tree over 1 KiB chunks, and its finalized output — the standard `blake3::hash`
of the content — is the root of that tree. Rucio uses exactly that value as the
root hash, so it is both the canonical file identifier *and* the anchor for
**verified streaming**: any byte range can be checked against the root with a
`log(n)`-sized proof, without trusting a separate per-chunk hash list.

```rust
pub fn hash_file(path: &Path) -> Result<FileHash> {
    // bao::compute_outboard streams the file once, returning the root
    // (== blake3::hash of the content) and the pre-order outboard (the tree
    // of interior hashes) used later to slice and verify any range.
    let (root, outboard, size) = bao::compute_outboard(path)?;
    Ok(FileHash { root_hash: *root.as_bytes(), size, outboard, /* mime */ })
}
```

The root hash serves as the canonical identifier for a file. Two files with
identical content have the same hash regardless of their name or location.

### Verified streaming with bao

The Merkle tree is materialised with the [`bao-tree`](https://crates.io/crates/bao-tree)
crate (the same building block iroh-blobs uses). The tree is built with a
**block size of 1 MiB** (`2^10` BLAKE3 chunks of 1 KiB); the table of interior
hashes — the *outboard* — is roughly `size / 16384` bytes, a few MB even for
tens of GB of data.

- The **outboard is not stored in the database.** For a completed share it
  lives as a regenerable sidecar `<outboard_dir>/<root_hex>.obao`, rebuilt from
  the file on demand if missing. For an in-progress download it is the
  `<part>.obao` companion of the `.part`, filled in chunk by chunk as proof
  nodes arrive.
- A transfer chunk (4 MiB, fixed) is exactly four 1 MiB blocks, so every chunk
  request covers whole subtrees. That alignment is what lets a node serve a
  chunk it already holds from a *partially* downloaded file (partial sharing),
  and what lets a chunk's slice be verified in isolation against the root.

This replaces the earlier scheme, where the manifest carried a flat list of
per-chunk hashes and the root was the hash of that list — that never tied the
parts back to the whole, and grew the manifest linearly with file size.

## FileHash and collect_files

`rucio-core::protocol::hashing` exports:

- **`FileHash`** — newtype wrapping `[u8; 32]` with hex `Display`.
- **`hash_file(path)`** — hashes a single file, returns `FileHash`.
- **`collect_files(dir)`** — walks a directory recursively, returns
  `Vec<(PathBuf, FileHash, u64, String)>` (path, hash, size, mime).
- **`detect_mime(path, bytes)`** — detects MIME type from the first bytes
  and the file extension; returns a string like `"video/mp4"`.

`collect_files` is called by the indexer when a directory is first added and
by the watcher when a file changes.

## Magnet link format

The root hash is the primary identifier in magnet links:

```
rucio:<hash_hex>?name=<url-encoded-name>&size=<bytes>[&peer=<multiaddr>]...
```

The scheme `rucio:` distinguishes these links from BitTorrent `magnet:` links
and makes them grep-friendly. There is no ambiguity — Rucio never parses
`magnet:` links.

### URL encoding

The `name` parameter is URL-encoded by `MagnetLink`'s `Display`
implementation:

```rust
impl fmt::Display for MagnetLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rucio:{}", hex::encode(self.root_hash))?;
        if let Some(name) = &self.name {
            write!(f, "?name={}", urlencoding::encode(name))?;
        }
        // ...
    }
}
```

`parse_magnet` URL-decodes the `name` field when parsing, so round-trips are
lossless for any file name.

## Offline hashing

Because `hash_file` and `collect_files` are in `rucio-core` with no async or
daemon dependency, the CLI can compute hashes and produce magnet links without
a running daemon:

```sh
rucio share magnet --file /path/to/file.mkv
```

This is useful for generating links on a machine where Rucio is not running
as a daemon (e.g. a seed box where you want to produce links to share
elsewhere).

## Identity key

The node's libp2p identity is an Ed25519 keypair stored in
`<config_dir>/identity.key`. It is generated once on first startup and never
changes. The key is not derived from any content hash — it is used only for
peer identity and transport encryption (Noise protocol).
