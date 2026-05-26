# Hashing

## Algorithm: BLAKE3

rucio uses [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) for all content
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

rucio computes a single **root hash** per file. This is not a Merkle tree
over chunks — it is a flat BLAKE3 hash of the entire file content.

```rust
pub fn hash_file(path: &Path) -> Result<FileHash> {
    let mut hasher = blake3::Hasher::new();
    // reads the file in 64 KiB buffers
    hasher.update_reader(file)?;
    Ok(FileHash { hash: hasher.finalize() })
}
```

The root hash serves as the canonical identifier for a file. Two files with
identical content have the same hash regardless of their name or location.

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
and makes them grep-friendly. There is no ambiguity — rucio never parses
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

This is useful for generating links on a machine where rucio is not running
as a daemon (e.g. a seed box where you want to produce links to share
elsewhere).

## Identity key

The node's libp2p identity is an Ed25519 keypair stored in
`<config_dir>/identity.key`. It is generated once on first startup and never
changes. The key is not derived from any content hash — it is used only for
peer identity and transport encryption (Noise protocol).
