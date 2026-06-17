//! BLAKE3 verified-streaming helpers (bao-tree) shared by the producer (who
//! serves chunks) and the consumer (who downloads and verifies them).
//!
//! The file's `root_hash` is the standard `blake3::hash` of its content, which
//! is also the root of the bao Merkle tree. Each transfer chunk is sent as a
//! self-verifying *slice* (proof + data) that the receiver checks against that
//! root — so a chunk is verified against the file's identity, not against a
//! separate per-chunk hash that the network would have to be trusted to supply.
//!
//! Block size is `2^10` chunks of 1 KiB = **1 MiB**. The transfer chunk (4 MiB)
//! is exactly four 1 MiB blocks, so every chunk request covers whole subtrees —
//! which is what lets a node serve a chunk it has from a partially-downloaded
//! file (partial sharing). The outboard (the tree of inner hashes) is about
//! `size / 16384` bytes — a few MB even for tens of GB of data.

use std::io;
use std::path::Path;

pub use bao_tree::io::DecodeError;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::io::sync::{decode_ranges, encode_ranges_validated, outboard};
use bao_tree::{BaoTree, BlockSize, ByteRanges, ChunkRanges};

/// Merkle tree block size: `2^10` BLAKE3 chunks of 1 KiB = 1 MiB blocks.
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(10);

/// Compute the bao root hash (identical to `blake3::hash` of the file content)
/// and the pre-order outboard, streaming the file. The file is never fully read
/// into RAM; the outboard itself is small and is returned as bytes to persist.
///
/// Blocking I/O — call inside `spawn_blocking` from async code.
pub fn compute_outboard(path: &Path) -> io::Result<(blake3::Hash, Vec<u8>, u64)> {
    let size = std::fs::metadata(path)?.len();
    let tree = BaoTree::new(size, BLOCK_SIZE);
    let mut ob = PreOrderMemOutboard {
        root: blake3::hash(&[]),
        tree,
        data: vec![0u8; tree.outboard_size() as usize],
    };
    let file = std::fs::File::open(path)?;
    let root = outboard(file, tree, &mut ob)?;
    ob.root = root;
    Ok((root, ob.data, size))
}

/// Translate a transfer chunk (index over `chunk_size`-byte chunks) into the bao
/// chunk ranges it covers, clamped to the end of the file.
pub fn chunk_ranges(chunk_idx: u32, chunk_size: u32, total_size: u64) -> ChunkRanges {
    let start = chunk_idx as u64 * chunk_size as u64;
    let end = start.saturating_add(chunk_size as u64).min(total_size);
    round_up_to_chunks(&ByteRanges::from(start..end))
}

/// Producer: build a self-verifying bao slice (proof + data) for `ranges` from
/// the data file and its outboard, validated against `root`. The returned bytes
/// are what travels in `ChunkResponse`.
///
/// Works with a *partial* outboard too (partial sharing): it succeeds only if
/// every requested range's hashes are present, else errors.
pub fn encode_slice(
    data_path: &Path,
    outboard_bytes: Vec<u8>,
    root: blake3::Hash,
    total_size: u64,
    ranges: &ChunkRanges,
) -> io::Result<Vec<u8>> {
    let tree = BaoTree::new(total_size, BLOCK_SIZE);
    let ob = PreOrderMemOutboard {
        root,
        tree,
        data: outboard_bytes,
    };
    let file = std::fs::File::open(data_path)?;
    let mut out = Vec::new();
    encode_ranges_validated(&file, &ob, ranges.as_ref(), &mut out)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(out)
}

/// Consumer: verify a received `slice` for `ranges` against `root`, writing the
/// verified bytes into `dest` at their correct offsets and folding the slice's
/// proof nodes into `partial_outboard` (which must be sized for the full tree:
/// `BaoTree::new(total_size, BLOCK_SIZE).outboard_size()`). Returns the updated
/// outboard bytes to persist. A hash mismatch yields `DecodeError`.
///
/// Blocking I/O — call inside `spawn_blocking`.
pub fn decode_slice_into(
    slice: &[u8],
    ranges: &ChunkRanges,
    dest: &mut std::fs::File,
    root: blake3::Hash,
    total_size: u64,
    partial_outboard: Vec<u8>,
) -> Result<Vec<u8>, DecodeError> {
    let tree = BaoTree::new(total_size, BLOCK_SIZE);
    let mut ob = PreOrderMemOutboard {
        root,
        tree,
        data: partial_outboard,
    };
    decode_ranges(slice, ranges.as_ref(), dest, &mut ob)?;
    Ok(ob.data)
}

/// The byte length of the (full) pre-order outboard for a file of `total_size`.
/// Use to allocate a fresh partial outboard buffer before the first chunk.
pub fn outboard_len(total_size: u64) -> usize {
    BaoTree::new(total_size, BLOCK_SIZE).outboard_size() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn root_equals_blake3_of_file() {
        // Multi-block file so the tree has interior nodes.
        let data = vec![0xABu8; 5 * 1024 * 1024];
        let f = write_tmp(&data);
        let (root, _ob, size) = compute_outboard(f.path()).unwrap();
        assert_eq!(size, data.len() as u64);
        assert_eq!(root, blake3::hash(&data));
    }

    #[test]
    fn empty_file_root() {
        let f = write_tmp(&[]);
        let (root, _ob, size) = compute_outboard(f.path()).unwrap();
        assert_eq!(size, 0);
        assert_eq!(root, blake3::hash(&[]));
    }

    #[test]
    fn roundtrip_chunk_verifies() {
        // 10 MiB → chunks of 4 MiB: 0..4, 4..8, 8..10.
        let data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let f = write_tmp(&data);
        let (root, ob, size) = compute_outboard(f.path()).unwrap();
        let chunk_size = 4 * 1024 * 1024u32;

        // Encode chunk 1 (bytes 4 MiB..8 MiB) and decode-verify it into a fresh part.
        let ranges = chunk_ranges(1, chunk_size, size);
        let slice = encode_slice(f.path(), ob.clone(), root, size, &ranges).unwrap();

        let mut part = tempfile::NamedTempFile::new().unwrap();
        part.as_file().set_len(size).unwrap();
        let out = decode_slice_into(
            &slice,
            &ranges,
            part.as_file_mut(),
            root,
            size,
            vec![0u8; outboard_len(size)],
        )
        .unwrap();
        assert_eq!(out.len(), outboard_len(size));

        // The decoded bytes match the source range.
        use std::io::{Read, Seek, SeekFrom};
        let mut buf = vec![0u8; chunk_size as usize];
        let mut pf = part.reopen().unwrap();
        pf.seek(SeekFrom::Start(4 * 1024 * 1024)).unwrap();
        pf.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[4 * 1024 * 1024..8 * 1024 * 1024]);
    }

    #[test]
    fn corrupt_slice_is_rejected() {
        let data: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let f = write_tmp(&data);
        let (root, ob, size) = compute_outboard(f.path()).unwrap();
        let chunk_size = 4 * 1024 * 1024u32;
        let ranges = chunk_ranges(0, chunk_size, size);
        let mut slice = encode_slice(f.path(), ob, root, size, &ranges).unwrap();

        // Flip a byte deep in the data section of the slice.
        let n = slice.len();
        slice[n - 1] ^= 0xFF;

        let mut part = tempfile::NamedTempFile::new().unwrap();
        part.as_file().set_len(size).unwrap();
        let res = decode_slice_into(
            &slice,
            &ranges,
            part.as_file_mut(),
            root,
            size,
            vec![0u8; outboard_len(size)],
        );
        assert!(res.is_err(), "corrupt slice must fail verification");
    }
}
