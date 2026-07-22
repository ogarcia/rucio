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

/// Rebuild the completed-chunk set of a (possibly partial) file from disk alone:
/// given its outboard and the data on disk, return the transfer-chunk indices
/// that are fully present *and* valid against `root` — no network, no
/// re-download. A transfer chunk counts as done only if every 1 MiB block it
/// covers validates, so any hole or corruption drops just its chunk.
///
/// `outboard_bytes` may be a partial outboard (nodes for undownloaded regions
/// left zero): validation simply skips what the outboard cannot cover. The
/// caller must pass the file's true `root` (from the link); a wrong or all-zero
/// outboard yields no valid chunks.
///
/// Blocking I/O — call inside `spawn_blocking`.
pub fn valid_chunks(
    outboard_bytes: Vec<u8>,
    part_path: &Path,
    root: blake3::Hash,
    total_size: u64,
    chunk_size: u32,
) -> io::Result<Vec<u32>> {
    let tree = BaoTree::new(total_size, BLOCK_SIZE);
    let ob = PreOrderMemOutboard {
        root,
        tree,
        data: outboard_bytes,
    };
    let file = std::fs::File::open(part_path)?;

    // Union of every valid range (in 1 KiB bao-chunk units).
    let all = ChunkRanges::all();
    let mut valid = ChunkRanges::empty();
    for item in bao_tree::io::sync::valid_ranges(&ob, &file, &all) {
        valid |= ChunkRanges::from(item?);
    }

    // A transfer chunk is done iff its whole byte range is covered.
    let n = total_size.div_ceil(chunk_size as u64) as u32;
    let done = (0..n)
        .filter(|&idx| chunk_ranges(idx, chunk_size, total_size).is_subset(&valid))
        .collect();
    Ok(done)
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

    fn ramp(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    /// Reproduce a mid-download `.part` + partial outboard on disk: "download"
    /// `chunks` by encoding each slice from the full outboard and decoding it
    /// into a fresh zeroed outboard + a sparse part file (holes stay zero).
    fn simulate_partial(
        src: &std::path::Path,
        full_ob: &[u8],
        root: blake3::Hash,
        size: u64,
        chunk_size: u32,
        chunks: &[u32],
    ) -> (tempfile::NamedTempFile, Vec<u8>) {
        let mut part = tempfile::NamedTempFile::new().unwrap();
        part.as_file().set_len(size).unwrap();
        let mut partial_ob = vec![0u8; outboard_len(size)];
        for &idx in chunks {
            let ranges = chunk_ranges(idx, chunk_size, size);
            let slice = encode_slice(src, full_ob.to_vec(), root, size, &ranges).unwrap();
            partial_ob =
                decode_slice_into(&slice, &ranges, part.as_file_mut(), root, size, partial_ob)
                    .unwrap();
        }
        (part, partial_ob)
    }

    #[test]
    fn valid_chunks_rebuilds_partial_doneset_from_disk() {
        let size = 18 * 1024 * 1024u64; // 5 chunks of 4 MiB (last is 2 MiB)
        let chunk_size = 4 * 1024 * 1024u32;
        let src = write_tmp(&ramp(size as usize));
        let (root, full_ob, _) = compute_outboard(src.path()).unwrap();

        // Downloaded chunks 0,2,4; chunks 1,3 are holes.
        let (part, partial_ob) =
            simulate_partial(src.path(), &full_ob, root, size, chunk_size, &[0, 2, 4]);

        // A) Rebuilt from .part + partial .obao, no network.
        assert_eq!(
            valid_chunks(partial_ob.clone(), part.path(), root, size, chunk_size).unwrap(),
            vec![0, 2, 4]
        );

        // B) Corrupt the whole chunk 2 region -> only chunk 2 drops.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = part.reopen().unwrap();
            f.seek(SeekFrom::Start(2 * chunk_size as u64)).unwrap();
            f.write_all(&vec![0xFFu8; chunk_size as usize]).unwrap();
            f.flush().unwrap();
        }
        assert_eq!(
            valid_chunks(partial_ob.clone(), part.path(), root, size, chunk_size).unwrap(),
            vec![0, 4]
        );

        // C) A lost outboard (all zeros) on a partial file recovers nothing:
        //    the 32-byte root alone cannot validate partial content.
        assert_eq!(
            valid_chunks(
                vec![0u8; outboard_len(size)],
                part.path(),
                root,
                size,
                chunk_size
            )
            .unwrap(),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn valid_chunks_complete_file_is_all_done() {
        // Non-block-aligned tail to exercise the ragged end.
        let size = 18 * 1024 * 1024u64 + 777;
        let chunk_size = 4 * 1024 * 1024u32;
        let src = write_tmp(&ramp(size as usize));
        let (root, full_ob, _) = compute_outboard(src.path()).unwrap();

        let n = size.div_ceil(chunk_size as u64) as u32;
        assert_eq!(
            valid_chunks(full_ob, src.path(), root, size, chunk_size).unwrap(),
            (0..n).collect::<Vec<_>>()
        );
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
