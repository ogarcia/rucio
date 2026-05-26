//! Per-slice download progress tracking for eMule `.part.met` files.
//!
//! Format: `version(4 LE) + num_slices(4 LE) + bitset(ceil(n/8) bytes)`
//! A set bit means the corresponding 9.28 MB slice is fully downloaded and
//! MD4-verified.

use std::path::Path;

/// Load per-slice completion state from a `.part.met` file.
///
/// Returns `vec![false; num_slices]` if the file is absent or incompatible.
pub fn load_progress(path: &Path, num_slices: usize) -> Vec<bool> {
    let data = std::fs::read(path).unwrap_or_default();
    if data.len() < 8 {
        return vec![false; num_slices];
    }
    let stored_n = u32::from_le_bytes(data[4..8].try_into().unwrap_or([0; 4])) as usize;
    if stored_n != num_slices {
        return vec![false; num_slices];
    }
    let bit_bytes = &data[8..];
    (0..num_slices)
        .map(|i| {
            let byte = bit_bytes.get(i / 8).copied().unwrap_or(0);
            byte & (1 << (i % 8)) != 0
        })
        .collect()
}

/// Persist per-slice completion state to a `.part.met` file.
pub fn save_progress(path: &Path, done: &[bool]) {
    let mut data = Vec::with_capacity(8 + done.len().div_ceil(8));
    data.extend_from_slice(&1u32.to_le_bytes()); // version
    data.extend_from_slice(&(done.len() as u32).to_le_bytes());
    let mut bits = vec![0u8; done.len().div_ceil(8)];
    for (i, &d) in done.iter().enumerate() {
        if d {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    data.extend_from_slice(&bits);
    let _ = std::fs::write(path, data);
}
