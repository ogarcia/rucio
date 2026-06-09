//! ed2k link parsing and hash computation.
//!
//! An ed2k link has the form:
//!   `ed2k://|file|<name>|<size>|<hash>|/`
//!
//! where `<hash>` is the hex-encoded ed2k hash (16 bytes, MD4-based).
//!
//! The ed2k hash of a file is defined as:
//!   - If `file_size <= CHUNK_SIZE`: MD4(file_bytes)
//!   - Otherwise: MD4(MD4(chunk_0) || MD4(chunk_1) || … || MD4(chunk_n))
//!
//! Chunk size is 9,728,000 bytes (eMule's canonical value).

use md4::{Digest, Md4};
use thiserror::Error;

/// eMule chunk size: 9,728,000 bytes (9500 KiB).
pub const CHUNK_SIZE: usize = 9_728_000;

/// A 16-byte ed2k (MD4-based) hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ed2kHash([u8; 16]);

impl Ed2kHash {
    /// Construct from a raw 16-byte array.
    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }

    /// Parse from a 32-character hex string (case-insensitive).
    pub fn from_hex(s: &str) -> Result<Self, Ed2kError> {
        if s.len() != 32 {
            return Err(Ed2kError::InvalidHash(s.to_owned()));
        }
        let mut bytes = [0u8; 16];
        hex::decode_to_slice(s, &mut bytes).map_err(|_| Ed2kError::InvalidHash(s.to_owned()))?;
        Ok(Self(bytes))
    }

    /// Return the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Hex-encode the hash (lowercase).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for Ed2kHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Compute the ed2k hash of `data` in-memory.
///
/// Suitable for small files or testing; for large files use
/// [`hash_reader`] to avoid loading the entire file into RAM.
pub fn hash_bytes(data: &[u8]) -> Ed2kHash {
    if data.len() <= CHUNK_SIZE {
        let digest = Md4::digest(data);
        Ed2kHash(digest.into())
    } else {
        let mut chunk_hashes: Vec<u8> = Vec::new();
        for chunk in data.chunks(CHUNK_SIZE) {
            let h = Md4::digest(chunk);
            chunk_hashes.extend_from_slice(&h);
        }
        let digest = Md4::digest(&chunk_hashes);
        Ed2kHash(digest.into())
    }
}

/// Compute the ed2k hash of data from a `Read` source.
///
/// Reads in `CHUNK_SIZE` blocks; memory usage is O(CHUNK_SIZE).
pub fn hash_reader<R: std::io::Read>(reader: R) -> std::io::Result<Ed2kHash> {
    Ok(hash_reader_full(reader)?.0)
}

/// Like [`hash_reader`], but also returns the per-`CHUNK_SIZE` MD4 part hashes.
///
/// The part hashes are what [`finalize_hashset`] needs to build the hashset we
/// serve to peers, so callers that intend to share the file (not just identify
/// it) should use this and feed the returned parts into `finalize_hashset`.
///
/// Reads in `CHUNK_SIZE` blocks; memory usage is O(CHUNK_SIZE) plus 16 bytes
/// per chunk for the accumulated part hashes.
pub fn hash_reader_full<R: std::io::Read>(
    mut reader: R,
) -> std::io::Result<(Ed2kHash, Vec<[u8; 16]>)> {
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut parts: Vec<[u8; 16]> = Vec::new();
    let mut total_bytes: u64 = 0;

    loop {
        let mut offset = 0;
        loop {
            match reader.read(&mut buf[offset..]) {
                Ok(0) => break,
                Ok(n) => {
                    offset += n;
                    if offset == CHUNK_SIZE {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        if offset == 0 {
            break;
        }
        total_bytes += offset as u64;
        parts.push(Md4::digest(&buf[..offset]).into());
        if offset < CHUNK_SIZE {
            break;
        }
    }

    let hash = if total_bytes <= CHUNK_SIZE as u64 && parts.len() == 1 {
        // Single chunk: the hash IS the chunk hash.
        Ed2kHash(parts[0])
    } else {
        let mut concat = Vec::with_capacity(parts.len() * 16);
        for p in &parts {
            concat.extend_from_slice(p);
        }
        Ed2kHash(Md4::digest(&concat).into())
    };
    Ok((hash, parts))
}

/// Build the ed2k part-hash set ("hashset") to serve to peers, from the list of
/// per-`CHUNK_SIZE` MD4 chunk hashes of a file, following eMule's conventions:
///
/// - A file smaller than one chunk has an **empty** hashset (its ed2k hash is
///   simply `MD4(data)`, so no part hashes are exchanged).
/// - When the size is an **exact multiple** of `CHUNK_SIZE`, eMule appends an
///   MD4 of a zero-length chunk (the "null chunk") to the list.
///
/// The result is verified against `target` (the file's known ed2k hash):
/// `MD4` of the concatenated part hashes must reproduce it (trying with and
/// without the null chunk for exact multiples, to tolerate either convention).
/// Returns the concatenated 16-byte part hashes (ready to store/serve), or an
/// empty vec when the file is single-part or no convention reproduces `target`
/// — in which case the caller serves no hashset.
pub fn finalize_hashset(chunk_hashes: &[[u8; 16]], size: u64, target: &Ed2kHash) -> Vec<u8> {
    if size < CHUNK_SIZE as u64 || chunk_hashes.is_empty() {
        return Vec::new();
    }
    let mut candidates: Vec<Vec<[u8; 16]>> = Vec::new();
    if size.is_multiple_of(CHUNK_SIZE as u64) {
        // eMule appends a null chunk for exact multiples; try that first, then
        // fall back to the plain list for older clients that did not.
        let null_chunk: [u8; 16] = Md4::digest(b"").into();
        let mut with_null = chunk_hashes.to_vec();
        with_null.push(null_chunk);
        candidates.push(with_null);
        candidates.push(chunk_hashes.to_vec());
    } else {
        candidates.push(chunk_hashes.to_vec());
    }
    for parts in candidates {
        let mut concat = Vec::with_capacity(parts.len() * 16);
        for p in &parts {
            concat.extend_from_slice(p);
        }
        let root: [u8; 16] = Md4::digest(&concat).into();
        if &root == target.as_bytes() {
            return concat;
        }
    }
    Vec::new()
}

// ── Link parsing ─────────────────────────────────────────────────────────────

/// A parsed `ed2k://|file|…|…|…|/` link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ed2kLink {
    /// Original file name (URL-decoded).
    pub name: String,
    /// File size in bytes.
    pub size: u64,
    /// ed2k hash.
    pub hash: Ed2kHash,
}

impl Ed2kLink {
    /// Parse an ed2k:// link string.
    pub fn parse(s: &str) -> Result<Self, Ed2kError> {
        // Strip optional trailing whitespace / newlines.
        let s = s.trim();
        // Expected: ed2k://|file|<name>|<size>|<hash>|/
        let s = s
            .strip_prefix("ed2k://")
            .ok_or_else(|| Ed2kError::InvalidLink(s.to_owned()))?;
        // Strip optional leading '|'.
        let s = s.strip_prefix('|').unwrap_or(s);
        // Split on '|'
        let parts: Vec<&str> = s.split('|').collect();
        // parts: ["file", name, size, hash, "/"]  (possibly more fields after hash)
        if parts.len() < 5 {
            return Err(Ed2kError::InvalidLink(s.to_owned()));
        }
        if parts[0] != "file" {
            return Err(Ed2kError::InvalidLink(format!(
                "unknown type: {}",
                parts[0]
            )));
        }
        let name = urlencoding::decode(parts[1])
            .unwrap_or_else(|_| parts[1].into())
            .into_owned();
        let size: u64 = parts[2]
            .parse()
            .map_err(|_| Ed2kError::InvalidLink(format!("bad size: {}", parts[2])))?;
        let hash = Ed2kHash::from_hex(parts[3])?;
        Ok(Self { name, size, hash })
    }

    /// Render back to a canonical ed2k:// string.
    pub fn to_link(&self) -> String {
        format!(
            "ed2k://|file|{}|{}|{}|/",
            urlencoding::encode(&self.name),
            self.size,
            self.hash
        )
    }
}

impl std::fmt::Display for Ed2kLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_link())
    }
}

impl std::str::FromStr for Ed2kLink {
    type Err = Ed2kError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ed2kLink::parse(s)
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum Ed2kError {
    #[error("invalid ed2k link: {0}")]
    InvalidLink(String),
    #[error("invalid ed2k hash: {0}")]
    InvalidHash(String),
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_empty() {
        // MD4("") = 31d6cfe0d16ae931b73c59d7e0c089c0
        let h = hash_bytes(b"");
        assert_eq!(h.to_hex(), "31d6cfe0d16ae931b73c59d7e0c089c0");
    }

    #[test]
    fn test_hash_small() {
        // Single chunk — result is MD4(data).
        let data = b"hello world";
        let h = hash_bytes(data);
        let expected = {
            let d = md4::Md4::digest(data);
            hex::encode(d)
        };
        assert_eq!(h.to_hex(), expected);
    }

    #[test]
    fn test_link_roundtrip() {
        let link = "ed2k://|file|test.txt|12345|d41d8cd98f00b204e9800998ecf8427e|/";
        let parsed = Ed2kLink::parse(link).unwrap();
        assert_eq!(parsed.name, "test.txt");
        assert_eq!(parsed.size, 12345);
        let rendered = parsed.to_link();
        // name has no special chars, should be identical
        assert!(rendered.contains("test.txt"));
        assert!(rendered.contains("12345"));
    }

    #[test]
    fn test_link_parse_error() {
        assert!(Ed2kLink::parse("http://example.com").is_err());
        assert!(Ed2kLink::parse("ed2k://|file|only-three|/").is_err());
    }

    #[test]
    fn test_hash_reader_matches_bytes() {
        let data = vec![42u8; 1024];
        let expected = hash_bytes(&data);
        let computed = hash_reader(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(expected, computed);
    }

    #[test]
    fn test_hash_reader_full_parts_rebuild_hash_and_hashset() {
        // A two-chunk file: hash_reader_full's parts must feed finalize_hashset
        // back to the same root hash it returned.
        let data = vec![7u8; CHUNK_SIZE + 100];
        let (hash, parts) = hash_reader_full(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(hash, hash_bytes(&data));
        assert_eq!(parts.len(), 2);
        let hashset = finalize_hashset(&parts, data.len() as u64, &hash);
        assert_eq!(hashset.len(), 32); // two 16-byte part hashes, verified
    }

    #[test]
    fn test_finalize_hashset_single_part_is_empty() {
        // A file smaller than one chunk carries no hashset.
        let parts = [[9u8; 16]];
        let target = Ed2kHash::from_bytes([9u8; 16]);
        assert!(finalize_hashset(&parts, 1234, &target).is_empty());
    }

    #[test]
    fn test_finalize_hashset_multipart_verifies() {
        // Non-exact multiple: two part hashes, no null chunk; root = MD4(concat).
        let parts = [[1u8; 16], [2u8; 16]];
        let mut concat = Vec::new();
        concat.extend_from_slice(&parts[0]);
        concat.extend_from_slice(&parts[1]);
        let target = Ed2kHash::from_bytes(Md4::digest(&concat).into());
        let hs = finalize_hashset(&parts, CHUNK_SIZE as u64 + 10, &target);
        assert_eq!(hs, concat);
    }

    #[test]
    fn test_finalize_hashset_exact_multiple_appends_null_chunk() {
        // Exact multiple: eMule appends MD4("") so root = MD4(chunk || null).
        let chunk = [7u8; 16];
        let null_chunk: [u8; 16] = Md4::digest(b"").into();
        let mut concat = Vec::new();
        concat.extend_from_slice(&chunk);
        concat.extend_from_slice(&null_chunk);
        let target = Ed2kHash::from_bytes(Md4::digest(&concat).into());
        let hs = finalize_hashset(&[chunk], CHUNK_SIZE as u64, &target);
        assert_eq!(hs, concat);
    }

    #[test]
    fn test_finalize_hashset_unverifiable_is_empty() {
        // If no convention reproduces the target, serve nothing.
        let parts = [[1u8; 16], [2u8; 16]];
        let target = Ed2kHash::from_bytes([0xABu8; 16]);
        assert!(finalize_hashset(&parts, CHUNK_SIZE as u64 + 10, &target).is_empty());
    }
}
