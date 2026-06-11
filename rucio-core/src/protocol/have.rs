//! Wire types for the `/rucio/have/1.0.0` request-response protocol.
//!
//! A lightweight *availability* query: ask a peer which chunks of a file it
//! currently holds and can serve. The answer is a compact little-endian
//! (LSB-first) bitmap, one bit per chunk — a few KB even for a file of tens of
//! thousands of chunks, so it is cheap enough to poll periodically as providers
//! come and go.
//!
//! Because Rucio announces a provider as soon as it has verified its first
//! chunk (partial sharing), the set of providers for a file may each hold only
//! part of it. The downloader ORs the bitmaps from every known provider to
//! learn the *aggregate* coverage of the swarm — whether the file is fully
//! available somewhere or only partially mirrored (e.g. the original seeder
//! went offline and only incomplete mirrors remain).
//!
//! The response echoes `root_hash` so a reply can be matched to its file
//! without the requester tracking outbound request ids.

/// Ask a peer which chunks of a file it holds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HaveRequest {
    /// BLAKE3 root hash of the file.
    pub root_hash: [u8; 32],
}

/// The response to a [`HaveRequest`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum HaveResponse {
    /// The peer knows this file (it shares it, complete or partial).
    Ok {
        /// Echoed so the requester can match the reply to its file.
        root_hash: [u8; 32],
        /// Total number of chunks the file has.
        pieces_total: u64,
        /// Little-endian (LSB-first) bitmap, one bit per chunk, set when the
        /// peer currently holds that chunk on disk. Bit `i` lives in
        /// `byte[i / 8] >> (i % 8)`. Length is `ceil(pieces_total / 8)` bytes.
        bitmap: Vec<u8>,
    },
    /// The peer does not know this file at all.
    NotFound,
}
