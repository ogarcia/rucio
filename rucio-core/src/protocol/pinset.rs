//! Wire types for the `/rucio/pinset/1.0.0` request-response protocol.
//!
//! A subscriber asks a peer for its **pin-set** — the list of root hashes that
//! peer keeps available on purpose and offers to mirror. The exchange runs over
//! the authenticated libp2p connection (the remote is cryptographically bound to
//! its `PeerId`), so the response needs no application-level signature: receiving
//! it on a connection to `PeerId X` already proves it came from X.

/// Request a peer's pin-set. No parameters — "send me your current pin-set".
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PinsetRequest;

/// One entry in a pin-set: a file the peer offers, with enough metadata to plan
/// a fetch (size for the quota selection, name for display).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinsetEntry {
    pub root_hash: [u8; 32],
    pub size: u64,
    pub name: String,
    /// Publishing collection this pin belongs to, as chosen by the publisher.
    /// `None` = uncollected. A subscriber can follow only selected collections
    /// of a peer (see `pin_subscription_collections`). Free-text per publisher;
    /// there is no global taxonomy.
    #[serde(default)]
    pub collection: Option<String>,
}

/// A peer's pin-set.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PinsetResponse {
    Ok {
        /// Content fingerprint of the pin-set: changes if and only if the set
        /// changes, so a subscriber can skip an unchanged re-sync. Not a
        /// monotonic counter — only equality is meaningful.
        version: u64,
        entries: Vec<PinsetEntry>,
    },
    /// The peer could not produce its pin-set (transient internal error).
    Error(String),
}

impl PinsetResponse {
    /// Compute the fingerprint of a set of entries: order-independent so it's
    /// stable regardless of how the rows come out of the database.
    pub fn fingerprint(entries: &[PinsetEntry]) -> u64 {
        // XOR of per-entry hashes → independent of order; folds hash+size+
        // collection so a resized file or a re-tagged pin also bumps it. Cheap
        // and collision-safe enough to gate a re-sync (a miss just re-fetches an
        // unchanged set).
        let mut acc: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis as a seed
        for e in entries {
            let mut h: u64 = 0;
            for chunk in e.root_hash.chunks(8) {
                let mut b = [0u8; 8];
                b[..chunk.len()].copy_from_slice(chunk);
                h ^= u64::from_le_bytes(b);
            }
            h ^= e.size.rotate_left(17);
            // Fold the collection label (FNV-1a over its bytes); None and ""
            // are treated alike since both mean "uncollected".
            let mut c: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in e.collection.as_deref().unwrap_or("").bytes() {
                c ^= byte as u64;
                c = c.wrapping_mul(0x0000_0100_0000_01b3);
            }
            h ^= c.rotate_left(33);
            acc ^= h;
        }
        acc
    }
}
